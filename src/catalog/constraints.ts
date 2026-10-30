// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Canonical constraint records for catalogs that state their own facts, the
// checks the DuckDB extension applies to them at ATTACH, and the catalog
// capability negotiation that makes them readable.

import { encodeASD, decodeASD } from "../codec/asd.js";
import {
  deserializeSchema, field as field_, schema as schema_, serializeSchema, struct as struct_,
  type VgiDataType, type VgiField, type VgiSchema,
} from "../arrow/index.js";
import {
  CatalogObjectRefSchema,
  ClientCapabilitiesSchema,
  ConstraintDefinitionSchema,
  ConstraintInfoSchema,
  ForeignKeyDefinitionSchema,
  SqlExpressionSchema,
} from "../generated/vgi-protocol-schemas.js";
import {
  decodeCatalogCapabilities, encodeCatalogCapabilities, type CatalogCapabilities,
} from "../generated/vgi-protocol-types.js";
import type { ClientCapabilities, TableInfo } from "../generated/vgi-client.js";
import { validateNumericCheckSql } from "./sql-grammar.js";
import { duckdbPrimitive, isGeneratedField, isRowIdField, ordinaryStructChildren } from "./column-types.js";
import { GENERATED_EXPRESSION_KEY, GENERATION_KEY, generatedExpressionMetadata } from "./column-expressions.js";

/** Read profile: the catalog serves canonical ConstraintInfo records. */
export const CONSTRAINT_RECORDS_READ = "vgi.constraint.records.read.v1";
/** Read profile: CHECK constraints carry DuckDB scalar SQL expressions. */
export const ROW_SCALAR_EXPRESSION = "vgi.expression.duckdb.scalar.v1";
/** DDL profile: CREATE TABLE may carry canonical constraint definitions. */
export const CONSTRAINT_CREATE = "vgi.constraint.create.v1";
/** DDL profile: `catalog_table_constraints_apply` (e.g. ALTER ... SET/DROP NOT NULL). */
export const CONSTRAINT_MUTATE = "vgi.constraint.mutate.v1";
/** Write profile: the provider enforces its ENFORCED records on every write. */
export const CONSTRAINT_ENFORCE = "vgi.constraint.enforce.v1";
/** Transaction profile: constraint DDL may run inside a catalog transaction. */
export const TRANSACTION_DDL = "vgi.transaction.ddl.v1";

export type ConstraintKind = "NOT_NULL" | "PRIMARY_KEY" | "UNIQUE" | "CHECK" | "FOREIGN_KEY";
/** Whether the authority enforces the constraint on every write path. */
export type ConstraintEnforcement = "ENFORCED" | "METADATA_ONLY";
/** Coverage of existing rows; only ALL_ROWS justifies engine facts. */
export type ConstraintValidation = "ALL_ROWS" | "FUTURE_WRITES" | "UNVALIDATED";

export interface ConstraintRecordSpec {
  /** Schema path of the owning table (one component: DuckDB reads no other). */
  schemaPath: readonly string[];
  /** Owning table name. */
  table: string;
  /** Stable provider-owned identity, unique within the table. Never derived by the SDK. */
  id: string;
  kind: ConstraintKind;
  /**
   * Raw component paths, e.g. `[["id"]]`; empty for CHECK. Only NOT_NULL may
   * name a nested STRUCT field (`[["bbox", "xmin"]]`).
   */
  columns: readonly (readonly string[])[];
  /**
   * The provider's attestation. The DuckDB extension reads only
   * ENFORCED + ALL_ROWS records, so state that only when every row the table
   * can ever return satisfies the constraint; the builder refuses anything else.
   */
  enforcement: ConstraintEnforcement;
  validation: ConstraintValidation;
  /** Optional SQL label (CONSTRAINT <name>); unique per table, case-insensitively. */
  declaredName?: string;
  /**
   * CHECK only: one numeric comparison, e.g. `price >= 0` or `lo < hi`. Each
   * operand is a numeric literal or a signed-integer/float column; DuckDB reads
   * nothing else.
   */
  checkSql?: string;
  /** FOREIGN_KEY only: the referenced key (same schema) and its constraint identity. */
  references?: {
    schemaPath: readonly string[];
    table: string;
    columns: readonly (readonly string[])[];
    /** `id` of the referenced table's PRIMARY_KEY or UNIQUE record. */
    constraintId: string;
  };
}

/** The evaluation context DuckDB accepts for a CHECK expression. */
const CHECK_CONTEXT = {
  timezone: "UTC", collation: "binary", default_order: "asc",
  default_null_order: "nulls_last", preserve_insertion_order: true,
} as const;

function identifier(value: unknown, what: string): asserts value is string {
  if (typeof value !== "string" || !value || value.includes("\0") || new TextEncoder().encode(value).length > 4096)
    throw new Error(`${what} must be a nonempty identifier without NUL (at most 4096 bytes)`);
}

function objectRef(kind: "table" | "constraint", schemaPath: readonly string[], name: string, ownerTable: string | null): Uint8Array {
  return encodeASD(CatalogObjectRefSchema, { kind, schema_path: schemaPath, name, owner_table: ownerTable });
}

function checkPaths(paths: readonly (readonly string[])[], what: string, topLevelOnly: boolean): void {
  if (paths.length > 1024) throw new Error(`${what}: too many column paths`);
  const seen = new Set<string>();
  for (const path of paths) {
    if (path.length === 0 || path.length > 64) throw new Error(`${what}: column path must have 1..64 components`);
    if (topLevelOnly && path.length !== 1)
      throw new Error(`${what}: DuckDB reads nested column paths only for NOT_NULL; name a top-level column`);
    path.forEach((part) => identifier(part, `${what}: column path component`));
    const key = JSON.stringify(path);
    if (seen.has(key)) throw new Error(`${what}: duplicate column path ${path.join(".")}`);
    seen.add(key);
  }
}

/** Encode one canonical `ConstraintInfo` record for `TableDescriptor.constraints`. */
export function constraintRecord(spec: ConstraintRecordSpec): Uint8Array {
  const what = `constraint '${spec.id}' on '${spec.table}'`;
  if (!spec.id || spec.id.includes("\0")) throw new Error("constraint id must be nonempty without NUL");
  identifier(spec.table, "table");
  if (spec.schemaPath.length !== 1) throw new Error(`${what}: DuckDB reads only one-component schema paths`);
  identifier(spec.schemaPath[0], "schema");
  if (spec.enforcement !== "ENFORCED" || spec.validation !== "ALL_ROWS")
    throw new Error(`${what} is ${spec.enforcement}/${spec.validation}; DuckDB accepts only ENFORCED + ALL_ROWS records`);
  if (spec.declaredName !== undefined) identifier(spec.declaredName, `${what}: declaredName`);
  if ((spec.kind === "CHECK") !== (spec.checkSql !== undefined))
    throw new Error("checkSql is required for CHECK and only CHECK");
  if (spec.kind === "CHECK" ? spec.columns.length !== 0 : spec.columns.length === 0)
    throw new Error(spec.kind === "CHECK" ? "CHECK takes no column paths" : `${spec.kind} requires column paths`);
  if (spec.kind === "NOT_NULL" && spec.columns.length !== 1) throw new Error("NOT_NULL requires exactly one column path");
  if ((spec.kind === "FOREIGN_KEY") !== (spec.references !== undefined))
    throw new Error("references is required for FOREIGN_KEY and only FOREIGN_KEY");
  checkPaths(spec.columns, what, spec.kind !== "NOT_NULL");
  if (spec.checkSql !== undefined) {
    // Column types are checked when the table is served; the shape is checkable now.
    try {
      validateNumericCheckSql(spec.checkSql, () => {});
    } catch (error) {
      throw new Error(`${what}: ${(error as Error).message} (CHECK SQL: ${spec.checkSql})`);
    }
  }
  const ref = spec.references;
  if (ref) {
    if (ref.columns.length !== spec.columns.length) throw new Error("foreign key local and referenced arity must match");
    if (JSON.stringify(ref.schemaPath) !== JSON.stringify(spec.schemaPath))
      throw new Error(`${what}: DuckDB reads only foreign keys to a table in the same schema`);
    identifier(ref.table, `${what}: referenced table`);
    identifier(ref.constraintId, `${what}: referenced constraintId`);
    checkPaths(ref.columns, `${what} (referenced)`, true);
  }
  const check = spec.checkSql === undefined ? null : encodeASD(SqlExpressionSchema, {
    sql: spec.checkSql,
    profile: ROW_SCALAR_EXPRESSION,
    schema_path: spec.schemaPath,
    evaluation_context: CHECK_CONTEXT,
  });
  const foreign = ref === undefined ? null : encodeASD(ForeignKeyDefinitionSchema, {
    referenced_table: objectRef("table", ref.schemaPath, ref.table, null),
    referenced_column_paths: ref.columns,
    match: "SIMPLE", on_update: "NO_ACTION", on_delete: "NO_ACTION",
  });
  return encodeASD(ConstraintInfoSchema, {
    ref: objectRef("constraint", spec.schemaPath, spec.id, spec.table),
    definition: encodeASD(ConstraintDefinitionSchema, {
      kind: spec.kind,
      column_paths: spec.columns,
      declared_name: spec.declaredName ?? null,
      check_expression: check,
      foreign_key: foreign,
    }),
    enforcement: spec.enforcement,
    validation: spec.validation,
    referenced_constraint: ref === undefined ? null : objectRef("constraint", ref.schemaPath, ref.constraintId, ref.table),
  });
}

// ---------------------------------------------------------------------------
// Capability negotiation
// ---------------------------------------------------------------------------

/** Profiles DuckDB's vgi extension offers, by category (BuildNativeConstraintCapabilities). */
const KNOWN: Record<keyof CatalogCapabilities, ReadonlySet<string>> = {
  read: new Set([CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION, "vgi.index.definition.read.v1"]),
  ddl: new Set([CONSTRAINT_CREATE, CONSTRAINT_MUTATE, "vgi.index.definition.create.v1", "vgi.index.drop.v1"]),
  write: new Set([CONSTRAINT_ENFORCE]),
  transaction: new Set([TRANSACTION_DDL]),
};
/** A profile is selected only together with its prerequisites. */
const REQUIRES: Record<string, readonly string[]> = {
  [ROW_SCALAR_EXPRESSION]: [CONSTRAINT_RECORDS_READ],
  [CONSTRAINT_CREATE]: [CONSTRAINT_RECORDS_READ, CONSTRAINT_ENFORCE],
  [CONSTRAINT_MUTATE]: [CONSTRAINT_RECORDS_READ],
  [CONSTRAINT_ENFORCE]: [CONSTRAINT_RECORDS_READ],
  "vgi.index.definition.create.v1": ["vgi.index.definition.read.v1"],
  "vgi.index.drop.v1": ["vgi.index.definition.read.v1"],
};

const CATEGORIES = ["read", "ddl", "write", "transaction"] as const;

/**
 * Build a catalog's profile advertisement (`CatalogAttachResult.catalog_capabilities`).
 * Returned by `CatalogInterface.catalogCapabilities()`; the attach handler
 * selects from it against the client's offer.
 */
export function catalogCapabilities(init: Partial<CatalogCapabilities> = {}): CatalogCapabilities {
  return {
    read: [...new Set(init.read ?? [])].sort(),
    ddl: [...new Set(init.ddl ?? [])].sort(),
    write: [...new Set(init.write ?? [])].sort(),
    transaction: [...new Set(init.transaction ?? [])].sort(),
  };
}

/** What a catalog that serves (and enforces/mutates) canonical constraint records advertises. */
export function constraintCapabilities(options: { create?: boolean; mutate?: boolean; enforce?: boolean; transactionDdl?: boolean } = {}): CatalogCapabilities {
  const enforce = options.enforce || options.create;
  return catalogCapabilities({
    read: [CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION],
    ddl: [...(options.create ? [CONSTRAINT_CREATE] : []), ...(options.mutate ? [CONSTRAINT_MUTATE] : [])],
    write: enforce ? [CONSTRAINT_ENFORCE] : [],
    transaction: options.transactionDdl ? [TRANSACTION_DDL] : [],
  });
}

function offeredCatalog(clientCapabilities: Uint8Array | ClientCapabilities | null | undefined): CatalogCapabilities | null {
  if (clientCapabilities == null) return null;
  const client = clientCapabilities instanceof Uint8Array
    ? decodeASD<{ catalog?: Uint8Array | null }>(ClientCapabilitiesSchema, clientCapabilities)
    : clientCapabilities;
  return client.catalog == null ? null : decodeCatalogCapabilities(client.catalog);
}

/**
 * Select the client-offered subset of `available`, canonically sorted, keeping
 * a profile only when its prerequisites were selected too (CONSTRAINT_CREATE
 * needs records read + enforcement; the scalar expression read needs records
 * read). The DDL transaction profile requires `supportsTransactions`.
 */
export function selectCatalogCapabilities(
  available: CatalogCapabilities,
  clientCapabilities: Uint8Array | ClientCapabilities | null | undefined,
  supportsTransactions = false,
): CatalogCapabilities {
  const offered = offeredCatalog(clientCapabilities);
  if (offered == null) return catalogCapabilities();
  const chosen = new Set<string>();
  for (const category of CATEGORIES)
    for (const p of available[category] ?? []) if (offered[category]?.includes(p)) chosen.add(p);
  if (!supportsTransactions) chosen.delete(TRANSACTION_DDL);
  // Drop profiles whose prerequisites are missing until nothing changes.
  for (let changed = true; changed;) {
    changed = false;
    for (const p of [...chosen])
      if ((REQUIRES[p] ?? []).some((r) => !chosen.has(r))) { chosen.delete(p); changed = true; }
  }
  const pick = (category: keyof CatalogCapabilities) => (available[category] ?? []).filter((p) => chosen.has(p));
  return catalogCapabilities({ read: pick("read"), ddl: pick("ddl"), write: pick("write"), transaction: pick("transaction") });
}

/**
 * The client-offered read profiles from `available`, in canonical (sorted) order.
 * A scalar-expression read is selected only alongside constraint records.
 */
export function selectReadProfiles(available: readonly string[], clientCapabilities: Uint8Array | null | undefined): string[] {
  return selectCatalogCapabilities(catalogCapabilities({ read: [...available] }), clientCapabilities).read;
}

/**
 * The ClientCapabilities a client sends when the caller supplies none: it can
 * read canonical constraint records (decode them with `readConstraintRecord`)
 * and their CHECK expressions. It offers no DDL or enforcement; pass an
 * explicit offer to negotiate those.
 */
export function defaultClientCapabilities(engine = "vgi-typescript"): ClientCapabilities {
  return {
    engine,
    native_formats: [],
    catalogs: [],
    can_stream: true,
    filter_encodings: [],
    catalog: encodeCatalogCapabilities(catalogCapabilities({ read: [CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION] })),
    query_pushdown: null,
  };
}

/** True when every category of `caps` is empty. */
export function capabilitiesEmpty(caps: CatalogCapabilities | null | undefined): boolean {
  return caps == null || CATEGORIES.every((c) => (caps[c] ?? []).length === 0);
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/** A decoded `ConstraintInfo` record, in `constraintRecord()`'s vocabulary. */
export interface ConstraintRecordView {
  schemaPath: string[];
  table: string;
  id: string;
  kind: ConstraintKind;
  columns: string[][];
  enforcement: ConstraintEnforcement;
  validation: ConstraintValidation;
  declaredName: string | null;
  checkSql: string | null;
  references: {
    schemaPath: string[];
    table: string;
    columns: string[][];
    constraintId: string | null;
    /** The referenced constraint's owner table (should equal `table`). */
    constraintTable?: string | null;
    constraintSchemaPath?: string[] | null;
    match?: string;
    onUpdate?: string;
    onDelete?: string;
  } | null;
}

type RefRow = { kind: string; schema_path: string[]; name: string; owner_table: string | null };

/** Decode a `TableInfo.constraints` entry (the inverse of `constraintRecord`). */
export function readConstraintRecord(bytes: Uint8Array): ConstraintRecordView {
  const info = decodeASD<any>(ConstraintInfoSchema, bytes);
  const ref = decodeASD<RefRow>(CatalogObjectRefSchema, info.ref);
  const def = decodeASD<any>(ConstraintDefinitionSchema, info.definition);
  const fk = def.foreign_key == null ? null : decodeASD<any>(ForeignKeyDefinitionSchema, def.foreign_key);
  const target = fk == null ? null : decodeASD<RefRow>(CatalogObjectRefSchema, fk.referenced_table);
  const targetKey = info.referenced_constraint == null ? null : decodeASD<RefRow>(CatalogObjectRefSchema, info.referenced_constraint);
  return {
    schemaPath: [...ref.schema_path],
    table: ref.owner_table ?? "",
    id: ref.name,
    kind: def.kind,
    columns: [...def.column_paths].map((p: Iterable<string>) => [...p]),
    enforcement: info.enforcement,
    validation: info.validation,
    declaredName: def.declared_name ?? null,
    checkSql: def.check_expression == null ? null : decodeASD<any>(SqlExpressionSchema, def.check_expression).sql,
    references: fk == null || target == null ? null : {
      schemaPath: [...target.schema_path],
      table: target.name,
      columns: [...fk.referenced_column_paths].map((p: Iterable<string>) => [...p]),
      constraintId: targetKey?.name ?? null,
      constraintTable: targetKey?.owner_table ?? null,
      constraintSchemaPath: targetKey == null ? null : [...targetKey.schema_path],
      match: fk.match,
      onUpdate: fk.on_update,
      onDelete: fk.on_delete,
    },
  };
}

// ---------------------------------------------------------------------------
// Serving: the checks DuckDB applies at ATTACH (DecodeNativeConstraints)
// ---------------------------------------------------------------------------

export interface ValidatedConstraints {
  records: ConstraintRecordView[];
  /** JSON-encoded column paths a NOT_NULL or PRIMARY_KEY record makes non-nullable. */
  required: Set<string>;
}

function fieldsByExactName(fields: readonly VgiField[], name: string): number[] {
  const out: number[] = [];
  fields.forEach((f, i) => { if (f.name === name) out.push(i); });
  return out;
}

/**
 * Check a table's canonical records exactly as the DuckDB extension does at
 * ATTACH, so a worker's own tests fail with the table, constraint id and path
 * instead of an opaque ATTACH error: identity (owner table and one-component
 * schema), unique ids and declared names, ENFORCED + ALL_ROWS only, at most one
 * PRIMARY KEY, resolvable column paths (nested only for NOT_NULL, never the
 * row-id column), the native CHECK grammar and its operand types, and foreign
 * keys DuckDB can represent (same schema, MATCH SIMPLE, NO ACTION/RESTRICT).
 * Foreign-key targets are checked across tables by `validateForeignKeyTargets`.
 */
export function validateTableConstraints(
  table: string,
  schemaPath: readonly string[],
  fields: readonly VgiField[],
  constraints: readonly Uint8Array[],
): ValidatedConstraints {
  if (constraints.length > 1024) throw new Error(`Table '${table}': too many constraints (at most 1024)`);
  const rowIds = fields.map((f, i) => (isRowIdField(f) ? i : -1)).filter((i) => i >= 0);
  if (rowIds.length > 1) throw new Error(`Table '${table}': at most one column may carry is_row_id`);
  const rowId = rowIds[0] ?? -1;
  const records: ConstraintRecordView[] = [];
  const required = new Set<string>();
  const ids = new Set<string>();
  const labels = new Set<string>();
  let primaryKeys = 0;
  for (const bytes of constraints) {
    let record: ConstraintRecordView;
    try {
      record = readConstraintRecord(bytes);
    } catch (error) {
      throw new Error(`Table '${table}': undecodable constraint record: ${(error as Error).message}`);
    }
    const where = `Table '${table}': constraint '${record.id}'`;
    const fail = (message: string): never => { throw new Error(`${where}: ${message}`); };
    if (schemaPath.length !== 1 || record.schemaPath.length !== 1 || record.schemaPath[0] !== schemaPath[0] || record.table !== table)
      fail(`belongs to ${JSON.stringify(record.schemaPath)}.${record.table || "?"}, not the serving table ` +
        `${JSON.stringify([...schemaPath])}.${table} (DuckDB reads only one-component schema paths)`);
    if (ids.has(record.id)) fail("duplicate constraint id");
    ids.add(record.id);
    if (record.declaredName != null) {
      const label = record.declaredName.toLowerCase();
      if (labels.has(label)) fail(`duplicate declared name '${record.declaredName}'`);
      labels.add(label);
    }
    if (record.enforcement !== "ENFORCED" || record.validation !== "ALL_ROWS")
      fail(`is ${record.enforcement}/${record.validation}; DuckDB accepts only ENFORCED + ALL_ROWS records`);
    try {
      checkPaths(record.columns, record.kind, record.kind !== "NOT_NULL");
    } catch (error) {
      fail((error as Error).message);
    }
    const resolveTop = (path: string[]): number => {
      const matches = fieldsByExactName(fields, path[0]);
      if (matches.length !== 1 || matches[0] === rowId)
        fail(`column path ${path.join(".")} names a missing, ambiguous or row-id column`);
      return matches[0];
    };
    switch (record.kind) {
      case "CHECK": {
        if (record.checkSql == null) fail("CHECK without an expression");
        validateCheckRecord(bytes, where, schemaPath, fields, rowId);
        break;
      }
      case "NOT_NULL": {
        if (record.columns.length !== 1) fail("NOT_NULL requires exactly one column path");
        const path = record.columns[0];
        let field = fields[resolveTop(path)];
        for (let depth = 1; depth < path.length; depth++) {
          const children = ordinaryStructChildren(field);
          if (!children) fail(`column path ${path.join(".")} must traverse ordinary STRUCT fields`);
          const matches = fieldsByExactName(children!, path[depth]);
          if (matches.length !== 1) fail(`column path ${path.join(".")} names a missing or ambiguous nested field`);
          field = children![matches[0]];
        }
        if (path.length === 1 && isGeneratedField(field))
          fail(`column ${path[0]} is a generated column, which DuckDB requires to be nullable`);
        required.add(JSON.stringify(path));
        break;
      }
      case "PRIMARY_KEY":
      case "UNIQUE": {
        for (const path of record.columns) {
          const index = resolveTop(path);
          if (record.kind === "PRIMARY_KEY" && isGeneratedField(fields[index]))
            fail(`PRIMARY KEY column ${path[0]} is a generated column, which DuckDB requires to be nullable`);
        }
        if (record.kind === "PRIMARY_KEY") {
          if (++primaryKeys > 1) fail("a table may have only one PRIMARY KEY");
          for (const path of record.columns) required.add(JSON.stringify(path));
        }
        break;
      }
      case "FOREIGN_KEY": {
        record.columns.forEach(resolveTop);
        const ref = record.references;
        if (ref == null || ref.constraintId == null) fail("FOREIGN_KEY requires a referenced table and referenced constraint");
        if (ref!.schemaPath.length !== 1 || ref!.schemaPath[0] !== schemaPath[0] ||
            JSON.stringify(ref!.constraintSchemaPath) !== JSON.stringify(ref!.schemaPath) || ref!.constraintTable !== ref!.table)
          fail("DuckDB reads only foreign keys to an identified key of a table in the same schema");
        if (ref!.columns.length !== record.columns.length) fail("foreign key arity mismatch");
        try {
          checkPaths(ref!.columns, "FOREIGN_KEY referenced", true);
        } catch (error) {
          fail((error as Error).message);
        }
        if (ref!.match !== undefined && ref!.match !== "SIMPLE") fail(`MATCH ${ref!.match} is unsupported; DuckDB reads only MATCH SIMPLE`);
        for (const action of [ref!.onUpdate, ref!.onDelete])
          if (action !== undefined && action !== "NO_ACTION" && action !== "RESTRICT")
            fail(`referential action ${action} is unsupported; DuckDB reads only NO_ACTION or RESTRICT`);
        break;
      }
      default:
        fail(`unknown constraint kind ${String(record.kind)}`);
    }
    records.push(record);
  }
  return { records, required };
}

function validateCheckRecord(
  bytes: Uint8Array,
  where: string,
  schemaPath: readonly string[],
  fields: readonly VgiField[],
  rowId: number,
): void {
  const info = decodeASD<any>(ConstraintInfoSchema, bytes);
  const def = decodeASD<any>(ConstraintDefinitionSchema, info.definition);
  const expression = decodeASD<any>(SqlExpressionSchema, def.check_expression);
  const fail = (message: string): never => { throw new Error(`${where}: ${message}`); };
  if (expression.profile !== ROW_SCALAR_EXPRESSION) fail(`unsupported CHECK expression profile ${expression.profile}`);
  if (JSON.stringify([...expression.schema_path]) !== JSON.stringify([...schemaPath])) fail("CHECK expression belongs to another schema");
  const context = expression.evaluation_context ?? {};
  for (const [key, value] of Object.entries(CHECK_CONTEXT))
    if (context[key] !== value) fail(`unsupported CHECK evaluation context (${key} must be ${String(value)})`);
  try {
    validateNumericCheckSql(expression.sql, (name) => {
      const lower = name.toLowerCase();
      const matches = fields.map((f, i) => (f.name.toLowerCase() === lower ? i : -1)).filter((i) => i >= 0);
      if (matches.length > 1) throw new Error(`ambiguous CHECK column reference ${name}`);
      if (matches.length === 0 || matches[0] === rowId) throw new Error(`CHECK references a missing or row-id column ${name}`);
      const column = fields[matches[0]];
      if (column.metadata.has("ARROW:extension:name")) throw new Error(`CHECK operand ${name} has an extension type`);
      if (isGeneratedField(column)) throw new Error(`CHECK operand ${name} is a generated column`);
      const type = duckdbPrimitive(column);
      if (!type || !["TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE"].includes(type))
        throw new Error(`CHECK requires primitive signed numeric columns; ${name} is not one`);
    });
  } catch (error) {
    fail(`${(error as Error).message} (CHECK SQL: ${expression.sql})`);
  }
}

/** A table's identity and records, for `validateForeignKeyTargets`. */
export interface ConstraintTableRef {
  schemaPath: readonly string[];
  name: string;
  constraints: readonly Uint8Array[];
}

/**
 * Check every FOREIGN_KEY among `tables` against its target: the referenced
 * table is among `tables`, and the referenced constraint id is that table's
 * PRIMARY_KEY or UNIQUE record over the referenced columns.
 */
export function validateForeignKeyTargets(tables: readonly ConstraintTableRef[]): void {
  const key = (schemaPath: readonly string[], name: string) => JSON.stringify([...schemaPath, name]);
  const byTable = new Map<string, ConstraintRecordView[]>();
  for (const t of tables) byTable.set(key(t.schemaPath, t.name), t.constraints.map(readConstraintRecord));
  for (const t of tables) {
    for (const record of byTable.get(key(t.schemaPath, t.name)) ?? []) {
      if (record.kind !== "FOREIGN_KEY" || record.references == null) continue;
      const ref = record.references;
      const where = `Table '${t.name}': constraint '${record.id}'`;
      const target = byTable.get(key(ref.schemaPath, ref.table));
      if (!target) throw new Error(`${where}: references table ${[...ref.schemaPath, ref.table].join(".")}, which this catalog does not serve`);
      const key_ = target.find((r) => r.id === ref.constraintId);
      if (!key_ || (key_.kind !== "PRIMARY_KEY" && key_.kind !== "UNIQUE"))
        throw new Error(`${where}: referenced constraint '${ref.constraintId}' is not a PRIMARY_KEY or UNIQUE record of '${ref.table}'`);
      const want = new Set(ref.columns.map((p) => JSON.stringify(p)));
      if (key_.columns.length !== want.size || !key_.columns.every((p) => want.has(JSON.stringify(p))))
        throw new Error(`${where}: referenced columns do not match constraint '${ref.constraintId}' of '${ref.table}'`);
    }
  }
}

/**
 * The served Arrow fields for a table's records: nullability is exactly the
 * records' (a field, descending through ordinary STRUCTs, is non-nullable iff
 * a NOT_NULL or PRIMARY_KEY record names its path; the top-level row-id column
 * is untouched). Returns `fields` itself when nothing changes.
 */
export function servedConstraintFields(fields: readonly VgiField[], required: ReadonlySet<string>): readonly VgiField[] {
  const rewrite = (list: readonly VgiField[], prefix: string[]): readonly VgiField[] => {
    let changed = false;
    const out = list.map((f) => {
      if (prefix.length === 0 && isRowIdField(f)) return f;
      const path = [...prefix, f.name];
      const nullable = !required.has(JSON.stringify(path));
      const children = ordinaryStructChildren(f);
      const newChildren = children ? rewrite(children, path) : null;
      if (nullable === f.nullable && (newChildren == null || newChildren === children)) return f;
      changed = true;
      const type: VgiDataType = newChildren && newChildren !== children ? struct_(newChildren) : f.type;
      return field_(f.name, type, nullable, new Map(f.metadata));
    });
    return changed ? out : list;
  };
  return rewrite(fields, []);
}

/**
 * @deprecated Since 0.48: tables are served with nullability derived from their
 * records automatically, so a non-nullable field no record names is no longer
 * an error. Use `validateTableConstraints` for the remaining ATTACH checks.
 */
export function validateConstraintNullability(
  table: string,
  fields: readonly VgiField[],
  constraints: readonly Uint8Array[],
): void {
  const schemaPath = constraints.length ? readConstraintRecord(constraints[0]).schemaPath : ["main"];
  validateTableConstraints(table, schemaPath, fields, constraints);
}

/**
 * Translate the pre-v3 `generated_expression` field metadata (raw SQL) to the
 * canonical `vgi.column.generated.v1` + `vgi.column.generation.v1 = virtual`
 * pair, when that is unambiguous: the field carries no canonical key.
 */
function translateLegacyGenerated(fields: readonly VgiField[], owner: readonly string[], table: string): readonly VgiField[] {
  let changed = false;
  const out = fields.map((f) => {
    const sql = f.metadata.get("generated_expression");
    if (sql === undefined) return f;
    if (f.metadata.has(GENERATED_EXPRESSION_KEY) || f.metadata.has(GENERATION_KEY))
      throw new Error(`Table '${table}': column ${f.name} carries both legacy generated_expression and canonical generated metadata`);
    const metadata = new Map(f.metadata);
    metadata.delete("generated_expression");
    metadata.set(GENERATED_EXPRESSION_KEY, generatedExpressionMetadata(sql, [...owner]));
    metadata.set(GENERATION_KEY, "virtual");
    changed = true;
    return field_(f.name, f.type, f.nullable, metadata);
  });
  return changed ? out : fields;
}

/**
 * The served form of a table's Arrow schema: legacy generated metadata
 * translated, records validated as DuckDB validates them at ATTACH, and
 * nullability made exactly the records' (see `servedConstraintFields`).
 * Returns the input schema itself when nothing changes.
 */
export function servedTableSchema(
  table: string,
  schemaPath: readonly string[],
  schema: VgiSchema,
  constraints: readonly Uint8Array[],
): VgiSchema {
  let fields = translateLegacyGenerated(schema.fields, schemaPath, table);
  const { required } = validateTableConstraints(table, schemaPath, fields, constraints);
  fields = servedConstraintFields(fields, required);
  return fields === schema.fields ? schema : schema_(fields, schema.metadata);
}

/**
 * Apply `servedTableSchema` to a TableInfo about to be sent. The original
 * column bytes are kept when nothing changes.
 */
export function servedTableInfo(info: TableInfo): TableInfo {
  if (!info.columns || info.columns.length === 0) {
    if ((info.constraints ?? []).length) throw new Error(`Table '${info.name}': constraints on a table without columns`);
    return info;
  }
  const schema = deserializeSchema(info.columns);
  const served = servedTableSchema(info.name, info.schema_path, schema, info.constraints ?? []);
  return served === schema ? info : { ...info, columns: serializeSchema(served) };
}

