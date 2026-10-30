// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Canonical constraint records for catalogs that state their own facts.

import { encodeASD, decodeASD } from "../codec/asd.js";
import { isStruct, type VgiDataType, type VgiField } from "../arrow/index.js";
import {
  CatalogObjectRefSchema,
  ClientCapabilitiesSchema,
  ConstraintDefinitionSchema,
  ConstraintInfoSchema,
  ForeignKeyDefinitionSchema,
  SqlExpressionSchema,
} from "../generated/vgi-protocol-schemas.js";
import { decodeCatalogCapabilities, type CatalogCapabilities } from "../generated/vgi-protocol-types.js";

/** Read profile: the catalog serves canonical ConstraintInfo records. */
export const CONSTRAINT_RECORDS_READ = "vgi.constraint.records.read.v1";
/** Read profile: CHECK constraints carry DuckDB scalar SQL expressions. */
export const ROW_SCALAR_EXPRESSION = "vgi.expression.duckdb.scalar.v1";

export type ConstraintKind = "NOT_NULL" | "PRIMARY_KEY" | "UNIQUE" | "CHECK" | "FOREIGN_KEY";
/** Whether the authority enforces the constraint on every write path. */
export type ConstraintEnforcement = "ENFORCED" | "METADATA_ONLY";
/** Coverage of existing rows; only ALL_ROWS justifies engine facts. */
export type ConstraintValidation = "ALL_ROWS" | "FUTURE_WRITES" | "UNVALIDATED";

export interface ConstraintRecordSpec {
  /** Schema path of the owning table. */
  schemaPath: readonly string[];
  /** Owning table name. */
  table: string;
  /** Stable provider-owned identity, unique within the table. Never derived by the SDK. */
  id: string;
  kind: ConstraintKind;
  /** Raw component paths, e.g. `[["id"]]` or `[["bbox", "xmin"]]`; empty for CHECK. */
  columns: readonly (readonly string[])[];
  /**
   * The provider's attestation. The DuckDB extension reads only
   * ENFORCED + ALL_ROWS records, so state that only when every row the table
   * can ever return satisfies the constraint.
   */
  enforcement: ConstraintEnforcement;
  validation: ConstraintValidation;
  /** Optional SQL label (CONSTRAINT <name>). */
  declaredName?: string;
  /** CHECK only: a DuckDB scalar expression over the table's columns. */
  checkSql?: string;
  /** FOREIGN_KEY only: the referenced key and its constraint identity. */
  references?: {
    schemaPath: readonly string[];
    table: string;
    columns: readonly (readonly string[])[];
    /** `id` of the referenced table's PRIMARY_KEY or UNIQUE record. */
    constraintId: string;
  };
}

function objectRef(kind: "table" | "constraint", schemaPath: readonly string[], name: string, ownerTable: string | null): Uint8Array {
  return encodeASD(CatalogObjectRefSchema, { kind, schema_path: schemaPath, name, owner_table: ownerTable });
}

/** Encode one canonical `ConstraintInfo` record for `TableDescriptor.constraints`. */
export function constraintRecord(spec: ConstraintRecordSpec): Uint8Array {
  if (!spec.id || spec.id.includes("\0")) throw new Error("constraint id must be nonempty without NUL");
  if ((spec.kind === "CHECK") !== (spec.checkSql !== undefined))
    throw new Error("checkSql is required for CHECK and only CHECK");
  if (spec.kind === "CHECK" ? spec.columns.length !== 0 : spec.columns.length === 0)
    throw new Error(spec.kind === "CHECK" ? "CHECK takes no column paths" : `${spec.kind} requires column paths`);
  if (spec.kind === "NOT_NULL" && spec.columns.length !== 1) throw new Error("NOT_NULL requires exactly one column path");
  if ((spec.kind === "FOREIGN_KEY") !== (spec.references !== undefined))
    throw new Error("references is required for FOREIGN_KEY and only FOREIGN_KEY");
  if (spec.references && spec.references.columns.length !== spec.columns.length)
    throw new Error("foreign key local and referenced arity must match");
  const check = spec.checkSql === undefined ? null : encodeASD(SqlExpressionSchema, {
    sql: spec.checkSql,
    profile: ROW_SCALAR_EXPRESSION,
    schema_path: spec.schemaPath,
    evaluation_context: {
      timezone: "UTC", collation: "binary", default_order: "asc",
      default_null_order: "nulls_last", preserve_insertion_order: true,
    },
  });
  const ref = spec.references;
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

/**
 * The client-offered profiles from `available`, in canonical (sorted) order.
 * A scalar-expression read is selected only alongside constraint records.
 */
export function selectReadProfiles(available: readonly string[], clientCapabilities: Uint8Array | null | undefined): string[] {
  if (clientCapabilities == null || available.length === 0) return [];
  const client = decodeASD<{ catalog?: Uint8Array | null }>(ClientCapabilitiesSchema, clientCapabilities);
  const offered: CatalogCapabilities["read"] = client.catalog == null ? [] : decodeCatalogCapabilities(client.catalog).read;
  const selected = available.filter((p) => offered.includes(p));
  if (!selected.includes(CONSTRAINT_RECORDS_READ)) return selected.filter((p) => p !== ROW_SCALAR_EXPRESSION).sort();
  return selected.sort();
}

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
  references: { schemaPath: string[]; table: string; columns: string[][]; constraintId: string | null } | null;
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
    },
  };
}

function ordinaryStructChildren(field: VgiField): readonly VgiField[] | null {
  if (!isStruct(field.type)) return null;
  for (const key of field.metadata.keys())
    if (key === "ARROW:extension:name" || key.startsWith("vgi.logical_type.")) return null;
  return (field.type as VgiDataType & { children: readonly VgiField[] }).children;
}

/**
 * Check a table's records the way the DuckDB extension does at ATTACH: every
 * record is ENFORCED + ALL_ROWS, and Arrow nullability agrees with the
 * NOT_NULL/PRIMARY_KEY records (a field, descending through ordinary STRUCTs,
 * is non-nullable exactly when a record names its path).
 */
export function validateConstraintNullability(
  table: string,
  fields: readonly VgiField[],
  constraints: readonly Uint8Array[],
): void {
  const required = new Set<string>();
  for (const bytes of constraints) {
    const record = readConstraintRecord(bytes);
    // DuckDB reads only facts that hold for every row; anything else fails ATTACH.
    if (record.enforcement !== "ENFORCED" || record.validation !== "ALL_ROWS")
      throw new Error(
        `Table '${table}': constraint '${record.id}' is ${record.enforcement}/${record.validation}; ` +
          "DuckDB accepts only ENFORCED + ALL_ROWS records"
      );
    if (record.kind === "NOT_NULL" || record.kind === "PRIMARY_KEY")
      for (const path of record.columns) required.add(JSON.stringify(path));
  }
  const walk = (children: readonly VgiField[], prefix: string[]) => {
    for (const field of children) {
      const path = [...prefix, field.name];
      const key = JSON.stringify(path);
      if (field.nullable === required.has(key)) {
        throw new Error(
          field.nullable
            ? `Table '${table}': column ${path.join(".")} is in a NOT_NULL/PRIMARY_KEY record but its Arrow field is nullable`
            : `Table '${table}': column ${path.join(".")} is non-nullable in Arrow but no NOT_NULL/PRIMARY_KEY record names it; ` +
              "declare one with constraintRecord() or make the field nullable"
        );
      }
      const nested = ordinaryStructChildren(field);
      if (nested) walk(nested, path);
    }
  };
  walk(fields, []);
}
