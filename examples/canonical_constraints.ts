// Copyright 2026 Query Farm LLC - https://query.farm
// Facts are published only after validation against the same immutable rows used by scans.

import { IMMUTABLE_CONSTRAINT_ROWS, IMMUTABLE_VERSIONED_ROWS, type FixtureRows } from "./immutable_constraint_rows.js";
import { encodeASD } from "../src/codec/asd.js";
import {
  CatalogObjectRefSchema, ConstraintDefinitionSchema, ConstraintInfoSchema,
  ForeignKeyDefinitionSchema, SqlExpressionSchema,
} from "../src/generated/vgi-protocol-schemas.js";

export const FIXTURE_CONSTRAINT_REVISION = new TextEncoder().encode("fixture.constraints.v1");

function ref(kind: string, name: string, ownerTable: string | null): Uint8Array {
  return encodeASD(CatalogObjectRefSchema, {
    kind, schema_path: ["data"], name, owner_table: ownerTable,
  });
}

type Kind = "NOT_NULL" | "PRIMARY_KEY" | "UNIQUE" | "CHECK" | "FOREIGN_KEY";
type Options = { checkSql?: string; targetTable?: string; targetPaths?: string[][]; targetKey?: string; version?: number };

/** Bounded proof for these read-only fixtures, not a general SQL validator. */
export function validateFixtureConstraint(
  rows: FixtureRows, kind: Kind, paths: string[][], options: Options = {},
  target: FixtureRows = IMMUTABLE_CONSTRAINT_ROWS.departments!,
): void {
  const fail = () => { throw new Error(`invalid immutable fixture ${kind} fact`); };
  const columns = Object.values(rows);
  const count = columns[0]?.length ?? 0;
  if (!columns.length || columns.some(column => column.length !== count)) fail();
  const keys = (source: FixtureRows, names: string[][]) => {
    if (!names.length || names.some(path => path.length !== 1 || !(path[0]! in source))) fail();
    return Array.from({ length: Object.values(source)[0]!.length }, (_, i) => names.map(path => source[path[0]!]![i]));
  };
  const key = (values: any[]) => JSON.stringify(values.map(value => [typeof value, String(value)]));
  if (kind === "CHECK") {
    if (paths.length || options.checkSql !== "budget >= 0" || !rows.budget ||
        rows.budget.some(value => value !== null && (typeof value !== "number" || !Number.isFinite(value) || value < 0))) fail();
    return;
  }
  const values = keys(rows, paths);
  if (kind === "NOT_NULL" || kind === "PRIMARY_KEY")
    if (values.some(tuple => tuple.some(value => value == null))) fail();
  if (kind === "PRIMARY_KEY" || kind === "UNIQUE") {
    const nonNull = values.filter(tuple => tuple.every(value => value != null)).map(key);
    if (new Set(nonNull).size !== nonNull.length) fail();
  }
  if (kind === "FOREIGN_KEY") {
    if (options.targetTable !== "departments" || options.targetKey !== "fixture.departments.primary" ||
        JSON.stringify(options.targetPaths) !== '[["id"]]') fail();
    validateFixtureConstraint(target, "PRIMARY_KEY", [["id"]]);
    const targetKeys = new Set(keys(target, options.targetPaths!).map(key));
    if (values.some(tuple => tuple.every(value => value != null) && !targetKeys.has(key(tuple)))) fail();
  }
}

/** IDs are explicit fixture identities, never inferred by the SDK from labels or positions. */
export function fixtureConstraint(
  table: string,
  id: string,
  kind: Kind,
  columnPaths: string[][],
  options: Options = {},
): Uint8Array {
  const rows = table === "versioned_constraints" ? IMMUTABLE_VERSIONED_ROWS[options.version ?? 3] : IMMUTABLE_CONSTRAINT_ROWS[table];
  if (!rows) throw new Error(`no immutable rows for ${table}`);
  validateFixtureConstraint(rows, kind, columnPaths, options);
  const check = options.checkSql === undefined ? null : encodeASD(SqlExpressionSchema, {
    sql: options.checkSql,
    profile: "vgi.expression.duckdb.scalar.v1",
    schema_path: ["data"],
    evaluation_context: {
      timezone: "UTC", collation: "binary", default_order: "asc",
      default_null_order: "nulls_last", preserve_insertion_order: true,
    },
  });
  const foreign = options.targetTable === undefined ? null : encodeASD(ForeignKeyDefinitionSchema, {
    referenced_table: ref("table", options.targetTable, null),
    referenced_column_paths: options.targetPaths,
    match: "SIMPLE", on_update: "NO_ACTION", on_delete: "NO_ACTION",
  });
  return encodeASD(ConstraintInfoSchema, {
    ref: ref("constraint", id, table),
    definition: encodeASD(ConstraintDefinitionSchema, {
      kind, column_paths: columnPaths, declared_name: null, check_expression: check, foreign_key: foreign,
    }),
    enforcement: "ENFORCED", validation: "ALL_ROWS",
    referenced_constraint: options.targetKey === undefined ? null
      : ref("constraint", options.targetKey, options.targetTable!),
  });
}

export function versionedFixtureConstraints(version: number): Uint8Array[] {
  const table = "versioned_constraints";
  const records = [fixtureConstraint(table, "fixture.versioned.id.required", "NOT_NULL", [["id"]], { version })];
  if (version >= 2) records.push(
    fixtureConstraint(table, "fixture.versioned.primary", "PRIMARY_KEY", [["id"]], { version }),
    fixtureConstraint(table, "fixture.versioned.name.required", "NOT_NULL", [["name"]], { version }),
    fixtureConstraint(table, "fixture.versioned.email.unique", "UNIQUE", [["email"]], { version }),
  );
  if (version >= 3) records.push(fixtureConstraint(table, "fixture.versioned.department.foreign", "FOREIGN_KEY", [["department_id"]], {
    version, targetTable: "departments", targetPaths: [["id"]], targetKey: "fixture.departments.primary",
  }));
  return records;
}
