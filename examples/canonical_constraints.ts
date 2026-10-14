// Copyright 2026 Query Farm LLC - https://query.farm
// Explicit immutable fixture declarations. These records claim no enforcement.

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

/** IDs are explicit fixture identities, never inferred by the SDK from labels or positions. */
export function fixtureConstraint(
  table: string,
  id: string,
  kind: "NOT_NULL" | "PRIMARY_KEY" | "UNIQUE" | "CHECK" | "FOREIGN_KEY",
  columnPaths: string[][],
  options: { checkSql?: string; targetTable?: string; targetPaths?: string[][]; targetKey?: string } = {},
): Uint8Array {
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
    enforcement: "METADATA_ONLY", validation: "UNVALIDATED",
    referenced_constraint: options.targetKey === undefined ? null
      : ref("constraint", options.targetKey, options.targetTable!),
  });
}
