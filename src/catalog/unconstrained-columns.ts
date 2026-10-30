// Copyright 2026 Query Farm LLC - https://query.farm
import { MethodNotImplementedError } from "@query-farm/vgi-rpc";
import { deserializeSchema, isStruct, type VgiField } from "../arrow/index.js";
import { decodeConstraintDefinition } from "../generated/vgi-protocol-types.js";

/**
 * Check CREATE TABLE columns against the canonical constraint definitions sent
 * with them: a column may be non-nullable only when a NOT_NULL or PRIMARY_KEY
 * definition names it (DuckDB marks exactly those), nested fields stay
 * nullable, and logical column types are refused (they need negotiation).
 * With no definitions every user column must be nullable.
 */
export function requireCreateColumns(columns: Uint8Array, constraintDefinitions: readonly Uint8Array[] = []): void {
  const required = new Set<string>();
  for (const bytes of constraintDefinitions) {
    let def;
    try {
      def = decodeConstraintDefinition(bytes);
    } catch (error) {
      throw new Error(`catalog_table_create: undecodable constraint definition: ${(error as Error).message}`);
    }
    if (def.kind === "NOT_NULL" || def.kind === "PRIMARY_KEY")
      for (const path of def.column_paths) required.add(JSON.stringify([...path]));
  }
  const visit = (field: VgiField, path: string[], depth: number): void => {
    if (depth > 64) throw new MethodNotImplementedError("catalog_table_create: column nesting exceeds supported depth");
    if (depth === 0 && field.metadata?.has("is_row_id")) return;
    for (const key of field.metadata?.keys() ?? []) {
      if (key === "ARROW:extension:name" || key.startsWith("vgi.logical_type.")) {
        throw new MethodNotImplementedError("catalog_table_create: logical column types require negotiation");
      }
    }
    if (!field.nullable && !required.has(JSON.stringify(path))) {
      throw new MethodNotImplementedError(
        `catalog_table_create: column ${path.join(".")} is non-nullable but no NOT_NULL/PRIMARY_KEY definition names it`,
      );
    }
    if (isStruct(field.type)) {
      for (const child of (field.type as VgiField["type"] & { children: readonly VgiField[] }).children)
        visit(child, [...path, child.name], depth + 1);
    }
  };
  for (const field of deserializeSchema(columns).fields) visit(field, [field.name], 0);
}

/** No constraint authority means every SQL user field must remain nullable. */
export function requireUnconstrainedColumns(columns: Uint8Array): void {
  requireCreateColumns(columns, []);
}
