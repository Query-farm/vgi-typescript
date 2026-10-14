// Copyright 2026 Query Farm LLC - https://query.farm
import { MethodNotImplementedError } from "@query-farm/vgi-rpc";
import { deserializeSchema, isStruct, type VgiField } from "../arrow/index.js";

/** No constraint authority means every SQL user field must remain nullable. */
export function requireUnconstrainedColumns(columns: Uint8Array): void {
  const visit = (field: VgiField, top: boolean, depth: number): void => {
    if (depth > 64) throw new MethodNotImplementedError("catalog_table_create: column nesting exceeds supported depth");
    if (top && field.metadata?.has("is_row_id")) return;
    for (const key of field.metadata?.keys() ?? []) {
      if (key === "ARROW:extension:name" || key.startsWith("vgi.logical_type.")) {
        throw new MethodNotImplementedError("catalog_table_create: logical column types require negotiation");
      }
    }
    if (!field.nullable) {
      throw new MethodNotImplementedError("catalog_table_create: nonnullable user columns require canonical constraints");
    }
    if (isStruct(field.type)) {
      for (const child of (field.type as VgiField["type"] & { children: readonly VgiField[] }).children) visit(child, false, depth + 1);
    }
  };
  for (const field of deserializeSchema(columns).fields) visit(field, true, 0);
}
