// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Empty (0-row) batch construction for the flechette backend.

import { columnFromArray, tableFromColumns } from "@uwdata/flechette";
import type { VgiSchema, VgiBatch } from "../types.js";

/**
 * Create an empty (0-row) batch with the given schema.
 *
 * For each field we build a 0-length column of the matching type. flechette
 * handles complex types (List, Map, Struct, Decimal) without the manual
 * makeData scaffolding the arrow-js backend needs.
 */
export function emptyBatch(schema: VgiSchema): VgiBatch {
  const cols: Record<string, ReturnType<typeof columnFromArray>> = {};
  for (const f of schema.fields) {
    cols[f.name] = columnFromArray([], f.type as any);
  }
  return tableFromColumns(cols) as unknown as VgiBatch;
}
