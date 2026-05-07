// Build batches from row objects or column arrays for the flechette backend.
//
// flechette's `columnFromArray(values, type)` handles all the complex cases
// (List, Map, Struct, Decimal, Timestamp[ns]) that the arrow-js backend has
// to assemble manually with `makeData` + offset arrays. Most of build.ts in
// impl-arrowjs (~415 lines) collapses to this much shorter file.

import {
  columnFromArray as f_columnFromArray,
  tableFromColumns,
  type Column,
} from "@uwdata/flechette";
import type { VgiSchema, VgiBatch, VgiDataType, VgiColumnData } from "../types.js";
import { emptyBatch } from "./empty.js";

/**
 * Build an opaque column-data handle from a JS array. Mirrors the
 * arrow-js backend's `columnFromArray` — flechette's native
 * `columnFromArray` returns a Column directly, so this is a simple
 * cast at the boundary.
 */
export function columnFromArray(values: any[], type: VgiDataType): VgiColumnData {
  return f_columnFromArray(values, type as any, {
    useBigInt: true,
    useBigIntTimestamp: true,
    useDecimalInt: true,
  }) as VgiColumnData;
}

/**
 * Build a batch from row objects.
 */
export function batchFromRows(
  rows: Record<string, any>[],
  schema: VgiSchema,
): VgiBatch {
  if (rows.length === 0) return emptyBatch(schema);
  const columns: Record<string, any[]> = {};
  for (const f of schema.fields) {
    columns[f.name] = rows.map((r) => r[f.name] ?? null);
  }
  return batchFromColumns(columns, schema);
}

/**
 * Build a batch from column arrays.
 */
export function batchFromColumns(
  columns: Record<string, any[]>,
  schema: VgiSchema,
): VgiBatch {
  const cols: Record<string, Column<any>> = {};
  for (const f of schema.fields) {
    const values = columns[f.name];
    if (values) {
      cols[f.name] = f_columnFromArray(values, f.type as any, {
        useBigInt: true,
        useBigIntTimestamp: true,
        useDecimalInt: true,
      });
    } else {
      // No values — emit an all-null column of the schema's expected length.
      const rowCount = inferRowCount(columns, schema);
      cols[f.name] = f_columnFromArray(
        new Array(rowCount).fill(null),
        f.type as any,
      );
    }
  }
  return tableFromColumns(cols) as unknown as VgiBatch;
}

function inferRowCount(
  columns: Record<string, any[]>,
  schema: VgiSchema,
): number {
  for (const f of schema.fields) {
    if (columns[f.name]) return columns[f.name].length;
  }
  return 0;
}
