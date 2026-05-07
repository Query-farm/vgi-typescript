// Mask-based row filtering for Arrow RecordBatches.

import { RecordBatch } from "@query-farm/apache-arrow";
import type { VgiBatch } from "../types.js";
import { emptyBatch } from "./empty.js";
import { batchFromColumns } from "./build.js";

/**
 * Filter a RecordBatch using a Uint8Array mask (0=exclude, nonzero=include).
 * Returns a new batch containing only the rows where mask[i] is nonzero.
 */
export function filterBatch(
  batch: RecordBatch | VgiBatch,
  mask: Uint8Array,
): RecordBatch {
  const a = batch as RecordBatch;
  const n = a.numRows;

  // Count passing rows
  let passCount = 0;
  for (let i = 0; i < n; i++) {
    if (mask[i]) passCount++;
  }

  // Fast paths
  if (passCount === n) return a;
  if (passCount === 0) return emptyBatch(a.schema);

  // Collect passing indices
  const indices: number[] = [];
  for (let i = 0; i < n; i++) {
    if (mask[i]) indices.push(i);
  }

  // Rebuild batch with only passing rows
  const columns: Record<string, any[]> = {};
  for (const field of a.schema.fields) {
    const col = a.getChild(field.name)!;
    columns[field.name] = indices.map((i) => col.get(i));
  }
  return batchFromColumns(columns, a.schema);
}
