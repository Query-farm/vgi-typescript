// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Mask-based row filtering for flechette tables.

import type { VgiBatch } from "../types.js";
import { emptyBatch } from "./empty.js";
import { batchFromColumns } from "./build.js";

/**
 * Filter a batch using a Uint8Array mask (0=exclude, nonzero=include).
 */
export function filterBatch(batch: VgiBatch, mask: Uint8Array): VgiBatch {
  const t = batch as any;
  const n = t.numRows;

  let passCount = 0;
  for (let i = 0; i < n; i++) if (mask[i]) passCount++;

  if (passCount === n) return batch;
  if (passCount === 0) return emptyBatch(t.schema);

  const indices: number[] = [];
  for (let i = 0; i < n; i++) if (mask[i]) indices.push(i);

  const columns: Record<string, any[]> = {};
  for (const f of t.schema.fields) {
    const col = t.getChild(f.name);
    if (col) columns[f.name] = indices.map((i) => col.at(i));
  }
  return batchFromColumns(columns, t.schema);
}
