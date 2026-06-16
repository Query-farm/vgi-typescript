// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Mask-based row filtering for flechette tables.

import type { VgiBatch, VgiDataType } from "../types.js";
import { emptyBatch } from "./empty.js";
import { batchFromColumns } from "./build.js";
import { codecFor } from "../codec/registry.js";
import { readCanonicalValue } from "./canonical.js";

/**
 * Filter a batch using a Uint8Array mask (0=exclude, nonzero=include).
 *
 * Rows are read in canonical form then mapped back to RICH so the rebuild goes
 * through the same codec/canonical path as every other column build — lossless,
 * Date-correct (flechette `.at()` returns epoch-ms for dates), and identical to
 * the arrow-js backend.
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
    if (!col) continue;
    const type = f.type as VgiDataType;
    const codec = codecFor(type);
    columns[f.name] = indices.map((i) =>
      codec.canonicalToRich(readCanonicalValue(type, col, i)),
    );
  }
  return batchFromColumns(columns, t.schema);
}
