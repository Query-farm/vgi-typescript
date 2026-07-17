// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Per-record-batch metadata attachment (flechette backend).

import type { VgiBatch } from "../types.js";

/**
 * Re-emit a batch with a different per-record-batch metadata map (same schema
 * + data). Shallow-clones the flechette Table so the caller's reference is
 * not mutated, then pins the map so that:
 *  - `batch.metadata` surfaces it to consumer code (matching arrow-js's
 *    RecordBatch.metadata getter behavior), and
 *  - the patched flechette encoder picks it up via `_vgiRecordMetadata` and
 *    emits it as the IPC Message's `custom_metadata` field.
 *
 * Mirrors vgi-rpc's arrow facade helper of the same name.
 */
export function withBatchMetadata(batch: VgiBatch, metadata: Map<string, string>): VgiBatch {
  const t = batch as any;
  const clone = Object.assign(Object.create(Object.getPrototypeOf(t)), t);
  if (metadata && metadata.size > 0) {
    clone._vgiRecordMetadata = metadata;
    clone.metadata = metadata;
  }
  return clone as unknown as VgiBatch;
}
