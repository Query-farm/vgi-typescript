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
  // Copy the own properties as descriptors, not by assignment. vgi-rpc's reader
  // pins a zero-column batch's row count as an own `numRows` over
  // `Table.prototype.numRows`, a getter-only accessor, and `Object.assign`
  // *assigns* each own property -- which throws in strict mode ("Attempted to
  // assign to readonly property") over an inherited getter-only accessor.
  const clone = Object.create(Object.getPrototypeOf(t), Object.getOwnPropertyDescriptors(t));
  // This backend's own reader supplies that row count through a Proxy instead
  // (see `deserializeBatch`), which no property copy carries: without this the
  // clone would derive 0 rows from its absent columns.
  if (clone.numRows !== t.numRows) {
    Object.defineProperty(clone, "numRows", { value: t.numRows, configurable: true, enumerable: true });
  }
  if (metadata && metadata.size > 0) {
    clone._vgiRecordMetadata = metadata;
    clone.metadata = metadata;
  }
  return clone as unknown as VgiBatch;
}
