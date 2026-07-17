// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Per-record-batch metadata attachment (arrow-js backend).

import { RecordBatch } from "@query-farm/apache-arrow";
import type { VgiBatch } from "../types.js";

/**
 * Re-emit a batch with a different per-record-batch metadata map (same schema
 * + data, no copy). The @query-farm/apache-arrow fork's RecordBatch carries
 * the map through IPC as the Message's `custom_metadata` field.
 *
 * Mirrors vgi-rpc's arrow facade helper of the same name — needed here so a
 * worker can bake metadata into the batch object itself (vgi-rpc's HTTP
 * exchange dispatch drops the separate emit-metadata argument for 0-row
 * batches; a batch-carried map survives).
 */
export function withBatchMetadata(batch: VgiBatch, metadata: Map<string, string>): VgiBatch {
  const a = batch as unknown as RecordBatch;
  return new RecordBatch(a.schema, a.data, metadata) as unknown as VgiBatch;
}
