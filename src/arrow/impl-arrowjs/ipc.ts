// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Arrow IPC stream serialization/deserialization for schemas and batches.

import {
  RecordBatch,
  Schema,
  RecordBatchStreamWriter,
  RecordBatchReader,
} from "@query-farm/apache-arrow";
import type { VgiSchema, VgiBatch } from "../types.js";
import { emptyBatch } from "./empty.js";

/**
 * Serialize a Schema to Arrow IPC bytes. Accepts arrow-js `Schema` or
 * facade `VgiSchema` (the latter is satisfied structurally by arrow-js
 * Schema instances at runtime).
 */
export function serializeSchema(schema: Schema | VgiSchema): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema as Schema);
  writer.close();
  return writer.toUint8Array(true);
}

/**
 * Deserialize an Arrow Schema from IPC bytes.
 * Note: In Bun, reader.schema is always undefined, so we must read from the batch.
 *
 * Returns arrow-js's `Schema` (which structurally satisfies VgiSchema).
 */
export function deserializeSchema(bytes: Uint8Array): Schema {
  const reader = RecordBatchReader.from(bytes);
  const batches = [...reader];
  if (batches.length > 0) {
    return batches[0].schema;
  }
  if (reader.schema) return reader.schema;
  throw new Error("Cannot deserialize schema from empty IPC stream");
}

/**
 * Serialize a RecordBatch to Arrow IPC bytes. Accepts arrow-js `RecordBatch`
 * or facade `VgiBatch`.
 */
export function serializeBatch(batch: RecordBatch | VgiBatch): Uint8Array {
  const a = batch as RecordBatch;
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, a.schema);
  // Use _writeRecordBatch to bypass schema comparison bug
  // (public write() silently drops batches on nullability mismatch)
  (writer as any)._writeRecordBatch(a);
  writer.close();
  return writer.toUint8Array(true);
}

/**
 * Deserialize a RecordBatch from Arrow IPC bytes.
 */
export function deserializeBatch(bytes: Uint8Array): RecordBatch {
  const reader = RecordBatchReader.from(bytes);
  const batches = [...reader];
  if (batches.length === 0) {
    const sch = reader.schema ?? new Schema([]);
    return emptyBatch(sch);
  }
  return batches[0];
}
