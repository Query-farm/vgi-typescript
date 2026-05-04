// Arrow IPC stream serialization/deserialization for schemas and batches.

import {
  RecordBatch,
  Schema,
  RecordBatchStreamWriter,
  RecordBatchReader,
} from "@query-farm/apache-arrow";
import { emptyBatch } from "./empty.js";

/**
 * Serialize a Schema to Arrow IPC bytes.
 */
export function serializeSchema(schema: Schema): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.close();
  return writer.toUint8Array(true);
}

/**
 * Deserialize an Arrow Schema from IPC bytes.
 * Note: In Bun, reader.schema is always undefined, so we must read from the batch.
 */
export function deserializeSchema(bytes: Uint8Array): Schema {
  const reader = RecordBatchReader.from(bytes);
  const batches = [...reader];
  if (batches.length > 0) {
    return batches[0].schema;
  }
  // Schema-only stream (no batches): try reader.schema as fallback,
  // otherwise parse the IPC messages manually
  if (reader.schema) return reader.schema;
  // For schema-only streams, create a dummy batch from the writer
  // and extract the schema from it
  throw new Error("Cannot deserialize schema from empty IPC stream");
}

/**
 * Serialize a RecordBatch to Arrow IPC bytes.
 */
export function serializeBatch(batch: RecordBatch): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, batch.schema);
  // Use _writeRecordBatch to bypass schema comparison bug
  // (public write() silently drops batches on nullability mismatch)
  (writer as any)._writeRecordBatch(batch);
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
    // In Bun, reader.schema is always undefined; fallback to empty schema
    const schema = reader.schema ?? new Schema([]);
    return emptyBatch(schema);
  }
  return batches[0];
}
