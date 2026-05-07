// Arrow IPC stream serialization/deserialization for the flechette backend.
//
// flechette's `tableFromIPC` accepts either Uint8Array or Uint8Array[] (the
// latter for stream-format messages produced incrementally), and
// `tableToIPC(table, { format: "stream" })` emits a self-contained stream.
// The same Schema/RecordBatch shapes are produced and consumed here as in
// impl-arrowjs (structurally compatible with VgiSchema / VgiBatch).

import {
  tableFromIPC,
  tableToIPC,
  tableFromColumns,
  columnFromArray,
  utf8 as f_utf8,
} from "@uwdata/flechette";

import type { VgiSchema, VgiBatch } from "../types.js";

// flechette decoding options that match how vgi-typescript callers expect
// values to come out: 64-bit ints stay as BigInt, decimals as scaled BigInts,
// timestamps as BigInt nanoseconds, Maps as [k,v] arrays.
const EXTRACT_OPTS = {
  useBigInt: true,
  useBigIntTimestamp: true,
  useDecimalInt: true,
  useMap: false,
} as const;

/**
 * Serialize a Schema to Arrow IPC bytes.
 *
 * flechette has no schema-only serialization helper, so we encode an empty
 * 0-row table that carries just the schema header. Decoders see the Schema
 * + an immediate EOS marker — same wire shape arrow-js's
 * `RecordBatchStreamWriter.close()` produces for an empty stream.
 */
export function serializeSchema(schema: VgiSchema): Uint8Array {
  // Build a 0-row table by creating a column for each field with an empty
  // typed array of the appropriate type, then encode as a stream.
  const cols: Record<string, ReturnType<typeof columnFromArray>> = {};
  for (const f of schema.fields) {
    cols[f.name] = columnFromArray([], f.type as any);
  }
  // Empty schema: still need at least one column to satisfy flechette, fall
  // back to a single nullable utf8 dummy that we strip on decode. (Schemas
  // with zero fields are extremely rare; handlers checking for them already
  // guard via fields.length.)
  if (schema.fields.length === 0) {
    return tableToIPC(tableFromColumns({ __placeholder: columnFromArray([], f_utf8()) }), {
      format: "stream",
    }) as Uint8Array;
  }
  const table = tableFromColumns(cols);
  return tableToIPC(table, { format: "stream" }) as Uint8Array;
}

/**
 * Deserialize an Arrow Schema from IPC bytes. Returns the schema of the
 * decoded table (which may be 0-row if the source was a schema-only stream).
 */
export function deserializeSchema(bytes: Uint8Array): VgiSchema {
  const table = tableFromIPC(bytes, EXTRACT_OPTS);
  // flechette's Table.schema is the structural shape we need.
  return table.schema as unknown as VgiSchema;
}

/**
 * Serialize a RecordBatch to Arrow IPC bytes (stream format).
 *
 * The batch passed in is structurally a flechette Table (1 record batch's
 * worth of data). flechette's `tableToIPC` round-trips it as a complete
 * stream — schema + one batch + EOS — which is what subprocess transport
 * consumers expect per call.
 */
export function serializeBatch(batch: VgiBatch): Uint8Array {
  // VgiBatch is structurally a flechette Table at this point.
  const table = batch as any;
  return tableToIPC(table, { format: "stream" }) as Uint8Array;
}

/**
 * Deserialize a RecordBatch from Arrow IPC bytes.
 *
 * We treat the decoded Table as the "batch" — flechette Tables expose
 * numRows / schema / getChild that satisfy VgiBatch's shape. For
 * multi-batch streams this collapses to a single logical batch, matching
 * the subprocess transport's one-batch-per-call convention.
 */
export function deserializeBatch(bytes: Uint8Array): VgiBatch {
  const table = tableFromIPC(bytes, EXTRACT_OPTS);
  return table as unknown as VgiBatch;
}
