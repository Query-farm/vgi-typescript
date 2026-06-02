// Copyright 2025, 2026 Query Farm LLC - https://query.farm
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
  columnFromArray,
  field as f_field,
  Table,
} from "@uwdata/flechette";

import type { VgiSchema, VgiBatch } from "../types.js";
import { readFirstRecordBatchMeta } from "./message-meta.js";

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
  // Build the Table directly so we preserve per-field `nullable`/`metadata`.
  // tableFromColumns rebuilds fields with nullable=true, which round-trips
  // wrong on the C++ side (rejects schema-conforming payloads as schema
  // mismatches). See build.ts for the same fix on batches.
  const cols = schema.fields.map((f) => columnFromArray([], f.type as any));
  const fields = schema.fields.map((f, i) =>
    f_field(f.name, cols[i].type as any, (f as any).nullable ?? true, (f as any).metadata ?? null),
  );
  const flechSchema = {
    version: 5,
    endianness: 0,
    fields,
    metadata: (schema as any).metadata ?? null,
  };
  return tableToIPC(new Table(flechSchema as any, cols), { format: "stream" }) as Uint8Array;
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
  return tableToIPC(batch as any, { format: "stream" }) as Uint8Array;
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
  const table: any = tableFromIPC(bytes, EXTRACT_OPTS);
  // flechette's IPC parser ignores the Message-level `custom_metadata` field
  // and reports row counts only via column children, so a "metadata-only"
  // batch over a 0-field schema (legal in vgi-rpc — used to ferry state
  // tokens / cancel signals) round-trips as numRows=0/metadata=undefined.
  // Backfill both from the wire bytes so VgiBatch matches arrow-js's shape.
  const meta = readFirstRecordBatchMeta(bytes);
  if (meta === null) return table as VgiBatch;
  const wantRows = table.numRows === 0 && meta.numRows > 0;
  const wantMeta = !table.metadata && meta.metadata.size > 0;
  if (!wantRows && !wantMeta) return table as VgiBatch;
  // flechette Tables freeze their properties; wrap with a Proxy so reads of
  // numRows / metadata see the backfilled values while everything else
  // (schema, getChild, factory, …) flows through to the underlying Table.
  return new Proxy(table, {
    get(target, prop, receiver) {
      if (wantRows && prop === "numRows") return meta.numRows;
      if (wantMeta && prop === "metadata") return meta.metadata;
      return Reflect.get(target, prop, receiver);
    },
  }) as unknown as VgiBatch;
}
