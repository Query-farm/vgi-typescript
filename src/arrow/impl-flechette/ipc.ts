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
} from "@query-farm/flechette";

import type { VgiSchema, VgiBatch } from "../types.js";
import { readFirstRecordBatchMeta } from "./message-meta.js";
import { aliasSchemaIntSigned } from "./compat.js";
import { toFlechetteType } from "./normalize-type.js";

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
 * Re-base an IPC stream onto an 8-byte-aligned ArrayBuffer offset.
 *
 * flechette decodes record-batch buffers zero-copy, as
 * `new ArrayType(bytes.buffer, bytes.byteOffset + region.offset, …)`. The Arrow
 * IPC stream format guarantees `region.offset` is 8-byte aligned *relative to
 * the message body*, so that view is only legal when the stream's own
 * `byteOffset` is 8-byte aligned too. `TypedArray` throws
 * `RangeError: Byte offset is not aligned` otherwise, and there is no way to
 * recover after the fact.
 *
 * Nothing upstream promises that alignment. VGI hands us IPC streams that are
 * views into a larger buffer at an arbitrary offset:
 *   * `splitLenPrefixed` subarrays past a 4-byte length prefix (odd by
 *     construction — buffering init/state payloads);
 *   * Arrow Binary column values, which are packed back-to-back in a shared
 *     data buffer with no per-value padding (nested IPC in state tokens,
 *     `init_request`, secret payloads);
 *   * HTTP body slices handed over by the runtime.
 *
 * arrow-js hides this by copying every misaligned buffer inside
 * `toArrayBufferView`, which is why the arrow-js backend never saw it. We copy
 * once here instead — the whole stream, so every body inside it keeps its
 * relative alignment — and only when the input is actually misaligned, so the
 * common already-aligned case stays zero-copy.
 */
function align8(bytes: Uint8Array): Uint8Array {
  if ((bytes.byteOffset & 7) === 0) return bytes;
  // `slice` copies into a fresh ArrayBuffer, whose byteOffset is 0.
  return bytes.slice();
}

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
  //
  // Types are normalized to flechette-native first. Callers routinely declare
  // schemas with arrow-js DataType instances (`new FixedSizeList(2, …)`), and
  // flechette's IPC writer reads the *flechette* property names off whatever
  // object it is handed — `stride`, not arrow-js's `listSize` — so a foreign
  // type serializes with its parameters silently zeroed. That is what emitted
  // `DOUBLE[0]` for a `DOUBLE[2]` argument and made DuckDB refuse the cast.
  const types = schema.fields.map((f) => toFlechetteType(f.type));
  const cols = types.map((t) => columnFromArray([], t as any));
  const fields = schema.fields.map((f, i) =>
    f_field(f.name, types[i], (f as any).nullable ?? true, (f as any).metadata ?? null),
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
  const table = tableFromIPC(align8(bytes), EXTRACT_OPTS);
  // flechette's Table.schema is the structural shape we need.
  return aliasSchemaIntSigned(table.schema) as unknown as VgiSchema;
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
  const t = batch as any;
  // Two things `tableToIPC` will not do on its own, and arrow-js's
  // `_writeRecordBatch` always does:
  //
  //  1. Emit a RecordBatch message when the table has no column data to walk.
  //     flechette derives its record batches from `columns[0].data`, so a
  //     zero-FIELD table (a legal vgi-rpc shape — `aggregate_update`'s ack,
  //     cancel signals, state-token carriers) encodes as schema + EOS with no
  //     batch at all. The C++ client reads that as "RPC returned an empty
  //     response" and fails the query.
  //  2. Pick up per-batch `custom_metadata`. `withBatchMetadata` pins the map
  //     on `_vgiRecordMetadata`, but only the `batchMetadata` encode option
  //     puts it on the wire — so cache/state metadata was being dropped on
  //     serialize even though it read back correctly in-process.
  //
  // Passing a one-entry positional `batchMetadata` covers both: it synthesises
  // the missing empty batch and attaches the map when there is one. Tables
  // that already have batches and no metadata are unaffected.
  const md: Map<string, string> | undefined = t._vgiRecordMetadata ?? t.metadata;
  const batchMetadata = [md && md.size > 0 ? md : undefined];
  return tableToIPC(t, { format: "stream", batchMetadata } as any) as Uint8Array;
}

/**
 * Deserialize a RecordBatch from Arrow IPC bytes.
 *
 * We treat the decoded Table as the "batch" — flechette Tables expose
 * numRows / schema / getChild that satisfy VgiBatch's shape. For
 * multi-batch streams this collapses to a single logical batch, matching
 * the subprocess transport's one-batch-per-call convention.
 */
export function deserializeBatch(input: Uint8Array): VgiBatch {
  const bytes = align8(input);
  const table: any = tableFromIPC(bytes, EXTRACT_OPTS);
  // Worker code reads `batch.schema.fields[i].type.isSigned` (arrow-js's
  // spelling) to pick promoted result types; flechette spells it `signed`.
  aliasSchemaIntSigned(table.schema);
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
