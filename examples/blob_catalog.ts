// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// The `blob_test` catalog: functions sized by their arguments, for the
// blob-vendor externalization tests (vgi test/sql/external_storage/).
//
// WARNING: EXAMPLE/TEST FUNCTIONS ONLY. The SDK-neutral contract is the vgi
// extension's docs/testing/blob-test-catalog.md; vgi-python's
// vgi/_test_fixtures/blob_catalog.py is the reference implementation. Over
// HTTP with VGI_BLOB_VENDOR_URL set:
//   * results over VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES come back as vendor
//     pointers, and requests over VGI_RPC_MAX_REQUEST_BYTES go up through
//     vendor upload URLs;
//   * storage values over VGI_STORAGE_EXTERNALIZE_BYTES go to the vendor
//     (VGI_STORAGE_EXTERNALIZE=1 over SQLite, or the cloudflare-do backend).
//
//   blob_repeat(s, n)       scalar           large output: s repeated n times
//   blob_len(s)             scalar           large input: strlen(s) in UTF-8 bytes
//   blob_rows(rows, width)  table            large output: rows x width chars
//   blob_param_echo(p)      table            large bind parameter: length + sha256
//   blob_buffer_echo(t)     table-buffering  stores each input batch, replays it
//   blob_tio(t)             table-in-out     stores each batch in state, reads back
//   blob_store_values(t)    table-in-out     stores each value of column 0 exactly
//
// The storage functions compute their outputs from what storage returns, not
// from their input, so the round trip is what the tests check. A separate
// catalog, so the example catalog's function counts do not change.

import { createHash } from "node:crypto";
import { Int64, Utf8 } from "@query-farm/apache-arrow";
import type { OutputCollector } from "@query-farm/vgi-rpc";
import {
  batchFromColumns,
  defineScalarFunction,
  defineTableBufferingFunction,
  defineTableFunction,
  defineTableInOutFunction,
  deserializeBatch,
  field,
  int64,
  iterRows,
  schema,
  serializeBatch,
  utf8,
  type CatalogDescriptor,
  type TableBufferingParams,
  type VgiBatch,
  type VgiFunction,
  type VgiSchema,
} from "../src/index.js";

export const BLOB_CATALOG_NAME = "blob_test";
const ROWS_PER_BATCH = 2048;
const TE = new TextEncoder();
const ns = (s: string) => TE.encode(s);

/** A value as text, the way DuckDB casts it for strlen()/sha256(). */
function asText(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  return typeof value === "string" ? value : String(value);
}

function checksum(raw: Uint8Array | null): { len: bigint | null; sha: string | null } {
  if (raw === null) return { len: null, sha: null };
  return { len: BigInt(raw.byteLength), sha: createHash("sha256").update(raw).digest("hex") };
}

/** Input columns, then `payload_len` and `payload_sha256`. */
function checksumSchema(fields: VgiSchema["fields"]): VgiSchema {
  return schema([...fields, field("payload_len", int64()), field("payload_sha256", utf8())]);
}

/** `batch` with the checksum columns of its first column (as text) appended. */
function withChecksums(batch: VgiBatch, outputSchema: VgiSchema): VgiBatch {
  const columns: Record<string, unknown[]> = {};
  for (const f of batch.schema.fields) columns[f.name] = [];
  const lens: Array<bigint | null> = [];
  const shas: Array<string | null> = [];
  const first = batch.schema.fields[0]?.name;
  for (const row of iterRows(batch)) {
    for (const f of batch.schema.fields) columns[f.name]!.push(row[f.name]);
    const text = first === undefined ? null : asText(row[first]);
    const { len, sha } = checksum(text === null ? null : TE.encode(text));
    lens.push(len);
    shas.push(sha);
  }
  columns.payload_len = lens;
  columns.payload_sha256 = shas;
  return batchFromColumns(columns as Record<string, any[]>, outputSchema);
}

function requireInputSchema(params: { bindCall: { input_schema?: VgiSchema | null } }): VgiSchema {
  const input = params.bindCall.input_schema;
  if (!input) throw new Error("input_schema is required");
  return input;
}

function freshKey(): Uint8Array {
  return crypto.getRandomValues(new Uint8Array(16));
}

// ============================================================================
// Scalars
// ============================================================================

const blob_repeat = defineScalarFunction({
  name: "blob_repeat",
  description: "Repeat a string n times (externalization test: large scalar output)",
  params: { s: new Utf8(), n: new Int64() },
  argDocs: { s: "String to repeat", n: "Number of copies" },
  returns: new Utf8(),
  compute: (batch: VgiBatch) => {
    const s = batch.getChildAt(0)!;
    const n = batch.getChildAt(1)!;
    const out: Array<string | null> = [];
    for (let i = 0; i < batch.numRows; i++) {
      const a = s.get(i) as string | null;
      const b = n.get(i) as bigint | number | null;
      out.push(a == null || b == null ? null : a.repeat(Math.max(Number(b), 0)));
    }
    return out;
  },
  examples: [{ sql: "SELECT blob_repeat('ab', 3)", description: "Returns 'ababab'" }],
});

const blob_len = defineScalarFunction({
  name: "blob_len",
  description: "UTF-8 byte length of a string (externalization test: large scalar input)",
  params: { s: new Utf8() },
  argDocs: { s: "String to measure" },
  returns: new Int64(),
  compute: (batch: VgiBatch) => {
    const s = batch.getChildAt(0)!;
    const out: Array<bigint | null> = [];
    for (let i = 0; i < batch.numRows; i++) {
      const v = s.get(i) as string | null;
      out.push(v == null ? null : BigInt(TE.encode(v).byteLength));
    }
    return out;
  },
  examples: [{ sql: "SELECT blob_len('abc')", description: "Returns 3" }],
});

// ============================================================================
// Table functions
// ============================================================================

const ROWS_SCHEMA = schema([field("i", int64()), field("payload", utf8())]);

interface BlobRowsArgs {
  rows: number;
  width: number;
}

const blob_rows = defineTableFunction<BlobRowsArgs, { next: number }>({
  name: "blob_rows",
  description: "Rows of a given width (externalization test: large table output)",
  args: { rows: new Int64(), width: new Int64() },
  argDocs: { rows: "Number of rows", width: "Characters per row" },
  argConstraints: { rows: { ge: 0 }, width: { ge: 0 } },
  onBind: () => ({ outputSchema: ROWS_SCHEMA }),
  cardinality: (params) => ({ estimate: Number(params.args.rows), max: Number(params.args.rows) }),
  initialState: () => ({ next: 0 }),
  process: (params, state, out) => {
    const total = Number(params.args.rows);
    const width = Number(params.args.width);
    if (state.next >= total) {
      out.finish();
      return;
    }
    const stop = Math.min(state.next + ROWS_PER_BATCH, total);
    const ids: bigint[] = [];
    const payloads: string[] = [];
    for (let i = state.next; i < stop; i++) {
      ids.push(BigInt(i));
      payloads.push(String.fromCharCode(97 + (i % 26)).repeat(width));
    }
    out.emit(batchFromColumns({ i: ids, payload: payloads }, params.outputSchema));
    state.next = stop;
  },
  examples: [{ sql: "SELECT * FROM blob_rows(3, 4)", description: "aaaa, bbbb, cccc" }],
});

const PARAM_ECHO_SCHEMA = schema([field("payload_len", int64()), field("payload_sha256", utf8())]);

const blob_param_echo = defineTableFunction<{ payload: string }, { emitted: boolean }>({
  name: "blob_param_echo",
  description: "Length and SHA-256 of a parameter (externalization test: large bind parameter)",
  args: { payload: new Utf8() },
  argDocs: { payload: "Parameter to measure" },
  onBind: () => ({ outputSchema: PARAM_ECHO_SCHEMA }),
  cardinality: () => ({ estimate: 1, max: 1 }),
  initialState: () => ({ emitted: false }),
  process: (params, state, out) => {
    if (state.emitted) {
      out.finish();
      return;
    }
    const { len, sha } = checksum(TE.encode(params.args.payload));
    out.emit(batchFromColumns({ payload_len: [len], payload_sha256: [sha] }, params.outputSchema));
    state.emitted = true;
  },
  examples: [{ sql: "SELECT * FROM blob_param_echo('abc')", description: "3, sha256('abc')" }],
});

// ============================================================================
// Table-buffering and table-in-out
// ============================================================================

const BUFFER_NS = ns("blob_buffer");

const blob_buffer_echo = defineTableBufferingFunction<Record<string, any>, { afterId: number }>({
  name: "blob_buffer_echo",
  description: "Buffer input batches in storage and replay them (externalization test: storage values)",
  onBind: (params) => ({ outputSchema: checksumSchema(requireInputSchema(params).fields) }),
  // One storage value per input batch, appended to the execution's log.
  process: async (batch, params) => {
    await params.storage.stateAppend(BUFFER_NS, ns(""), serializeBatch(batch));
    return params.executionId;
  },
  combine: async (_stateIds, params) => [params.executionId],
  initialFinalizeState: () => ({ afterId: -1 }),
  // One stored batch per tick, rebuilt from what storage returned.
  finalize: async (params: TableBufferingParams, _fid: Uint8Array, state: { afterId: number }, out: OutputCollector) => {
    const rows = await params.storage.stateLogScan(BUFFER_NS, ns(""), state.afterId, 1);
    if (rows.length === 0) {
      out.finish();
      return;
    }
    const [logId, value] = rows[0]!;
    out.emit(withChecksums(deserializeBatch(value), params.outputSchema));
    state.afterId = logId;
  },
  examples: [
    {
      sql: "SELECT * FROM blob_buffer_echo((SELECT repeat('x', 10) AS s))",
      description: "s, payload_len, payload_sha256",
    },
  ],
});

const TIO_NS = ns("blob_tio");

const blob_tio = defineTableInOutFunction({
  name: "blob_tio",
  description: "Round-trip each batch through storage (externalization test: table-in-out state)",
  onBind: (params) => ({ outputSchema: checksumSchema(requireInputSchema(params).fields) }),
  process: async (params, _state: null, batch: VgiBatch, out: OutputCollector) => {
    const key = freshKey();
    await params.storage.statePut(TIO_NS, key, serializeBatch(batch));
    const stored = await params.storage.stateGet(TIO_NS, key);
    if (stored === null) throw new Error("blob_tio: the batch just stored is missing");
    await params.storage.stateDelete(TIO_NS, [key]);
    out.emit(withChecksums(deserializeBatch(stored), params.outputSchema));
  },
  examples: [
    { sql: "SELECT * FROM blob_tio((SELECT repeat('x', 10) AS s))", description: "s, payload_len, payload_sha256" },
  ],
});

const VALUES_NS = ns("blob_values");

const blob_store_values = defineTableInOutFunction({
  name: "blob_store_values",
  description: "Store each value of the first column exactly and read it back (externalization test)",
  onBind: (params) => ({ outputSchema: checksumSchema(requireInputSchema(params).fields.slice(1)) }),
  process: async (params, _state: null, batch: VgiBatch, out: OutputCollector) => {
    const fields = batch.schema.fields;
    const rows = [...iterRows(batch)];
    const prefix = freshKey();
    const keys = rows.map((_, i) => {
      const k = new Uint8Array(prefix.length + 4);
      k.set(prefix);
      new DataView(k.buffer).setUint32(prefix.length, i, false);
      return k;
    });
    // One storage value per non-NULL value, of exactly its UTF-8 bytes.
    const items: Array<[Uint8Array, Uint8Array]> = [];
    const itemIndex: number[] = [];
    rows.forEach((row, i) => {
      const text = asText(row[fields[0]!.name]);
      if (text !== null) {
        items.push([keys[i]!, TE.encode(text)]);
        itemIndex.push(i);
      }
    });
    const back: Array<Uint8Array | null> = rows.map(() => null);
    if (items.length > 0) {
      await params.storage.statePutMany(VALUES_NS, items);
      const stored = await params.storage.stateGetMany(
        VALUES_NS,
        items.map(([k]) => k),
      );
      await params.storage.stateDelete(
        VALUES_NS,
        items.map(([k]) => k),
      );
      stored.forEach((got, j) => {
        if (got === null) throw new Error("blob_store_values: a value just stored is missing");
        back[itemIndex[j]!] = got;
      });
    }
    const columns: Record<string, any[]> = {};
    for (const f of fields.slice(1)) columns[f.name] = rows.map((r) => r[f.name]);
    const sums = back.map(checksum);
    columns.payload_len = sums.map((c) => c.len);
    columns.payload_sha256 = sums.map((c) => c.sha);
    out.emit(batchFromColumns(columns, params.outputSchema));
  },
  examples: [
    {
      sql: "SELECT * FROM blob_store_values((SELECT repeat('x', 10) AS s, 1 AS id))",
      description: "id, payload_len, payload_sha256",
    },
  ],
});

// ============================================================================
// Catalog
// ============================================================================

export const blobCatalogFunctions: VgiFunction[] = [
  blob_repeat,
  blob_len,
  blob_rows,
  blob_param_echo,
  blob_buffer_echo,
  blob_tio,
  blob_store_values,
];

export const blobCatalog: CatalogDescriptor = {
  name: BLOB_CATALOG_NAME,
  defaultSchema: "main",
  comment: "Functions sized by their arguments, for blob externalization tests",
  schemas: [
    {
      name: "main",
      comment: "Externalization test functions",
      functions: blobCatalogFunctions,
    },
  ],
};
