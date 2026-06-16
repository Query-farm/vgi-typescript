// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// `accumulate` fixture catalog — a per-ATTACH, name-keyed row accumulator
// backed by attach-scoped persistent BoundStorage. Port of vgi-python's
// `accumulate` test fixture (vgi/_test_fixtures/accumulate/worker.py) and the
// Rust mirror (vgi-rust/vgi-example-worker/src/accumulate/mod.rs).
//
// Functions:
//  - accumulate(name, <rows>, ttl, max_row_size, result) — append rows to a
//    named collection, stamping one call-time `_timestamp`, and optionally
//    return its contents. A table-buffering (Sink -> Combine -> Source)
//    operator: the input is staged across the parallel sink, combine() runs
//    once to stamp + persist + evict + stage the result, and the source streams
//    it back one batch per tick.
//  - accumulate_read(name) — read a collection's rows without modifying it.
//  - accumulate_clear(name) — drop a collection; returns rows removed.
//
// Collections persist across queries: the persistent store is scoped to the
// random per-ATTACH `attach_opaque_data` minted by ReadOnlyCatalogInterface,
// so a collection survives the fresh worker a subprocess/HTTP-transport query
// spawns, and two ATTACH sessions never share a collection under the same name.
//
// TS storage note: the SDK's BoundStorage exposes only exact-key K/V
// (stateGet/statePut) plus an append-only log (stateAppend/stateLogScan) — it
// has neither the ranged K/V scans/deletes nor the atomic counters the Python
// fixture's per-segment store relies on. So this port stores a whole collection
// as ONE persistent K/V value: a length-prefixed list of per-call *segments*,
// each carrying its call-time micros header. That preserves every observable
// semantic the fixtures exercise (persistence, one _timestamp per call, name
// independence, schema pinning, ttl / max_row_size eviction, result modes,
// clear) without needing primitives the TS storage layer lacks.

import {
  Schema,
  Field,
  Int64,
  Utf8,
  Timestamp,
  TimeUnit,
  Interval,
  IntervalUnit,
} from "@query-farm/apache-arrow";
import {
  defineTableFunction,
  defineTableBufferingFunction,
  batchFromColumns,
  serializeBatch,
  deserializeBatch,
  serializeSchema,
  deserializeSchema,
  BoundStorage,
  functionStorage,
  type CatalogDescriptor,
  type CatalogInfo,
  type CatalogAttachResult,
  type VgiSchema,
  type VgiBatch,
  type VgiFunction,
  type TableBufferingBindParams,
  type TableBufferingParams,
  type TableProcessParams,
  type TableBindParams,
} from "../src/index.js";
import { ReadOnlyCatalogInterface } from "../src/catalog/read-only.js";
import type { FunctionRegistry } from "../src/functions/registry.js";
import type { OutputCollector } from "@query-farm/vgi-rpc";

const TE = new TextEncoder();
const ns = (s: string) => TE.encode(s);

export const ACCUMULATE_CATALOG_NAME = "accumulate";
const DATA_VERSION = "2.0.0";
const IMPLEMENTATION_VERSION = "vgi-fixture";

// Column appended to every output row holding the per-call ingest time. A
// tz-naive microsecond timestamp surfaces as DuckDB TIMESTAMP (not TIMESTAMP
// WITH TIME ZONE). Underscore-prefixed to avoid colliding with a user column.
const TIMESTAMP_COLUMN = "_timestamp";
const TS_TYPE = () => new Timestamp(TimeUnit.MICROSECOND);
const MAX_NAME_BYTES = 255;
const OUT_BATCH_ROWS = 65536;

// Execution-scoped (per-query) namespaces for the buffering Sink->Combine->
// Source handoff and for accumulate_read's snapshot.
const NS_IN = ns("acc_in");
const NS_OUT = ns("acc_out");
const NS_READ = ns("acc_read");

// Persistent (attach-scoped) namespaces.
const NS_META = ns("acc_meta"); // key=name -> pinned output schema IPC
const NS_DATA = ns("acc_data"); // key=name -> length-prefixed segment list

// ---------------------------------------------------------------------------
// Schema / time helpers
// ---------------------------------------------------------------------------

function nowMicros(): bigint {
  return BigInt(Date.now()) * 1000n;
}

function outputSchemaOf(input: VgiSchema): Schema {
  const fields = [...input.fields.map((f) => f as Field)];
  fields.push(new Field(TIMESTAMP_COLUMN, TS_TYPE(), false));
  return new Schema(fields);
}

function inputSchemaOf(output: VgiSchema): Schema {
  return new Schema(
    output.fields.filter((f) => f.name !== TIMESTAMP_COLUMN).map((f) => f as Field),
  );
}

/** Names + types match (metadata ignored). */
function inputFieldsMatch(pinned: VgiSchema, incoming: VgiSchema): boolean {
  if (pinned.fields.length !== incoming.fields.length) return false;
  for (let i = 0; i < pinned.fields.length; i++) {
    const a = pinned.fields[i];
    const b = incoming.fields[i];
    if (a.name !== b.name) return false;
    if (String(a.type) !== String(b.type)) return false;
  }
  return true;
}

function validateName(name: string): void {
  if (!name || !name.trim()) {
    throw new Error("collection name must be a non-empty string");
  }
  if (TE.encode(name).length > MAX_NAME_BYTES) {
    throw new Error(`collection name must be at most ${MAX_NAME_BYTES} bytes`);
  }
}

/**
 * Read a named INTERVAL (MonthDayNano) argument as microseconds; months are
 * treated as 30 days. Returns null when the argument is absent/null.
 *
 * DuckDB sends a MonthDayNano interval; arrow-js surfaces it as an Int32Array
 * `[months, days, nanoLow, nanoHigh]` (the i64 nanoseconds split into two
 * signed-low/high int32s), which the arg-extraction stringifies to
 * "months,days,nanoLow,nanoHigh". We also accept the array/typed-array form and
 * a plain `{months, days, nanoseconds}` object for robustness across backends.
 */
function intervalToMicros(iv: any): bigint | null {
  if (iv == null) return null;
  let months = 0n;
  let days = 0n;
  let nanos = 0n;
  const fromQuad = (q: number[]): void => {
    months = BigInt(q[0] ?? 0);
    days = BigInt(q[1] ?? 0);
    // i64 nanoseconds = high * 2^32 + (low as unsigned 32-bit).
    const low = BigInt((q[2] ?? 0) >>> 0);
    const high = BigInt(q[3] ?? 0);
    nanos = high * 4_294_967_296n + low;
  };
  if (typeof iv === "string") {
    fromQuad(iv.split(",").map((s) => Number(s)));
  } else if (Array.isArray(iv) || ArrayBuffer.isView(iv)) {
    fromQuad(Array.from(iv as any, (x: any) => Number(x ?? 0)));
  } else if (typeof iv === "object") {
    months = BigInt(iv.months ?? 0);
    days = BigInt(iv.days ?? 0);
    nanos = BigInt(iv.nanoseconds ?? iv.nanos ?? 0);
  } else if (typeof iv === "bigint" || typeof iv === "number") {
    return BigInt(iv); // already microseconds
  }
  return (months * 30n + days) * 86_400_000_000n + nanos / 1000n;
}

// ---------------------------------------------------------------------------
// Length-prefixed segment list: each segment = [ts:i64 LE][len:u32 LE][bytes].
// One segment per call (carrying its call-time micros), so a TTL cutoff and a
// row cap both reduce to operations over whole / boundary segments.
// ---------------------------------------------------------------------------

interface Segment {
  ts: bigint; // call-time micros
  bytes: Uint8Array; // serialized stamped batch
}

function encodeSegments(segs: Segment[]): Uint8Array {
  let total = 0;
  for (const s of segs) total += 12 + s.bytes.length;
  const out = new Uint8Array(total);
  const dv = new DataView(out.buffer);
  let off = 0;
  for (const s of segs) {
    dv.setBigInt64(off, s.ts, true);
    off += 8;
    dv.setUint32(off, s.bytes.length, true);
    off += 4;
    out.set(s.bytes, off);
    off += s.bytes.length;
  }
  return out;
}

function decodeSegments(buf: Uint8Array | null): Segment[] {
  if (!buf || buf.length === 0) return [];
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const segs: Segment[] = [];
  let off = 0;
  while (off + 12 <= buf.length) {
    const ts = dv.getBigInt64(off, true);
    off += 8;
    const len = dv.getUint32(off, true);
    off += 4;
    segs.push({ ts, bytes: buf.subarray(off, off + len) });
    off += len;
  }
  return segs;
}

function segRows(seg: Segment): number {
  return deserializeBatch(seg.bytes).numRows;
}

function totalRows(segs: Segment[]): number {
  let n = 0;
  for (const s of segs) n += segRows(s);
  return n;
}

/** A persistent store scoped to one ATTACH session (survives across queries). */
class Store {
  private ps: BoundStorage;
  constructor(attachId: Uint8Array) {
    // Scope by the per-ATTACH opaque data; fall back to a shared default scope
    // when none was carried (mirrors the Python fixture's `b"default"`).
    const scope = attachId && attachId.length > 0 ? attachId : ns("default");
    this.ps = new BoundStorage(functionStorage, scope);
  }

  async getSchema(name: string): Promise<VgiSchema | null> {
    const blob = await this.ps.stateGet(NS_META, ns(name));
    // A 0-length blob is the tombstone clear() writes (the TS storage layer has
    // no delete), so treat it as absent — the name is free to re-accumulate.
    return blob && blob.length > 0 ? deserializeSchema(blob) : null;
  }

  async putSchema(name: string, out: VgiSchema): Promise<void> {
    await this.ps.statePut(NS_META, ns(name), serializeSchema(out));
  }

  async getSegments(name: string): Promise<Segment[]> {
    return decodeSegments(await this.ps.stateGet(NS_DATA, ns(name)));
  }

  async putSegments(name: string, segs: Segment[]): Promise<void> {
    await this.ps.statePut(NS_DATA, ns(name), encodeSegments(segs));
  }

  async clear(name: string): Promise<number> {
    const segs = await this.getSegments(name);
    const total = totalRows(segs);
    await this.ps.statePut(NS_DATA, ns(name), new Uint8Array(0));
    await this.ps.statePut(NS_META, ns(name), new Uint8Array(0));
    return total;
  }
}

// ---------------------------------------------------------------------------
// Batch materialization helpers
// ---------------------------------------------------------------------------

/** Materialize a column's values into a JS array. */
function colValues(batch: VgiBatch, name: string): any[] {
  const col = batch.getChild(name);
  const out: any[] = [];
  for (let i = 0; i < batch.numRows; i++) out.push(col ? col.get(i) : null);
  return out;
}

/**
 * Merge input batches column-wise and append a single-valued `_timestamp`
 * column, producing one stamped batch for this call.
 */
function stamp(inputBatches: VgiBatch[], outputSchema: VgiSchema, ts: bigint): VgiBatch {
  const inputFields = outputSchema.fields.filter((f) => f.name !== TIMESTAMP_COLUMN);
  const columns: Record<string, any[]> = {};
  for (const f of inputFields) columns[f.name] = [];
  let n = 0;
  for (const b of inputBatches) {
    for (const f of inputFields) {
      const col = b.getChild(f.name);
      for (let i = 0; i < b.numRows; i++) columns[f.name].push(col ? col.get(i) : null);
    }
    n += b.numRows;
  }
  columns[TIMESTAMP_COLUMN] = new Array(n).fill(ts);
  return batchFromColumns(columns, outputSchema);
}

/**
 * Rebuild a stamped segment keeping rows [offset, end). The `_timestamp` column
 * is reconstructed from the segment's call-time micros (one ts per segment), so
 * we never depend on the timestamp `.get()` representation, which differs
 * across Arrow backends (arrow-js: ms number; flechette: micros bigint).
 */
function sliceSegment(seg: Segment, outputSchema: VgiSchema, offset: number, end: number): Segment {
  const batch = deserializeBatch(seg.bytes);
  const columns: Record<string, any[]> = {};
  for (const f of outputSchema.fields) {
    if (f.name === TIMESTAMP_COLUMN) continue;
    const col = batch.getChild(f.name);
    const arr: any[] = [];
    for (let i = offset; i < end; i++) arr.push(col ? col.get(i) : null);
    columns[f.name] = arr;
  }
  columns[TIMESTAMP_COLUMN] = new Array(end - offset).fill(seg.ts);
  return { ts: seg.ts, bytes: serializeBatch(batchFromColumns(columns, outputSchema)) };
}

// ---------------------------------------------------------------------------
// Eviction
// ---------------------------------------------------------------------------

/** Drop whole segments whose call time is < cutoff. */
function evictTtl(segs: Segment[], cutoff: bigint): Segment[] {
  if (cutoff <= 0n) return segs;
  return segs.filter((s) => s.ts >= cutoff);
}

/** Keep only the newest `max` rows (drop oldest segments, trim one boundary). */
function evictMaxRows(segs: Segment[], outputSchema: VgiSchema, max: number): Segment[] {
  const total = totalRows(segs);
  if (total <= max) return segs;
  let overflow = total - max;
  const kept: Segment[] = [];
  let dropping = true;
  for (const seg of segs) {
    if (!dropping) {
      kept.push(seg);
      continue;
    }
    const n = segRows(seg);
    if (overflow >= n) {
      overflow -= n;
      if (overflow === 0) dropping = false;
      // whole segment dropped
    } else {
      // boundary segment: keep its newest rows
      kept.push(sliceSegment(seg, outputSchema, overflow, n));
      overflow = 0;
      dropping = false;
    }
  }
  return kept;
}

// ---------------------------------------------------------------------------
// Output staging / draining (execution-scoped log)
// ---------------------------------------------------------------------------

/** Stage a batch into an execution-scoped log in <= OUT_BATCH_ROWS slices. */
async function stageBatch(storage: BoundStorage, nsKey: Uint8Array, batch: VgiBatch): Promise<void> {
  const total = batch.numRows;
  if (total === 0) {
    await storage.stateAppend(nsKey, ns(""), serializeBatch(batch));
    return;
  }
  for (let off = 0; off < total; off += OUT_BATCH_ROWS) {
    const len = Math.min(OUT_BATCH_ROWS, total - off);
    const slice = (batch as any).slice(off, len);
    await storage.stateAppend(nsKey, ns(""), serializeBatch(slice));
  }
}

async function stageSegments(storage: BoundStorage, nsKey: Uint8Array, segs: Segment[]): Promise<void> {
  for (const seg of segs) {
    await storage.stateAppend(nsKey, ns(""), seg.bytes);
  }
}

// ---------------------------------------------------------------------------
// accumulate(name, <rows>, ttl, max_row_size, result)
// ---------------------------------------------------------------------------

interface AccumulateArgs {
  name: string;
  ttl: any;
  max_row_size: number;
  result: string;
}
interface DrainState {
  afterId: number;
}

const accumulate = defineTableBufferingFunction<AccumulateArgs, DrainState>({
  name: "accumulate",
  description: "Append rows to a named collection; return all/new/no rows with a _timestamp column",
  args: { name: new Utf8() },
  namedArgs: {
    ttl: new Interval(IntervalUnit.MONTH_DAY_NANO),
    max_row_size: new Int64(),
    result: new Utf8(),
  },
  argDefaults: { ttl: null, max_row_size: 0, result: "all" },
  categories: ["stateful", "utility"],
  examples: [
    {
      sql: "SELECT * FROM accumulate('events', (VALUES (1), (2)) t(x))",
      description: "Accumulate two rows under 'events' and return the full collection",
    },
  ],
  onBind: async (params: TableBufferingBindParams<AccumulateArgs>) => {
    validateName(params.args.name);
    const input = params.bindCall.input_schema;
    if (!input) throw new Error("accumulate requires a table input");
    if (input.fields.some((f) => f.name === TIMESTAMP_COLUMN)) {
      throw new Error(
        `input may not contain a reserved '${TIMESTAMP_COLUMN}' column; ` +
          "accumulate adds this column to its output",
      );
    }
    const out = outputSchemaOf(input);
    const store = new Store(params.bindCall.attach_opaque_data ?? new Uint8Array(0));
    const existing = await store.getSchema(params.args.name);
    if (existing == null) {
      await store.putSchema(params.args.name, out);
    } else if (!inputFieldsMatch(inputSchemaOf(existing), input)) {
      throw new Error(
        `input schema for accumulate('${params.args.name}', ...) does not match the ` +
          "schema already accumulated under that name",
      );
    }
    return { outputSchema: out };
  },
  // Sink: stage each input batch into the execution-scoped log.
  process: async (batch, params) => {
    await params.storage.stateAppend(NS_IN, ns(""), serializeBatch(batch));
    return params.executionId;
  },
  // Combine: append, evict, stage the requested result.
  combine: async (_stateIds, params) => {
    const name = params.args.name;
    const store = new Store(params.attachId);
    const outputSchema = params.outputSchema;

    const staged = await params.storage.stateLogScan(NS_IN, ns(""), -1, null);
    const inputBatches = staged.map(([, v]) => deserializeBatch(v));

    const ts = nowMicros();
    const newSeg: Segment = {
      ts,
      bytes: serializeBatch(stamp(inputBatches, outputSchema, ts)),
    };
    const newRows = deserializeBatch(newSeg.bytes).numRows;

    let segs = await store.getSegments(name);
    if (newRows > 0) segs.push(newSeg);

    const micros = intervalToMicros(params.args.ttl);
    if (micros != null) segs = evictTtl(segs, ts - micros);

    const maxRows = Number(params.args.max_row_size ?? 0);
    if (maxRows > 0) segs = evictMaxRows(segs, outputSchema, maxRows);

    await store.putSegments(name, segs);

    const mode = params.args.result ?? "all";
    if (mode === "none") {
      // nothing
    } else if (mode === "new") {
      if (newRows > 0) await stageSegments(params.storage, NS_OUT, [newSeg]);
    } else {
      await stageSegments(params.storage, NS_OUT, segs);
    }
    return [params.executionId];
  },
  initialFinalizeState: () => ({ afterId: -1 }),
  // Source: drain the staged result, one batch per tick.
  finalize: async (params, _fid, state, out) => {
    const rows = await params.storage.stateLogScan(NS_OUT, ns(""), state.afterId, 1);
    if (rows.length === 0) {
      out.finish();
      return;
    }
    const [logId, value] = rows[0];
    out.emit(deserializeBatch(value));
    state.afterId = logId;
  },
});

// ---------------------------------------------------------------------------
// accumulate_read(name)
// ---------------------------------------------------------------------------

interface ReadArgs {
  name: string;
}
interface ReadState {
  staged: boolean;
  afterId: number;
}

const accumulate_read = defineTableFunction<ReadArgs, ReadState>({
  name: "accumulate_read",
  description: "Read an accumulated collection's rows without modifying it",
  args: { name: new Utf8() },
  categories: ["stateful", "utility"],
  examples: [
    { sql: "SELECT * FROM accumulate_read('events')", description: "Return all rows accumulated under 'events'" },
  ],
  onBind: async (params: TableBindParams<ReadArgs>) => {
    validateName(params.args.name);
    const store = new Store(params.bindCall.attach_opaque_data ?? new Uint8Array(0));
    const schema = await store.getSchema(params.args.name);
    if (schema == null) {
      throw new Error(`no accumulation named '${params.args.name}' in this session`);
    }
    return { outputSchema: schema };
  },
  initialState: () => ({ staged: false, afterId: -1 }),
  process: async (
    params: TableProcessParams<ReadArgs>,
    state: ReadState,
    out: OutputCollector,
  ) => {
    if (!state.staged) {
      const store = new Store(params.initCall.bind_call.attach_opaque_data ?? new Uint8Array(0));
      const segs = await store.getSegments(params.args.name);
      await stageSegments(params.storage!, NS_READ, segs);
      state.staged = true;
    }
    const rows = await params.storage!.stateLogScan(NS_READ, ns(""), state.afterId, 1);
    if (rows.length === 0) {
      out.finish();
      return;
    }
    const [logId, value] = rows[0];
    out.emit(deserializeBatch(value));
    state.afterId = logId;
  },
});

// ---------------------------------------------------------------------------
// accumulate_clear(name)
// ---------------------------------------------------------------------------

const CLEAR_SCHEMA = new Schema([
  new Field("name", new Utf8(), false),
  new Field("rows_cleared", new Int64(), false),
]);

interface ClearArgs {
  name: string;
}
interface ClearState {
  done: boolean;
}

const accumulate_clear = defineTableFunction<ClearArgs, ClearState>({
  name: "accumulate_clear",
  description: "Remove an accumulated collection by name; returns rows cleared",
  args: { name: new Utf8() },
  categories: ["stateful", "utility"],
  examples: [
    { sql: "SELECT * FROM accumulate_clear('events')", description: "Clear the 'events' collection, returning how many rows were removed" },
  ],
  onBind: (params: TableBindParams<ClearArgs>) => {
    validateName(params.args.name);
    return { outputSchema: CLEAR_SCHEMA };
  },
  initialState: () => ({ done: false }),
  process: async (
    params: TableProcessParams<ClearArgs>,
    state: ClearState,
    out: OutputCollector,
  ) => {
    if (state.done) {
      out.finish();
      return;
    }
    const store = new Store(params.initCall.bind_call.attach_opaque_data ?? new Uint8Array(0));
    const rowsCleared = await store.clear(params.args.name);
    out.emit(
      batchFromColumns(
        { name: [params.args.name], rows_cleared: [BigInt(rowsCleared)] },
        CLEAR_SCHEMA,
      ),
    );
    state.done = true;
  },
});

// ---------------------------------------------------------------------------
// Catalog & registration
// ---------------------------------------------------------------------------

export const accumulateFunctions: VgiFunction[] = [
  accumulate,
  accumulate_read,
  accumulate_clear,
];

export const accumulateCatalog: CatalogDescriptor = {
  name: ACCUMULATE_CATALOG_NAME,
  defaultSchema: "main",
  comment: "Row accumulation keyed by name, persisted via FunctionStorage and scoped per ATTACH",
  schemas: [
    {
      name: "main",
      comment: "Stateful row accumulation functions",
      functions: accumulateFunctions,
    },
  ],
};

/**
 * Catalog interface that advertises the accumulate catalog's stable data
 * version (so `vgi_catalogs()` surfaces `2.0.0`) and resolves it on attach.
 * Per-ATTACH scoping comes for free: ReadOnlyCatalogInterface.attach() mints a
 * random `attach_opaque_data` carried back on every call.
 */
export class AccumulateCatalog extends ReadOnlyCatalogInterface {
  override catalogsInfo(): CatalogInfo[] {
    return [
      {
        name: ACCUMULATE_CATALOG_NAME,
        implementation_version: IMPLEMENTATION_VERSION,
        data_version_spec: DATA_VERSION,
        attach_option_specs: [],
        releases: [],
      } as CatalogInfo,
    ];
  }

  override async attach(
    name: string,
    options?: Record<string, unknown>,
    dataVersionSpec?: string | null,
    implementationVersion?: string | null,
  ): Promise<CatalogAttachResult> {
    const result = await super.attach(name, options, dataVersionSpec, implementationVersion);
    return {
      ...result,
      resolved_data_version: DATA_VERSION,
      resolved_implementation_version: IMPLEMENTATION_VERSION,
    };
  }
}

export function createAccumulateCatalog(registry: FunctionRegistry): AccumulateCatalog {
  return new AccumulateCatalog(accumulateCatalog, registry);
}
