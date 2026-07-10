// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Result-cache fixtures — table generators that advertise `vgi.cache.*`.
// Ports vgi-python/vgi/_test_fixtures/table/cache.py.
//
// These exist so the SQL integration tests (and the C++ result cache) can
// exercise cacheable table-function results end to end. Each generator returns
// a small deterministic result and folds cache-control metadata onto its
// **first** emitted batch via `out.emit(batch, cacheControlMetadata({...}))`.

import { Schema, Field, Int64, Utf8, List, Struct, Decimal, Timestamp, TimeUnit } from "@query-farm/apache-arrow";
import {
  defineTableFunction,
  batchFromColumns,
  serializeBatch,
  cacheControlMetadata,
  CACHE_SCOPE_TRANSACTION,
  DEFAULT_MAX_WORKERS,
  OrderPreservation,
  type VgiFunction,
} from "../src/index.js";
import type { OutputCollector } from "@query-farm/vgi-rpc";

// Default freshness lifetime (seconds) for the fixtures that don't take a
// `ttl` argument. Long enough that TTL never lapses mid-test.
const DEFAULT_TTL_SECONDS = 300;

// Process-global monotonic counter. Incremented once per *real* invocation of
// `cache_nonce` (in initialState, which the client only reaches on a cache
// MISS). A pooled worker persists it across calls, so a served-from-cache hit
// never advances it — that's exactly the HIT/MISS signal tests assert on.
let nonceCounter = 0;
const nextNonce = (): number => nonceCounter++;

// Queue item encodings. Values stay well within 2^53 for the test sizes, so
// float64 slots are exact — matching packTriple/packOne in table_partition.ts.
function packPair(a: number, b: number): Uint8Array {
  const buf = new ArrayBuffer(16);
  const dv = new DataView(buf);
  dv.setFloat64(0, a);
  dv.setFloat64(8, b);
  return new Uint8Array(buf);
}
function unpackPair(bytes: Uint8Array): [number, number] {
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return [dv.getFloat64(0), dv.getFloat64(8)];
}
function packTriple(a: number, b: number, c: number): Uint8Array {
  const buf = new ArrayBuffer(24);
  const dv = new DataView(buf);
  dv.setFloat64(0, a);
  dv.setFloat64(8, b);
  dv.setFloat64(16, c);
  return new Uint8Array(buf);
}
function unpackTriple(bytes: Uint8Array): [number, number, number] {
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  return [dv.getFloat64(0), dv.getFloat64(8), dv.getFloat64(16)];
}

// Mark a field as a partition column — see table_partition.ts.
function partitionField(name: string, type: any): Field {
  return new Field(name, type, true, new Map([["vgi.partition_column", "true"]]));
}

function b64encode(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
  return btoa(s);
}

// Build the `vgi_partition_values#b64` metadata payload — a 2-row (min, max)
// Arrow batch over the partition fields, IPC-serialized then base64'd.
function partitionValuesMetadata(
  fields: Field[],
  values: Record<string, [any, any]>,
  extra?: Map<string, string>,
): Map<string, string> {
  const schema = new Schema(fields);
  const cols: Record<string, any[]> = {};
  for (const f of fields) cols[f.name] = [values[f.name][0], values[f.name][1]];
  const md = new Map<string, string>(extra ?? []);
  md.set("vgi_partition_values#b64", b64encode(serializeBatch(batchFromColumns(cols, schema))));
  return md;
}

const int64Range = (start: number, end: number): bigint[] => {
  const out: bigint[] = [];
  for (let i = start; i < end; i++) out.push(BigInt(i));
  return out;
};

// Shared state shapes.
interface CountdownState {
  remaining: number;
  currentIndex: number;
}
interface NonceState {
  nonce: number;
  done: boolean;
}

const N_SCHEMA = new Schema([new Field("n", new Int64(), true)]);
const V_SCHEMA = new Schema([new Field("v", new Int64(), true)]);
const NONCE_SCHEMA = new Schema([new Field("nonce", new Int64(), true)]);

// ---------------------------------------------------------------------------
// cacheable_numbers
// ---------------------------------------------------------------------------
interface CacheableNumbersArgs {
  n: number;
  ttl: number;
}

const cacheable_numbers = defineTableFunction<CacheableNumbersArgs, CountdownState>({
  name: "cacheable_numbers",
  description: "Emits n rows [0..n) and advertises a cache TTL",
  args: { n: new Int64(), ttl: new Int64() },
  argDefaults: { n: 10, ttl: DEFAULT_TTL_SECONDS },
  argDocs: { n: "Number of rows to generate", ttl: "Cache TTL in seconds" },
  argConstraints: { n: { ge: 0 }, ttl: { ge: 0 } },
  onBind: () => ({ outputSchema: N_SCHEMA }),
  initialState: (p) => ({ remaining: Number(p.args.n), currentIndex: 0 }),
  process: (params, state, out) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }
    const first = state.currentIndex === 0;
    const size = Math.min(state.remaining, 1000);
    const batch = batchFromColumns(
      { n: int64Range(state.currentIndex, state.currentIndex + size) },
      params.outputSchema,
    );
    out.emit(batch, first ? cacheControlMetadata({ ttl: Number(params.args.ttl) }) : undefined);
    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [
    { sql: "SELECT * FROM cacheable_numbers(10)", description: "Cacheable sequence 0-9 with the default TTL" },
    { sql: "SELECT * FROM cacheable_numbers(10, ttl := 60)", description: "Cacheable sequence 0-9 with a 60s TTL" },
  ],
  categories: ["generator", "cache"],
  tags: { category: "cache", type: "generator" },
});

// ---------------------------------------------------------------------------
// cache_nonce
// ---------------------------------------------------------------------------
const cache_nonce = defineTableFunction<Record<string, never>, NonceState>({
  name: "cache_nonce",
  description: "Emits one row with a per-invocation nonce; cacheable",
  onBind: () => ({ outputSchema: NONCE_SCHEMA }),
  initialState: () => ({ nonce: nextNonce(), done: false }),
  process: (params, state, out) => {
    if (state.done) {
      out.finish();
      return;
    }
    out.emit(
      batchFromColumns({ nonce: [BigInt(state.nonce)] }, params.outputSchema),
      cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS }),
    );
    state.done = true;
  },
  examples: [
    { sql: "SELECT * FROM cache_nonce()", description: "One-row cacheable result; nonce is stable on a cache hit" },
  ],
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "nonce" },
});

// ---------------------------------------------------------------------------
// cache_no_store
// ---------------------------------------------------------------------------
interface RowsArg {
  n: number;
}

const cache_no_store = defineTableFunction<RowsArg, CountdownState>({
  name: "cache_no_store",
  description: "Emits n rows but advertises no_store (never cached)",
  args: { n: new Int64() },
  argDefaults: { n: 10 },
  argDocs: { n: "Number of rows to generate" },
  argConstraints: { n: { ge: 0 } },
  onBind: () => ({ outputSchema: N_SCHEMA }),
  initialState: (p) => ({ remaining: Number(p.args.n), currentIndex: 0 }),
  process: (params, state, out) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }
    const first = state.currentIndex === 0;
    const size = Math.min(state.remaining, 1000);
    const batch = batchFromColumns(
      { n: int64Range(state.currentIndex, state.currentIndex + size) },
      params.outputSchema,
    );
    out.emit(batch, first ? cacheControlMetadata({ noStore: true }) : undefined);
    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [{ sql: "SELECT * FROM cache_no_store(5)", description: "Emit 5 rows that must never be cached" }],
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "no_store" },
});

// ---------------------------------------------------------------------------
// cache_scoped_txn
// ---------------------------------------------------------------------------
interface ScopedTxnState extends CountdownState {
  nonce: number;
}

const SCOPED_TXN_SCHEMA = new Schema([
  new Field("n", new Int64(), true),
  new Field("nonce", new Int64(), true),
]);

const cache_scoped_txn = defineTableFunction<RowsArg, ScopedTxnState>({
  name: "cache_scoped_txn",
  description: "Emits n rows and advertises scope=transaction",
  args: { n: new Int64() },
  argDefaults: { n: 10 },
  argDocs: { n: "Number of rows to generate" },
  argConstraints: { n: { ge: 0 } },
  onBind: () => ({ outputSchema: SCOPED_TXN_SCHEMA }),
  // The nonce is bumped once per REAL invocation (a cache MISS), so a
  // same-transaction HIT returns the SAME nonce while a new-transaction MISS
  // returns a fresh one — the hit/miss is provable from the value, not the log.
  initialState: (p) => ({ remaining: Number(p.args.n), currentIndex: 0, nonce: nextNonce() }),
  process: (params, state, out) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }
    const first = state.currentIndex === 0;
    const size = Math.min(state.remaining, 1000);
    const batch = batchFromColumns(
      {
        n: int64Range(state.currentIndex, state.currentIndex + size),
        nonce: new Array(size).fill(BigInt(state.nonce)),
      },
      params.outputSchema,
    );
    out.emit(
      batch,
      first ? cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS, scope: CACHE_SCOPE_TRANSACTION }) : undefined,
    );
    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [{ sql: "SELECT * FROM cache_scoped_txn(5)", description: "Transaction-scoped cacheable result" }],
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "scope" },
});

// ---------------------------------------------------------------------------
// cache_big
// ---------------------------------------------------------------------------
interface CacheBigArgs {
  rows: number;
}

const cache_big = defineTableFunction<CacheBigArgs, CountdownState>({
  name: "cache_big",
  description: "Emits many small batches totaling `rows` rows; cacheable",
  args: { rows: new Int64() },
  argDefaults: { rows: 5000 },
  argDocs: { rows: "Number of rows to generate" },
  argConstraints: { rows: { ge: 0 } },
  onBind: () => ({ outputSchema: N_SCHEMA }),
  initialState: (p) => ({ remaining: Number(p.args.rows), currentIndex: 0 }),
  process: (params, state, out) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }
    const first = state.currentIndex === 0;
    const size = Math.min(state.remaining, 1000);
    const batch = batchFromColumns(
      { n: int64Range(state.currentIndex, state.currentIndex + size) },
      params.outputSchema,
    );
    out.emit(batch, first ? cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS }) : undefined);
    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [{ sql: "SELECT count(*) FROM cache_big(50000)", description: "Large multi-batch cacheable result" }],
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "multi_batch" },
});

// ---------------------------------------------------------------------------
// cache_revalidatable — conditional revalidation (304 / not_modified)
// ---------------------------------------------------------------------------
// Advertises ttl=0 + etag + revalidatable — the "no-cache" semantic: the client
// stores the payload but marks it immediately stale, so every repeat sends a
// conditional request (`vgi.cache.if_none_match`) on the first tick. This
// fixture's data never changes, so process() sees the matching validator and
// answers with a 0-row `not_modified` batch instead of re-emitting — the client
// reuses the STORED nonce. A stable nonce across repeats therefore proves the
// not_modified path served cached bytes without re-streaming.
const REVALIDATABLE_ETAG = '"rev-v1"';

const cache_revalidatable = defineTableFunction<Record<string, never>, NonceState>({
  name: "cache_revalidatable",
  description: "Emits one nonce row; always-revalidate (304 not_modified)",
  onBind: () => ({ outputSchema: NONCE_SCHEMA }),
  initialState: () => ({ nonce: nextNonce(), done: false }),
  process: (params, state, out) => {
    if (state.done) {
      out.finish();
      return;
    }
    if (params.ifNoneMatch === REVALIDATABLE_ETAG) {
      // 304 Not Modified: the client's stored copy is still valid. Emit a 0-row
      // not_modified batch (fresh validators + ttl=0 so it keeps revalidating).
      out.emit(
        batchFromColumns({ nonce: [] as bigint[] }, params.outputSchema),
        cacheControlMetadata({ notModified: true, ttl: 0, etag: REVALIDATABLE_ETAG, revalidatable: true }),
      );
      state.done = true;
      return;
    }
    out.emit(
      batchFromColumns({ nonce: [BigInt(state.nonce)] }, params.outputSchema),
      cacheControlMetadata({ ttl: 0, etag: REVALIDATABLE_ETAG, revalidatable: true }),
    );
    state.done = true;
  },
  examples: [
    {
      sql: "SELECT * FROM cache_revalidatable()",
      description: "Conditionally-revalidated result (304 reuses stored bytes)",
    },
  ],
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "revalidatable" },
});

// ---------------------------------------------------------------------------
// cache_multicol — multi-column cacheable result (projection-coverage reuse)
// ---------------------------------------------------------------------------
interface CacheMultiColArgs {
  n: number;
  ttl: number;
}

const MULTICOL_SCHEMA = new Schema([
  new Field("a", new Int64(), true),
  new Field("b", new Int64(), true),
  new Field("c", new Int64(), true),
]);

const cache_multicol = defineTableFunction<CacheMultiColArgs, CountdownState>({
  name: "cache_multicol",
  description: "Emits n rows of (a, b, c); cacheable, multi-column",
  args: { n: new Int64(), ttl: new Int64() },
  argDefaults: { n: 4, ttl: DEFAULT_TTL_SECONDS },
  argDocs: { n: "Number of rows to generate", ttl: "Cache TTL in seconds" },
  argConstraints: { n: { ge: 0 }, ttl: { ge: 0 } },
  onBind: () => ({ outputSchema: MULTICOL_SCHEMA }),
  initialState: (p) => ({ remaining: Number(p.args.n), currentIndex: 0 }),
  process: (params, state, out) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }
    const rows = int64Range(0, state.remaining);
    out.emit(
      batchFromColumns(
        { a: rows, b: rows.map((i) => i * 10n), c: rows.map((i) => i * 100n) },
        params.outputSchema,
      ),
      cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS }),
    );
    state.remaining = 0;
  },
  examples: [
    { sql: "SELECT b FROM cache_multicol()", description: "Subset projection reuses the full-result cache entry" },
  ],
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "multicol" },
});

// ---------------------------------------------------------------------------
// cache_whoami — identity-echoing cacheable result (cache token isolation)
// ---------------------------------------------------------------------------
// The linchpin of the cache token-isolation test: two attaches of the same
// worker with different bearer tokens map to different principals, so their
// results MUST land under different (identity-scoped) cache keys and never
// cross-serve. Bearer/OAuth identity is HTTP-only; over subprocess every caller
// is "anonymous".
const WHOAMI_SCHEMA = new Schema([new Field("who", new Utf8(), true)]);

const cache_whoami = defineTableFunction<Record<string, never>, NonceState>({
  name: "cache_whoami",
  description: "Emits the caller's auth principal; cacheable (identity-scoped)",
  onBind: () => ({ outputSchema: WHOAMI_SCHEMA }),
  initialState: () => ({ nonce: 0, done: false }),
  process: (params, state, out: OutputCollector) => {
    if (state.done) {
      out.finish();
      return;
    }
    const who = out.auth?.principal || "anonymous";
    out.emit(
      batchFromColumns({ who: [who] }, params.outputSchema),
      cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS }),
    );
    state.done = true;
  },
  examples: [
    {
      sql: "SELECT who FROM cache_whoami()",
      description: "One-row cacheable result echoing the caller's principal",
    },
  ],
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "identity" },
});

// ---------------------------------------------------------------------------
// cache_versioned_scan — time-travel cacheable result (AT cache isolation)
// ---------------------------------------------------------------------------
// Version → row data (fixed schema, so table_get needs no per-version override;
// only the scan-function arg changes). The catalog maps AT → the version arg.
const CACHE_VERSIONED_DATA: Record<number, number[]> = {
  1: [101, 102, 103],
  2: [201, 202],
  3: [301, 302, 303, 304],
};
export const CACHE_VERSIONED_CURRENT = 3;

interface CacheVersionedArgs {
  version: number;
}

const CACHE_VERSIONED_SCHEMA = new Schema([new Field("v", new Int64(), true)]);

const cache_versioned_scan = defineTableFunction<CacheVersionedArgs, NonceState>({
  name: "cache_versioned_scan",
  description: "Version-specific rows; cacheable (AT-keyed)",
  args: { version: new Int64() },
  argDocs: { version: "Data version, resolved from the AT clause by the catalog" },
  onBind: () => ({ outputSchema: CACHE_VERSIONED_SCHEMA }),
  initialState: () => ({ nonce: 0, done: false }),
  process: (params, state, out) => {
    if (state.done) {
      out.finish();
      return;
    }
    const version = Number(params.args.version);
    const data = CACHE_VERSIONED_DATA[version] ?? CACHE_VERSIONED_DATA[CACHE_VERSIONED_CURRENT];
    out.emit(
      batchFromColumns({ v: data.map((n) => BigInt(n)) }, params.outputSchema),
      cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS }),
    );
    state.done = true;
  },
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "time_travel" },
});

// ---------------------------------------------------------------------------
// cache_projection — projection-pushdown cacheable result (cross-serve check)
// ---------------------------------------------------------------------------
// Because projection pushdown is on, `SELECT a` and `SELECT b` push distinct
// projection_ids that are part of the cache key — each column's scan caches only
// its own bytes under a distinct key, and one column's result can never be
// served for another's.
const CACHE_PROJ_DATA: Record<string, bigint[]> = {
  a: [1n, 2n, 3n],
  b: [10n, 20n, 30n],
  c: [100n, 200n, 300n],
};

const cache_projection = defineTableFunction<Record<string, never>, NonceState>({
  name: "cache_projection",
  description: "3-column projection-pushdown generator; cacheable",
  projectionPushdown: true,
  onBind: () => ({ outputSchema: MULTICOL_SCHEMA }),
  initialState: () => ({ nonce: 0, done: false }),
  process: (params, state, out) => {
    if (state.done) {
      out.finish();
      return;
    }
    const cols: Record<string, bigint[]> = {};
    for (const f of params.outputSchema.fields) cols[f.name] = CACHE_PROJ_DATA[f.name];
    out.emit(
      batchFromColumns(cols, params.outputSchema),
      cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS }),
    );
    state.done = true;
  },
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "projection" },
});

// ---------------------------------------------------------------------------
// cache_poison — cacheable first batch then a mid-stream worker error
// ---------------------------------------------------------------------------
// Adversarial check of the never-partial invariant: a worker error AFTER a
// cacheable batch has streamed must commit NOTHING to the cache (the failing
// thread never reaches EOS, so `eos < launched` and no entry is stored).
interface PoisonState {
  emitted: boolean;
  poisoned: boolean;
}

const cache_poison = defineTableFunction<Record<string, never>, PoisonState>({
  name: "cache_poison",
  description: "Cacheable first batch then a mid-stream error (never-partial check)",
  onBind: () => ({ outputSchema: N_SCHEMA }),
  initialState: () => ({ emitted: false, poisoned: false }),
  process: (params, state, out) => {
    if (!state.emitted) {
      out.emit(
        batchFromColumns({ n: [0n, 1n, 2n] }, params.outputSchema),
        cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS }),
      );
      state.emitted = true;
      return;
    }
    throw new Error("cache_poison: intentional mid-stream failure after a cacheable batch");
  },
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "poison" },
});

// ---------------------------------------------------------------------------
// cache_external_fail — cacheable first batch then an unresolvable pointer batch
// ---------------------------------------------------------------------------
// An unreachable loopback URL (http, no TLS handshake). Port 9 (discard) is
// closed, so resolution fails fast with connection-refused.
const UNRESOLVABLE_LOCATION = "http://127.0.0.1:9/vgi-cache-poison-nonexistent";

const cache_external_fail = defineTableFunction<Record<string, never>, PoisonState>({
  name: "cache_external_fail",
  description: "Cacheable first batch then an unresolvable external-location pointer",
  onBind: () => ({ outputSchema: N_SCHEMA }),
  initialState: () => ({ emitted: false, poisoned: false }),
  process: (params, state, out) => {
    if (!state.emitted) {
      out.emit(
        batchFromColumns({ n: [0n, 1n, 2n] }, params.outputSchema),
        cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS }),
      );
      state.emitted = true;
      return;
    }
    if (!state.poisoned) {
      // 0-row pointer batch to an unreachable URL — the client tries to fetch
      // it and throws, aborting the scan before EOS.
      out.emit(
        batchFromColumns({ n: [] as bigint[] }, params.outputSchema),
        new Map([["vgi_rpc.location", UNRESOLVABLE_LOCATION]]),
      );
      state.poisoned = true;
      return;
    }
    // Reached only if resolution somehow succeeded; keeps the producer from
    // looping forever on a transport that doesn't resolve external locations.
    out.finish();
  },
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "poison_external" },
});

// ---------------------------------------------------------------------------
// cache_bench — parametrizable large cacheable result (scaling bench + S8 guard)
// ---------------------------------------------------------------------------
// `rows` is POSITIONAL (unlike the other cache fixtures' named-with-default
// args) so the direct path `vgi_table_function(w, 'cache_bench', [rows])` really
// honors the requested row count — the scaling bench and the S8 flat-RAM guard
// need a result whose size they control.
interface CacheBenchArgs {
  rows: number;
}

const cache_bench = defineTableFunction<CacheBenchArgs, CountdownState>({
  name: "cache_bench",
  description: "Emits `rows` int64 rows (positional arg); cacheable — scaling bench fixture",
  args: { rows: new Int64() },
  argDocs: { rows: "Number of rows to generate" },
  argConstraints: { rows: { ge: 0 } },
  onBind: () => ({ outputSchema: V_SCHEMA }),
  initialState: (p) => ({ remaining: Number(p.args.rows), currentIndex: 0 }),
  process: (params, state, out) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }
    const first = state.currentIndex === 0;
    const size = Math.min(state.remaining, 2048);
    const batch = batchFromColumns(
      { v: int64Range(state.currentIndex, state.currentIndex + size) },
      params.outputSchema,
    );
    out.emit(batch, first ? cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS }) : undefined);
    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [
    { sql: "SELECT count(*) FROM cache_bench(1000000)", description: "Million-row cacheable result for scaling tests" },
  ],
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "bench" },
});

// ---------------------------------------------------------------------------
// cache_parallel — MULTI-WORKER cacheable result (parallel capture)
// ---------------------------------------------------------------------------
// Work-queue fan-out (like partitioned_sequence): the primary worker enqueues
// fixed-size (start, end) chunks at onInit; ANY worker pops a chunk and emits
// batches for it. maxWorkers is DEFAULT (clamped to `SET threads`), so a cached
// scan captures ONE SUBSTREAM PER WORKER THREAD — the only cache fixture that
// exercises parallel capture. Values are the plain sequence [0..rows), so COUNT
// and SUM hold regardless of how chunks were distributed across workers.
const CACHE_PARALLEL_MAX_CHUNKS = 24;

interface CacheParallelArgs {
  rows: number;
  batch_size: number;
}
interface CacheParallelState {
  advertised: boolean;
  currentStart: number | null;
  currentEnd: number | null;
  currentIdx: number;
}

const cache_parallel = defineTableFunction<CacheParallelArgs, CacheParallelState>({
  name: "cache_parallel",
  description: "Multi-worker cacheable sequence (one substream per worker); parallel-capture fixture",
  args: { rows: new Int64(), batch_size: new Int64() },
  argDefaults: { batch_size: 24000 },
  argDocs: { rows: "Total number of rows to generate", batch_size: "Rows per output batch" },
  argConstraints: { rows: { ge: 0 }, batch_size: { ge: 1 } },
  maxWorkers: DEFAULT_MAX_WORKERS,
  onBind: () => ({ outputSchema: V_SCHEMA }),
  onInit: async (params) => {
    const rows = Number(params.args.rows);
    const chunk = Math.max(1, Math.ceil(rows / CACHE_PARALLEL_MAX_CHUNKS));
    const items: Uint8Array[] = [];
    for (let start = 0; start < rows; start += chunk) items.push(packPair(start, Math.min(start + chunk, rows)));
    // Always push (registers the invocation), even when there is no work.
    await params.storage.queuePush(items);
    return { max_workers: DEFAULT_MAX_WORKERS, execution_id: params.executionId, opaque_data: null };
  },
  initialState: () => ({ advertised: false, currentStart: null, currentEnd: null, currentIdx: 0 }),
  process: async (params, state, out) => {
    if (state.currentStart === null || state.currentIdx >= (state.currentEnd ?? 0)) {
      const item = await params.storage!.queuePop();
      if (item === null) {
        out.finish();
        return;
      }
      const [start, end] = unpackPair(item);
      state.currentStart = start;
      state.currentEnd = end;
      state.currentIdx = start;
    }
    const batchEnd = Math.min(state.currentIdx + Number(params.args.batch_size), state.currentEnd ?? 0);
    const batch = batchFromColumns({ v: int64Range(state.currentIdx, batchEnd) }, params.outputSchema);
    out.emit(batch, state.advertised ? undefined : cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS }));
    state.advertised = true;
    state.currentIdx = batchEnd;
  },
  examples: [
    {
      sql: "SELECT count(*) FROM cache_parallel(1000000)",
      description: "Parallel-captured cacheable result across workers",
    },
  ],
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "parallel" },
});

// ---------------------------------------------------------------------------
// cache_ordered — MULTI-WORKER, ORDER-SENSITIVE cacheable result
// ---------------------------------------------------------------------------
// Like cache_parallel but opts into supportsBatchIndex / FIXED_ORDER: each chunk
// carries a monotonic partition_id emitted as the batch's batch_index. Capture
// still fans out across workers, but on a cache HIT the single-thread replay
// re-sorts the flattened substreams by batch_index — proving the replay
// reconstructs SOURCE ORDER, not just the row set.
interface CacheOrderedArgs {
  rows: number;
  chunk_size: number;
}
interface CacheOrderedState {
  advertised: boolean;
  partitionId: number | null;
  currentStart: number | null;
  currentEnd: number | null;
  currentIdx: number;
}

const cache_ordered = defineTableFunction<CacheOrderedArgs, CacheOrderedState>({
  name: "cache_ordered",
  description:
    "Multi-worker order-sensitive cacheable sequence (batch_index); order-preservation cache fixture",
  // Named-with-default (not positional) so this can back a catalog *data Table*
  // — the parallel + order-sensitive capture path only exists on the catalog
  // scan (the direct vgi_table_function() path serializes FIXED_ORDER to one
  // thread).
  args: { rows: new Int64(), chunk_size: new Int64() },
  argDefaults: { rows: 200000, chunk_size: 1000 },
  argDocs: { rows: "Total number of rows to generate", chunk_size: "Rows per partition" },
  argConstraints: { rows: { ge: 0 }, chunk_size: { ge: 1 } },
  maxWorkers: DEFAULT_MAX_WORKERS,
  preservesOrder: OrderPreservation.FIXED_ORDER,
  supportsBatchIndex: true,
  onBind: () => ({ outputSchema: N_SCHEMA }),
  onInit: async (params) => {
    const rows = Number(params.args.rows);
    const chunk = Number(params.args.chunk_size);
    const items: Uint8Array[] = [];
    let pid = 0;
    for (let start = 0; start < rows; start += chunk) {
      items.push(packTriple(pid++, start, Math.min(start + chunk, rows)));
    }
    await params.storage.queuePush(items);
    return { max_workers: DEFAULT_MAX_WORKERS, execution_id: params.executionId, opaque_data: null };
  },
  initialState: () => ({ advertised: false, partitionId: null, currentStart: null, currentEnd: null, currentIdx: 0 }),
  process: async (params, state, out) => {
    if (state.partitionId === null || state.currentIdx >= (state.currentEnd ?? 0)) {
      const item = await params.storage!.queuePop();
      if (item === null) {
        out.finish();
        return;
      }
      const [pid, start, end] = unpackTriple(item);
      state.partitionId = pid;
      state.currentStart = start;
      state.currentEnd = end;
      state.currentIdx = start;
    }
    const batchEnd = Math.min(state.currentIdx + 256, state.currentEnd ?? 0);
    const batchIndexMeta = new Map([["vgi_batch_index", String(state.partitionId)]]);
    const metadata = state.advertised
      ? batchIndexMeta
      : cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS }, batchIndexMeta);
    out.emit(batchFromColumns({ n: int64Range(state.currentIdx, batchEnd) }, params.outputSchema), metadata);
    state.advertised = true;
    state.currentIdx = batchEnd;
  },
  examples: [
    { sql: "SELECT * FROM cache_ordered(1000)", description: "Order-preserving cacheable result across workers" },
  ],
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "ordered" },
});

// ---------------------------------------------------------------------------
// cache_interleaved — batch_index reassembly (real reorder on serve)
// ---------------------------------------------------------------------------
// Partitions are enqueued in DESCENDING order, so emission order ≠ batch_index
// order. The live (uncached) scan returns rows in emission order — NOT
// monotonic; a cached serve flattens and stable-sorts by batch_index, producing
// strictly 0,1,…,N-1. The gap between the two proves the replay sort genuinely
// reorders real multi-batch output.
interface CacheInterleavedArgs {
  rows: number;
  chunk_size: number;
}

const cache_interleaved = defineTableFunction<CacheInterleavedArgs, CacheOrderedState>({
  name: "cache_interleaved",
  description: "Parallel batch_index-tagged cacheable sequence; cache serve reassembles order",
  args: { rows: new Int64(), chunk_size: new Int64() },
  // Chunk spans MANY batches (chunk / BATCH_SIZE) so the serve reassembly is
  // tested across batch boundaries, not a single already-sorted batch.
  argDefaults: { chunk_size: 20000 },
  argDocs: { rows: "Total number of rows to generate", chunk_size: "Rows per partition" },
  argConstraints: { rows: { ge: 0 }, chunk_size: { ge: 1 } },
  maxWorkers: DEFAULT_MAX_WORKERS,
  supportsBatchIndex: true,
  onBind: () => ({ outputSchema: N_SCHEMA }),
  onInit: async (params) => {
    const rows = Number(params.args.rows);
    const chunk = Number(params.args.chunk_size);
    const items: Uint8Array[] = [];
    let pid = 0;
    for (let start = 0; start < rows; start += chunk) {
      items.push(packTriple(pid++, start, Math.min(start + chunk, rows)));
    }
    items.reverse(); // highest partition_id popped first → scrambled arrival
    await params.storage.queuePush(items);
    return { max_workers: DEFAULT_MAX_WORKERS, execution_id: params.executionId, opaque_data: null };
  },
  initialState: () => ({ advertised: false, partitionId: null, currentStart: null, currentEnd: null, currentIdx: 0 }),
  process: async (params, state, out) => {
    if (state.partitionId === null || state.currentIdx >= (state.currentEnd ?? 0)) {
      const item = await params.storage!.queuePop();
      if (item === null) {
        out.finish();
        return;
      }
      const [pid, start, end] = unpackTriple(item);
      state.partitionId = pid;
      state.currentStart = start;
      state.currentEnd = end;
      state.currentIdx = start;
    }
    const batchEnd = Math.min(state.currentIdx + 2048, state.currentEnd ?? 0);
    const batchIndexMeta = new Map([["vgi_batch_index", String(state.partitionId)]]);
    const metadata = state.advertised
      ? batchIndexMeta
      : cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS }, batchIndexMeta);
    out.emit(batchFromColumns({ n: int64Range(state.currentIdx, batchEnd) }, params.outputSchema), metadata);
    state.advertised = true;
    state.currentIdx = batchEnd;
  },
  examples: [
    {
      sql: "SELECT count(*) FROM cache_interleaved(100000)",
      description: "Parallel batch_index reassembly on cache serve",
    },
  ],
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "interleaved" },
});

// ---------------------------------------------------------------------------
// cache_types — nested / wide / NULL columns through the spill + disk blob
// ---------------------------------------------------------------------------
// Every other cacheable fixture emits flat int64/string, so the disk blob and
// the streaming TOC (seek-past-payload) path is otherwise only exercised on
// fixed-width int64. This emits STRUCT / LIST / DECIMAL / TIMESTAMP / string
// columns WITH interleaved NULLs (validity bitmaps + variable/nested buffers)
// across many batches, so a spilled + streamed serve must reassemble all of that
// byte-identically — not just a matching COUNT.
const CACHE_TYPES_SCHEMA = new Schema([
  new Field("id", new Int64(), true),
  new Field("tags", new List(new Field("item", new Int64(), true)), true),
  new Field(
    "attrs",
    new Struct([new Field("x", new Int64(), true), new Field("y", new Utf8(), true)]),
    true,
  ),
  new Field("amt", new Decimal(2, 18, 128), true),
  new Field("ts", new Timestamp(TimeUnit.MICROSECOND), true),
  new Field("label", new Utf8(), true),
]);

interface CacheTypesArgs {
  rows: number;
}

const cache_types = defineTableFunction<CacheTypesArgs, CountdownState>({
  name: "cache_types",
  description: "Nested/wide/NULL cacheable result (STRUCT/LIST/DECIMAL/TIMESTAMP + NULLs)",
  args: { rows: new Int64() },
  argDocs: { rows: "Total number of rows to generate" },
  argConstraints: { rows: { ge: 0 } },
  maxWorkers: DEFAULT_MAX_WORKERS,
  onBind: () => ({ outputSchema: CACHE_TYPES_SCHEMA }),
  initialState: (p) => ({ remaining: Number(p.args.rows), currentIndex: 0 }),
  process: (params, state, out) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }
    const first = state.currentIndex === 0;
    const size = Math.min(state.remaining, 2048);

    const ids: bigint[] = [];
    const tags: (bigint[] | null)[] = [];
    const attrs: ({ x: bigint; y: string } | null)[] = [];
    // decimal128(18, 2) canonical representation is the unscaled bigint, so
    // `j.<j%100 padded to 2>` becomes j*100 + (j % 100).
    const amt: (bigint | null)[] = [];
    const ts: (bigint | null)[] = [];
    const label: (string | null)[] = [];

    for (let j = state.currentIndex; j < state.currentIndex + size; j++) {
      ids.push(BigInt(j));
      if (j % 5 === 0) {
        // NULL row in every nullable column
        tags.push(null);
        attrs.push(null);
        amt.push(null);
        ts.push(null);
        label.push(null);
      } else {
        tags.push([BigInt(j), BigInt(j + 1), BigInt(j + 2)]);
        attrs.push({ x: BigInt(j), y: `y${j}` });
        amt.push(BigInt(j) * 100n + BigInt(j % 100));
        ts.push(BigInt(j)); // int64 micros → timestamp('us')
        label.push(`label-${j}`);
      }
    }
    const batch = batchFromColumns({ id: ids, tags, attrs, amt, ts, label }, params.outputSchema);
    out.emit(batch, first ? cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS }) : undefined);
    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [{ sql: "SELECT count(*) FROM cache_types(10000)", description: "Nested/NULL cacheable result" }],
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "types" },
});

// ---------------------------------------------------------------------------
// cache_filtered — cacheable + STATIC filter pushdown (filter_bytes in the key)
// ---------------------------------------------------------------------------
// The key includes `filter_bytes`, but no other cacheable fixture pushes
// filters, so the "a pushed WHERE n>=5 must never cross-serve a pushed WHERE
// n>=7" boundary would otherwise be uncovered.
interface CacheFilteredArgs {
  rows: number;
}

const cache_filtered = defineTableFunction<CacheFilteredArgs, CountdownState>({
  name: "cache_filtered",
  description: "Cacheable sequence with static filter pushdown (filter_bytes keying)",
  // named-default so it can back a catalog data Table (filter pushdown is wired
  // on the catalog scan path, not the direct vgi_table_function path).
  args: { rows: new Int64() },
  argDefaults: { rows: 100 },
  argDocs: { rows: "Total number of rows to generate" },
  argConstraints: { rows: { ge: 0 } },
  filterPushdown: true,
  autoApplyFilters: true,
  onBind: () => ({ outputSchema: N_SCHEMA }),
  initialState: (p) => ({ remaining: Number(p.args.rows), currentIndex: 0 }),
  process: (params, state, out) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }
    const first = state.currentIndex === 0;
    const size = Math.min(state.remaining, 2048);
    const batch = batchFromColumns(
      { n: int64Range(state.currentIndex, state.currentIndex + size) },
      params.outputSchema,
    );
    out.emit(batch, first ? cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS }) : undefined);
    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [
    {
      sql: "SELECT count(*) FROM cache_filtered(100) WHERE n >= 50",
      description: "Cacheable filtered result; WHERE keys the entry",
    },
  ],
  categories: ["generator", "cache", "testing"],
  tags: { category: "cache", type: "filtered" },
});

// ---------------------------------------------------------------------------
// cache_partitioned — partition_values (min/max hints) through the spill blob
// ---------------------------------------------------------------------------
// No other cacheable fixture emits partition_values, so the non-empty pv_bytes
// framing in the disk blob is otherwise untested. A single-valued `country`
// partition column makes each batch carry pv; forced to spill and served back,
// any misframed pv_len would misalign the streaming TOC seek.
const CACHE_COUNTRIES = ["AU", "BR", "CA", "FR", "US"];
const CACHE_PARTITIONED_FIELDS = [new Field("country", new Utf8(), true)];
const CACHE_PARTITIONED_SCHEMA = new Schema([
  partitionField("country", new Utf8()),
  new Field("sales", new Int64(), true),
]);

interface CachePartitionedArgs {
  rows_per_country: number;
}
interface CachePartitionedState {
  countryIdx: number;
  advertised: boolean;
}

const cache_partitioned = defineTableFunction<CachePartitionedArgs, CachePartitionedState>({
  name: "cache_partitioned",
  description: "Cacheable single-value-partitioned result (partition_values through the spill blob)",
  args: { rows_per_country: new Int64() },
  argDocs: { rows_per_country: "Rows per country partition" },
  argConstraints: { rows_per_country: { ge: 1 } },
  partitionKind: "SINGLE_VALUE_PARTITIONS",
  onBind: () => ({ outputSchema: CACHE_PARTITIONED_SCHEMA }),
  initialState: () => ({ countryIdx: 0, advertised: false }),
  process: (params, state, out) => {
    if (state.countryIdx >= CACHE_COUNTRIES.length) {
      out.finish();
      return;
    }
    const country = CACHE_COUNTRIES[state.countryIdx];
    const rpc = Number(params.args.rows_per_country);
    const base = state.countryIdx * 1_000_000;
    const countries = new Array(rpc).fill(country);
    const sales = int64Range(base, base + rpc);

    const cacheMeta = state.advertised ? undefined : cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS });
    out.emit(
      batchFromColumns({ country: countries, sales }, params.outputSchema),
      partitionValuesMetadata(CACHE_PARTITIONED_FIELDS, { country: [country, country] }, cacheMeta),
    );
    state.advertised = true;
    state.countryIdx += 1;
  },
  examples: [
    {
      sql: "SELECT country, SUM(sales) FROM cache_partitioned(100) GROUP BY country",
      description: "Partitioned cacheable aggregate over country",
    },
  ],
  categories: ["generator", "cache", "testing", "partitioning"],
  tags: { category: "cache", type: "partitioned" },
});

export const cacheTableFunctions: VgiFunction[] = [
  cacheable_numbers,
  cache_nonce,
  cache_no_store,
  cache_scoped_txn,
  cache_big,
  cache_revalidatable,
  cache_multicol,
  cache_whoami,
  cache_versioned_scan,
  cache_projection,
  cache_poison,
  cache_external_fail,
  cache_bench,
  cache_parallel,
  cache_ordered,
  cache_interleaved,
  cache_types,
  cache_filtered,
  cache_partitioned,
];
