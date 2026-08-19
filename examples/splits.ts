// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Split-capable table generators, the TypeScript half of the cross-SDK splits suite.
//
// Every fixture here is a TWIN of one in vgi-python of the same name. The shared
// SQL suite runs unchanged against every SDK's worker, so a wire disagreement
// between two SDKs shows up as the same named test failing under one of them —
// which only works if the fixtures agree on BEHAVIOUR, not merely on name.
//
// The shapes cover the ways a split scan goes wrong rather than the ways it goes
// right: zero splits (legal, must be an empty result), zero-ROW splits (the
// likelier shape — a filter pruned one — and the one that silently truncates a
// scan if a reader treats an empty split as EOS), skew, and far more splits than
// reader threads (which forces sequential re-init on a reused connection).

import { Schema, Field, Int64, Utf8, Bool } from "@query-farm/apache-arrow";
import {
  defineTableFunction,
  batchFromColumns,
  cacheControlMetadata,
  serializeBatch,
  type TableBindParams,
  type TableProcessParams,
  type PushdownFilters,
  type VgiFunction,
} from "../src/index.js";
import type { OutputCollector } from "@query-farm/vgi-rpc";
import type { PlanResult, TableFunctionPlanRequest } from "../src/protocol/serializers/splits.js";

const SPLIT_SCHEMA = new Schema([new Field("n", new Int64(), true)]);

interface SplitArgs {
  n: number;
  splits: number;
}

/** The half-open range `[lo, hi)` one split owns. */
interface Range {
  lo: number;
  hi: number;
}

interface SplitState {
  ranges: Range[];
  ordinals: number[];
  idx: number;
  cur: number;
  emittedInSplit: number;
  cacheAdvertised: boolean;
}

// A split NAMES the work rather than describing it: a redemption reads the same
// rows however many times it runs and whichever process runs it, which is
// exactly what a retrying engine requires.
function encode(r: Range): Uint8Array {
  const buf = new ArrayBuffer(16);
  const view = new DataView(buf);
  view.setBigInt64(0, BigInt(r.lo), true);
  view.setBigInt64(8, BigInt(r.hi), true);
  return new Uint8Array(buf);
}

function decode(payload: Uint8Array): Range {
  if (payload.length !== 16) {
    throw new Error(`split payload must be 16 bytes, got ${payload.length}`);
  }
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  return { lo: Number(view.getBigInt64(0, true)), hi: Number(view.getBigInt64(8, true)) };
}

/** `(ordinal, lo, hi)` — the batch-index fixture needs the split's position too. */
function encodeOrdinal(ordinal: number, r: Range): Uint8Array {
  const buf = new ArrayBuffer(24);
  const view = new DataView(buf);
  view.setBigInt64(0, BigInt(ordinal), true);
  view.setBigInt64(8, BigInt(r.lo), true);
  view.setBigInt64(16, BigInt(r.hi), true);
  return new Uint8Array(buf);
}

function decodeOrdinal(payload: Uint8Array): [number, Range] {
  if (payload.length !== 24) {
    throw new Error(`batch-index split payload must be 24 bytes, got ${payload.length}`);
  }
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  return [
    Number(view.getBigInt64(0, true)),
    { lo: Number(view.getBigInt64(8, true)), hi: Number(view.getBigInt64(16, true)) },
  ];
}

/** Divide `[0, n)` into `k` contiguous ranges, remainder over the first few. */
function evenRanges(n: number, k: number): Range[] {
  if (k <= 0) return [];
  const total = Math.max(0, n);
  const base = Math.floor(total / k);
  const extra = total % k;
  const out: Range[] = [];
  let lo = 0;
  for (let i = 0; i < k; i++) {
    const hi = lo + base + (i < extra ? 1 : 0);
    out.push({ lo, hi });
    lo = hi;
  }
  return out;
}

/**
 * Seed a reader's cursor from the verified split payloads.
 *
 * No payloads at all means the client stopped planning (`vgi_split_scans` off).
 * A split-only function has no way to know what to read then, and failing here
 * is the point: quietly returning zero rows would be A DIFFERENT ANSWER to the
 * same query, which is worse than an error. Distinct from a plan that
 * legitimately produced ZERO splits — there the client never inits at all.
 */
function seedState(
  name: string,
  params: TableProcessParams<SplitArgs>,
  withOrdinals = false,
): SplitState {
  if (params.splitPayloads === undefined) {
    throw new Error(
      `${name} is split-only but was initialized with no split tokens; ` +
        `vgi_split_scans is probably off, and this function has no ` +
        `primary/secondary path to fall back to`,
    );
  }
  const ranges: Range[] = [];
  const ordinals: number[] = [];
  for (const p of params.splitPayloads) {
    if (withOrdinals) {
      const [ord, r] = decodeOrdinal(p);
      ordinals.push(ord);
      ranges.push(r);
    } else {
      ranges.push(decode(p));
    }
  }
  return {
    ranges,
    ordinals,
    idx: 0,
    cur: ranges.length > 0 ? ranges[0].lo : 0,
    emittedInSplit: 0,
    cacheAdvertised: false,
  };
}

/**
 * Emit one batch, walking THIS reader's claimed ranges in order.
 *
 * A zero-row range is STEPPED OVER, never reported as end-of-stream: finishing
 * here would truncate the reader's remaining claims and the query would look
 * correct while missing rows.
 */
function emitNext(
  params: TableProcessParams<SplitArgs>,
  state: SplitState,
  out: OutputCollector,
  opts: { maxBatch?: number; cacheTtl?: number; batchStride?: number } = {},
): void {
  const maxBatch = opts.maxBatch ?? 1024;
  for (;;) {
    if (state.idx >= state.ranges.length) {
      out.finish();
      return;
    }
    const r = state.ranges[state.idx];
    if (state.cur >= r.hi) {
      state.idx += 1;
      state.emittedInSplit = 0;
      if (state.idx < state.ranges.length) state.cur = state.ranges[state.idx].lo;
      continue;
    }
    const size = Math.min(r.hi - state.cur, maxBatch);
    const values: bigint[] = [];
    for (let i = 0; i < size; i++) values.push(BigInt(state.cur + i));
    state.cur += size;

    let meta: Map<string, string> | undefined;
    if (opts.cacheTtl !== undefined && !state.cacheAdvertised) {
      // The FIRST batch of this reader's stream — the only one the client reads
      // freshness from. Every reader advertises the same value, because the
      // result is one entry with one lifetime and a per-split TTL would be
      // decided by whichever reader happened to arrive first.
      state.cacheAdvertised = true;
      meta = cacheControlMetadata({ ttl: opts.cacheTtl });
    }
    if (opts.batchStride !== undefined) {
      const ordinal = state.ordinals[state.idx] ?? state.idx;
      const index = ordinal * opts.batchStride + state.emittedInSplit;
      state.emittedInSplit += 1;
      meta = new Map([...(meta ?? new Map()), ["vgi_batch_index", String(index)]]);
    }
    out.emit(batchFromColumns({ n: values }, params.outputSchema), meta);
    return;
  }
}

/** The plan every sequence-shaped fixture returns, given its ranges. */
function planOf(ranges: Range[], n: number, withOrdinals = false, base = 0): PlanResult {
  return {
    splits: ranges.map((r, i) => ({
      // The ordinal is what a split's batch-index space keys on, so it has to
      // survive into redemption — which means it belongs in the payload, and the
      // decoder must be told to expect it.
      payload: withOrdinals ? encodeOrdinal(base + i, r) : encode(r),
      estimatedRows: r.hi - r.lo,
      rowsExact: true,
      estimatedBytes: (r.hi - r.lo) * 8,
    })),
    estimatedTotalSplits: ranges.length,
    estimatedTotalRows: n,
  };
}

/**
 * Build a sequence-shaped split fixture. Only the range DIVISION differs between
 * most of them, so the plan/redeem/emit machinery is shared and each fixture
 * supplies just its shape.
 */
function splitFixture(cfg: {
  name: string;
  description: string;
  ranges: (args: SplitArgs) => Range[];
  splitTokenTtlSeconds?: number;
  catalogVersion?: number;
  perPage?: number;
  cacheTtl?: number;
  batchStride?: number;
}): VgiFunction {
  return defineTableFunction<SplitArgs, SplitState>({
    name: cfg.name,
    description: cfg.description,
    // position -1 = a NAMED argument, so the shared SQL suite's
    // `split_sequence(n := 10, splits := 4)` binds identically across SDKs.
    // Both NAMED: in this SDK an arg with a default gets a string position (named)
  // and one without gets an integer position (positional). The shared suite calls
  // `split_sequence(n := 30, splits := 3)`, so `n` needs a default too.
  args: { n: new Int64(), splits: new Int64() },
    argDefaults: { n: 0, splits: 4 },
    // The declaration is what a distributed engine reads to decide it can retry
    // a task against this function — and what makes the client call plan() at
    // all. Without it the scan is silently never divided.
    supportsSplits: true,
    supportsBatchIndex: cfg.batchStride !== undefined,
    splitTokenTtlSeconds: cfg.splitTokenTtlSeconds,
    onBind: () => ({ outputSchema: SPLIT_SCHEMA }),
    plan: (params: TableBindParams<SplitArgs>, request: TableFunctionPlanRequest): PlanResult => {
      const n = Number(params.args.n ?? 0);
      const all = cfg.ranges({ n, splits: Number(params.args.splits ?? 4) });

      if (cfg.perPage !== undefined) {
        // Pagination: hand out one window per call, cursoring on the page index.
        // The range list is regenerable from the bind arguments alone, so the
        // cursor needs to carry nothing else.
        const cursor = request.cursor;
        let page = 0;
        if (cursor && cursor.length === 8) {
          const v = new DataView(cursor.buffer, cursor.byteOffset, cursor.byteLength);
          page = Number(v.getBigUint64(0, true));
        }
        const lo = page * cfg.perPage;
        const window = all.slice(lo, lo + cfg.perPage);
        const done = lo + cfg.perPage >= all.length;
        const result = planOf(window, n, cfg.batchStride !== undefined, lo);
        if (!done) {
          const buf = new ArrayBuffer(8);
          new DataView(buf).setBigUint64(0, BigInt(page + 1), true);
          result.nextCursors = [new Uint8Array(buf)];
          result.estimatedTotalRows = null;
        }
        return result;
      }

      const result = planOf(all, n, cfg.batchStride !== undefined);
      if (cfg.catalogVersion !== undefined) result.catalogVersion = cfg.catalogVersion;
      return result;
    },
    // The explicit opt-in: a worker that mints splits must be able to redeem
    // them. The ranges are read off `splitPayloads` in initialState, so there is
    // nothing to do here beyond declaring the capability.
    onSplit: () => undefined,
    initialState: (params: TableProcessParams<SplitArgs>) =>
      seedState(cfg.name, params, cfg.batchStride !== undefined),
    process: (params, state, out) =>
      emitNext(params, state, out, {
        maxBatch: cfg.cacheTtl !== undefined ? 16 : cfg.batchStride !== undefined ? 64 : 1024,
        cacheTtl: cfg.cacheTtl,
        batchStride: cfg.batchStride,
      }),
  }) as unknown as VgiFunction;
}

// --- the fixtures ---------------------------------------------------------

/** The parity twin: `split_sequence(n)` must equal `sequence(n)` row for row. */
export const splitSequence = splitFixture({
  name: "split_sequence",
  description: "Split-capable twin of sequence(n): 0..n-1 divided into `splits` ranges",
  ranges: (a) => evenRanges(a.n, a.splits),
});

/** Returns NO splits. Legal, and it must produce an empty result rather than a
 *  crash — a fully-pruned scan reaches exactly this. */
export const splitZero = splitFixture({
  name: "split_zero",
  description: "Returns zero splits: a legal empty result, not an error",
  ranges: () => [],
});

/** Interleaves EMPTY splits between non-empty ones. This is the shape that
 *  silently truncates a scan if a reader mistakes an empty split for
 *  end-of-stream, and it is far likelier in practice than zero splits. */
export const splitEmptyRanges = splitFixture({
  name: "split_empty_ranges",
  description: "Some splits yield zero rows; the scan must not end early",
  ranges: (a) =>
    evenRanges(a.n, a.splits).flatMap((r) => [{ lo: r.lo, hi: r.lo }, r]),
});

/** One split holds ~99% of the rows, so greedy per-split claiming is
 *  distinguishable from static assignment: under greedy claiming the fast
 *  readers keep working while one reader owns the big split. The row count is
 *  identical either way, so this is about MAKESPAN, not correctness. */
export const splitSkewed = splitFixture({
  name: "split_skewed",
  description: "One split ~100x the others: exercises greedy claiming under skew",
  ranges: (a) => {
    if (a.n <= 0 || a.splits <= 0) return [];
    const head = Math.floor((a.n * 99) / 100);
    return [
      { lo: 0, hi: head },
      ...evenRanges(a.n - head, a.splits - 1).map((r) => ({
        lo: head + r.lo,
        hi: head + r.hi,
      })),
    ];
  },
});

/** Far more splits than reader threads, which forces sequential re-init on a
 *  REUSED connection — the path where a split-init failure would otherwise pool
 *  a connection with an unanswered init in flight. */
export const splitMany = splitFixture({
  name: "split_many",
  description: "Far more splits than threads: exercises greedy claiming and re-init",
  ranges: (a) => evenRanges(a.n, a.splits <= 0 ? 1000 : a.splits),
});

/** Enumerates its plan over several pages, each disjoint from the last.
 *
 *  Disjointness is the worker's obligation and no client checks it — a dedup was
 *  tried and removed, because it needed a copy of every token, it compared token
 *  bytes and so could never fire on a keyed worker, and the most a client can do
 *  with a duplicate is refuse anyway. This is the well-behaved side of that. */
export const splitPaginated = splitFixture({
  name: "split_paginated",
  description: "Plan enumerated over several disjoint pages",
  ranges: (a) => evenRanges(a.n, a.splits),
  perPage: 4,
});

/** Pins its plan to a catalog version that has moved on.
 *
 *  The only way a bad split token is reachable through SQL, and deliberately so:
 *  the framework owns the envelope, so a worker cannot mint a wrong fingerprint
 *  or clear a seal even on purpose. What it CAN do is plan against a snapshot
 *  that is no longer current — exactly the situation SPLIT_SNAPSHOT_EXPIRED
 *  names. The refusal must stay distinguishable from SPLIT_TOKEN_INVALID,
 *  because only this one means "re-run the query". */
export const splitStalePlan = splitFixture({
  name: "split_stale_plan",
  description: "Plans against a catalog version that is not the live one",
  ranges: (a) => evenRanges(a.n, a.splits),
  // Any value the live catalog will not report. The fixture catalog's version is
  // small, so a large constant is reliably "not current" without depending on
  // what that version happens to be.
  catalogVersion: 987654321,
});

/** Declares a split-token lifetime shorter than any client's scheduling horizon.
 *
 *  An expired token is a failed query, not a degradation: nothing re-plans when
 *  one expires, because a distributed engine retries the serialized task it was
 *  handed and has no path back to the planner. So the only useful moment to
 *  notice a too-short lifetime is BEFORE the plan is issued. One second is
 *  unusable everywhere — even DuckDB, whose horizon is the shortest of any
 *  engine because it plans at execution start, can take longer to reach a split. */
export const splitShortTtl = splitFixture({
  name: "split_short_ttl",
  description: "Declares a 1s split-token TTL, below any client horizon",
  ranges: (a) => evenRanges(a.n, a.splits),
  splitTokenTtlSeconds: 1,
});

/** Split-capable AND supports_batch_index, which together are a contract.
 *
 *  A batch index must be globally monotonic per reader, and greedy per-split
 *  claiming re-initializes the same connection for each split — so every split
 *  starts a fresh stream, and a worker that restarted its numbering per split
 *  would hand one reader a DECREASING index.
 *
 *  What makes it work is that the client's claim counter hands each reader
 *  strictly ASCENDING split indices, so a worker deriving its index from the
 *  split's position in a globally-ordered space is monotonic by construction.
 *  That is the whole reason claiming is greedy rather than grouped, and it is
 *  NOT something multi-token init provides — a group's tokens carry no ordering. */
export const splitBatchIndex = splitFixture({
  name: "split_batch_index",
  description: "Split-capable with per-split batch_index space",
  ranges: (a) => evenRanges(a.n, a.splits),
  batchStride: 1000,
});

/** A split scan whose result is cacheable, so never-partial becomes assertable.
 *
 *  The result cache knows nothing about splits, deliberately: its key describes
 *  the QUERY, while splits are how the rows were produced. What that makes
 *  testable is that a scan abandoned partway — by a LIMIT satisfied early, or by
 *  an error — commits NOTHING: storing what was captured would put a SUBSET
 *  under a key claiming to be the whole answer, and every later identical query
 *  would return missing rows with no error at all. */
export const splitCacheable = splitFixture({
  name: "split_cacheable",
  description: "Split-capable and cacheable, for the never-partial gate",
  ranges: (a) => evenRanges(a.n, a.splits),
  cacheTtl: 300,
});

// --- fixtures with shapes of their own ------------------------------------

interface SplitFailArgs {
  n: number;
  splits: number;
  fail_at: number;
  fail_in_init: boolean;
}

interface FailState extends SplitState {
  failAt: number;
}

function encodeFail(ordinal: number, r: Range): Uint8Array {
  return encodeOrdinal(ordinal, r);
}

/**
 * Fails on a chosen split, in either of the two places that matter. They are
 * genuinely different failure paths, not variations:
 *
 * - `fail_in_init` fails while REDEEMING the token, before any row is produced.
 *   The client must not return that connection to the pool — the init request is
 *   on the wire with no answer, so a later checkout would read this split's init
 *   response as its own stream header: silent cross-query corruption on the
 *   `pool true` default.
 * - Otherwise it fails MID-STREAM, after emitting rows, so the capture is
 *   genuinely partial when it dies. A partial result committed as complete is
 *   the failure class the never-partial gate exists to prevent.
 */
export const splitFailAt = defineTableFunction<SplitFailArgs, FailState>({
  name: "split_fail_at",
  description: "Fails on a chosen split, at init or mid-stream",
  args: {
    n: new Int64(),
    splits: new Int64(),
    fail_at: new Int64(),
    fail_in_init: new Bool(),
  },
  argDefaults: { n: 0, splits: 4, fail_at: -1, fail_in_init: false },
  supportsSplits: true,
  onBind: () => ({ outputSchema: SPLIT_SCHEMA }),
  plan: (params: TableBindParams<SplitFailArgs>): PlanResult => {
    const ranges = evenRanges(Number(params.args.n ?? 0), Number(params.args.splits ?? 4));
    return {
      splits: ranges.map((r, i) => ({
        payload: encodeFail(i, r),
        estimatedRows: r.hi - r.lo,
        rowsExact: true,
      })),
      estimatedTotalSplits: ranges.length,
    };
  },
  // Redemption is where the init-time failure lands, so the client's
  // connection-poisoning path is exercised rather than the mid-stream one.
  onSplit: (payloads: Uint8Array[], params: TableBindParams<SplitFailArgs>) => {
    if (!params.args.fail_in_init) return;
    for (const p of payloads) {
      const [ordinal] = decodeOrdinal(p);
      if (ordinal === Number(params.args.fail_at)) {
        throw new Error(`split ${ordinal} refuses to initialize (fixture)`);
      }
    }
  },
  initialState: (params: TableProcessParams<SplitFailArgs>) => ({
    ...seedState("split_fail_at", params, true),
    failAt: Number(params.args.fail_at ?? -1),
  }),
  process: (params, state, out) => {
    for (;;) {
      if (state.idx >= state.ranges.length) {
        out.finish();
        return;
      }
      const r = state.ranges[state.idx];
      if (state.cur >= r.hi) {
        state.idx += 1;
        if (state.idx < state.ranges.length) state.cur = state.ranges[state.idx].lo;
        continue;
      }
      // Mid-stream: emit some rows first, so the capture is genuinely partial
      // when it dies rather than empty.
      if (state.ordinals[state.idx] === state.failAt && state.cur > r.lo) {
        throw new Error(`split ${state.failAt} failed mid-stream (fixture)`);
      }
      const size = Math.min(r.hi - state.cur, 8);
      const values: bigint[] = [];
      for (let i = 0; i < size; i++) values.push(BigInt(state.cur + i));
      state.cur += size;
      out.emit(batchFromColumns({ n: values }, params.outputSchema));
      return;
    }
  },
}) as unknown as VgiFunction;

/**
 * Paginates forever: every plan page returns a cursor and never exhausts it.
 *
 * A worker can hang a client this way by accident as easily as on purpose, and
 * the failure mode is the bad one: a client that stopped early would scan a
 * PARTIAL enumeration and report it as the whole answer. The client must hit its
 * page cap and throw an error naming it — never truncate and proceed.
 */
export const splitEndlessCursor = defineTableFunction<SplitArgs, SplitState>({
  name: "split_endless_cursor",
  description: "Paginates forever: the client must hit its page cap, not truncate",
  // Both NAMED: in this SDK an arg with a default gets a string position (named)
  // and one without gets an integer position (positional). The shared suite calls
  // `split_sequence(n := 30, splits := 3)`, so `n` needs a default too.
  args: { n: new Int64(), splits: new Int64() },
  argDefaults: { n: 0, splits: 4 },
  supportsSplits: true,
  onBind: () => ({ outputSchema: SPLIT_SCHEMA }),
  plan: (_params, request: TableFunctionPlanRequest): PlanResult => {
    const page = request.cursor?.length ?? 0;
    return {
      splits: [{ payload: encode({ lo: 0, hi: 1 }) }],
      nextCursors: [new Uint8Array(page + 1).fill(120)],
    };
  },
  onSplit: () => undefined,
  initialState: (params) => seedState("split_endless_cursor", params),
  process: (params, state, out) => emitNext(params, state, out),
}) as unknown as VgiFunction;

const ECHO_SCHEMA = new Schema([
  new Field("split_ordinal", new Int64(), true),
  new Field("saw_filters", new Bool(), true),
  new Field("n_projection", new Int64(), true),
]);

interface EchoArgs {
  splits: number;
}
interface EchoState {
  rows: Array<[number, boolean, number]>;
  done: boolean;
}

/**
 * Reports, per split, what pushdown the PLAN call actually received.
 *
 * A row-count assertion cannot catch a pushdown regression — the rows are the
 * same either way — so this makes the pushdown itself the data. What it reports
 * is recorded at PLAN time and baked into each split's payload, which is the
 * claim under test: filters and projection must reach `plan()`, not merely reach
 * the per-split `init()` afterwards.
 */
export const splitEchoFilters = defineTableFunction<EchoArgs, EchoState>({
  name: "split_echo_filters",
  description: "Reports, per split, what pushdown the plan call received",
  args: { splits: new Int64() },
  argDefaults: { splits: 3 },
  // filter_pushdown declares that this worker APPLIES the filter, so DuckDB
  // stops re-checking it above the scan. Declaring it while only REPORTING the
  // filter would be the "wrong answers if declared falsely" hazard in miniature.
  // autoApplyFilters makes the declaration true.
  filterPushdown: true,
  autoApplyFilters: true,
  supportsSplits: true,
  onBind: () => ({ outputSchema: ECHO_SCHEMA }),
  plan: (params: TableBindParams<EchoArgs>, request: TableFunctionPlanRequest): PlanResult => {
    const sawFilters = request.pushdown_filters != null;
    const nProjection = request.projection_ids?.length ?? 0;
    const n = Number(params.args.splits ?? 3);
    const splits = [];
    for (let i = 0; i < n; i++) {
      splits.push({ payload: encodeOrdinal(i, { lo: sawFilters ? 1 : 0, hi: nProjection }) });
    }
    return { splits, estimatedTotalSplits: n };
  },
  onSplit: () => undefined,
  initialState: (params: TableProcessParams<EchoArgs>): EchoState => {
    if (params.splitPayloads === undefined) {
      throw new Error("split_echo_filters is split-only but was initialized with no split tokens");
    }
    return {
      rows: params.splitPayloads.map((p) => {
        const [ordinal, r] = decodeOrdinal(p);
        return [ordinal, r.lo === 1, r.hi] as [number, boolean, number];
      }),
      done: false,
    };
  },
  process: (params, state, out) => {
    if (state.done) {
      out.finish();
      return;
    }
    state.done = true;
    out.emit(
      batchFromColumns(
        {
          split_ordinal: state.rows.map((r) => BigInt(r[0])),
          saw_filters: state.rows.map((r) => r[1]),
          n_projection: state.rows.map((r) => BigInt(r[2])),
        },
        params.outputSchema,
      ),
    );
  },
}) as unknown as VgiFunction;

const PARTITION_COUNTRIES = ["US", "DE", "JP", "BR"];

/** Mark a field as a partition column, as `vgi.schema_utils.partition_field` does. */
function partitionField(name: string, type: any): Field {
  return new Field(name, type, true, new Map([["vgi.partition_column", "true"]]));
}

const PARTITION_FIELDS = [partitionField("country", new Utf8())];
const PARTITION_SCHEMA = new Schema([
  PARTITION_FIELDS[0],
  new Field("sales", new Int64(), true),
]);

function b64(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
  return btoa(s);
}

/**
 * The `vgi_partition_values#b64` payload: a 2-row (min, max) batch over the
 * partition columns. Under SINGLE_VALUE min == max, which is what lets the
 * client read row 0 as the exact partition key.
 */
function partitionValuesMetadata(country: string): Map<string, string> {
  const schema = new Schema(PARTITION_FIELDS);
  const batch = batchFromColumns({ country: [country, country] }, schema);
  return new Map([["vgi_partition_values#b64", b64(serializeBatch(batch))]]);
}

interface PartArgs {
  rows_per_country: number;
}
interface PartState {
  indices: number[];
  at: number;
  rows: number;
}

/**
 * One split per partition — the shape a partitioned table naturally takes.
 *
 * A partition and a split are different things that usually coincide: a
 * partition is a property of the DATA (every row here shares a value), a split
 * is a unit of WORK. A worker that already stores data per partition has its
 * split boundaries handed to it, so this is the common case rather than a
 * contrived one.
 *
 * What needs asserting is that the two survive each other. Splits are claimed
 * greedily, in an order nobody chose, by readers that each end up holding
 * several — so the association between a batch and the partition value it
 * carries has to hold through re-init on a reused connection and across the
 * boundary where one reader moves from one partition to the next. Losing it does
 * not raise: it produces a GROUP BY that silently mixes partitions.
 */
export const splitPartitioned = defineTableFunction<PartArgs, PartState>({
  name: "split_partitioned",
  description: "One split per partition, with partition values on each batch",
  args: { rows_per_country: new Int64() },
  argDefaults: { rows_per_country: 5 },
  supportsSplits: true,
  partitionKind: "SINGLE_VALUE_PARTITIONS",
  onBind: () => ({ outputSchema: PARTITION_SCHEMA }),
  // The payload names the partition by INDEX, so a redemption reads the same
  // partition however many times it runs and in whichever process.
  plan: (): PlanResult => ({
    splits: PARTITION_COUNTRIES.map((_, i) => ({
      payload: encode({ lo: i, hi: i }),
    })),
    estimatedTotalSplits: PARTITION_COUNTRIES.length,
  }),
  onSplit: () => undefined,
  initialState: (params: TableProcessParams<PartArgs>): PartState => {
    if (params.splitPayloads === undefined) {
      throw new Error("split_partitioned is split-only but was initialized with no split tokens");
    }
    return {
      indices: params.splitPayloads.map((p) => decode(p).lo),
      at: 0,
      rows: Number(params.args.rows_per_country ?? 5),
    };
  },
  process: (params, state, out) => {
    // A partition with zero rows is STEPPED OVER, never reported as
    // end-of-stream — the same rule every split fixture follows, and here it is
    // reachable through `rows_per_country := 0`.
    for (;;) {
      if (state.at >= state.indices.length) {
        out.finish();
        return;
      }
      const ci = state.indices[state.at];
      state.at += 1;
      if (state.rows <= 0 || ci < 0 || ci >= PARTITION_COUNTRIES.length) continue;
      // Each partition's values are offset by its own index, so swapping two
      // splits' labels MOVES the per-partition sums. With identical values
      // everywhere a mislabelled partition would be invisible in the totals.
      const base = ci * 100;
      const country: string[] = [];
      const sales: bigint[] = [];
      for (let i = 1; i <= state.rows; i++) {
        country.push(PARTITION_COUNTRIES[ci]);
        sales.push(BigInt(base + i));
      }
      out.emit(
        batchFromColumns({ country, sales }, params.outputSchema),
        partitionValuesMetadata(PARTITION_COUNTRIES[ci]),
      );
      return;
    }
  },
}) as unknown as VgiFunction;

/**
 * The CANONICAL cross-SDK rendering of a pushed-down filter set.
 *
 * Every SDK must produce this byte-for-byte, because the shared SQL suite
 * asserts on the string. A language's own debug formatting cannot be used —
 * Python's `repr(PushdownFilters)` is Python-shaped and no other SDK can
 * reproduce it, so a test asserting it could only ever pass against that one
 * worker, which defeats the point of a shared suite.
 *
 * For each filtered column in sorted order: `col>=min` and/or `col<=max`, joined
 * by `,`. Bounds are normalized to INCLUSIVE integers, because that is the only
 * form every SDK can produce (Rust's ColumnBounds carries no inclusive flag).
 * Values are included deliberately: without them a tightening Top-N filter and a
 * loose one render identically and the test cannot tell them apart.
 */
export function renderFiltersCanonical(pf: PushdownFilters | undefined): string {
  if (!pf || pf.filters.length === 0) return "(none)";
  const bounds = new Map<string, { min?: bigint; max?: bigint }>();
  const note = (col: string, key: "min" | "max", v: bigint) => {
    const b = bounds.get(col) ?? {};
    // Widest-wins, matching the other SDKs: a looser bound is always sound, and
    // agreeing on the rule matters more than tightness for a rendering.
    if (key === "min") b.min = b.min === undefined ? v : (v < b.min ? v : b.min);
    else b.max = b.max === undefined ? v : (v > b.max ? v : b.max);
    bounds.set(col, b);
  };
  // Recursive: a compound predicate arrives as and([constant, constant]), so
  // walking only the top level renders "(none)" for exactly the multi-clause
  // filters worth asserting on.
  const walk = (f: any): void => {
    switch (f.type) {
      case "and":
      case "or":
        for (const c of f.children) walk(c);
        return;
      case "constant": {
        const v = BigInt(f.value);
        // Exclusive comparisons are tightened by one: bounds are integer here,
        // so `< v` is exactly `<= v - 1` and the normalization is lossless.
        if (f.op === "ge") note(f.columnName, "min", v);
        else if (f.op === "gt") note(f.columnName, "min", v + 1n);
        else if (f.op === "le") note(f.columnName, "max", v);
        else if (f.op === "lt") note(f.columnName, "max", v - 1n);
        else if (f.op === "eq") {
          note(f.columnName, "min", v);
          note(f.columnName, "max", v);
        }
        return;
      }
      case "in": {
        // An IN set implies bounds — [min(values), max(values)] — and a join-key
        // filter IS an IN set once its side batch is resolved. Skipping them
        // means a worker pruning by range gets NOTHING from a join-key pushdown,
        // the single most valuable pushdown a scan receives.
        let lo: bigint | undefined;
        let hi: bigint | undefined;
        for (const raw of f.values) {
          if (raw == null) continue;
          const v = BigInt(raw);
          if (lo === undefined || v < lo) lo = v;
          if (hi === undefined || v > hi) hi = v;
        }
        if (lo !== undefined) note(f.columnName, "min", lo);
        if (hi !== undefined) note(f.columnName, "max", hi);
        return;
      }
      default:
        return;
    }
  };
  for (const f of pf.filters) walk(f);

  const parts: string[] = [];
  for (const col of [...bounds.keys()].sort()) {
    const b = bounds.get(col)!;
    if (b.min !== undefined) parts.push(`${col}>=${b.min}`);
    if (b.max !== undefined) parts.push(`${col}<=${b.max}`);
  }
  return parts.length > 0 ? parts.join(",") : "(none)";
}

const DYN_SCHEMA = new Schema([
  new Field("n", new Int64(), true),
  new Field("pushed_filters", new Utf8(), true),
]);

/**
 * Echoes the DYNAMIC filter each tick carried, per split.
 *
 * A plan is built from STATIC filters only — join-key values are not known when
 * the plan RPC fires, so they cannot prune the split SET. They arrive later, per
 * tick, and prune WITHIN each split. Both halves have to keep working once a
 * reader re-initializes the same connection per split: the tick filter state is
 * a property of the connection, and a split that lost it would silently stop
 * pruning.
 *
 * "Silently" is the operative word, and it is why this reports the filter as
 * DATA rather than leaving the test to infer it from row counts. A scan that
 * stopped receiving dynamic filters returns exactly the same rows — DuckDB
 * re-checks the predicate above the scan — just after shipping more of them.
 */
export const splitDynamicFilter = defineTableFunction<SplitArgs, SplitState>({
  name: "split_dynamic_filter",
  description: "Echoes the dynamic filter each tick carried, per split",
  // Both NAMED: in this SDK an arg with a default gets a string position (named)
  // and one without gets an integer position (positional). The shared suite calls
  // `split_sequence(n := 30, splits := 3)`, so `n` needs a default too.
  args: { n: new Int64(), splits: new Int64() },
  argDefaults: { n: 0, splits: 4 },
  projectionPushdown: true,
  filterPushdown: true,
  autoApplyFilters: true,
  supportsSplits: true,
  onBind: () => ({ outputSchema: DYN_SCHEMA }),
  // Report the row count, which decides which side of a join this lands on.
  // Without it DuckDB assumes a default (large) cardinality and puts the scan on
  // the BUILD side of a hash join — where no join-key IN filter is pushed into
  // it, because the filter goes to the probe side. The scan then reads
  // everything and DuckDB filters above it: right answers, no pushdown, and
  // nothing in the result to say so. Nothing about splits causes that.
  cardinality: (params: TableBindParams<SplitArgs>) => ({
    estimate: Number(params.args.n ?? 0),
    max: Number(params.args.n ?? 0),
  }),
  plan: (params: TableBindParams<SplitArgs>): PlanResult => {
    const ranges = evenRanges(Number(params.args.n ?? 0), Number(params.args.splits ?? 4));
    return {
      splits: ranges.map((r) => ({ payload: encode(r) })),
      estimatedTotalSplits: ranges.length,
    };
  },
  onSplit: () => undefined,
  initialState: (params) => seedState("split_dynamic_filter", params),
  process: (params, state, out) => {
    const rendered = renderFiltersCanonical(params.pushdownFilters);
    for (;;) {
      if (state.idx >= state.ranges.length) {
        out.finish();
        return;
      }
      const r = state.ranges[state.idx];
      if (state.cur >= r.hi) {
        state.idx += 1;
        if (state.idx < state.ranges.length) state.cur = state.ranges[state.idx].lo;
        continue;
      }
      const size = Math.min(r.hi - state.cur, 4);
      const n: bigint[] = [];
      const filters: string[] = [];
      for (let i = 0; i < size; i++) {
        n.push(BigInt(state.cur + i));
        filters.push(rendered);
      }
      state.cur += size;
      out.emit(batchFromColumns({ n, pushed_filters: filters }, params.outputSchema));
      return;
    }
  },
}) as unknown as VgiFunction;

/** Every split fixture, for registration in the example worker. */
export const splitFunctions: VgiFunction[] = [
  splitSequence,
  splitZero,
  splitEmptyRanges,
  splitSkewed,
  splitMany,
  splitPaginated,
  splitStalePlan,
  splitShortTtl,
  splitBatchIndex,
  splitCacheable,
  splitFailAt,
  splitEndlessCursor,
  splitEchoFilters,
  splitPartitioned,
  splitDynamicFilter,
];
