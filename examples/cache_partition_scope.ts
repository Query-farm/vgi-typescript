// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Per-PARTITION result-cache fixtures (`vgi.cache.partition_scope`).
// Ports vgi-python/vgi/_test_fixtures/table/cache.py's partition-scope block
// (and matches vgi-go examples/table/cache_partition_scope.go).
//
// A SINGLE_VALUE_PARTITIONS function that advertises `partitionScope` has its
// result ADDITIONALLY stored split by partition value (one entry per distinct
// partition tuple), so a later `=`/`IN`-filtered scan on the partition
// column(s) serves the requested partitions from cache without calling the
// worker. The whole-scan entry is still stored, so the opt-in is purely
// additive.
//
// See cache.ts for the whole-scan cacheable fixtures; these four back
// vgi's test/sql/integration/cache/partition_scope*.test.

import { Schema, Field, Int64, Utf8 } from "@query-farm/apache-arrow";
import {
  defineTableFunction,
  batchFromColumns,
  serializeBatch,
  cacheControlMetadata,
  DEFAULT_MAX_WORKERS,
  type VgiFunction,
} from "../src/index.js";

// Freshness lifetime for every fixture here — long enough that TTL never
// lapses mid-test.
const DEFAULT_TTL_SECONDS = 300;

// Mark a field as a partition column — see table_partition.ts / cache.ts.
function partitionField(name: string, type: any): Field {
  return new Field(name, type, true, new Map([["vgi.partition_column", "true"]]));
}

function b64encode(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
  return btoa(s);
}

// Build the `vgi_partition_values#b64` metadata payload — a 2-row (min, max)
// Arrow batch over the partition fields, IPC-serialized then base64'd. `extra`
// entries are merged in first (we always fold the cache-control keys through
// here so one metadata map carries both).
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

// What every per-partition fixture advertises.
//
// Unlike the whole-scan fixtures (which advertise on the first batch only) this
// goes on EVERY batch: the extension latches the first cache-control it sees,
// and on a fall-through scan the leading partition can be filtered to zero
// rows, which would drop a first-batch-only advertisement.
const partitionScopeCacheControl = (): Map<string, string> =>
  cacheControlMetadata({ ttl: DEFAULT_TTL_SECONDS, partitionScope: true });

// Queue item encoding: one float64 index. Values stay well within 2^53 for the
// test sizes — matches packOne in table_partition.ts.
function packOne(a: number): Uint8Array {
  const buf = new ArrayBuffer(8);
  new DataView(buf).setFloat64(0, a);
  return new Uint8Array(buf);
}
function unpackOne(bytes: Uint8Array): number {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getFloat64(0);
}

const int64Range = (start: number, end: number): bigint[] => {
  const out: bigint[] = [];
  for (let i = start; i < end; i++) out.push(BigInt(i));
  return out;
};

// A plain cursor over a fixed partition list.
interface PartitionCursorState {
  idx: number;
}

// ---------------------------------------------------------------------------
// cache_partition_scope — the baseline per-partition opt-in
// ---------------------------------------------------------------------------
// filterPushdown + autoApplyFilters means a `WHERE country = ...` predicate
// reaches the worker as a real filter (so the client can enumerate the
// requested set) and the framework prunes emitted batches to it — required for
// row correctness on a fall-through scan, because DuckDB does NOT re-apply a
// pushed predicate above the scan.

const SCOPE_COUNTRIES = ["AU", "BR", "CA", "FR", "US"];
const COUNTRY_PV_FIELDS = [new Field("country", new Utf8(), true)];
const SCOPE_SCHEMA = new Schema([
  partitionField("country", new Utf8()),
  new Field("sales", new Int64(), true),
]);

interface RowsPerCountryArgs {
  rows_per_country: number;
}

const cache_partition_scope = defineTableFunction<RowsPerCountryArgs, PartitionCursorState>({
  name: "cache_partition_scope",
  description:
    "Per-partition cacheable single-value-partitioned result (vgi.cache.partition_scope)",
  args: { rows_per_country: new Int64() },
  argDocs: { rows_per_country: "Rows per country partition" },
  argConstraints: { rows_per_country: { ge: 1 } },
  partitionKind: "SINGLE_VALUE_PARTITIONS",
  filterPushdown: true,
  autoApplyFilters: true,
  onBind: () => ({ outputSchema: SCOPE_SCHEMA }),
  initialState: () => ({ idx: 0 }),
  process: (params, state, out) => {
    if (state.idx >= SCOPE_COUNTRIES.length) {
      out.finish();
      return;
    }
    const country = SCOPE_COUNTRIES[state.idx];
    const rows = Number(params.args.rows_per_country);
    const base = state.idx * 1_000_000;
    out.emit(
      batchFromColumns(
        { country: new Array(rows).fill(country), sales: int64Range(base, base + rows) },
        params.outputSchema,
      ),
      partitionValuesMetadata(
        COUNTRY_PV_FIELDS,
        { country: [country, country] },
        partitionScopeCacheControl(),
      ),
    );
    state.idx += 1;
  },
  examples: [
    {
      sql: "SELECT * FROM cache_partition_scope(10) WHERE country = 'US'",
      description: "Per-partition cache serve for one country",
    },
  ],
  categories: ["generator", "cache", "testing", "partitioning"],
  tags: { category: "cache", type: "partitioned" },
});

// ---------------------------------------------------------------------------
// cache_partition_parallel — work-queue fan-out (PARALLEL capture) + a NULL partition
// ---------------------------------------------------------------------------
// The multi-worker partner of cache_partition_scope: partitions are handed out
// through the shared work queue, so a `threads=N` + `pool false` scan fans them
// across N workers and the per-partition split at commit must bucket batches
// drawn from MULTIPLE capture substreams.
//
// The list includes a null entry: SINGLE_VALUE permits a NULL partition, and
// `IS NULL` is deliberately NOT enumerable (only `=`/`IN`), so the fixture also
// pins the correct non-serve behaviour. The partition value is always supplied
// explicitly — an all-NULL column cannot distinguish "no rows" from "the NULL
// partition".

const PARALLEL_COUNTRIES: (string | null)[] = ["AU", "CA", "US", null];

const cache_partition_parallel = defineTableFunction<RowsPerCountryArgs, PartitionCursorState>({
  name: "cache_partition_parallel",
  description:
    "Per-partition cacheable; work-queue fan-out (parallel capture); one NULL partition",
  args: { rows_per_country: new Int64() },
  argDocs: { rows_per_country: "Rows per country partition" },
  argConstraints: { rows_per_country: { ge: 1 } },
  maxWorkers: DEFAULT_MAX_WORKERS,
  partitionKind: "SINGLE_VALUE_PARTITIONS",
  filterPushdown: true,
  autoApplyFilters: true,
  onBind: () => ({ outputSchema: SCOPE_SCHEMA }),
  onInit: async (params) => {
    await params.storage.queuePush(PARALLEL_COUNTRIES.map((_, i) => packOne(i)));
    return { max_workers: DEFAULT_MAX_WORKERS, execution_id: params.executionId, opaque_data: null };
  },
  initialState: () => ({ idx: -1 }),
  process: async (params, state, out) => {
    const item = await params.storage!.queuePop();
    if (item === null) {
      out.finish();
      return;
    }
    state.idx = unpackOne(item);
    const country = PARALLEL_COUNTRIES[state.idx];
    const rows = Number(params.args.rows_per_country);
    const base = state.idx * 1_000_000;
    out.emit(
      batchFromColumns(
        { country: new Array(rows).fill(country), sales: int64Range(base, base + rows) },
        params.outputSchema,
      ),
      partitionValuesMetadata(
        COUNTRY_PV_FIELDS,
        { country: [country, country] },
        partitionScopeCacheControl(),
      ),
    );
  },
  categories: ["generator", "cache", "testing", "partitioning"],
  tags: { category: "cache", type: "partitioned" },
});

// ---------------------------------------------------------------------------
// cache_partition_multicol — MULTI-COLUMN (region, year) SINGLE_VALUE partitions
// ---------------------------------------------------------------------------
// Exercises cross-product enumeration (region IN x year IN), 2-column tuple
// canonicalization, and the partial-constraint fall-through (region
// constrained, year free -> not enumerable).
//
// The years are NON-contiguous on purpose: DuckDB rewrites
// `year IN (2020, 2021)` (contiguous ints) into a BETWEEN range, which is not
// enumerable, so a gap keeps the pushed filter an IN filter and the
// cross-product path is actually taken.

const REGION_YEARS: [string, number][] = [
  ["EU", 2020],
  ["EU", 2022],
  ["US", 2020],
  ["US", 2022],
];
const REGION_YEAR_PV_FIELDS = [
  new Field("region", new Utf8(), true),
  new Field("year", new Int64(), true),
];
const MULTICOL_SCHEMA = new Schema([
  partitionField("region", new Utf8()),
  partitionField("year", new Int64()),
  new Field("amount", new Int64(), true),
]);

interface RowsPerPartitionArgs {
  rows_per_partition: number;
}

const cache_partition_multicol = defineTableFunction<RowsPerPartitionArgs, PartitionCursorState>({
  name: "cache_partition_multicol",
  description: "Per-partition cacheable over (region, year) SINGLE_VALUE partition columns",
  args: { rows_per_partition: new Int64() },
  argDocs: { rows_per_partition: "Rows per (region, year) partition" },
  argConstraints: { rows_per_partition: { ge: 1 } },
  partitionKind: "SINGLE_VALUE_PARTITIONS",
  filterPushdown: true,
  autoApplyFilters: true,
  onBind: () => ({ outputSchema: MULTICOL_SCHEMA }),
  initialState: () => ({ idx: 0 }),
  process: (params, state, out) => {
    if (state.idx >= REGION_YEARS.length) {
      out.finish();
      return;
    }
    const [region, year] = REGION_YEARS[state.idx];
    const rows = Number(params.args.rows_per_partition);
    const base = state.idx * 1000;
    out.emit(
      batchFromColumns(
        {
          region: new Array(rows).fill(region),
          year: new Array(rows).fill(BigInt(year)),
          amount: int64Range(base, base + rows),
        },
        params.outputSchema,
      ),
      partitionValuesMetadata(
        REGION_YEAR_PV_FIELDS,
        { region: [region, region], year: [BigInt(year), BigInt(year)] },
        partitionScopeCacheControl(),
      ),
    );
    state.idx += 1;
  },
  categories: ["generator", "cache", "testing", "partitioning"],
  tags: { category: "cache", type: "partitioned" },
});

// ---------------------------------------------------------------------------
// cache_partition_proj — projection pushdown + per-partition cache
// ---------------------------------------------------------------------------
// Projection becomes part of the cache key, and the explicit partition value
// keeps the split working even when the partition column itself is projected
// OUT of the emitted batch. `extra` is a non-partition column to project away
// while keeping `country` (so a `WHERE country = X` can still push).

const PROJ_COUNTRIES = ["CA", "US"];
const PROJ_SCHEMA = new Schema([
  partitionField("country", new Utf8()),
  new Field("sales", new Int64(), true),
  new Field("extra", new Int64(), true),
]);

const cache_partition_proj = defineTableFunction<RowsPerCountryArgs, PartitionCursorState>({
  name: "cache_partition_proj",
  description: "Per-partition cacheable with projection pushdown + explicit partition_values",
  args: { rows_per_country: new Int64() },
  argDocs: { rows_per_country: "Rows per country partition" },
  argConstraints: { rows_per_country: { ge: 1 } },
  partitionKind: "SINGLE_VALUE_PARTITIONS",
  projectionPushdown: true,
  filterPushdown: true,
  autoApplyFilters: true,
  onBind: () => ({ outputSchema: PROJ_SCHEMA }),
  initialState: () => ({ idx: 0 }),
  process: (params, state, out) => {
    if (state.idx >= PROJ_COUNTRIES.length) {
      out.finish();
      return;
    }
    const country = PROJ_COUNTRIES[state.idx];
    const rows = Number(params.args.rows_per_country);
    const base = state.idx * 1_000_000;
    // Emit only the projected columns — params.outputSchema already reflects
    // the pushdown.
    const all: Record<string, any[]> = {
      country: new Array(rows).fill(country),
      sales: int64Range(base, base + rows),
      extra: int64Range(base + 500, base + 500 + rows),
    };
    const cols: Record<string, any[]> = {};
    for (const f of params.outputSchema.fields) cols[f.name] = all[f.name];
    out.emit(
      batchFromColumns(cols, params.outputSchema),
      partitionValuesMetadata(
        COUNTRY_PV_FIELDS,
        { country: [country, country] },
        partitionScopeCacheControl(),
      ),
    );
    state.idx += 1;
  },
  categories: ["generator", "cache", "testing", "partitioning"],
  tags: { category: "cache", type: "partitioned" },
});

export const cachePartitionScopeTableFunctions: VgiFunction[] = [
  cache_partition_scope,
  cache_partition_parallel,
  cache_partition_multicol,
  cache_partition_proj,
];
