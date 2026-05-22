// batch_index + partition_columns + transaction_storage table fixtures.
// Ports vgi-python/vgi/_test_fixtures/table/{batch_index,batch_index_broken,
// partition_columns,partition_columns_broken,transaction_storage}.py.

import { Schema, Field, Int64, Float64, Utf8 } from "@query-farm/apache-arrow";
import {
  defineTableFunction,
  batchFromColumns,
  serializeBatch,
  DEFAULT_MAX_WORKERS,
  OrderPreservation,
  functionStorage,
  type TableBindParams,
  type TableProcessParams,
} from "../src/index.js";
import type { OutputCollector } from "vgi-rpc";
import type { VgiFunction } from "../src/index.js";

const CHUNK_SIZE = 1000;
const BATCH_SIZE = 1000;

// Queue item encoding: (partition_id, start, end) as three doubles big enough
// for our test sizes. We use a DataView of 24 bytes (3 x float64) — values stay
// well within 2^53 for the test sizes, matching packQQ's approach in table.ts.
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
function packOne(a: number): Uint8Array {
  const buf = new ArrayBuffer(8);
  new DataView(buf).setFloat64(0, a);
  return new Uint8Array(buf);
}
function unpackOne(bytes: Uint8Array): number {
  return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getFloat64(0);
}

// Mark a field as a partition column. The C++ binder walks the bind output
// schema for fields whose metadata carries vgi.partition_column = "true" to
// resolve partition_column_indices. Mirrors vgi.schema_utils.partition_field.
function partitionField(name: string, type: any): Field {
  return new Field(name, type, true, new Map([["vgi.partition_column", "true"]]));
}

function b64encode(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i++) s += String.fromCharCode(bytes[i]);
  return btoa(s);
}

// Build the vgi_partition_values#b64 metadata payload: a 2-row (min,max) Arrow
// batch over the partition fields, serialized as IPC stream + base64. Mirrors
// vgi-python protocol._build_partition_values_batch / _serialize.
function partitionValuesMetadata(
  fields: Field[],
  values: Record<string, [any, any]>,
): Map<string, string> {
  const schema = new Schema(fields);
  const cols: Record<string, any[]> = {};
  for (const f of fields) cols[f.name] = [values[f.name][0], values[f.name][1]];
  const batch = batchFromColumns(cols, schema);
  const ipc = serializeBatch(batch);
  return new Map([["vgi_partition_values#b64", b64encode(ipc)]]);
}

// =============================================================================
// batch_index reference fixtures
// =============================================================================

interface BatchIndexArgs {
  count: number;
}
interface BatchIndexState {
  partitionId: number | null;
  currentStart: number | null;
  currentEnd: number | null;
  currentIdx: number;
}

const BATCH_INDEX_SCHEMA = new Schema([new Field("n", new Int64(), true)]);

const partitioned_batch_index = defineTableFunction<BatchIndexArgs, BatchIndexState>({
  name: "partitioned_batch_index",
  description:
    "Multi-worker partitioned sequence with per-batch batch_index tagging; parallel scan + ordered sink reassembly.",
  args: { count: new Int64() },
  maxWorkers: DEFAULT_MAX_WORKERS,
  preservesOrder: OrderPreservation.FIXED_ORDER,
  supportsBatchIndex: true,
  projectionPushdown: true,
  onBind: () => ({ outputSchema: BATCH_INDEX_SCHEMA }),
  cardinality: (p: TableBindParams<BatchIndexArgs>) => ({ estimate: p.args.count, max: p.args.count }),
  onInit: async (params) => {
    const items: Uint8Array[] = [];
    let partitionId = 0;
    for (let start = 0; start < params.args.count; start += CHUNK_SIZE) {
      const end = Math.min(start + CHUNK_SIZE, params.args.count);
      items.push(packTriple(partitionId, start, end));
      partitionId++;
    }
    await params.storage.queuePush(items);
    return { max_workers: DEFAULT_MAX_WORKERS, execution_id: params.executionId, opaque_data: null };
  },
  initialState: () => ({ partitionId: null, currentStart: null, currentEnd: null, currentIdx: 0 }),
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
    const batchEnd = Math.min(state.currentIdx + BATCH_SIZE, state.currentEnd!);
    const values: bigint[] = [];
    for (let i = state.currentIdx; i < batchEnd; i++) values.push(BigInt(i));
    out.emit(
      batchFromColumns({ n: values }, params.outputSchema),
      new Map([["vgi_batch_index", String(state.partitionId)]]),
    );
    state.currentIdx = batchEnd;
  },
  examples: [{ sql: "SELECT * FROM partitioned_batch_index(100)", description: "Generate 0..99 in parallel" }],
  categories: ["generator", "utility"],
});

interface BatchIndexMarkedArgs {
  count: number;
  chunk_size: number;
}

const BATCH_INDEX_MARKED_SCHEMA = new Schema([
  new Field("partition_id", new Int64(), true),
  new Field("seq", new Int64(), true),
]);
const MARKED_BATCH_SIZE = 256;

const partitioned_batch_index_marked = defineTableFunction<BatchIndexMarkedArgs, BatchIndexState>({
  name: "partitioned_batch_index_marked",
  description:
    "Two-column batch_index demo: rows are (partition_id, seq). Tests assert that DuckDB's ordered sinks reassemble output in partition_id order under parallel execution.",
  args: { count: new Int64(), chunk_size: new Int64() },
  argDefaults: { chunk_size: 1000 },
  maxWorkers: DEFAULT_MAX_WORKERS,
  preservesOrder: OrderPreservation.FIXED_ORDER,
  supportsBatchIndex: true,
  projectionPushdown: false,
  onBind: () => ({ outputSchema: BATCH_INDEX_MARKED_SCHEMA }),
  cardinality: (p: TableBindParams<BatchIndexMarkedArgs>) => ({ estimate: p.args.count, max: p.args.count }),
  onInit: async (params) => {
    const chunk = Number(params.args.chunk_size ?? 1000);
    const items: Uint8Array[] = [];
    let partitionId = 0;
    for (let start = 0; start < params.args.count; start += chunk) {
      const end = Math.min(start + chunk, params.args.count);
      items.push(packTriple(partitionId, start, end));
      partitionId++;
    }
    await params.storage.queuePush(items);
    return { max_workers: DEFAULT_MAX_WORKERS, execution_id: params.executionId, opaque_data: null };
  },
  initialState: () => ({ partitionId: null, currentStart: null, currentEnd: null, currentIdx: 0 }),
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
    const batchEnd = Math.min(state.currentIdx + MARKED_BATCH_SIZE, state.currentEnd!);
    const partitionIds: bigint[] = [];
    const seqs: bigint[] = [];
    for (let i = state.currentIdx; i < batchEnd; i++) {
      partitionIds.push(BigInt(state.partitionId!));
      seqs.push(BigInt(i - (state.currentStart ?? 0)));
    }
    out.emit(
      batchFromColumns({ partition_id: partitionIds, seq: seqs }, params.outputSchema),
      new Map([["vgi_batch_index", String(state.partitionId)]]),
    );
    state.currentIdx = batchEnd;
  },
  examples: [
    { sql: "SELECT * FROM partitioned_batch_index_marked(100, chunk_size := 25) LIMIT 5", description: "First 5 rows" },
  ],
  categories: ["generator", "utility", "testing"],
});

// =============================================================================
// batch_index broken fixtures (C++ contract enforcement)
// =============================================================================

interface BrokenArgs {
  count: number;
}
interface BrokenState {
  emitted: boolean;
}
const BROKEN_N_SCHEMA = new Schema([new Field("n", new Int64(), true)]);

const broken_missing_batch_index_tag = defineTableFunction<BrokenArgs, BrokenState>({
  name: "broken_missing_batch_index_tag",
  description:
    "DELIBERATELY BROKEN: declares supports_batch_index=True but emits a data batch with no vgi_batch_index metadata. C++ extension's contract check raises.",
  args: { count: new Int64() },
  preservesOrder: OrderPreservation.FIXED_ORDER,
  supportsBatchIndex: true,
  onBind: () => ({ outputSchema: BROKEN_N_SCHEMA }),
  initialState: () => ({ emitted: false }),
  process: (params, state, out) => {
    if (state.emitted) {
      out.finish();
      return;
    }
    const values: bigint[] = [];
    for (let i = 0; i < params.args.count; i++) values.push(BigInt(i));
    // Emit WITHOUT vgi_batch_index metadata — the C++ side raises.
    out.emit(batchFromColumns({ n: values }, params.outputSchema));
    state.emitted = true;
  },
  categories: ["testing", "broken"],
});

const broken_non_monotone_batch_index = defineTableFunction<BrokenArgs, BrokenState>({
  name: "broken_non_monotone_batch_index",
  description:
    "DELIBERATELY BROKEN: emits batches with strictly decreasing partition_id on one stream. C++ extension's monotonicity check raises (DuckDB's debug-only assertion is not relied upon).",
  args: { count: new Int64() },
  preservesOrder: OrderPreservation.FIXED_ORDER,
  supportsBatchIndex: true,
  onBind: () => ({ outputSchema: BROKEN_N_SCHEMA }),
  initialState: () => ({ emitted: false }),
  process: (params, state, out) => {
    if (state.emitted) {
      // Second call: emit with a LOWER batch_index than the first.
      out.emit(batchFromColumns({ n: [42n] }, params.outputSchema), new Map([["vgi_batch_index", "3"]]));
      out.finish();
      return;
    }
    const values: bigint[] = [];
    for (let i = 0; i < params.args.count; i++) values.push(BigInt(i));
    out.emit(batchFromColumns({ n: values }, params.outputSchema), new Map([["vgi_batch_index", "10"]]));
    state.emitted = true;
  },
  categories: ["testing", "broken"],
});

const broken_batch_index_overflow = defineTableFunction<BrokenArgs, BrokenState>({
  name: "broken_batch_index_overflow",
  description:
    "DELIBERATELY BROKEN: emits a batch tagged with a partition_id well above DuckDB's BATCH_INCREMENT=10^13 per-pipeline cap. C++ extension rejects at parse time.",
  args: { count: new Int64() },
  preservesOrder: OrderPreservation.FIXED_ORDER,
  supportsBatchIndex: true,
  onBind: () => ({ outputSchema: BROKEN_N_SCHEMA }),
  initialState: () => ({ emitted: false }),
  process: (params, state, out) => {
    if (state.emitted) {
      out.finish();
      return;
    }
    const values: bigint[] = [];
    for (let i = 0; i < params.args.count; i++) values.push(BigInt(i));
    // 2^60 — far above the 10^13 cap.
    out.emit(
      batchFromColumns({ n: values }, params.outputSchema),
      new Map([["vgi_batch_index", String(1n << 60n)]]),
    );
    state.emitted = true;
  },
  categories: ["testing", "broken"],
});

// =============================================================================
// partition_columns reference fixtures (v2 PartitionColumns / Hive-style)
// =============================================================================

const COUNTRIES = ["AU", "BR", "CA", "FR", "US"];
const COUNTRY_FIELDS = [new Field("country", new Utf8(), true)];
const COUNTRY_SCHEMA = new Schema([
  partitionField("country", new Utf8()),
  new Field("sales", new Int64(), true),
]);

interface CountryArgs {
  rows_per_country: number;
}
interface PartitionState {
  idx: number;
}

const country_partitioned_sales = defineTableFunction<CountryArgs, PartitionState>({
  name: "country_partitioned_sales",
  description:
    "Per-country sales rows, one Arrow batch per country. Declares country as a SINGLE_VALUE partition column.",
  args: { rows_per_country: new Int64() },
  maxWorkers: DEFAULT_MAX_WORKERS,
  partitionKind: "SINGLE_VALUE_PARTITIONS",
  onBind: () => ({ outputSchema: COUNTRY_SCHEMA }),
  onInit: async (params) => {
    const items = COUNTRIES.map((_, i) => packOne(i));
    await params.storage.queuePush(items);
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
    const country = COUNTRIES[state.idx];
    const rpc = params.args.rows_per_country;
    const base = state.idx * 1_000_000;
    const sales: bigint[] = [];
    const countries: string[] = [];
    for (let i = 0; i < rpc; i++) {
      sales.push(BigInt(base + i));
      countries.push(country);
    }
    out.emit(
      batchFromColumns({ country: countries, sales }, params.outputSchema),
      partitionValuesMetadata(COUNTRY_FIELDS, { country: [country, country] }),
    );
  },
  examples: [
    {
      sql: "SELECT country, SUM(sales) FROM country_partitioned_sales(100) GROUP BY country",
      description: "Partitioned aggregate over country",
    },
  ],
  categories: ["generator", "partitioning"],
});

const REGIONS_YEARS: [string, number][] = [
  ["AMER", 2023],
  ["AMER", 2024],
  ["EMEA", 2023],
  ["EMEA", 2024],
  ["APAC", 2023],
  ["APAC", 2024],
];
const REGION_YEAR_FIELDS = [new Field("region", new Utf8(), true), new Field("year", new Int64(), true)];
const REGION_YEAR_SCHEMA = new Schema([
  partitionField("region", new Utf8()),
  partitionField("year", new Int64()),
  new Field("value", new Float64(), true),
]);

interface RegionYearArgs {
  rows_per_partition: number;
}

const region_year_partitioned = defineTableFunction<RegionYearArgs, PartitionState>({
  name: "region_year_partitioned",
  description:
    "Per-(region, year) value rows. Declares both region and year as SINGLE_VALUE partition columns; GROUP BY region, year plans as PARTITIONED_AGGREGATE.",
  args: { rows_per_partition: new Int64() },
  maxWorkers: DEFAULT_MAX_WORKERS,
  partitionKind: "SINGLE_VALUE_PARTITIONS",
  onBind: () => ({ outputSchema: REGION_YEAR_SCHEMA }),
  onInit: async (params) => {
    const items = REGIONS_YEARS.map((_, i) => packOne(i));
    await params.storage.queuePush(items);
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
    const [region, year] = REGIONS_YEARS[state.idx];
    const rpp = params.args.rows_per_partition;
    const base = state.idx * 1000;
    const regions: string[] = [];
    const years: bigint[] = [];
    const values: number[] = [];
    for (let i = 0; i < rpp; i++) {
      regions.push(region);
      years.push(BigInt(year));
      values.push(base + i);
    }
    out.emit(
      batchFromColumns({ region: regions, year: years, value: values }, params.outputSchema),
      partitionValuesMetadata(REGION_YEAR_FIELDS, { region: [region, region], year: [BigInt(year), BigInt(year)] }),
    );
  },
  examples: [
    {
      sql: "SELECT region, year, AVG(value) FROM region_year_partitioned(100) GROUP BY region, year",
      description: "Partitioned aggregate over (region, year)",
    },
  ],
  categories: ["generator", "partitioning"],
});

const CATEGORIES = ["books", "music", "video"];
const CATEGORY_FIELDS = [new Field("category", new Utf8(), true)];
const CATEGORY_SCHEMA = new Schema([
  partitionField("category", new Utf8()),
  new Field("revenue", new Int64(), true),
]);

interface CategoryArgs {
  rows_per_category: number;
}

const partitioned_with_explicit_override = defineTableFunction<CategoryArgs, PartitionState>({
  name: "partitioned_with_explicit_override",
  description:
    "Partition column ``category`` is in the bind schema and the emitted batches; worker uses the explicit ``partition_values=`` override on ``out.emit`` to exercise the override code path.",
  args: { rows_per_category: new Int64() },
  maxWorkers: DEFAULT_MAX_WORKERS,
  partitionKind: "SINGLE_VALUE_PARTITIONS",
  onBind: () => ({ outputSchema: CATEGORY_SCHEMA }),
  onInit: async (params) => {
    const items = CATEGORIES.map((_, i) => packOne(i));
    await params.storage.queuePush(items);
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
    const category = CATEGORIES[state.idx];
    const rpc = params.args.rows_per_category;
    const revenue: bigint[] = [];
    const categories: string[] = [];
    for (let i = 0; i < rpc; i++) {
      revenue.push(BigInt((state.idx + 1) * 100 + i));
      categories.push(category);
    }
    // Explicit partition_values override (even though the column is present).
    out.emit(
      batchFromColumns({ category: categories, revenue }, params.outputSchema),
      partitionValuesMetadata(CATEGORY_FIELDS, { category: [category, category] }),
    );
  },
  categories: ["generator", "partitioning", "testing"],
});

const DISJOINT_KEY_FIELDS = [new Field("key", new Int64(), true)];
const DISJOINT_SCHEMA = new Schema([
  partitionField("key", new Int64()),
  new Field("value", new Int64(), true),
]);

interface DisjointArgs {
  partitions: number;
  rows_per_partition: number;
}

const disjoint_range_partitioned = defineTableFunction<DisjointArgs, PartitionState>({
  name: "disjoint_range_partitioned",
  description:
    "Disjoint per-chunk integer ranges on ``key``. Declares DISJOINT_PARTITIONS (wire-level only; DuckDB falls back to HASH_GROUP_BY for now).",
  args: { partitions: new Int64(), rows_per_partition: new Int64() },
  argDefaults: { rows_per_partition: 10 },
  maxWorkers: DEFAULT_MAX_WORKERS,
  partitionKind: "DISJOINT_PARTITIONS",
  onBind: () => ({ outputSchema: DISJOINT_SCHEMA }),
  onInit: async (params) => {
    const items: Uint8Array[] = [];
    for (let i = 0; i < params.args.partitions; i++) items.push(packOne(i));
    await params.storage.queuePush(items);
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
    const rpp = Number(params.args.rows_per_partition ?? 10);
    const base = state.idx * 1000;
    const keys: bigint[] = [];
    const values: bigint[] = [];
    for (let i = 0; i < rpp; i++) {
      keys.push(BigInt(base + i));
      values.push(BigInt(state.idx * 10 + i));
    }
    out.emit(
      batchFromColumns({ key: keys, value: values }, params.outputSchema),
      // DISJOINT: min/max are the range bounds of this chunk.
      partitionValuesMetadata(DISJOINT_KEY_FIELDS, { key: [BigInt(base), BigInt(base + rpp - 1)] }),
    );
  },
  categories: ["generator", "partitioning", "testing"],
});

// =============================================================================
// partition_columns broken fixtures
// =============================================================================

const BROKEN_COUNTRY_SCHEMA = new Schema([
  partitionField("country", new Utf8()),
  new Field("sales", new Int64(), true),
]);

const broken_missing_partition_values = defineTableFunction<BrokenArgs, BrokenState>({
  name: "broken_missing_partition_values",
  description:
    "DELIBERATELY BROKEN: declares partition_kind + partition-annotated field but emits a data batch without vgi_partition_values#b64 metadata. C++ extension's contract check raises.",
  args: { count: new Int64() },
  partitionKind: "SINGLE_VALUE_PARTITIONS",
  onBind: () => ({ outputSchema: BROKEN_COUNTRY_SCHEMA }),
  initialState: () => ({ emitted: false }),
  process: (params, state, out) => {
    if (state.emitted) {
      out.finish();
      return;
    }
    const sales: bigint[] = [];
    const countries: string[] = [];
    for (let i = 0; i < params.args.count; i++) {
      sales.push(BigInt(i));
      countries.push("US");
    }
    // Emit WITHOUT vgi_partition_values#b64 metadata — C++ side raises.
    out.emit(batchFromColumns({ country: countries, sales }, params.outputSchema));
    state.emitted = true;
  },
  categories: ["testing", "broken"],
});

const broken_partition_min_neq_max = defineTableFunction<BrokenArgs, BrokenState>({
  name: "broken_partition_min_neq_max",
  description:
    "DELIBERATELY BROKEN: declares SINGLE_VALUE_PARTITIONS but supplies an explicit partition_values override with min != max. The framework's wrapper validation doesn't compare min vs max for SINGLE_VALUE; the C++ extension's defense-in-depth check in InstallBatch raises.",
  args: { count: new Int64() },
  partitionKind: "SINGLE_VALUE_PARTITIONS",
  onBind: () => ({ outputSchema: BROKEN_COUNTRY_SCHEMA }),
  initialState: () => ({ emitted: false }),
  process: (params, state, out) => {
    if (state.emitted) {
      out.finish();
      return;
    }
    const sales: bigint[] = [];
    const countries: string[] = [];
    for (let i = 0; i < params.args.count; i++) {
      sales.push(BigInt(i));
      countries.push("US");
    }
    // min=US, max=BR — violates SINGLE_VALUE; C++ defense-in-depth raises.
    out.emit(
      batchFromColumns({ country: countries, sales }, params.outputSchema),
      partitionValuesMetadata(COUNTRY_FIELDS, { country: ["US", "BR"] }),
    );
    state.emitted = true;
  },
  categories: ["testing", "broken"],
});

const NO_ANNOTATION_SCHEMA = new Schema([
  new Field("country", new Utf8(), true),
  new Field("sales", new Int64(), true),
]);

const broken_partition_values_no_annotation = defineTableFunction<BrokenArgs, BrokenState>({
  name: "broken_partition_values_no_annotation",
  description:
    "DELIBERATELY BROKEN: no field carries vgi.partition_column metadata (and partition_kind defaults to NOT_PARTITIONED), but the worker passes partition_values= on out.emit. The framework rejects with RuntimeError before the wire.",
  args: { count: new Int64() },
  // No partitionKind — defaults to NOT_PARTITIONED.
  onBind: () => ({ outputSchema: NO_ANNOTATION_SCHEMA }),
  initialState: () => ({ emitted: false }),
  process: (params, state, out) => {
    if (state.emitted) {
      out.finish();
      return;
    }
    // Worker-side raise: partition_values requires partition-annotated fields.
    throw new Error(
      "out.emit(partition_values=...) requires partition-annotated fields in the bind schema.",
    );
  },
  categories: ["testing", "broken"],
});

const ABSENT_SCHEMA = new Schema([
  partitionField("category", new Utf8()),
  new Field("revenue", new Int64(), true),
]);

const broken_partition_column_absent_from_batch = defineTableFunction<BrokenArgs, BrokenState>({
  name: "broken_partition_column_absent_from_batch",
  description:
    "DELIBERATELY BROKEN: declares partition_kind on 'category' but emits a batch without 'category' AND doesn't supply an explicit partition_values override. The framework's auto-extract fails with RuntimeError before the wire.",
  args: { count: new Int64() },
  partitionKind: "SINGLE_VALUE_PARTITIONS",
  onBind: () => ({ outputSchema: ABSENT_SCHEMA }),
  initialState: () => ({ emitted: false }),
  process: (params, state, out) => {
    if (state.emitted) {
      out.finish();
      return;
    }
    // Worker-side raise: 'category' is partition-annotated but absent from
    // the emitted batch and no explicit override was supplied.
    throw new Error(
      "column 'category' is partition-annotated but absent from emitted batch; pass partition_values=...",
    );
  },
  categories: ["testing", "broken"],
});

// =============================================================================
// transaction_storage: tx_cached_value
// =============================================================================

interface TxArgs {
  key: string;
  seed: number;
}
interface TxState {
  value: number;
  emitted: boolean;
}
const TX_SCHEMA = new Schema([new Field("v", new Int64(), true)]);
const TX_NS = new TextEncoder().encode("_vgi/transaction_state");

function txStorageKey(userKey: string): Uint8Array {
  return new TextEncoder().encode(`vgi-fixture:tx_cached_value:${userKey}`);
}
function packI64(v: number): Uint8Array {
  const buf = new ArrayBuffer(8);
  new DataView(buf).setBigInt64(0, BigInt(v));
  return new Uint8Array(buf);
}
function unpackI64(bytes: Uint8Array): number {
  return Number(new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getBigInt64(0));
}

const tx_cached_value = defineTableFunction<TxArgs, TxState>({
  name: "tx_cached_value",
  description: "Return a value cached per (transaction_opaque_data, key) via transaction_storage.",
  args: { key: new Utf8(), seed: new Int64() },
  maxWorkers: 1,
  onBind: async (params: TableBindParams<TxArgs>) => {
    // transaction_opaque_data scopes the cache. Outside a transaction it is
    // null → no caching (each call uses its own seed). Within a transaction
    // the C++ extension threads a stable token, so repeated binds for the
    // same key see the cached value.
    const txScope = params.bindCall.transaction_opaque_data;
    let value: number;
    if (txScope != null) {
      const key = txStorageKey(params.args.key);
      const cached = await functionStorage.stateGet(txScope, TX_NS, key);
      if (cached != null) {
        value = unpackI64(cached);
      } else {
        value = params.args.seed;
        await functionStorage.statePut(txScope, TX_NS, key, packI64(value));
      }
    } else {
      value = params.args.seed;
    }
    return { outputSchema: TX_SCHEMA, opaqueData: packI64(value) };
  },
  cardinality: () => ({ estimate: 1, max: 1 }),
  onInit: (params) => ({
    max_workers: 1,
    execution_id: params.executionId,
    opaque_data: params.initCall.bind_opaque_data ?? null,
  }),
  initialState: (params: TableProcessParams<TxArgs>) => {
    const opaque = params.initResponse.opaque_data;
    if (!opaque || opaque.length !== 8) {
      throw new Error("tx_cached_value: bind must populate opaque_data with an 8-byte int");
    }
    return { value: unpackI64(opaque), emitted: false };
  },
  process: (params, state, out) => {
    if (state.emitted) {
      out.finish();
      return;
    }
    out.emit(batchFromColumns({ v: [BigInt(state.value)] }, params.outputSchema));
    state.emitted = true;
  },
  categories: ["test", "transaction-storage"],
  tags: { category: "test" },
});

export const partitionTableFunctions: VgiFunction[] = [
  partitioned_batch_index,
  partitioned_batch_index_marked,
  broken_missing_batch_index_tag,
  broken_non_monotone_batch_index,
  broken_batch_index_overflow,
  country_partitioned_sales,
  region_year_partitioned,
  partitioned_with_explicit_override,
  disjoint_range_partitioned,
  broken_missing_partition_values,
  broken_partition_min_neq_max,
  broken_partition_values_no_annotation,
  broken_partition_column_absent_from_batch,
  tx_cached_value,
];
