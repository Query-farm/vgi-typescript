// Example table function implementations.
// Ports all 18 table function groups from vgi-python/vgi/examples/table.py.

import {
  Schema,
  Field,
  Int8,
  Int64,
  Float64,
  Bool,
  Utf8,
  Null,
  DataType,
  Dictionary,
  Struct,
  List,
  Decimal,
  FixedSizeBinary,
  Binary,
} from "@query-farm/apache-arrow";
import {
  defineTableFunction,
  batchFromColumns,
  emptyBatch,
  formatPushedFilters,
  reprPushedFilters,
  DEFAULT_MAX_WORKERS,
  ArgumentValidationError,
  OrderPreservation,
  ComparisonOp,
  type TableBindParams,
  type TableProcessParams,
  type TableCardinality,
  type BoundStorage,
  type Filter,
  type PushdownFilters,
} from "../src/index.js";
import type { OutputCollector } from "vgi-rpc";
import type { VgiFunction } from "../src/index.js";

// ============================================================================
// 1. sequence - Args: count, batch_size?, increment?. Produces {n: int64}
// ============================================================================

interface SequenceArgs {
  count: number;
  batch_size: number;
  increment: number;
}

interface CountdownState {
  remaining: number;
  currentIndex: number;
}

const SEQUENCE_SCHEMA = new Schema([new Field("n", new Int64(), true)]);

const sequence = defineTableFunction<SequenceArgs, CountdownState>({
  name: "sequence",
  description: "Generates a sequence of integers from 0 to n-1",
  args: {
    count: new Int64(),
    batch_size: new Int64(),
    increment: new Int64(),
  },
  argDefaults: {
    batch_size: 1000,
    increment: 1,
  },
  projectionPushdown: true,
  filterPushdown: true,
  autoApplyFilters: true,
  onBind: (params: TableBindParams<SequenceArgs>) => {
    // Reject NULL positional/named args explicitly so callers don't see an
    // opaque TypeError downstream. Inspect the raw Arguments map: an
    // explicitly-NULL named arg lives in `named` with a null value, whereas
    // an omitted arg isn't present at all.
    const rawArgs = params.bindCall.arguments;
    if (rawArgs.positional.length > 0 && rawArgs.positional[0] === null) {
      throw new ArgumentValidationError("sequence: count cannot be NULL");
    }
    if (rawArgs.named.has("batch_size") && rawArgs.named.get("batch_size") === null) {
      throw new ArgumentValidationError("sequence: batch_size cannot be NULL");
    }
    if (rawArgs.named.has("increment") && rawArgs.named.get("increment") === null) {
      throw new ArgumentValidationError("sequence: increment cannot be NULL");
    }
    if (params.args.count == null) {
      throw new ArgumentValidationError("sequence: count cannot be NULL");
    }
    // Validate ge=1 constraints on tunables: batch_size=0 would loop
    // forever in process() and increment=0 would yield duplicate values
    // forever.
    const batchSize = Number(params.args.batch_size ?? 1000);
    const increment = Number(params.args.increment ?? 1);
    if (!(batchSize >= 1)) {
      throw new ArgumentValidationError(`sequence: batch_size must be >= 1, got ${batchSize}`);
    }
    if (!(increment >= 1)) {
      throw new ArgumentValidationError(`sequence: increment must be >= 1, got ${increment}`);
    }
    return { outputSchema: SEQUENCE_SCHEMA };
  },
  cardinality: (params: TableBindParams<SequenceArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  statistics: (params: TableBindParams<SequenceArgs>) => {
    const count = params.args.count;
    const increment = params.args.increment ?? 1;
    if (!(count > 0)) return [];
    const maxValue = BigInt((count - 1) * increment);
    return [
      {
        columnName: "n",
        arrowType: new Int64(),
        min: 0n,
        max: maxValue,
        hasNull: false,
        hasNotNull: true,
        distinctCount: BigInt(count),
        containsUnicode: null,
        maxStringLength: null,
      },
    ];
  },
  initialState: (params: TableProcessParams<SequenceArgs>) => ({
    remaining: params.args.count,
    currentIndex: 0,
  }),
  process: (
    params: TableProcessParams<SequenceArgs>,
    state: CountdownState,
    out: OutputCollector
  ) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }

    const size = Math.min(state.remaining, params.args.batch_size);
    const values: bigint[] = [];
    for (let i = 0; i < size; i++) {
      values.push(BigInt((state.currentIndex + i) * params.args.increment));
    }

    out.emit(batchFromColumns({ n: values }, params.outputSchema));

    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [
    { sql: "SELECT * FROM sequence(10)", description: "Generate integers 0-9" },
    { sql: "SELECT * FROM sequence(1000, batch_size := 100)", description: "Generate integers 0-999 in batches of 100" },
    { sql: "SELECT * FROM sequence(5, batch_size := 10000, increment := 10)", description: "Generate 0, 10, 20, 30, 40" },
  ],
  categories: ["generator", "utility"],
  tags: { category: "generator", type: "utility" },
});

// ============================================================================
// 2. nested_sequence - Args: count, batch_size?, history_size?
// ============================================================================

interface NestedSequenceArgs {
  count: number;
  batch_size: number;
  history_size: number;
}

const NESTED_SEQUENCE_SCHEMA = new Schema([
  new Field("n", new Int64(), true),
  new Field(
    "metadata",
    new Struct([
      new Field("index", new Int64(), true),
      new Field("label", new Utf8(), true),
    ]),
    true
  ),
  new Field("history", new List(new Field("item", new Int64(), true)), true),
]);

const nested_sequence = defineTableFunction<NestedSequenceArgs, CountdownState>({
  name: "nested_sequence",
  description: "Generates a sequence with nested struct and list columns",
  args: {
    count: new Int64(),
    batch_size: new Int64(),
    history_size: new Int64(),
  },
  argDefaults: {
    batch_size: 1000,
    history_size: 20,
  },
  projectionPushdown: true,
  filterPushdown: true,
  autoApplyFilters: true,
  onBind: () => ({ outputSchema: NESTED_SEQUENCE_SCHEMA }),
  cardinality: (params: TableBindParams<NestedSequenceArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  initialState: (params: TableProcessParams<NestedSequenceArgs>) => ({
    remaining: params.args.count,
    currentIndex: 0,
  }),
  process: (
    params: TableProcessParams<NestedSequenceArgs>,
    state: CountdownState,
    out: OutputCollector
  ) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }

    const size = Math.min(state.remaining, params.args.batch_size);
    const projectionIds = params.initCall.projectionIds;
    const projectedCols = projectionIds
      ? new Set(projectionIds.map((i) => NESTED_SEQUENCE_SCHEMA.fields[i].name))
      : new Set(NESTED_SEQUENCE_SCHEMA.fields.map((f) => f.name));

    const data: Record<string, any[]> = {};

    if (projectedCols.has("n")) {
      const nValues: bigint[] = [];
      for (let i = 0; i < size; i++) {
        nValues.push(BigInt(state.currentIndex + i));
      }
      data["n"] = nValues;
    }

    if (projectedCols.has("metadata")) {
      const metaValues: { index: bigint; label: string }[] = [];
      for (let i = 0; i < size; i++) {
        const idx = state.currentIndex + i;
        metaValues.push({ index: BigInt(idx), label: `row_${idx}` });
      }
      data["metadata"] = metaValues;
    }

    if (projectedCols.has("history")) {
      const histValues: bigint[][] = [];
      for (let i = 0; i < size; i++) {
        const idx = state.currentIndex + i;
        const start = Math.max(0, idx - params.args.history_size + 1);
        const hist: bigint[] = [];
        for (let j = start; j <= idx; j++) {
          hist.push(BigInt(j));
        }
        histValues.push(hist);
      }
      data["history"] = histValues;
    }

    out.emit(batchFromColumns(data, params.outputSchema));

    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [
    { sql: "SELECT * FROM nested_sequence(10)", description: "Generate 10 rows with nested columns" },
    { sql: "SELECT n, metadata FROM nested_sequence(100) WHERE n >= 50", description: "Filter and project nested sequence" },
  ],
  categories: ["generator", "utility", "testing"],
  tags: { category: "generator", type: "testing" },
});

// ============================================================================
// 3. double_sequence - Args: count, batch_size?, increment?. {n: float64}
// ============================================================================

interface DoubleSequenceArgs {
  count: number;
  batch_size: number;
  increment: number;
}

const DOUBLE_SEQUENCE_SCHEMA = new Schema([new Field("n", new Float64(), true)]);

const double_sequence = defineTableFunction<DoubleSequenceArgs, CountdownState>({
  name: "double_sequence",
  description: "Generates a sequence of floating-point numbers from 0 to n-1",
  args: {
    count: new Int64(),
    batch_size: new Int64(),
    increment: new Float64(),
  },
  argDefaults: {
    batch_size: 1000,
    increment: 1.0,
  },
  onBind: () => ({ outputSchema: DOUBLE_SEQUENCE_SCHEMA }),
  cardinality: (params: TableBindParams<DoubleSequenceArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  statistics: (params: TableBindParams<DoubleSequenceArgs>) => {
    const count = params.args.count;
    const increment = params.args.increment ?? 1;
    if (!(count > 0)) return [];
    return [
      {
        columnName: "n",
        arrowType: new Float64(),
        min: 0.0,
        max: (count - 1) * increment,
        hasNull: false,
        hasNotNull: true,
        distinctCount: BigInt(count),
        containsUnicode: null,
        maxStringLength: null,
      },
    ];
  },
  initialState: (params: TableProcessParams<DoubleSequenceArgs>) => ({
    remaining: params.args.count,
    currentIndex: 0,
  }),
  process: (
    params: TableProcessParams<DoubleSequenceArgs>,
    state: CountdownState,
    out: OutputCollector
  ) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }

    const size = Math.min(state.remaining, params.args.batch_size);
    const values: number[] = [];
    for (let i = 0; i < size; i++) {
      values.push((state.currentIndex + i) * params.args.increment);
    }

    out.emit(batchFromColumns({ n: values }, params.outputSchema));

    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [
    { sql: "SELECT * FROM double_sequence(10)", description: "Generate floats 0.0-9.0" },
    { sql: "SELECT * FROM double_sequence(1000, batch_size := 100)", description: "Generate floats 0.0-999.0 in batches of 100" },
    { sql: "SELECT * FROM double_sequence(5, increment=0.5)", description: "Generate 0.0, 0.5, 1.0, 1.5, 2.0" },
  ],
  categories: ["generator", "utility"],
  tags: { category: "generator", type: "utility" },
});

// ============================================================================
// 4. generator_exception - Args: fail_after. Throws after N batches
// ============================================================================

interface GeneratorExceptionArgs {
  fail_after: number;
}

interface GeneratorExceptionState {
  batchCount: number;
}

const GENERATOR_EXCEPTION_SCHEMA = new Schema([new Field("n", new Int64(), true)]);

const generator_exception = defineTableFunction<GeneratorExceptionArgs, GeneratorExceptionState>({
  name: "generator_exception",
  description: "Raises an exception after N batches for testing",
  args: {
    fail_after: new Int64(),
  },
  onBind: () => ({ outputSchema: GENERATOR_EXCEPTION_SCHEMA }),
  initialState: () => ({ batchCount: 0 }),
  process: (
    params: TableProcessParams<GeneratorExceptionArgs>,
    state: GeneratorExceptionState,
    out: OutputCollector
  ) => {
    if (state.batchCount >= params.args.fail_after) {
      throw new Error(`Intentional failure after ${params.args.fail_after} batches`);
    }

    out.emit(batchFromColumns({ n: [BigInt(state.batchCount)] }, params.outputSchema));
    state.batchCount += 1;
  },
  categories: ["testing"],
  tags: { category: "testing", type: "error-handling" },
});

// ============================================================================
// 5. logging_generator - Args: count. Logs via out.clientLog
// ============================================================================

interface LoggingGeneratorArgs {
  count: number;
}

interface LoggingGeneratorState {
  index: number;
}

const LOGGING_GENERATOR_SCHEMA = new Schema([new Field("n", new Int64(), true)]);

const logging_generator = defineTableFunction<LoggingGeneratorArgs, LoggingGeneratorState>({
  name: "logging_generator",
  description: "Emits log messages during generation",
  args: {
    count: new Int64(),
  },
  onBind: () => ({ outputSchema: LOGGING_GENERATOR_SCHEMA }),
  initialState: () => ({ index: 0 }),
  process: (
    params: TableProcessParams<LoggingGeneratorArgs>,
    state: LoggingGeneratorState,
    out: OutputCollector
  ) => {
    if (state.index === 0) {
      out.clientLog("INFO", `Starting generation of ${params.args.count} values`);
    }

    if (state.index >= params.args.count) {
      out.clientLog("INFO", "Generation complete");
      out.finish();
      return;
    }

    out.emit(batchFromColumns({ n: [BigInt(state.index)] }, params.outputSchema));
    state.index += 1;
  },
  categories: ["testing"],
});

// ============================================================================
// 6. partitioned_sequence - Multi-worker with shared queue
// ============================================================================

interface PartitionedSequenceArgs {
  count: number;
  increment: number;
}

interface PartitionedSequenceState {
  currentStart: number | null;
  currentEnd: number | null;
  currentIdx: number;
}

const PARTITIONED_SEQUENCE_SCHEMA = new Schema([new Field("n", new Int64(), true)]);
const CHUNK_SIZE = 1000;
const BATCH_SIZE = 1000;

/** Pack two numbers as big-endian uint64 pair (16 bytes), matching Python struct.pack(">QQ") */
function packQQ(a: number, b: number): Uint8Array {
  const buf = new DataView(new ArrayBuffer(16));
  buf.setBigUint64(0, BigInt(a));
  buf.setBigUint64(8, BigInt(b));
  return new Uint8Array(buf.buffer);
}

/** Unpack big-endian uint64 pair, matching Python struct.unpack(">QQ") */
function unpackQQ(data: Uint8Array): [number, number] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  return [Number(view.getBigUint64(0)), Number(view.getBigUint64(8))];
}

const partitioned_sequence = defineTableFunction<PartitionedSequenceArgs, PartitionedSequenceState>({
  name: "partitioned_sequence",
  description: "Generates a partitioned sequence for multi-worker execution",
  args: {
    count: new Int64(),
    increment: new Int64(),
  },
  argDefaults: {
    increment: 1,
  },
  maxWorkers: DEFAULT_MAX_WORKERS,
  onBind: () => ({ outputSchema: PARTITIONED_SEQUENCE_SCHEMA }),
  cardinality: (params: TableBindParams<PartitionedSequenceArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  onInit: async (params) => {
    const workItems: Uint8Array[] = [];
    for (let start = 0; start < params.args.count; start += CHUNK_SIZE) {
      const end = Math.min(start + CHUNK_SIZE, params.args.count);
      workItems.push(packQQ(start, end));
    }
    await params.storage.queuePush(workItems);
    return {
      max_workers: DEFAULT_MAX_WORKERS,
      execution_id: params.executionId,
      opaque_data: null,
    };
  },
  initialState: () => ({
    currentStart: null,
    currentEnd: null,
    currentIdx: 0,
  }),
  process: async (
    params: TableProcessParams<PartitionedSequenceArgs>,
    state: PartitionedSequenceState,
    out: OutputCollector
  ) => {
    // Need a new chunk?
    if (state.currentStart === null || state.currentIdx >= (state.currentEnd ?? 0)) {
      const workData = await params.storage!.queuePop();
      if (workData === null) {
        out.finish();
        return;
      }
      [state.currentStart, state.currentEnd] = unpackQQ(workData);
      state.currentIdx = state.currentStart;
    }

    const batchEnd = Math.min(state.currentIdx + BATCH_SIZE, state.currentEnd!);
    const values: bigint[] = [];
    for (let idx = state.currentIdx; idx < batchEnd; idx++) {
      values.push(BigInt(idx * params.args.increment));
    }

    out.emit(batchFromColumns({ n: values }, params.outputSchema));
    state.currentIdx = batchEnd;
  },
  examples: [
    { sql: "SELECT * FROM partitioned_sequence(100)", description: "Generate 0-99 in parallel across workers" },
    { sql: "SELECT * FROM partitioned_sequence(5, increment=10)", description: "Generate 0, 10, 20, 30, 40 in parallel" },
  ],
  categories: ["generator", "utility"],
});

// ============================================================================
// 6b. partitioned_{preserves_order,no_order_guarantee,fixed_order}
//
// Three clones of partitioned_sequence that differ only in
// Meta.preserves_order. Used by integration/table/order_preservation_modes.test
// to verify the parsed value flows end-to-end onto DuckDB's
// TableFunction::order_preservation_type. Argument is `count` only (no
// `increment`); output values are the raw integers 0..count-1.
// Mirrors vgi-python's _BasePartitionedOrderMode trio.
// ============================================================================

interface OrderModeArgs {
  count: number;
}

interface OrderModeState {
  currentStart: number | null;
  currentEnd: number | null;
  currentIdx: number;
}

const ORDER_MODE_SCHEMA = new Schema([new Field("n", new Int64(), true)]);

function makeOrderModeFunction(name: string, mode: OrderPreservation, description: string) {
  return defineTableFunction<OrderModeArgs, OrderModeState>({
    name,
    description,
    args: { count: new Int64() },
    maxWorkers: DEFAULT_MAX_WORKERS,
    preservesOrder: mode,
    onBind: () => ({ outputSchema: ORDER_MODE_SCHEMA }),
    cardinality: (params: TableBindParams<OrderModeArgs>) => ({
      estimate: params.args.count,
      max: params.args.count,
    }),
    onInit: async (params) => {
      const workItems: Uint8Array[] = [];
      for (let start = 0; start < params.args.count; start += CHUNK_SIZE) {
        const end = Math.min(start + CHUNK_SIZE, params.args.count);
        workItems.push(packQQ(start, end));
      }
      await params.storage.queuePush(workItems);
      return {
        max_workers: DEFAULT_MAX_WORKERS,
        execution_id: params.executionId,
        opaque_data: null,
      };
    },
    initialState: () => ({ currentStart: null, currentEnd: null, currentIdx: 0 }),
    process: async (
      params: TableProcessParams<OrderModeArgs>,
      state: OrderModeState,
      out: OutputCollector
    ) => {
      if (state.currentStart === null || state.currentIdx >= (state.currentEnd ?? 0)) {
        const workData = await params.storage!.queuePop();
        if (workData === null) {
          out.finish();
          return;
        }
        [state.currentStart, state.currentEnd] = unpackQQ(workData);
        state.currentIdx = state.currentStart;
      }
      const batchEnd = Math.min(state.currentIdx + BATCH_SIZE, state.currentEnd!);
      const values: bigint[] = [];
      for (let idx = state.currentIdx; idx < batchEnd; idx++) {
        values.push(BigInt(idx));
      }
      out.emit(batchFromColumns({ n: values }, params.outputSchema));
      state.currentIdx = batchEnd;
    },
    examples: [
      { sql: `SELECT * FROM ${name}(100)`, description },
    ],
    categories: ["generator", "utility"],
  });
}

const partitioned_preserves_order = makeOrderModeFunction(
  "partitioned_preserves_order",
  OrderPreservation.PRESERVES_ORDER,
  "Multi-worker partitioned sequence; preserves_order=PRESERVES_ORDER (DuckDB INSERTION_ORDER).",
);

const partitioned_no_order_guarantee = makeOrderModeFunction(
  "partitioned_no_order_guarantee",
  OrderPreservation.NO_ORDER_GUARANTEE,
  "Multi-worker partitioned sequence; preserves_order=NO_ORDER_GUARANTEE (DuckDB NO_ORDER).",
);

const partitioned_fixed_order = makeOrderModeFunction(
  "partitioned_fixed_order",
  OrderPreservation.FIXED_ORDER,
  "Multi-worker partitioned sequence; preserves_order=FIXED_ORDER (DuckDB serialises pipeline to a single worker).",
);

// ============================================================================
// 7. projected_data - 4 columns, projection pushdown
// ============================================================================

interface ProjectedDataArgs {
  count: number;
}

const PROJECTED_DATA_SCHEMA = new Schema([
  new Field("id", new Int64(), true),
  new Field("name", new Utf8(), true),
  new Field("value", new Float64(), true),
  new Field("extra", new Int64(), true),
]);

const PROJECTED_BATCH_SIZE = 1000;

const projected_data = defineTableFunction<ProjectedDataArgs, CountdownState>({
  name: "projected_data",
  description: "Generates data with 4 columns, supporting projection pushdown",
  args: {
    count: new Int64(),
  },
  projectionPushdown: true,
  onBind: () => ({ outputSchema: PROJECTED_DATA_SCHEMA }),
  cardinality: (params: TableBindParams<ProjectedDataArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  initialState: (params: TableProcessParams<ProjectedDataArgs>) => ({
    remaining: params.args.count,
    currentIndex: 0,
  }),
  process: (
    params: TableProcessParams<ProjectedDataArgs>,
    state: CountdownState,
    out: OutputCollector
  ) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }

    const projectionIds = params.initCall.projectionIds;
    const projectedIndices = projectionIds ?? [0, 1, 2, 3];
    const batchSize = Math.min(state.remaining, PROJECTED_BATCH_SIZE);

    const columns: Record<string, any[]> = {};

    for (const idx of projectedIndices) {
      const fieldName = PROJECTED_DATA_SCHEMA.fields[idx].name;
      if (fieldName === "id") {
        const vals: bigint[] = [];
        for (let i = 0; i < batchSize; i++) {
          vals.push(BigInt(state.currentIndex + i));
        }
        columns["id"] = vals;
      } else if (fieldName === "name") {
        const vals: string[] = [];
        for (let i = 0; i < batchSize; i++) {
          vals.push(`item_${state.currentIndex + i}`);
        }
        columns["name"] = vals;
      } else if (fieldName === "value") {
        const vals: number[] = [];
        for (let i = 0; i < batchSize; i++) {
          vals.push((state.currentIndex + i) * 1.5);
        }
        columns["value"] = vals;
      } else if (fieldName === "extra") {
        const vals: bigint[] = [];
        for (let i = 0; i < batchSize; i++) {
          const v = state.currentIndex + i;
          vals.push(BigInt(v * v));
        }
        columns["extra"] = vals;
      }
    }

    out.emit(batchFromColumns(columns, params.outputSchema));

    state.currentIndex += batchSize;
    state.remaining -= batchSize;
  },
  examples: [
    { sql: "SELECT * FROM projected_data(10)", description: "Generate 10 rows with all 4 columns" },
    { sql: "SELECT id, value FROM projected_data(10)", description: "Generate 10 rows with only id and value columns" },
  ],
  categories: ["generator", "utility"],
});

// ============================================================================
// 8. settings_aware - Dynamic schema from Settings
// ============================================================================

interface SettingsAwareArgs {
  count: number;
}

interface SettingsAwareState {
  remaining: number;
  currentIndex: number;
}

const SETTINGS_AWARE_BATCH_SIZE = 1000;

const settings_aware = defineTableFunction<SettingsAwareArgs, SettingsAwareState>({
  name: "settings_aware",
  description: "Generates data demonstrating settings are passed",
  args: {
    count: new Int64(),
  },
  requiredSettings: ["vgi_verbose_mode", "greeting", "multiplier"],
  onBind: (params: TableBindParams<SettingsAwareArgs>) => {
    const fields: Field[] = [
      new Field("id", new Int64(), true),
      new Field("greeting", new Utf8(), true),
      new Field("value", new Float64(), true),
    ];

    const verboseValue = params.settings.vgi_verbose_mode ?? "false";
    const verboseStr = typeof verboseValue === "bigint" ? String(verboseValue) : String(verboseValue);
    if (verboseStr === "true") {
      fields.push(new Field("details", new Utf8(), true));
    }

    return { outputSchema: new Schema(fields) };
  },
  cardinality: (params: TableBindParams<SettingsAwareArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  initialState: (params: TableProcessParams<SettingsAwareArgs>) => ({
    remaining: params.args.count,
    currentIndex: 0,
  }),
  process: (
    params: TableProcessParams<SettingsAwareArgs>,
    state: SettingsAwareState,
    out: OutputCollector
  ) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }

    const verboseRaw = params.settings.vgi_verbose_mode ?? "false";
    const verbose = String(verboseRaw) === "true";
    const greeting = String(params.settings.greeting ?? "Hello");
    const multiplierStr = String(params.settings.multiplier ?? "1");
    const multiplier = parseInt(multiplierStr, 10) || 1;

    const size = Math.min(state.remaining, SETTINGS_AWARE_BATCH_SIZE);

    const ids: bigint[] = [];
    const greetings: string[] = [];
    const values: number[] = [];
    const details: string[] = [];

    for (let i = 0; i < size; i++) {
      const idx = state.currentIndex + i;
      ids.push(BigInt(idx));
      greetings.push(greeting);
      values.push(idx * 2.5 * multiplier);
      if (verbose) {
        details.push(`row_${idx}`);
      }
    }

    const data: Record<string, any[]> = {
      id: ids,
      greeting: greetings,
      value: values,
    };

    if (verbose) {
      data["details"] = details;
    }

    out.emit(batchFromColumns(data, params.outputSchema));

    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [
    { sql: "SELECT * FROM settings_aware(5)", description: "Generate 5 rows showing setting values" },
  ],
  categories: ["generator", "settings"],
});

// ============================================================================
// 9. ten_thousand - No args, fixed 10k rows
// ============================================================================

interface TenThousandState {
  start: number;
}

const TEN_THOUSAND_SCHEMA = new Schema([new Field("n", new Int64(), true)]);
const TEN_THOUSAND_BATCH_SIZE = 1000;

const ten_thousand = defineTableFunction<Record<string, any>, TenThousandState>({
  name: "ten_thousand",
  description: "Generates 10000 integers from 0 to 9999",
  onBind: () => ({ outputSchema: TEN_THOUSAND_SCHEMA }),
  cardinality: () => ({
    estimate: 10000,
    max: 10000,
  }),
  initialState: () => ({ start: 0 }),
  process: (
    _params: TableProcessParams<Record<string, any>>,
    state: TenThousandState,
    out: OutputCollector
  ) => {
    if (state.start >= 10000) {
      out.finish();
      return;
    }

    const end = Math.min(state.start + TEN_THOUSAND_BATCH_SIZE, 10000);
    const values: bigint[] = [];
    for (let i = state.start; i < end; i++) {
      values.push(BigInt(i));
    }

    out.emit(batchFromColumns({ n: values }, _params.outputSchema));
    state.start = end;
  },
  examples: [
    { sql: "SELECT * FROM ten_thousand()", description: "Generate integers 0-9999" },
  ],
  categories: ["generator", "utility"],
});

// ============================================================================
// 10. constant_columns - Varargs with dynamic schema
// ============================================================================

interface ConstantColumnsArgs {
  count: number;
  [key: string]: any;
}

interface ConstantColumnsState {
  remaining: number;
}

const CONSTANT_COLUMNS_BATCH_SIZE = 2048;

const constant_columns = defineTableFunction<ConstantColumnsArgs, ConstantColumnsState>({
  name: "constant_columns",
  description: "Generates rows with constant values from varargs",
  args: {
    count: new Int64(),
    values: new Null(),
  },
  varargs: ["values"],
  onBind: (params: TableBindParams<ConstantColumnsArgs>) => {
    // The vararg values come from the bind call arguments starting at position 1
    const bindArgs = params.bindCall.arguments;
    const argsSchema = bindArgs.argumentsSchema;
    const fields: Field[] = [];

    // Iterate over all positional args after count (position 0)
    for (let i = 1; i < bindArgs.length; i++) {
      // Use the argument's Arrow type from the schema when available
      let dt: DataType;
      const schemaField = argsSchema?.fields.find(
        f => f.name === `positional_${i}`
      );
      if (schemaField && !(schemaField.type instanceof Null)) {
        dt = schemaField.type;
        // Convert DuckDB extension types to Arrow-native types
        const extName = schemaField.metadata?.get?.("ARROW:extension:name");
        if (extName === "arrow.bool8") {
          dt = new Bool();
        } else if (dt instanceof FixedSizeBinary) {
          const extMeta = schemaField.metadata?.get?.("ARROW:extension:metadata");
          if (extMeta) {
            try {
              const parsed = JSON.parse(extMeta);
              if (parsed.type_name === "hugeint" || parsed.type_name === "uhugeint") {
                dt = new Decimal(0, 38, 128);
              }
            } catch { /* ignore */ }
          }
        }
      } else {
        // Fallback: infer from JS value
        const val = bindArgs.positional[i];
        if (val === null || val === undefined) {
          dt = new Null();
        } else if (typeof val === "bigint") {
          dt = new Int64();
        } else if (typeof val === "number") {
          dt = Number.isInteger(val) ? new Int64() : new Float64();
        } else if (typeof val === "string") {
          dt = new Utf8();
        } else {
          dt = new Utf8();
        }
      }
      fields.push(new Field(`col_${i - 1}`, dt, true));
    }

    return { outputSchema: new Schema(fields) };
  },
  cardinality: (params: TableBindParams<ConstantColumnsArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  initialState: (params: TableProcessParams<ConstantColumnsArgs>) => ({
    remaining: params.args.count,
  }),
  process: (
    params: TableProcessParams<ConstantColumnsArgs>,
    state: ConstantColumnsState,
    out: OutputCollector
  ) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }

    const size = Math.min(state.remaining, CONSTANT_COLUMNS_BATCH_SIZE);

    // Extract the constant values from bind call arguments
    const bindArgs = params.initCall.bind_call.arguments;
    const columns: Record<string, any[]> = {};

    for (let i = 0; i < params.outputSchema.fields.length; i++) {
      const field = params.outputSchema.fields[i];
      const rawVal = bindArgs.positional[i + 1]; // +1 to skip count
      let val: any = rawVal;

      // For complex types (Decimal, List, Map, Struct), keep raw Arrow value
      // since buildColumnData handles them directly
      if (DataType.isDecimal(field.type) || DataType.isList(field.type) ||
          DataType.isMap(field.type) || DataType.isStruct(field.type)) {
        // Keep val as-is — batchFromColumns handles complex types
      } else {
        // Unwrap arrow scalar if needed
        if (val !== null && val !== undefined && typeof val === "object" && typeof val.valueOf === "function") {
          val = val.valueOf();
        }
        // For Int64 fields, convert to BigInt
        if (DataType.isInt(field.type) && (field.type as any).bitWidth === 64) {
          if (typeof val === "number") val = BigInt(val);
        }
      }
      const arr = new Array(size).fill(val);
      columns[field.name] = arr;
    }

    out.emit(batchFromColumns(columns, params.outputSchema));

    state.remaining -= size;
  },
  examples: [
    { sql: "SELECT * FROM constant_columns(5, 42, 'hello')", description: "Generate 5 rows with columns containing 42 and 'hello'" },
    { sql: "SELECT * FROM constant_columns(3, 1, 2, 3, 'test')", description: "Generate 3 rows with 4 columns of mixed types" },
  ],
  categories: ["generator", "utility"],
});

// ============================================================================
// 11. named_params_echo - Echoes named parameter values in output columns
// ============================================================================

interface NamedParamsEchoArgs {
  count: number;
  greeting: string;
  multiplier: number;
  scale: number;
  enabled: boolean;
}

const NAMED_PARAMS_ECHO_SCHEMA = new Schema([
  new Field("id", new Int64(), true),
  new Field("greeting", new Utf8(), true),
  new Field("value", new Int64(), true),
  new Field("float_value", new Float64(), true),
  new Field("enabled", new Bool(), true),
]);

const named_params_echo = defineTableFunction<NamedParamsEchoArgs, CountdownState>({
  name: "named_params_echo",
  description: "Echoes named parameter values in output columns",
  args: {
    count: new Int64(),
    greeting: new Utf8(),
    multiplier: new Int64(),
    scale: new Float64(),
    enabled: new Bool(),
  },
  argDefaults: {
    greeting: "hello",
    multiplier: 1,
    scale: 1.0,
    enabled: true,
  },
  onBind: () => ({ outputSchema: NAMED_PARAMS_ECHO_SCHEMA }),
  cardinality: (params: TableBindParams<NamedParamsEchoArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  initialState: (params: TableProcessParams<NamedParamsEchoArgs>) => ({
    remaining: params.args.count,
    currentIndex: 0,
  }),
  process: (
    params: TableProcessParams<NamedParamsEchoArgs>,
    state: CountdownState,
    out: OutputCollector
  ) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }

    const size = Math.min(state.remaining, 1000);
    const ids: bigint[] = [];
    const greetings: string[] = [];
    const values: bigint[] = [];
    const floatValues: number[] = [];
    const enabledValues: boolean[] = [];

    for (let i = 0; i < size; i++) {
      const idx = state.currentIndex + i;
      ids.push(BigInt(idx));
      greetings.push(params.args.greeting);
      values.push(BigInt(idx * params.args.multiplier));
      floatValues.push(idx * params.args.scale);
      enabledValues.push(params.args.enabled);
    }

    out.emit(batchFromColumns({
      id: ids,
      greeting: greetings,
      value: values,
      float_value: floatValues,
      enabled: enabledValues,
    }, params.outputSchema));

    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [
    { sql: "SELECT * FROM named_params_echo(3)", description: "Echo default parameter values for 3 rows" },
    { sql: "SELECT * FROM named_params_echo(3, greeting := 'hi', multiplier := 10)", description: "Echo custom greeting and multiplier" },
  ],
  categories: ["generator", "testing"],
  tags: { category: "testing", type: "params" },
});

// ============================================================================
// 12. struct_settings - Generates sequence configured by struct setting
// ============================================================================

interface StructSettingsArgs {
  count: number;
}

interface StructSettingsState {
  remaining: number;
  currentIndex: number;
  start: number;
  step: number;
  label: string;
}

const STRUCT_SETTINGS_SCHEMA = new Schema([
  new Field("n", new Int64(), true),
  new Field("label", new Utf8(), true),
]);

const struct_settings = defineTableFunction<StructSettingsArgs, StructSettingsState>({
  name: "struct_settings",
  description: "Generate a sequence configured by a struct setting",
  args: {
    count: new Int64(),
  },
  requiredSettings: ["config"],
  onBind: () => ({ outputSchema: STRUCT_SETTINGS_SCHEMA }),
  cardinality: (params: TableBindParams<StructSettingsArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  initialState: (params: TableProcessParams<StructSettingsArgs>) => {
    const config = params.settings.config;
    // config is a struct — could be an Arrow StructRow or a plain object
    let cfg: Record<string, any>;
    if (config && typeof config === "object" && typeof config.toJSON === "function") {
      cfg = config.toJSON();
    } else if (config && typeof config === "object") {
      cfg = config;
    } else {
      cfg = { start: 0, step: 1, label: "item" };
    }
    return {
      remaining: params.args.count,
      currentIndex: 0,
      start: Number(cfg.start ?? 0),
      step: Number(cfg.step ?? 1),
      label: String(cfg.label ?? "item"),
    };
  },
  process: (
    params: TableProcessParams<StructSettingsArgs>,
    state: StructSettingsState,
    out: OutputCollector
  ) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }

    const size = Math.min(state.remaining, 1000);
    const nValues: bigint[] = [];
    const labelValues: string[] = [];

    for (let i = 0; i < size; i++) {
      nValues.push(BigInt(state.start + (state.currentIndex + i) * state.step));
      labelValues.push(`${state.label}_${state.currentIndex + i}`);
    }

    out.emit(batchFromColumns({ n: nValues, label: labelValues }, params.outputSchema));

    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [
    { sql: "SELECT * FROM struct_settings(5)", description: "Generate 5 rows configured by the config setting" },
  ],
  categories: ["generator", "settings"],
});

// ============================================================================
// 13. secret_demo - Lists secret key-value pairs as rows
// ============================================================================

const SECRET_DEMO_SCHEMA = new Schema([
  new Field("key", new Utf8(), true),
  new Field("value", new Utf8(), true),
  new Field("arrow_type", new Utf8(), true),
]);

const secret_demo = defineTableFunction<Record<string, any>, null>({
  name: "secret_demo",
  description: "Lists secret key-value pairs as rows",
  requiredSecrets: ["vgi_example"],
  onBind: () => ({ outputSchema: SECRET_DEMO_SCHEMA }),
  process: (
    params: TableProcessParams<Record<string, any>>,
    _state: null,
    out: OutputCollector
  ) => {
    const secretDict = params.secrets.vgi_example;
    if (!secretDict || Object.keys(secretDict).length === 0) {
      out.emit(emptyBatch(params.outputSchema));
      out.finish();
      return;
    }

    const keys: string[] = [];
    const values: string[] = [];
    const arrowTypes: string[] = [];

    for (const [k, v] of Object.entries(secretDict)) {
      keys.push(k);
      if (v === null || v === undefined) {
        values.push("NULL");
      } else if (typeof v === "bigint") {
        values.push(String(Number(v)));
      } else if (typeof v === "boolean") {
        values.push(v ? "true" : "false");
      } else {
        values.push(String(v));
      }
      // Determine arrow type name
      if (typeof v === "string") arrowTypes.push("Utf8");
      else if (typeof v === "bigint") arrowTypes.push("Int64");
      else if (typeof v === "number") {
        if (Number.isInteger(v)) arrowTypes.push("Int32");
        else arrowTypes.push("Float64");
      }
      else if (typeof v === "boolean") arrowTypes.push("Bool");
      else arrowTypes.push("Utf8");
    }

    out.emit(batchFromColumns({
      key: keys,
      value: values,
      arrow_type: arrowTypes,
    }, params.outputSchema));
    out.finish();
  },
  examples: [
    { sql: "SELECT * FROM secret_demo()", description: "List secret key-value pairs" },
  ],
  categories: ["testing", "secrets"],
});

// ============================================================================
// 14. scoped_secret_demo - Two-phase bind with scoped secret lookup
// ============================================================================

const SCOPED_SECRET_DEMO_SCHEMA = new Schema([
  new Field("scope", new Utf8(), true),
  new Field("found", new Bool(), true),
  new Field("secret_keys", new Utf8(), true),
]);

const scoped_secret_demo = defineTableFunction<{ path: string }, null>({
  name: "scoped_secret_demo",
  description: "Demonstrates scoped secret lookup via two-phase bind",
  args: {
    path: new Utf8(),
  },
  onBind: (params: TableBindParams<{ path: string }>) => {
    if (!params.resolvedSecretsProvided) {
      // First bind: request secret lookup with scope from args
      return {
        outputSchema: SCOPED_SECRET_DEMO_SCHEMA,
        lookupSecretTypes: ["vgi_example"],
        lookupScopes: [params.args.path],
        lookupNames: [null as any],
      };
    }

    // Second bind: secrets are resolved
    return { outputSchema: SCOPED_SECRET_DEMO_SCHEMA };
  },
  process: (
    params: TableProcessParams<{ path: string }>,
    _state: null,
    out: OutputCollector
  ) => {
    const scope = params.args.path;
    const secretDict = params.secrets.vgi_example;
    const found = !!secretDict && Object.keys(secretDict).length > 0;
    const secretKeys = found
      ? Object.keys(secretDict).sort().join(",")
      : "";

    out.emit(batchFromColumns({
      scope: [scope],
      found: [found],
      secret_keys: [secretKeys],
    }, params.outputSchema));
    out.finish();
  },
  examples: [
    { sql: "SELECT * FROM scoped_secret_demo('/my/scope')", description: "Lookup scoped secret" },
  ],
  categories: ["testing", "secrets"],
});

// ============================================================================
// 15. filter_echo - Echoes pushed-down filter predicates in output
// ============================================================================

interface FilterEchoArgs {
  count: number;
  batch_size: number;
}

interface FilterEchoState {
  remaining: number;
  currentIndex: number;
  filterStr: string;
}

const FILTER_ECHO_SCHEMA = new Schema([
  new Field("n", new Int64(), true),
  new Field("s", new Utf8(), true),
  new Field("pushed_filters", new Utf8(), true),
]);

const filter_echo = defineTableFunction<FilterEchoArgs, FilterEchoState>({
  name: "filter_echo",
  description: "Echoes pushed-down filter predicates in output",
  args: {
    count: new Int64(),
    batch_size: new Int64(),
  },
  argDefaults: {
    batch_size: 2048,
  },
  projectionPushdown: true,
  filterPushdown: true,
  autoApplyFilters: true,
  onBind: () => ({ outputSchema: FILTER_ECHO_SCHEMA }),
  cardinality: (params: TableBindParams<FilterEchoArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  initialState: (params: TableProcessParams<FilterEchoArgs>) => ({
    remaining: params.args.count,
    currentIndex: 0,
    filterStr: formatPushedFilters(params.pushdownFilters),
  }),
  process: (
    params: TableProcessParams<FilterEchoArgs>,
    state: FilterEchoState,
    out: OutputCollector
  ) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }

    const size = Math.min(state.remaining, params.args.batch_size);
    const nValues: bigint[] = [];
    const sValues: string[] = [];
    const filterValues: string[] = [];

    for (let i = 0; i < size; i++) {
      const idx = state.currentIndex + i;
      nValues.push(BigInt(idx));
      sValues.push(`row_${idx}`);
      filterValues.push(state.filterStr);
    }

    out.emit(batchFromColumns({
      n: nValues,
      s: sValues,
      pushed_filters: filterValues,
    }, params.outputSchema));

    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [
    { sql: "SELECT * FROM filter_echo(10)", description: "Generate 10 rows showing pushed filters" },
    { sql: "SELECT pushed_filters FROM filter_echo(10) WHERE n >= 8", description: "See which filters were pushed down" },
  ],
  categories: ["generator", "diagnostic"],
});

// ============================================================================
// 15b. filter_echo_partitioned — same shape as filter_echo, but advertises
//      maxWorkers > 1 so DuckDB can partition the producer across parallel
//      workers. Each worker independently observes the pushed filters.
// ============================================================================

// Multi-worker via a shared work queue: onInit pushes (start,end) chunks once,
// each worker's process pops one chunk per tick. The framework spawns
// maxWorkers worker processes; each gets a distinct PID, so
// COUNT(DISTINCT worker_pid) > 1 confirms multi-worker engagement.
const FILTER_ECHO_PARTITIONED_SCHEMA = new Schema([
  new Field("n", new Int64(), true),
  new Field("worker_pid", new Int64(), true),
  new Field("pushed_filters", new Utf8(), true),
]);

interface FilterEchoPartitionedState {
  filterStr: string;
  // Per-tick chunk being consumed.
  chunkStart: number | null;
  chunkEnd: number;
  chunkIdx: number;
}

const FEPCHUNK = 1000;
const FEPBATCH = 1000;

const filter_echo_partitioned = defineTableFunction<FilterEchoArgs, FilterEchoPartitionedState>({
  name: "filter_echo_partitioned",
  description: "Multi-worker partitioned sequence that echoes pushed-down filters",
  args: {
    count: new Int64(),
  },
  projectionPushdown: true,
  filterPushdown: true,
  autoApplyFilters: true,
  maxWorkers: 4,
  onBind: () => ({ outputSchema: FILTER_ECHO_PARTITIONED_SCHEMA }),
  cardinality: (params: TableBindParams<FilterEchoArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  // Populate the shared work queue once. Subsequent process() ticks across
  // every worker process pop chunks from this queue.
  onInit: async (params) => {
    const count = Number(params.args.count);
    const chunks: Uint8Array[] = [];
    for (let start = 0; start < count; start += FEPCHUNK) {
      const end = Math.min(start + FEPCHUNK, count);
      const buf = new ArrayBuffer(16);
      const view = new DataView(buf);
      view.setBigUint64(0, BigInt(start), false);
      view.setBigUint64(8, BigInt(end), false);
      chunks.push(new Uint8Array(buf));
    }
    await params.storage.queuePush(chunks);
    return {
      execution_id: params.executionId,
      max_workers: 4,
      opaque_data: null,
    };
  },
  initialState: (params: TableProcessParams<FilterEchoArgs>) => ({
    filterStr: formatPushedFilters(params.pushdownFilters),
    chunkStart: null,
    chunkEnd: 0,
    chunkIdx: 0,
  }),
  process: async (
    params: TableProcessParams<FilterEchoArgs>,
    state: FilterEchoPartitionedState,
    out: OutputCollector
  ) => {
    if (state.chunkStart === null || state.chunkIdx >= state.chunkEnd) {
      const work = await params.storage?.queuePop();
      if (!work) { out.finish(); return; }
      const view = new DataView(work.buffer, work.byteOffset, work.byteLength);
      state.chunkStart = Number(view.getBigUint64(0, false));
      state.chunkEnd = Number(view.getBigUint64(8, false));
      state.chunkIdx = state.chunkStart;
    }
    const end = Math.min(state.chunkIdx + FEPBATCH, state.chunkEnd);
    const size = end - state.chunkIdx;
    const ns: bigint[] = [], pids: bigint[] = [], filters: string[] = [];
    const pid = BigInt(process.pid);
    for (let k = state.chunkIdx; k < end; k++) {
      ns.push(BigInt(k));
      pids.push(pid);
      filters.push(state.filterStr);
    }
    out.emit(batchFromColumns({ n: ns, worker_pid: pids, pushed_filters: filters }, params.outputSchema));
    state.chunkIdx = end;
    void size;
  },
  categories: ["generator", "diagnostic", "parallel"],
});

// ============================================================================
// 15d. profiling_demo — registration stub. Real impl exposes a
//      dynamic_to_string() callback that returns per-worker counters under
//      EXPLAIN ANALYZE; that hook isn't yet wired through the framework.
// ============================================================================

const PROFILING_DEMO_SCHEMA = new Schema([
  new Field("a", new Int64(), true),
  new Field("b", new Int64(), true),
]);

interface ProfilingState { remaining: number; idx: number; rowsProduced: bigint; batchesEmitted: bigint; startNs: bigint }

const profiling_demo = defineTableFunction<{ count: number; batch_size: number }, ProfilingState>({
  name: "profiling_demo",
  description: "EXPLAIN ANALYZE profiling probe (dynamic_to_string)",
  args: { count: new Int64(), batch_size: new Int64() },
  argDefaults: { batch_size: 2048 },
  onBind: () => ({ outputSchema: PROFILING_DEMO_SCHEMA }),
  initialState: (params) => ({
    remaining: Number(params.args.count),
    idx: 0,
    rowsProduced: 0n,
    batchesEmitted: 0n,
    startNs: BigInt(Date.now()) * 1_000_000n,
  }),
  process: async (params, state, out) => {
    if (state.remaining <= 0) { out.finish(); return; }
    const size = Math.min(state.remaining, Number(params.args.batch_size));
    const a: bigint[] = [], b: bigint[] = [];
    for (let i = 0; i < size; i++) { a.push(BigInt(state.idx + i)); b.push(BigInt(state.idx + i) * 2n); }
    state.idx += size;
    state.remaining -= size;
    state.rowsProduced += BigInt(size);
    state.batchesEmitted += 1n;
    out.emit(batchFromColumns({ a, b }, params.outputSchema));

    // Persist a snapshot per tick so dynamic_to_string can read the latest
    // values across worker pool boundaries — matches Python's pattern in
    // ProfilingDemoFunction.
    if (params.storage) {
      const elapsedMs = (BigInt(Date.now()) * 1_000_000n - state.startNs) / 1_000_000n;
      const enc = new TextEncoder();
      const bytes = enc.encode(JSON.stringify({
        rows_produced: state.rowsProduced.toString(),
        batches_emitted: state.batchesEmitted.toString(),
        elapsed_ms: elapsedMs.toString(),
      }));
      await params.storage.put(bytes);
    }
  },
  dynamicToString: async (_params, _executionId, storage) => {
    // Sum across every persisted snapshot (one per process tick / worker).
    // Last snapshot wins per-key in vgi-python; we sum rows/batches and take
    // the max elapsed.
    let rows = 0n, batches = 0n, elapsed = 0n;
    const dec = new TextDecoder();
    for (const bytes of await storage.collect()) {
      try {
        const snap = JSON.parse(dec.decode(bytes));
        const r = BigInt(snap.rows_produced ?? "0");
        const b = BigInt(snap.batches_emitted ?? "0");
        const e = BigInt(snap.elapsed_ms ?? "0");
        if (r > rows) rows = r;
        if (b > batches) batches = b;
        if (e > elapsed) elapsed = e;
      } catch { /* ignore corrupt snapshots */ }
    }
    return {
      rows_produced: rows.toString(),
      batches_emitted: batches.toString(),
      elapsed_ms: elapsed.toString(),
    };
  },
  categories: ["test", "profiling"],
});

// ============================================================================
// 15c. slow_cancellable — slow producer with on_cancel probe. Registration
//      stub: produces rows with a sleep, but on_cancel hooks aren't yet wired
//      through the framework so cancel-on-LIMIT can't write the probe file.
//      cancel_on_limit.test relies on that probe file.
// ============================================================================

const SLOW_CANCELLABLE_SCHEMA = new Schema([
  new Field("n", new Int64(), true),
]);

interface SlowCancellableArgs {
  probe_path: string;
  sleep_ms: number;
  count: number;
}

interface SlowCancellableState {
  emitted: number;
  total: number;
  sleepMs: number;
}

const slow_cancellable = defineTableFunction<SlowCancellableArgs, SlowCancellableState>({
  name: "slow_cancellable",
  description: "Slow producer with on_cancel probe (test fixture)",
  args: {
    probe_path: new Utf8(),
    sleep_ms: new Int64(),
    count: new Int64(),
  },
  argDefaults: {
    sleep_ms: 50,
    count: 1_000_000,
  },
  onBind: () => ({ outputSchema: SLOW_CANCELLABLE_SCHEMA }),
  initialState: (params: TableProcessParams<SlowCancellableArgs>) => ({
    emitted: 0,
    total: Number(params.args.count),
    sleepMs: Number(params.args.sleep_ms),
  }),
  process: async (params, state, out) => {
    if (state.emitted >= state.total) {
      out.finish();
      return;
    }
    if (state.sleepMs > 0) {
      await new Promise((r) => setTimeout(r, state.sleepMs));
    }
    out.emit(batchFromColumns({ n: [BigInt(state.emitted)] }, params.outputSchema));
    state.emitted += 1;
  },
  categories: ["test"],
});

// ============================================================================
// 16. make_series (5 overloads)
// ============================================================================

const MAKE_SERIES_SCHEMA = new Schema([new Field("value", new Int64(), true)]);
const MAKE_SERIES_BATCH_SIZE = 1024;

interface MakeSeriesRangeState {
  current: bigint;
  stop: bigint;
  step: bigint;
  offset: number;
}

interface MakeSeriesCsvState {
  values: bigint[];
  offset: number;
}

function makeSeriesRangeEmit(state: MakeSeriesRangeState, params: TableProcessParams<any>, out: OutputCollector): void {
  if (state.step > 0n ? state.current >= state.stop : state.current <= state.stop) {
    out.finish();
    return;
  }
  const batch: bigint[] = [];
  for (let i = 0; i < MAKE_SERIES_BATCH_SIZE && (state.step > 0n ? state.current < state.stop : state.current > state.stop); i++) {
    batch.push(state.current);
    state.current += state.step;
  }
  out.emit(batchFromColumns({ value: batch }, params.outputSchema));
}

function makeSeriesCsvEmit(state: MakeSeriesCsvState, params: TableProcessParams<any>, out: OutputCollector): void {
  if (state.offset >= state.values.length) {
    out.finish();
    return;
  }
  const end = Math.min(state.offset + MAKE_SERIES_BATCH_SIZE, state.values.length);
  const batch = state.values.slice(state.offset, end);
  out.emit(batchFromColumns({ value: batch }, params.outputSchema));
  state.offset = end;
}

const make_series_count = defineTableFunction<{ count: number }, MakeSeriesRangeState>({
  name: "make_series",
  description: "Generate integers from 0 to count-1",
  args: { count: new Int64() },
  onBind: () => ({ outputSchema: MAKE_SERIES_SCHEMA }),
  cardinality: (params: TableBindParams<{ count: number }>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  initialState: (params: TableProcessParams<{ count: number }>) => ({
    current: 0n, stop: BigInt(params.args.count), step: 1n, offset: 0,
  }),
  process: (params, state, out) => makeSeriesRangeEmit(state, params, out),
});

const make_series_csv = defineTableFunction<{ values: string }, MakeSeriesCsvState>({
  name: "make_series",
  description: "Parse comma-separated integers into rows",
  args: { values: new Utf8() },
  onBind: () => ({ outputSchema: MAKE_SERIES_SCHEMA }),
  initialState: (params: TableProcessParams<{ values: string }>) => {
    const values = String(params.args.values)
      .split(",")
      .map(s => s.trim())
      .filter(s => s.length > 0)
      .map(s => BigInt(parseInt(s, 10)));
    return { values, offset: 0 };
  },
  process: (params, state, out) => makeSeriesCsvEmit(state, params, out),
});

const make_series_range = defineTableFunction<{ start: number; stop: number }, MakeSeriesRangeState>({
  name: "make_series",
  description: "Generate integers from start to stop-1",
  args: { start: new Int64(), stop: new Int64() },
  onBind: () => ({ outputSchema: MAKE_SERIES_SCHEMA }),
  cardinality: (params: TableBindParams<{ start: number; stop: number }>) => {
    const count = Math.max(0, params.args.stop - params.args.start);
    return { estimate: count, max: count };
  },
  initialState: (params: TableProcessParams<{ start: number; stop: number }>) => ({
    current: BigInt(params.args.start), stop: BigInt(params.args.stop), step: 1n, offset: 0,
  }),
  process: (params, state, out) => makeSeriesRangeEmit(state, params, out),
});

const MAKE_SERIES_FLOAT_SCHEMA = new Schema([new Field("value", new Float64(), true)]);

interface MakeSeriesFloatState {
  current: number;
  stop: number;
  step: number;
  offset: number;
}

const make_series_float_step = defineTableFunction<{ step: number }, MakeSeriesFloatState>({
  name: "make_series",
  description: "Generate 10 float values with given step size",
  args: { step: new Float64() },
  onBind: () => ({ outputSchema: MAKE_SERIES_FLOAT_SCHEMA }),
  cardinality: () => ({ estimate: 10, max: 10 }),
  initialState: (params: TableProcessParams<{ step: number }>) => ({
    current: 0, stop: 10, step: params.args.step, offset: 0,
  }),
  process: (params, state, out) => {
    if (state.offset >= state.stop) { out.finish(); return; }
    const batch: number[] = [];
    const end = Math.min(state.offset + MAKE_SERIES_BATCH_SIZE, state.stop);
    for (let i = state.offset; i < end; i++) {
      batch.push(i * state.step);
    }
    out.emit(batchFromColumns({ value: batch }, params.outputSchema));
    state.offset = end;
  },
});

const make_series_step = defineTableFunction<{ start: number; stop: number; step: number }, MakeSeriesRangeState>({
  name: "make_series",
  description: "Generate integers from start to stop-1 with step",
  args: { start: new Int64(), stop: new Int64(), step: new Int64() },
  onBind: () => ({ outputSchema: MAKE_SERIES_SCHEMA }),
  initialState: (params: TableProcessParams<{ start: number; stop: number; step: number }>) => {
    const step = params.args.step;
    if (step === 0) throw new Error("make_series step cannot be zero");
    return { current: BigInt(params.args.start), stop: BigInt(params.args.stop), step: BigInt(step), offset: 0 };
  },
  process: (params, state, out) => makeSeriesRangeEmit(state, params, out),
});

// ============================================================================
// 17. make_pairs (3 overloads)
// ============================================================================

const MAKE_PAIRS_INT_SCHEMA = new Schema([
  new Field("a", new Int64(), true),
  new Field("b", new Int64(), true),
]);

const MAKE_PAIRS_STR_SCHEMA = new Schema([
  new Field("a", new Utf8(), true),
  new Field("b", new Utf8(), true),
]);

interface MakePairsIntState {
  current: bigint;
  stop: bigint;
  offset: number;
}

interface MakePairsStrState {
  rows: { a: string; b: string }[];
  offset: number;
}

const make_pairs_int = defineTableFunction<{ start: number; stop: number }, MakePairsIntState>({
  name: "make_pairs",
  description: "Generate integer pairs (i, i*2)",
  args: { start: new Int64(), stop: new Int64() },
  onBind: () => ({ outputSchema: MAKE_PAIRS_INT_SCHEMA }),
  cardinality: (params: TableBindParams<{ start: number; stop: number }>) => {
    const count = Math.max(0, params.args.stop - params.args.start);
    return { estimate: count, max: count };
  },
  initialState: (params: TableProcessParams<{ start: number; stop: number }>) => ({
    current: BigInt(params.args.start), stop: BigInt(params.args.stop), offset: 0,
  }),
  process: (params, state, out) => {
    if (state.current >= state.stop) { out.finish(); return; }
    const a: bigint[] = [];
    const b: bigint[] = [];
    for (let i = 0; i < 1024 && state.current < state.stop; i++) {
      a.push(state.current);
      b.push(state.current * 2n);
      state.current += 1n;
    }
    out.emit(batchFromColumns({ a, b }, params.outputSchema));
  },
});

const MAKE_PAIRS_MIXED_SCHEMA = new Schema([
  new Field("a", new Int64(), true),
  new Field("b", new Utf8(), true),
]);

interface MakePairsMixedState {
  current: bigint;
  stop: bigint;
  label: string;
  offset: number;
}

const make_pairs_mixed = defineTableFunction<{ start: number; label: string }, MakePairsMixedState>({
  name: "make_pairs",
  description: "Generate mixed int/string pairs",
  args: { start: new Int64(), label: new Utf8() },
  onBind: () => ({ outputSchema: MAKE_PAIRS_MIXED_SCHEMA }),
  initialState: (params: TableProcessParams<{ start: number; label: string }>) => ({
    current: BigInt(params.args.start), stop: BigInt(params.args.start) + 5n, label: params.args.label, offset: 0,
  }),
  process: (params, state, out) => {
    if (state.current >= state.stop) { out.finish(); return; }
    const a: bigint[] = [];
    const b: string[] = [];
    let idx = Number(state.current - BigInt(params.args.start));
    for (let i = 0; i < 1024 && state.current < state.stop; i++) {
      a.push(state.current);
      b.push(`${state.label}${idx}`);
      state.current += 1n;
      idx++;
    }
    out.emit(batchFromColumns({ a, b }, params.outputSchema));
  },
});

const make_pairs_str = defineTableFunction<{ prefix: string; suffix: string }, MakePairsStrState>({
  name: "make_pairs",
  description: "Generate string pairs with prefix and suffix",
  args: { prefix: new Utf8(), suffix: new Utf8() },
  onBind: () => ({ outputSchema: MAKE_PAIRS_STR_SCHEMA }),
  cardinality: () => ({ estimate: 5, max: 5 }),
  initialState: (params: TableProcessParams<{ prefix: string; suffix: string }>) => {
    const rows: { a: string; b: string }[] = [];
    for (let i = 0; i < 5; i++) {
      rows.push({ a: `${params.args.prefix}${i}`, b: `${params.args.suffix}${i}` });
    }
    return { rows, offset: 0 };
  },
  process: (params, state, out) => {
    if (state.offset >= state.rows.length) { out.finish(); return; }
    const end = Math.min(state.offset + 1024, state.rows.length);
    const batch = state.rows.slice(state.offset, end);
    out.emit(batchFromColumns({
      a: batch.map(r => r.a),
      b: batch.map(r => r.b),
    }, params.outputSchema));
    state.offset = end;
  },
});

// ============================================================================
// 18. repeat_value (2 overloads with varargs)
// ============================================================================

interface RepeatValueState {
  rows: any[][];
  offset: number;
}

const repeat_value_int = defineTableFunction<{ count: number; [key: string]: any }, RepeatValueState>({
  name: "repeat_value",
  description: "Repeat integer values for count rows",
  args: {
    count: new Int64(),
    values: new Int64(),
  },
  varargs: ["values"],
  onBind: (params: TableBindParams<any>) => {
    const bindArgs = params.bindCall.arguments;
    const numValues = bindArgs.length - 1; // subtract count
    const fields: Field[] = [];
    for (let i = 0; i < numValues; i++) {
      fields.push(new Field(`v${i}`, new Int64(), true));
    }
    return { outputSchema: new Schema(fields) };
  },
  initialState: (params: TableProcessParams<any>) => {
    const count = params.args.count;
    const bindArgs = params.initCall.bind_call.arguments;
    const numValues = bindArgs.length - 1;
    const row: any[] = [];
    for (let i = 0; i < numValues; i++) {
      const v = bindArgs.positional[i + 1];
      row.push(typeof v === "bigint" ? v : v != null ? BigInt(v) : null);
    }
    const rows: any[][] = [];
    for (let i = 0; i < count; i++) rows.push(row);
    return { rows, offset: 0 };
  },
  process: (params, state, out) => {
    if (state.offset >= state.rows.length) { out.finish(); return; }
    const end = Math.min(state.offset + 1024, state.rows.length);
    const batch = state.rows.slice(state.offset, end);
    const columns: Record<string, any[]> = {};
    for (let i = 0; i < params.outputSchema.fields.length; i++) {
      columns[params.outputSchema.fields[i].name] = batch.map(r => r[i]);
    }
    out.emit(batchFromColumns(columns, params.outputSchema));
    state.offset = end;
  },
});

const repeat_value_str = defineTableFunction<{ count: number; [key: string]: any }, RepeatValueState>({
  name: "repeat_value",
  description: "Repeat string values for count rows",
  args: {
    count: new Int64(),
    values: new Utf8(),
  },
  varargs: ["values"],
  onBind: (params: TableBindParams<any>) => {
    const bindArgs = params.bindCall.arguments;
    const numValues = bindArgs.length - 1;
    const fields: Field[] = [];
    for (let i = 0; i < numValues; i++) {
      fields.push(new Field(`v${i}`, new Utf8(), true));
    }
    return { outputSchema: new Schema(fields) };
  },
  initialState: (params: TableProcessParams<any>) => {
    const count = params.args.count;
    const bindArgs = params.initCall.bind_call.arguments;
    const numValues = bindArgs.length - 1;
    const row: any[] = [];
    for (let i = 0; i < numValues; i++) {
      const v = bindArgs.positional[i + 1];
      row.push(v != null ? String(v) : null);
    }
    const rows: any[][] = [];
    for (let i = 0; i < count; i++) rows.push(row);
    return { rows, offset: 0 };
  },
  process: (params, state, out) => {
    if (state.offset >= state.rows.length) { out.finish(); return; }
    const end = Math.min(state.offset + 1024, state.rows.length);
    const batch = state.rows.slice(state.offset, end);
    const columns: Record<string, any[]> = {};
    for (let i = 0; i < params.outputSchema.fields.length; i++) {
      columns[params.outputSchema.fields[i].name] = batch.map(r => r[i]);
    }
    out.emit(batchFromColumns(columns, params.outputSchema));
    state.offset = end;
  },
});

// ============================================================================
// 19. rowid_sequence - Generates rows with a row_id column
// ============================================================================

interface RowIdSequenceArgs {
  count: number;
  layout: string;
  row_id_type: string;
}

const rowid_sequence = defineTableFunction<RowIdSequenceArgs, CountdownState>({
  name: "rowid_sequence",
  description: "Sequence with row_id column",
  args: {
    count: new Int64(),
    layout: new Utf8(),
    row_id_type: new Utf8(),
  },
  argDefaults: {
    layout: "first",
    row_id_type: "int64",
  },
  projectionPushdown: true,
  onBind: (params: TableBindParams<RowIdSequenceArgs>) => {
    const layout = params.args.layout;
    const rowIdType = params.args.row_id_type;

    const allowedLayouts = ["first", "last", "middle"];
    const allowedRowIdTypes = ["int64", "string", "struct"];
    if (!allowedLayouts.includes(layout)) {
      throw new ArgumentValidationError(
        `rowid_sequence: layout must be one of the allowed choices ${JSON.stringify(allowedLayouts)}, got '${layout}'`
      );
    }
    if (!allowedRowIdTypes.includes(rowIdType)) {
      throw new ArgumentValidationError(
        `rowid_sequence: row_id_type must be one of the allowed choices ${JSON.stringify(allowedRowIdTypes)}, got '${rowIdType}'`
      );
    }

    // Build the row_id field with is_row_id metadata
    const ridMetadata = new Map([["is_row_id", ""]]);
    let ridField: Field;
    if (rowIdType === "string") {
      ridField = new Field("row_id", new Utf8(), true, ridMetadata);
    } else if (rowIdType === "struct") {
      ridField = new Field("row_id", new Struct([
        new Field("a", new Int64(), true),
        new Field("b", new Utf8(), true),
      ]), true, ridMetadata);
    } else {
      ridField = new Field("row_id", new Int64(), true, ridMetadata);
    }

    const nameField = new Field("name", new Utf8(), true);
    const valueField = new Field("value", new Utf8(), true);

    let fields: Field[];
    if (layout === "middle") {
      fields = [nameField, ridField, valueField];
    } else if (layout === "last") {
      fields = [nameField, valueField, ridField];
    } else {
      fields = [ridField, nameField, valueField];
    }

    return { outputSchema: new Schema(fields) };
  },
  cardinality: (params: TableBindParams<RowIdSequenceArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  initialState: (params: TableProcessParams<RowIdSequenceArgs>) => ({
    remaining: params.args.count,
    currentIndex: 0,
  }),
  process: (params: TableProcessParams<RowIdSequenceArgs>, state: CountdownState, out: OutputCollector) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }

    const size = Math.min(state.remaining, 1000);
    const start = state.currentIndex;

    const columns: Record<string, any[]> = {};
    for (const f of params.outputSchema.fields) {
      if (f.name === "row_id") {
        if (DataType.isUtf8(f.type)) {
          columns.row_id = Array.from({ length: size }, (_, i) => `rid_${start + i}`);
        } else if (DataType.isStruct(f.type)) {
          columns.row_id = Array.from({ length: size }, (_, i) => ({
            a: BigInt(start + i),
            b: `s_${start + i}`,
          }));
        } else {
          columns.row_id = Array.from({ length: size }, (_, i) => BigInt(start + i));
        }
      } else if (f.name === "name") {
        columns.name = Array.from({ length: size }, (_, i) => `item_${start + i}`);
      } else if (f.name === "value") {
        columns.value = Array.from({ length: size }, (_, i) => `val_${start + i}`);
      }
    }

    out.emit(batchFromColumns(columns, params.outputSchema));
    state.currentIndex += size;
    state.remaining -= size;
  },
});

// ============================================================================
// 20. late_materialization - rowid generator participating in DuckDB's
// late-materialization optimizer. Port of vgi-python's
// _test_fixtures/table/late_materialization.py.
//
// Schema (row_id int64 [is_row_id], ord int64, payload utf8, pushed utf8):
//   * row_id == row index — unique/deterministic/snapshot-stable, satisfying
//     the late-mat worker contract (so the narrow ordering scan and the wide
//     re-fetch scan resolve the same logical row, even across processes).
//   * ord is a scrambled function of the index so a Top-N on ord scatters the
//     survivor rowids — exercising the exact IN-list pushdown path.
//   * payload is the wide column the rewrite avoids materializing.
//   * pushed is the witness: it echoes, per row, the rowid filter the worker
//     received (in=<n> join keys, rng=<lo>..<hi> bounds). The rewrite's output
//     columns come from the wide scan, so selecting `pushed` reports exactly
//     what was pushed there — over both subprocess and HTTP transports.
// ============================================================================

interface LateMatArgs {
  count: number;
  batch_size: number;
  dup_row_id: boolean;
  null_ord_stride: number;
}

interface LateMatState {
  remaining: number;
  currentIndex: number;
  // Serialized (not transient) so the HTTP rehydrate path — which deserializes
  // user state without re-running initialState — preserves the observed filter.
  witness: string;
}

const LATE_MAT_ROWID = "row_id";
// Scramble multiplier (odd) used to turn the monotonic index into a scattered
// ordering key, matching the Python fixture.
const LATE_MAT_SCRAMBLE = 2654435761n;
const LATE_MAT_NO_WITNESS = "rid:in=0;rng=none";

function lateMatScrambleOrd(index: number): bigint {
  return (BigInt(index) * LATE_MAT_SCRAMBLE) % 1_000_000_007n;
}

// Summarize the rowid filter the worker received as a stable string:
//   in=<n>            — total number of rowid IN-list (join-key) values
//   rng=<lo>..<hi>    — min/max rowid range bounds, or `none` if absent
function rowidPushdownWitness(filters: PushdownFilters | undefined): string {
  if (!filters) return LATE_MAT_NO_WITNESS;
  let inCount = 0;
  let lo: bigint | null = null;
  let hi: bigint | null = null;
  const toBig = (v: any): bigint => (typeof v === "bigint" ? v : BigInt(v));
  const walk = (f: Filter): void => {
    if (f.type === "and" || f.type === "or") {
      for (const child of f.children) walk(child);
    } else if (f.type === "in" && f.columnName === LATE_MAT_ROWID) {
      inCount += f.values.size;
    } else if (f.type === "constant" && f.columnName === LATE_MAT_ROWID) {
      const v = toBig(f.value);
      switch (f.op) {
        case ComparisonOp.GT:
        case ComparisonOp.GE:
          lo = lo === null || v < lo ? v : lo;
          break;
        case ComparisonOp.LT:
        case ComparisonOp.LE:
          hi = hi === null || v > hi ? v : hi;
          break;
        case ComparisonOp.EQ:
          lo = v;
          hi = v;
          break;
      }
    }
  };
  for (const f of filters.filters) walk(f);
  const rng = lo !== null || hi !== null ? `${lo}..${hi}` : "none";
  return `rid:in=${inCount};rng=${rng}`;
}

const LATE_MAT_SCHEMA = new Schema([
  new Field("row_id", new Int64(), true, new Map([["is_row_id", ""]])),
  new Field("ord", new Int64(), true),
  new Field("payload", new Utf8(), true),
  new Field("pushed", new Utf8(), true),
]);

const late_materialization = defineTableFunction<LateMatArgs, LateMatState>({
  name: "late_materialization",
  description: "Rowid generator that participates in late materialization",
  args: {
    count: new Int64(),
    batch_size: new Int64(),
    dup_row_id: new Bool(),
    null_ord_stride: new Int64(),
  },
  argDefaults: {
    batch_size: 2048,
    dup_row_id: false,
    null_ord_stride: 0,
  },
  projectionPushdown: true,
  filterPushdown: true,
  autoApplyFilters: true,
  lateMaterialization: true,
  onBind: () => ({ outputSchema: LATE_MAT_SCHEMA }),
  cardinality: (params: TableBindParams<LateMatArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  // For the wide probe scan, the SEMI join's build side completes before the
  // scan inits, so the surviving rowid filter arrives on the init-time
  // pushdownFilters (already populated on processParams here). process()
  // additionally latches anything that shows up per-tick.
  initialState: (params: TableProcessParams<LateMatArgs>) => ({
    remaining: params.args.count,
    currentIndex: 0,
    witness: rowidPushdownWitness(params.pushdownFilters),
  }),
  process: (params: TableProcessParams<LateMatArgs>, state: LateMatState, out: OutputCollector) => {
    // Refresh the witness from the per-tick dynamic filters. Once a rowid
    // filter is present, latch it (guard against a transient empty tick
    // clobbering it).
    const tickWitness = rowidPushdownWitness(params.pushdownFilters);
    if (tickWitness !== LATE_MAT_NO_WITNESS || state.witness === LATE_MAT_NO_WITNESS) {
      state.witness = tickWitness;
    }

    if (state.remaining <= 0) {
      out.finish();
      return;
    }

    const size = Math.min(state.remaining, params.args.batch_size);
    const start = state.currentIndex;
    const stride = params.args.null_ord_stride;

    const columns: Record<string, any[]> = {};
    for (const f of params.outputSchema.fields) {
      if (f.name === "row_id") {
        columns.row_id = Array.from({ length: size }, (_, j) => {
          const i = start + j;
          return params.args.dup_row_id ? BigInt(Math.floor(i / 2)) : BigInt(i);
        });
      } else if (f.name === "ord") {
        columns.ord = Array.from({ length: size }, (_, j) => {
          const i = start + j;
          return stride > 0 && i % stride === 0 ? null : lateMatScrambleOrd(i);
        });
      } else if (f.name === "payload") {
        columns.payload = Array.from({ length: size }, (_, j) => `payload_${start + j}`);
      } else if (f.name === "pushed") {
        columns.pushed = new Array(size).fill(state.witness);
      }
    }

    out.emit(batchFromColumns(columns, params.outputSchema));
    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [
    {
      sql: "SELECT row_id, payload FROM late_materialization(100000) ORDER BY ord LIMIT 10",
      description: "Top-N is late-materialized: payload fetched only for survivors",
    },
  ],
  categories: ["generator", "diagnostic"],
});

// ============================================================================
// 21. value_prune - exercises PushdownFilters.getColumnValues('n'), the
// partition-pruning accessor. Resolves the discrete value set for `n` up front
// and echoes it in the `resolved` column ("(scan)" when not enumerable), so the
// accessor's AND-descent / OR-union behaviour is directly observable. Port of
// vgi-python's ValuePruneFunction. See value_prune.test.
// ============================================================================

interface ValuePruneArgs {
  count: number;
  batch_size: number;
}

interface ValuePruneState {
  values: number[];
  resolved: string;
  cursor: number;
}

const VALUE_PRUNE_SCHEMA = new Schema([
  new Field("n", new Int64(), true),
  new Field("resolved", new Utf8(), true),
]);

const value_prune = defineTableFunction<ValuePruneArgs, ValuePruneState>({
  name: "value_prune",
  description: "Prunes the key set via getColumnValues('n'); echoes the resolved discrete values",
  args: {
    count: new Int64(),
    batch_size: new Int64(),
  },
  argDefaults: {
    batch_size: 2048,
  },
  filterPushdown: true,
  autoApplyFilters: true,
  projectionPushdown: true,
  onBind: () => ({ outputSchema: VALUE_PRUNE_SCHEMA }),
  cardinality: (params: TableBindParams<ValuePruneArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  // Resolve the discrete key set for `n` from the init-time pushdown filters
  // (already populated on processParams here). Serialized into state so the
  // HTTP rehydrate path preserves the resolution across a token round-trip.
  initialState: (params: TableProcessParams<ValuePruneArgs>) => {
    const count = params.args.count;
    const discrete = params.pushdownFilters
      ? params.pushdownFilters.getColumnValues("n")
      : null;
    if (discrete !== null) {
      const nums = discrete
        .filter((v) => v !== null && v !== undefined)
        .map((v) => Number(v))
        .sort((a, b) => a - b);
      const resolved = nums.join(",");
      const emit = nums.filter((v) => v >= 0 && v < count);
      return { values: emit, resolved, cursor: 0 };
    }
    return {
      values: Array.from({ length: count }, (_, i) => i),
      resolved: "(scan)",
      cursor: 0,
    };
  },
  process: (params: TableProcessParams<ValuePruneArgs>, state: ValuePruneState, out: OutputCollector) => {
    if (state.cursor >= state.values.length) {
      out.finish();
      return;
    }
    const size = Math.min(state.values.length - state.cursor, params.args.batch_size);
    const chunk = state.values.slice(state.cursor, state.cursor + size);
    const columns: Record<string, any[]> = {};
    for (const f of params.outputSchema.fields) {
      if (f.name === "n") {
        columns.n = chunk.map((v) => BigInt(v));
      } else if (f.name === "resolved") {
        columns.resolved = new Array(chunk.length).fill(state.resolved);
      }
    }
    out.emit(batchFromColumns(columns, params.outputSchema));
    state.cursor += size;
  },
  examples: [
    {
      sql: "SELECT DISTINCT resolved FROM value_prune(100) WHERE n IN (5, 50, 95)",
      description: "Resolve a discrete key set from an IN predicate",
    },
  ],
  categories: ["generator", "diagnostic"],
});

// ============================================================================
// 22. dict_filter_echo - emits a dictionary<int8, utf8> column with no ENUM
// metadata, so DuckDB types it as plain VARCHAR and pushes VARCHAR (string)
// literals down. The auto-applied filter must compare (dictionary column,
// string literal) without throwing — the evaluator reads the column through
// the dictionary-decoding cell accessor, so a string<->string comparison
// results. Port of vgi-python's DictFilterEchoFunction. See
// dictionary_varchar.test.
// ============================================================================

const DICT_FILTER_VALUES = ["red", "green", "blue"];

const DICT_FILTER_ECHO_SCHEMA = new Schema([
  new Field("n", new Int64(), true),
  new Field("s", new Dictionary(new Utf8(), new Int8()), true),
]);

interface DictFilterEchoArgs {
  count: number;
  batch_size: number;
}

interface DictFilterEchoState {
  remaining: number;
  currentIndex: number;
}

const dict_filter_echo = defineTableFunction<DictFilterEchoArgs, DictFilterEchoState>({
  name: "dict_filter_echo",
  description: "Emits a dictionary-encoded VARCHAR column for filter-pushdown testing",
  args: {
    count: new Int64(),
    batch_size: new Int64(),
  },
  argDefaults: {
    batch_size: 2048,
  },
  filterPushdown: true,
  autoApplyFilters: true,
  projectionPushdown: true,
  onBind: () => ({ outputSchema: DICT_FILTER_ECHO_SCHEMA }),
  cardinality: (params: TableBindParams<DictFilterEchoArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  initialState: (params: TableProcessParams<DictFilterEchoArgs>) => ({
    remaining: params.args.count,
    currentIndex: 0,
  }),
  process: (params: TableProcessParams<DictFilterEchoArgs>, state: DictFilterEchoState, out: OutputCollector) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }
    const size = Math.min(state.remaining, params.args.batch_size);
    const start = state.currentIndex;
    const columns: Record<string, any[]> = {};
    for (const f of params.outputSchema.fields) {
      if (f.name === "n") {
        columns.n = Array.from({ length: size }, (_, j) => BigInt(start + j));
      } else if (f.name === "s") {
        columns.s = Array.from(
          { length: size },
          (_, j) => DICT_FILTER_VALUES[(start + j) % DICT_FILTER_VALUES.length],
        );
      }
    }
    out.emit(batchFromColumns(columns, params.outputSchema));
    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [
    {
      sql: "SELECT * FROM dict_filter_echo(6) WHERE s = 'green'",
      description: "Filter a dictionary-encoded column by an equality predicate",
    },
    {
      sql: "SELECT * FROM dict_filter_echo(6) WHERE s IN ('red', 'blue')",
      description: "Filter a dictionary-encoded column by an IN predicate",
    },
  ],
  categories: ["generator", "diagnostic", "testing"],
});

// ============================================================================
// versioned_data_scan — time travel with schema evolution
// ============================================================================

const VERSIONED_SCHEMAS: Record<number, Schema> = {
  1: new Schema([new Field("id", new Int64(), true)]),
  2: new Schema([
    new Field("id", new Int64(), true),
    new Field("name", new Utf8(), true),
    new Field("score", new Float64(), true),
    new Field("active", new Bool(), true),
  ]),
  3: new Schema([
    new Field("id", new Int64(), true),
    new Field("score", new Float64(), true),
  ]),
};

const VERSIONED_DATA: Record<number, Record<string, any[]>> = {
  1: { id: [1n, 2n, 3n] },
  2: {
    id: [1n, 2n, 3n, 4n, 5n],
    name: ["alice", "bob", "carol", "dave", "eve"],
    score: [10.0, 20.0, 30.0, 40.0, 50.0],
    active: [true, false, true, false, true],
  },
  3: { id: [1n, 2n, 3n, 4n], score: [15.0, 25.0, 35.0, 45.0] },
};

const CURRENT_VERSION = 3;

export function resolveVersion(atUnit?: string | null, atValue?: string | null): number {
  if (!atUnit) return CURRENT_VERSION;

  if (atUnit.toUpperCase() === "VERSION") {
    const version = parseInt(String(atValue), 10);
    if (!(version in VERSIONED_SCHEMAS)) {
      throw new Error(`Unknown version: ${version}. Valid versions: ${Object.keys(VERSIONED_SCHEMAS).map(Number).sort()}`);
    }
    return version;
  }

  if (atUnit.toUpperCase() === "TIMESTAMP") {
    const year = parseInt(String(atValue).slice(0, 4), 10);
    if (year < 2020) {
      throw new Error(`No version exists at timestamp '${atValue}': table did not exist before 2020`);
    }
    if (year <= 2020) return 1;
    if (year <= 2021) return 2;
    return 3;
  }

  throw new Error(`Unsupported at_unit: '${atUnit}'`);
}

export function getVersionedSchema(version: number): Schema {
  return VERSIONED_SCHEMAS[version];
}

interface VersionedDataArgs {
  version: number;
}

interface VersionedDataState {
  done: boolean;
}

const versioned_data_scan = defineTableFunction<VersionedDataArgs, VersionedDataState>({
  name: "versioned_data_scan",
  description: "Returns versioned data with schema evolution",
  args: { version: new Int64() },
  argDefaults: { version: CURRENT_VERSION },
  categories: ["generator", "testing"],
  maxWorkers: 1,
  onBind: (params: TableBindParams<VersionedDataArgs>) => {
    const version = params.args.version;
    if (!(version in VERSIONED_SCHEMAS)) {
      throw new Error(`Unknown version: ${version}. Valid versions: ${Object.keys(VERSIONED_SCHEMAS).map(Number).sort()}`);
    }
    return { outputSchema: VERSIONED_SCHEMAS[version] };
  },
  initialState: () => ({ done: false }),
  process(params: TableProcessParams<VersionedDataArgs>, state: VersionedDataState, out: OutputCollector) {
    if (state.done) {
      out.finish();
      return;
    }
    state.done = true;
    const version = params.args.version;
    const data = VERSIONED_DATA[version];
    out.emit(batchFromColumns(data, params.outputSchema));
  },
});

// ============================================================================
// Static scan function helper (equivalent to Python _static_scan_function)
// ============================================================================

function defineStaticScanFunction(
  funcName: string,
  funcDescription: string,
  outputSchema: Schema,
  data: Record<string, any[]>,
): VgiFunction {
  return defineTableFunction<Record<string, never>, { done: boolean }>({
    name: funcName,
    description: funcDescription,
    maxWorkers: 1,
    onBind: () => ({ outputSchema }),
    initialState: () => ({ done: false }),
    process(_params, state, out) {
      if (state.done) { out.finish(); return; }
      state.done = true;
      out.emit(batchFromColumns(data, outputSchema));
    },
  });
}

// ============================================================================
// Constraint example scan functions
// ============================================================================

const DEPARTMENTS_SCHEMA = new Schema([
  new Field("id", new Int64(), true),
  new Field("name", new Utf8(), true),
  new Field("budget", new Float64(), true),
]);

const departments_scan = defineStaticScanFunction(
  "departments_scan", "Scan departments table", DEPARTMENTS_SCHEMA, {
    id: [1n, 2n, 3n],
    name: ["Engineering", "Sales", "HR"],
    budget: [500000.0, 300000.0, 200000.0],
  },
);

const EMPLOYEES_SCHEMA = new Schema([
  new Field("id", new Int64(), true),
  new Field("name", new Utf8(), true),
  new Field("email", new Utf8(), true),
  new Field("department_id", new Int64(), true),
]);

const employees_scan = defineStaticScanFunction(
  "employees_scan", "Scan employees table", EMPLOYEES_SCHEMA, {
    id: [1n, 2n, 3n, 4n, 5n],
    name: ["Alice", "Bob", "Carol", "Dave", "Eve"],
    email: ["alice@co.com", "bob@co.com", "carol@co.com", "dave@co.com", "eve@co.com"],
    department_id: [1n, 1n, 2n, 2n, 3n],
  },
);

const PRODUCTS_SCHEMA = new Schema([
  new Field("id", new Int64(), true),
  new Field("name", new Utf8(), true),
  new Field("quantity", new Int64(), true),
  new Field("price", new Float64(), true),
]);

const products_scan = defineStaticScanFunction(
  "products_scan", "Scan products table", PRODUCTS_SCHEMA, {
    id: [1n, 2n, 3n],
    name: ["Widget", "Gadget", "Doohickey"],
    quantity: [100n, 50n, 200n],
    price: [9.99, 24.99, 4.99],
  },
);

const PROJECTS_SCHEMA = new Schema([
  new Field("department_id", new Int64(), true),
  new Field("project_code", new Utf8(), true),
  new Field("title", new Utf8(), true),
]);

const projects_scan = defineStaticScanFunction(
  "projects_scan", "Scan projects table", PROJECTS_SCHEMA, {
    department_id: [1n, 1n, 2n],
    project_code: ["P001", "P002", "P003"],
    title: ["Backend API", "Frontend UI", "Sales Portal"],
  },
);

const COLORS_SCAN_SCHEMA = new Schema([
  new Field("id", new Int64(), true),
  new Field("color", new Utf8(), true),
  new Field("hex_code", new Utf8(), true),
]);

const colors_scan = defineStaticScanFunction(
  "colors_scan", "Scan colors table", COLORS_SCAN_SCHEMA, {
    id: [1n, 2n, 3n],
    color: ["blue", "green", "red"],
    hex_code: ["#0000FF", "#00FF00", "#FF0000"],
  },
);

// Build a little-endian WKB Point (byte_order=1, type=1, x, y) — same format
// DuckDB's spatial extension produces from ST_Point. Shared by
// geo_points_scan and (future) spatial_filter_example.
function wkbPoint(x: number, y: number): Uint8Array {
  const buf = new ArrayBuffer(21);
  const view = new DataView(buf);
  view.setUint8(0, 1);
  view.setUint32(1, 1, true);
  view.setFloat64(5, x, true);
  view.setFloat64(13, y, true);
  return new Uint8Array(buf);
}

// geo_points backing scan: 5x5 grid of WKB points (0..4 x 0..4).
const GEO_POINTS_SCAN_SCHEMA = new Schema([
  new Field("id", new Int64(), true),
  new Field("geom", new Binary(), true, new Map<string, string>([
    ["ARROW:extension:name", "geoarrow.wkb"],
    ["ARROW:extension:metadata", "{}"],
  ])),
]);

function geoGridWkb(): { ids: bigint[]; geoms: Uint8Array[] } {
  const ids: bigint[] = [];
  const geoms: Uint8Array[] = [];
  let id = 1n;
  for (let x = 0; x < 5; x++) {
    for (let y = 0; y < 5; y++) {
      ids.push(id++);
      geoms.push(wkbPoint(x, y));
    }
  }
  return { ids, geoms };
}

const geo_points_scan = (() => {
  const { ids, geoms } = geoGridWkb();
  return defineStaticScanFunction(
    "geo_points_scan", "Scan geo_points table", GEO_POINTS_SCAN_SCHEMA, {
      id: ids,
      geom: geoms,
    },
  );
})();

// ============================================================================
// versioned_constraints_scan — time travel with evolving constraints
// ============================================================================

const VC_SCHEMAS: Record<number, Schema> = {
  1: new Schema([new Field("id", new Int64(), true), new Field("name", new Utf8(), true)]),
  2: new Schema([
    new Field("id", new Int64(), true),
    new Field("name", new Utf8(), true),
    new Field("email", new Utf8(), true),
  ]),
  3: new Schema([
    new Field("id", new Int64(), true),
    new Field("name", new Utf8(), true),
    new Field("email", new Utf8(), true),
    new Field("department_id", new Int64(), true),
  ]),
};

const VC_DATA: Record<number, Record<string, any[]>> = {
  1: { id: [1n, 2n], name: ["Alice", "Bob"] },
  2: { id: [1n, 2n, 3n], name: ["Alice", "Bob", "Carol"], email: ["a@co", "b@co", "c@co"] },
  3: {
    id: [1n, 2n, 3n], name: ["Alice", "Bob", "Carol"],
    email: ["a@co", "b@co", "c@co"], department_id: [1n, 2n, 1n],
  },
};

const VC_CURRENT = 3;

export function resolveVersionedConstraintsVersion(atUnit?: string | null, atValue?: string | null): number {
  if (!atUnit) return VC_CURRENT;
  if (atUnit.toUpperCase() === "VERSION") {
    const version = parseInt(String(atValue), 10);
    if (!(version in VC_SCHEMAS)) {
      throw new Error(`Unknown version: ${version}. Valid versions: ${Object.keys(VC_SCHEMAS).map(Number).sort()}`);
    }
    return version;
  }
  throw new Error(`Unsupported at_unit: '${atUnit}'`);
}

export function getVersionedConstraintsSchema(version: number): Schema {
  return VC_SCHEMAS[version];
}

const versioned_constraints_scan = defineTableFunction<{ version: number }, { done: boolean }>({
  name: "versioned_constraints_scan",
  description: "Scan versioned constraints table",
  args: { version: new Int64() },
  argDefaults: { version: VC_CURRENT },
  maxWorkers: 1,
  onBind: (params: TableBindParams<{ version: number }>) => {
    const version = params.args.version;
    if (!(version in VC_SCHEMAS)) {
      throw new Error(`Unknown version: ${version}`);
    }
    return { outputSchema: VC_SCHEMAS[version] };
  },
  initialState: () => ({ done: false }),
  process(params: TableProcessParams<{ version: number }>, state: { done: boolean }, out: OutputCollector) {
    if (state.done) { out.finish(); return; }
    state.done = true;
    out.emit(batchFromColumns(VC_DATA[params.args.version], params.outputSchema));
  },
});

// ============================================================================
// order_echo - echoes ORDER BY + LIMIT pushdown hints
// ============================================================================

interface OrderEchoArgs {
  count: number;
  batch_size: number;
}

interface OrderEchoState {
  remaining: number;
  currentIndex: number;
  orderColumn: string;
  orderDirection: string;
  orderNullOrder: string;
  orderLimit: bigint;
}

const ORDER_ECHO_SCHEMA = new Schema([
  new Field("n", new Int64(), true),
  new Field("s", new Utf8(), true),
  new Field("order_column", new Utf8(), true),
  new Field("order_direction", new Utf8(), true),
  new Field("order_null_order", new Utf8(), true),
  new Field("order_limit", new Int64(), true),
]);

const order_echo = defineTableFunction<OrderEchoArgs, OrderEchoState>({
  name: "order_echo",
  description: "Echoes ORDER BY + LIMIT pushdown hints in output",
  args: {
    count: new Int64(),
    batch_size: new Int64(),
  },
  argDefaults: {
    batch_size: 2048,
  },
  projectionPushdown: true,
  filterPushdown: true,
  autoApplyFilters: true,
  onBind: () => ({ outputSchema: ORDER_ECHO_SCHEMA }),
  cardinality: (params: TableBindParams<OrderEchoArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  initialState: (params: TableProcessParams<OrderEchoArgs>) => {
    const init = params.initCall;
    return {
      remaining: params.args.count,
      currentIndex: 0,
      orderColumn: init.order_by_column_name ?? "(none)",
      orderDirection: init.order_by_direction ? String(init.order_by_direction) : "(none)",
      orderNullOrder: init.order_by_null_order ? String(init.order_by_null_order) : "(none)",
      orderLimit: init.order_by_limit ?? -1n,
    };
  },
  process: (
    params: TableProcessParams<OrderEchoArgs>,
    state: OrderEchoState,
    out: OutputCollector
  ) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }
    const size = Math.min(state.remaining, params.args.batch_size);
    const n: bigint[] = [];
    const s: string[] = [];
    const oc: string[] = [];
    const od: string[] = [];
    const on: string[] = [];
    const ol: bigint[] = [];
    for (let i = 0; i < size; i++) {
      const idx = state.currentIndex + i;
      n.push(BigInt(idx));
      s.push(`row_${idx}`);
      oc.push(state.orderColumn);
      od.push(state.orderDirection);
      on.push(state.orderNullOrder);
      ol.push(state.orderLimit);
    }
    out.emit(
      batchFromColumns(
        {
          n,
          s,
          order_column: oc,
          order_direction: od,
          order_null_order: on,
          order_limit: ol,
        },
        params.outputSchema
      )
    );
    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [
    {
      sql: "SELECT * FROM order_echo(100) ORDER BY n LIMIT 5",
      description: "See which ORDER BY hint was pushed down",
    },
  ],
  categories: ["generator", "diagnostic"],
});

// ============================================================================
// sample_echo - echoes TABLESAMPLE pushdown hints
// ============================================================================

interface SampleEchoArgs {
  count: number;
  batch_size: number;
}

interface SampleEchoState {
  remaining: number;
  currentIndex: number;
  samplePercentage: number;
  sampleSeed: bigint;
}

const SAMPLE_ECHO_SCHEMA = new Schema([
  new Field("n", new Int64(), true),
  new Field("s", new Utf8(), true),
  new Field("sample_percentage", new Float64(), true),
  new Field("sample_seed", new Int64(), true),
]);

const sample_echo = defineTableFunction<SampleEchoArgs, SampleEchoState>({
  name: "sample_echo",
  description: "Echoes TABLESAMPLE pushdown hints in output",
  args: {
    count: new Int64(),
    batch_size: new Int64(),
  },
  argDefaults: {
    batch_size: 2048,
  },
  projectionPushdown: true,
  samplingPushdown: true,
  onBind: () => ({ outputSchema: SAMPLE_ECHO_SCHEMA }),
  cardinality: (params: TableBindParams<SampleEchoArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  initialState: (params: TableProcessParams<SampleEchoArgs>) => {
    const init = params.initCall;
    return {
      remaining: params.args.count,
      currentIndex: 0,
      samplePercentage: init.tablesample_percentage ?? -1.0,
      sampleSeed: init.tablesample_seed ?? -1n,
    };
  },
  process: (
    params: TableProcessParams<SampleEchoArgs>,
    state: SampleEchoState,
    out: OutputCollector
  ) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }
    const size = Math.min(state.remaining, params.args.batch_size);
    const n: bigint[] = [];
    const s: string[] = [];
    const sp: number[] = [];
    const ss: bigint[] = [];
    for (let i = 0; i < size; i++) {
      const idx = state.currentIndex + i;
      n.push(BigInt(idx));
      s.push(`row_${idx}`);
      sp.push(state.samplePercentage);
      ss.push(state.sampleSeed);
    }
    out.emit(
      batchFromColumns(
        {
          n,
          s,
          sample_percentage: sp,
          sample_seed: ss,
        },
        params.outputSchema
      )
    );
    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [
    {
      sql: "SELECT * FROM sample_echo(100) TABLESAMPLE SYSTEM(10%)",
      description: "See which TABLESAMPLE hint was pushed down",
    },
  ],
  categories: ["generator", "diagnostic"],
});

// ============================================================================
// dynamic_filter_echo - generates descending integers and echoes the current
// per-tick pushdown filter (so ORDER BY ... LIMIT demonstrates the filter
// tightening as DuckDB's Top-N heap narrows). Filter is read from
// params.pushdownFilters on every process() call, not once at init — the
// framework updates it from vgi_pushdown_filters tick metadata.
// ============================================================================

interface DynFilterEchoArgs {
  count: number;
  batch_size: number;
}

interface DynFilterEchoState {
  remaining: number;
  currentIndex: number;
  totalCount: number;
}

const DYN_FILTER_ECHO_SCHEMA = new Schema([
  new Field("n", new Int64(), true),
  new Field("pushed_filters", new Utf8(), true),
]);

const dynamic_filter_echo = defineTableFunction<DynFilterEchoArgs, DynFilterEchoState>({
  name: "dynamic_filter_echo",
  description: "Generates descending integers, echoes dynamic tick filter per batch",
  args: {
    count: new Int64(),
    batch_size: new Int64(),
  },
  argDefaults: {
    batch_size: 100,
  },
  projectionPushdown: true,
  filterPushdown: true,
  autoApplyFilters: true,
  onBind: () => ({ outputSchema: DYN_FILTER_ECHO_SCHEMA }),
  cardinality: (params: TableBindParams<DynFilterEchoArgs>) => ({
    estimate: params.args.count,
    max: params.args.count,
  }),
  initialState: (params: TableProcessParams<DynFilterEchoArgs>) => ({
    remaining: params.args.count,
    currentIndex: 0,
    totalCount: params.args.count,
  }),
  process: (
    params: TableProcessParams<DynFilterEchoArgs>,
    state: DynFilterEchoState,
    out: OutputCollector,
  ) => {
    if (state.remaining <= 0) {
      out.finish();
      return;
    }
    const size = Math.min(state.remaining, params.args.batch_size);
    // Descending order so ORDER BY n ASC LIMIT K forces the Top-N heap to
    // keep tightening as lower values arrive in later batches.
    const ns: bigint[] = [];
    for (let i = 0; i < size; i++) {
      const idx = state.currentIndex + i;
      ns.push(BigInt(state.totalCount - 1 - idx));
    }
    // reprPushedFilters mirrors vgi-python's _format_pushed_filters_safe so
    // tests pattern-match the same Python-repr format ("ConstantFilter(n <…)")
    // on both workers.
    const filterStr = reprPushedFilters(params.pushdownFilters);
    const filterValues = new Array(size).fill(filterStr);
    out.emit(
      batchFromColumns({ n: ns, pushed_filters: filterValues }, params.outputSchema),
    );
    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [
    {
      sql: "SELECT n, pushed_filters FROM dynamic_filter_echo(10000) ORDER BY n LIMIT 5",
      description: "See how the Top-N dynamic filter tightens per batch",
    },
  ],
  categories: ["generator", "diagnostic"],
});

// ============================================================================
// spatial_filter_example — grid of WKB points for spatial filter pushdown
// ============================================================================

interface SpatialFilterArgs { count: number; batch_size: number; }
interface SpatialFilterState {
  remaining: number;
  currentIndex: number;
  totalCount: number;
}

const SPATIAL_FILTER_SCHEMA = new Schema([
  new Field("n", new Int64(), true),
  new Field("x", new Float64(), true),
  new Field("y", new Float64(), true),
  new Field("geom", new Binary(), true, new Map<string, string>([
    ["ARROW:extension:name", "geoarrow.wkb"],
    ["ARROW:extension:metadata", "{}"],
  ])),
]);

const spatial_filter_example = defineTableFunction<SpatialFilterArgs, SpatialFilterState>({
  name: "spatial_filter_example",
  description: "Generates points on a grid with geometry for spatial filter testing",
  args: { count: new Int64(), batch_size: new Int64() },
  argDefaults: { batch_size: 1024 },
  projectionPushdown: true,
  filterPushdown: true,
  autoApplyFilters: true,
  supportedExpressionFilters: ["&&", "st_intersects_extent"],
  onBind: () => ({ outputSchema: SPATIAL_FILTER_SCHEMA }),
  cardinality: (p: TableBindParams<SpatialFilterArgs>) => ({ estimate: p.args.count, max: p.args.count }),
  initialState: (p: TableProcessParams<SpatialFilterArgs>) => ({
    remaining: p.args.count, currentIndex: 0, totalCount: p.args.count,
  }),
  process: (p, state, out) => {
    if (state.remaining <= 0) { out.finish(); return; }
    const cols = Math.max(1, Math.ceil(Math.sqrt(state.totalCount)));
    const size = Math.min(state.remaining, p.args.batch_size);
    const ns: bigint[] = [], xs: number[] = [], ys: number[] = [], geoms: Uint8Array[] = [];
    for (let i = 0; i < size; i++) {
      const idx = state.currentIndex + i;
      const x = (idx % cols) / cols;
      const y = Math.floor(idx / cols) / cols;
      ns.push(BigInt(idx));
      xs.push(x);
      ys.push(y);
      geoms.push(wkbPoint(x, y));
    }
    out.emit(batchFromColumns({ n: ns, x: xs, y: ys, geom: geoms }, p.outputSchema));
    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [{
    sql: "SELECT * FROM spatial_filter_example(100) WHERE geom && ST_MakeEnvelope(0,0,0.5,0.5)",
    description: "Filter grid points by bounding box",
  }],
  categories: ["generator", "spatial", "testing"],
});

// ============================================================================
// expression_filter_test — rows with list + string cols for non-spatial filters
// ============================================================================

interface ExprFilterArgs { count: number; batch_size: number; }
interface ExprFilterState { remaining: number; currentIndex: number; }

const EXPR_FILTER_SCHEMA = new Schema([
  new Field("id", new Int64(), true),
  new Field("name", new Utf8(), true),
  new Field("tags", new List(new Field("item", new Utf8(), true)), true),
  new Field("score", new Float64(), true),
]);

const expression_filter_test = defineTableFunction<ExprFilterArgs, ExprFilterState>({
  name: "expression_filter_test",
  description: "Generates rows for non-spatial expression filter testing",
  args: { count: new Int64(), batch_size: new Int64() },
  argDefaults: { batch_size: 1024 },
  projectionPushdown: true,
  filterPushdown: true,
  autoApplyFilters: true,
  supportedExpressionFilters: ["list_contains", "prefix", "starts_with", "contains"],
  onBind: () => ({ outputSchema: EXPR_FILTER_SCHEMA }),
  cardinality: (p: TableBindParams<ExprFilterArgs>) => ({ estimate: p.args.count, max: p.args.count }),
  initialState: (p: TableProcessParams<ExprFilterArgs>) => ({
    remaining: p.args.count, currentIndex: 0,
  }),
  process: (p, state, out) => {
    if (state.remaining <= 0) { out.finish(); return; }
    const size = Math.min(state.remaining, p.args.batch_size);
    const ids: bigint[] = [], names: string[] = [], tags: string[][] = [], scores: number[] = [];
    for (let i = 0; i < size; i++) {
      const idx = state.currentIndex + i;
      ids.push(BigInt(idx));
      names.push(`item_${idx}`);
      tags.push([`tag_${idx % 5}`, `tag_${(idx + 1) % 5}`]);
      scores.push(idx * 1.1);
    }
    out.emit(batchFromColumns({ id: ids, name: names, tags, score: scores }, p.outputSchema));
    state.currentIndex += size;
    state.remaining -= size;
  },
  examples: [{
    sql: "SELECT * FROM expression_filter_test(100) WHERE list_contains(tags, 'tag_0')",
    description: "Filter rows by tag presence",
  }],
  categories: ["generator", "testing"],
});

// ============================================================================
// Export all table functions
// ============================================================================

export const tableFunctions: VgiFunction[] = [
  sequence,
  nested_sequence,
  double_sequence,
  generator_exception,
  logging_generator,
  partitioned_sequence,
  partitioned_preserves_order,
  partitioned_no_order_guarantee,
  partitioned_fixed_order,
  projected_data,
  settings_aware,
  ten_thousand,
  constant_columns,
  named_params_echo,
  struct_settings,
  secret_demo,
  scoped_secret_demo,
  filter_echo,
  filter_echo_partitioned,
  slow_cancellable,
  profiling_demo,
  make_series_count,
  make_series_csv,
  make_series_range,
  make_series_float_step,
  make_series_step,
  make_pairs_int,
  make_pairs_mixed,
  make_pairs_str,
  repeat_value_int,
  repeat_value_str,
  rowid_sequence,
  late_materialization,
  value_prune,
  dict_filter_echo,
  versioned_data_scan,
  departments_scan,
  employees_scan,
  products_scan,
  projects_scan,
  colors_scan,
  versioned_constraints_scan,
  order_echo,
  sample_echo,
  dynamic_filter_echo,
  spatial_filter_example,
  expression_filter_test,
];
