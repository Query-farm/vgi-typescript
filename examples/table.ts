// Example table function implementations.
// Ports all 18 table function groups from vgi-python/vgi/examples/table.py.

import {
  Schema,
  Field,
  Int64,
  Float64,
  Bool,
  Utf8,
  Null,
  DataType,
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
  type TableBindParams,
  type TableProcessParams,
  type TableCardinality,
  type BoundStorage,
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
  onBind: () => ({ outputSchema: SEQUENCE_SCHEMA }),
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
  onInit: (params) => {
    const workItems: Uint8Array[] = [];
    for (let start = 0; start < params.args.count; start += CHUNK_SIZE) {
      const end = Math.min(start + CHUNK_SIZE, params.args.count);
      workItems.push(packQQ(start, end));
    }
    params.storage.queuePush(workItems);
    return {
      maxWorkers: DEFAULT_MAX_WORKERS,
      executionId: params.executionId,
      opaqueData: null,
    };
  },
  initialState: () => ({
    currentStart: null,
    currentEnd: null,
    currentIdx: 0,
  }),
  process: (
    params: TableProcessParams<PartitionedSequenceArgs>,
    state: PartitionedSequenceState,
    out: OutputCollector
  ) => {
    // Need a new chunk?
    if (state.currentStart === null || state.currentIdx >= (state.currentEnd ?? 0)) {
      const workData = params.storage!.queuePop();
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
    const bindArgs = params.initCall.bindCall.arguments;
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
    const bindArgs = params.initCall.bindCall.arguments;
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
    const bindArgs = params.initCall.bindCall.arguments;
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
      orderColumn: init.orderByColumnName ?? "(none)",
      orderDirection: init.orderByDirection ? String(init.orderByDirection) : "(none)",
      orderNullOrder: init.orderByNullOrder ? String(init.orderByNullOrder) : "(none)",
      orderLimit: init.orderByLimit ?? -1n,
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
      samplePercentage: init.tablesamplePercentage ?? -1.0,
      sampleSeed: init.tablesampleSeed ?? -1n,
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
// Export all table functions
// ============================================================================

export const tableFunctions: VgiFunction[] = [
  sequence,
  nested_sequence,
  double_sequence,
  generator_exception,
  logging_generator,
  partitioned_sequence,
  projected_data,
  settings_aware,
  ten_thousand,
  constant_columns,
  named_params_echo,
  struct_settings,
  secret_demo,
  scoped_secret_demo,
  filter_echo,
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
  versioned_data_scan,
  departments_scan,
  employees_scan,
  products_scan,
  projects_scan,
  colors_scan,
  geo_points_scan,
  versioned_constraints_scan,
  order_echo,
  sample_echo,
  dynamic_filter_echo,
];
