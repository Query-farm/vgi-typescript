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
} from "@query-farm/apache-arrow";
import {
  defineTableFunction,
  batchFromColumns,
  emptyBatch,
  formatPushedFilters,
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
];
