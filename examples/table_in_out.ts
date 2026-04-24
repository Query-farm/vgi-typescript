// Example table-in/table-out function implementations.
// Ports all 7 table-in-out functions from vgi-python/vgi/examples/table_in_out.py.

import {
  Schema,
  Field,
  Int64,
  Float64,
  Bool,
  Utf8,
  DataType,
  RecordBatch,
} from "@query-farm/apache-arrow";
import {
  defineTableInOutFunction,
  batchFromColumns,
  emptyBatch,
  serializeBatch,
  deserializeBatch,
  type TableInOutBindParams,
  type TableInOutProcessParams,
} from "../src/index.js";
import type { OutputCollector } from "vgi-rpc";
import type { VgiFunction } from "../src/index.js";

// ============================================================================
// 1. echo - Passthrough
// ============================================================================

const echo = defineTableInOutFunction({
  name: "echo",
  description: "Passthrough function that emits each input batch unchanged",
  projectionPushdown: true,
  filterPushdown: true,
  autoApplyFilters: true,
  onBind: (params: TableInOutBindParams) => {
    if (!params.bindCall.input_schema) {
      throw new Error("input_schema is required");
    }
    return { outputSchema: params.bindCall.input_schema };
  },
  // Default process: emits input batch unchanged (passthrough)
  examples: [
    { sql: "SELECT * FROM echo((SELECT * FROM input_table))", description: "Pass through all rows unchanged" },
  ],
  categories: ["utility", "debug"],
  tags: { category: "debug", type: "passthrough" },
});

// ============================================================================
// 2. buffer_input - Buffer all input, emit on finalize
// ============================================================================

const buffer_input = defineTableInOutFunction({
  name: "buffer_input",
  description: "Collects all input batches and emits during finalization",
  maxWorkers: 1,
  onInit: (params) => ({
    max_workers: 1,
    execution_id: params.executionId,
    opaque_data: null,
  }),
  process: (
    params: TableInOutProcessParams,
    _state: null,
    batch: RecordBatch,
    out: OutputCollector
  ) => {
    if (batch.numRows > 0) {
      params.storage.queuePush([serializeBatch(batch)]);
    }
    out.emit(emptyBatch(params.outputSchema));
  },
  finalize: (params: TableInOutProcessParams, _states: null[]) => {
    const batches: RecordBatch[] = [];
    for (;;) {
      const item = params.storage.queuePop();
      if (!item) break;
      batches.push(deserializeBatch(item));
    }
    return batches;
  },
  examples: [
    { sql: "SELECT * FROM buffer_input((SELECT * FROM input_table))", description: "Buffer all input and emit on finalize" },
  ],
  categories: ["utility", "buffer"],
});

// ============================================================================
// 3. repeat_inputs - Duplicate batches N times
// ============================================================================

interface RepeatInputsArgs {
  repeat_count: number;
}

const repeat_inputs = defineTableInOutFunction<RepeatInputsArgs>({
  name: "repeat_inputs",
  description: "Duplicates each input batch N times",
  args: {
    repeat_count: new Int64(),
  },
  onBind: (params: TableInOutBindParams<RepeatInputsArgs>) => {
    if (params.args.repeat_count < 1) {
      throw new Error("Repeat count must be at least 1");
    }
    if (!params.bindCall.input_schema) {
      throw new Error("input_schema is required but was None");
    }
    return { outputSchema: params.bindCall.input_schema };
  },
  process: (
    params: TableInOutProcessParams<RepeatInputsArgs>,
    _state: null,
    batch: RecordBatch,
    out: OutputCollector
  ) => {
    // Concatenate the batch repeat_count times
    const repeatCount = params.args.repeat_count;
    const columns: Record<string, any[]> = {};

    for (const field of params.outputSchema.fields) {
      const col = batch.getChild(field.name);
      const values: any[] = [];
      if (col) {
        for (let rep = 0; rep < repeatCount; rep++) {
          for (let i = 0; i < col.length; i++) {
            values.push(col.get(i));
          }
        }
      }
      columns[field.name] = values;
    }

    out.emit(batchFromColumns(columns, params.outputSchema));
  },
  examples: [
    { sql: "SELECT * FROM repeat_inputs(3, (SELECT * FROM input_table))", description: "Repeat each row 3 times" },
  ],
  categories: ["transform", "augmentation"],
});

// ============================================================================
// 4. sum_all_columns - Column-wise sum aggregation
// ============================================================================

interface SumAllColumnsArgs {
  logging: boolean;
}

interface SumAllColumnsState {
  sums: Record<string, bigint | number>;
}

function buildNumericOutputSchema(inputSchema: Schema): Schema {
  const fields: Field[] = [];
  for (const field of inputSchema.fields) {
    if (DataType.isInt(field.type)) {
      fields.push(new Field(field.name, new Int64(), true));
    } else if (DataType.isFloat(field.type)) {
      fields.push(new Field(field.name, new Float64(), true));
    }
    // Skip non-numeric types
  }
  return new Schema(fields);
}

const sum_all_columns = defineTableInOutFunction<SumAllColumnsArgs, SumAllColumnsState>({
  name: "sum_all_columns",
  description: "Computes column-wise sums across all batches",
  namedArgs: {
    logging: new Bool(),
  },
  argDefaults: {
    logging: false,
  },
  onBind: (params: TableInOutBindParams<SumAllColumnsArgs>) => {
    if (!params.bindCall.input_schema) {
      throw new Error("input_schema is required");
    }
    return { outputSchema: buildNumericOutputSchema(params.bindCall.input_schema) };
  },
  initialState: (params: TableInOutProcessParams<SumAllColumnsArgs>) => {
    const sums: Record<string, bigint | number> = {};
    for (const field of params.outputSchema.fields) {
      if (DataType.isInt(field.type)) {
        sums[field.name] = BigInt(0);
      } else {
        sums[field.name] = 0;
      }
    }
    return { sums };
  },
  process: (
    params: TableInOutProcessParams<SumAllColumnsArgs>,
    state: SumAllColumnsState,
    batch: RecordBatch,
    out: OutputCollector
  ) => {
    if (params.args.logging) {
      out.clientLog("INFO", `Processing batch with ${batch.numRows} rows`);
    }

    for (const name of Object.keys(state.sums)) {
      const col = batch.getChild(name);
      if (!col) continue;
      for (let i = 0; i < col.length; i++) {
        const v = col.get(i);
        if (v === null || v === undefined) continue;
        if (typeof state.sums[name] === "bigint") {
          const bigV = typeof v === "bigint" ? v : BigInt(v);
          state.sums[name] = (state.sums[name] as bigint) + bigV;
        } else {
          state.sums[name] = (state.sums[name] as number) + Number(v);
        }
      }
    }

    out.emit(emptyBatch(params.outputSchema));
  },
  finalize: (params: TableInOutProcessParams<SumAllColumnsArgs>, states: SumAllColumnsState[]) => {
    // Merge sums from all workers (framework auto-collected from SQLite)
    const merged: Record<string, bigint | number> = {};
    for (const field of params.outputSchema.fields) {
      merged[field.name] = DataType.isInt(field.type) ? BigInt(0) : 0;
    }
    for (const s of states) {
      if (!s?.sums) continue;
      for (const field of params.outputSchema.fields) {
        const v = s.sums[field.name];
        if (v === null || v === undefined) continue;
        if (typeof merged[field.name] === "bigint") {
          merged[field.name] = (merged[field.name] as bigint) + (typeof v === "bigint" ? v : BigInt(v));
        } else {
          merged[field.name] = (merged[field.name] as number) + Number(v);
        }
      }
    }
    const columns: Record<string, any[]> = {};
    for (const field of params.outputSchema.fields) {
      columns[field.name] = [merged[field.name] ?? (DataType.isInt(field.type) ? BigInt(0) : 0)];
    }
    return [batchFromColumns(columns, params.outputSchema)];
  },
  examples: [
    { sql: "SELECT * FROM sum_all_columns((SELECT * FROM input_table))", description: "Sum all numeric columns" },
  ],
  categories: ["aggregation", "numeric"],
});

// ============================================================================
// 5. exception_process - Throws on even batches
// ============================================================================

interface ExceptionProcessState {
  batchCount: number;
}

const exception_process = defineTableInOutFunction<SumAllColumnsArgs, ExceptionProcessState>({
  name: "exception_process",
  description: "Test function that raises exception during process",
  namedArgs: {
    logging: new Bool(),
  },
  argDefaults: {
    logging: false,
  },
  onBind: (params: TableInOutBindParams<SumAllColumnsArgs>) => {
    if (!params.bindCall.input_schema) {
      throw new Error("input_schema is required");
    }
    return { outputSchema: buildNumericOutputSchema(params.bindCall.input_schema) };
  },
  initialState: () => ({
    batchCount: 0,
  }),
  process: (
    params: TableInOutProcessParams<SumAllColumnsArgs>,
    state: ExceptionProcessState,
    _batch: RecordBatch,
    out: OutputCollector
  ) => {
    state.batchCount += 1;
    if (state.batchCount % 2 === 0) {
      throw new Error(`Intentional exception on batch ${state.batchCount}`);
    }
    out.emit(emptyBatch(params.outputSchema));
  },
  categories: ["test", "error"],
});

// ============================================================================
// 6. exception_finalize - Throws during finalize
// ============================================================================

const exception_finalize = defineTableInOutFunction<SumAllColumnsArgs>({
  name: "exception_finalize",
  description: "Test function that raises exception during finalize",
  namedArgs: {
    logging: new Bool(),
  },
  argDefaults: {
    logging: false,
  },
  onBind: (params: TableInOutBindParams<SumAllColumnsArgs>) => {
    if (!params.bindCall.input_schema) {
      throw new Error("input_schema is required");
    }
    return { outputSchema: buildNumericOutputSchema(params.bindCall.input_schema) };
  },
  process: (
    params: TableInOutProcessParams<SumAllColumnsArgs>,
    _state: null,
    _batch: RecordBatch,
    out: OutputCollector
  ) => {
    out.emit(emptyBatch(params.outputSchema));
  },
  finalize: (_params: TableInOutProcessParams<SumAllColumnsArgs>, _states: null[]) => {
    throw new Error("Intentional exception during finalize()");
  },
  categories: ["test", "error"],
});

// ============================================================================
// 7. sum_all_columns_simple_distributed - Simpler distributed sum
// ============================================================================

interface SimpleDistributedState {
  partialSums: Record<string, bigint | number>;
}

const sum_all_columns_simple_distributed = defineTableInOutFunction<Record<string, any>, SimpleDistributedState>({
  name: "sum_all_columns_simple_distributed",
  description: "Distributed sum using simple callback API",
  onBind: (params: TableInOutBindParams) => {
    if (!params.bindCall.input_schema) {
      throw new Error("input_schema is required");
    }
    return { outputSchema: buildNumericOutputSchema(params.bindCall.input_schema) };
  },
  initialState: (params: TableInOutProcessParams) => {
    const partialSums: Record<string, bigint | number> = {};
    for (const field of params.outputSchema.fields) {
      if (DataType.isInt(field.type)) {
        partialSums[field.name] = BigInt(0);
      } else {
        partialSums[field.name] = 0;
      }
    }
    return { partialSums };
  },
  process: (
    params: TableInOutProcessParams,
    state: SimpleDistributedState,
    batch: RecordBatch,
    out: OutputCollector
  ) => {
    // Accumulate column sums
    for (const name of Object.keys(state.partialSums)) {
      const col = batch.getChild(name);
      if (!col) continue;
      for (let i = 0; i < col.length; i++) {
        const v = col.get(i);
        if (v === null || v === undefined) continue;
        if (typeof state.partialSums[name] === "bigint") {
          const bigV = typeof v === "bigint" ? v : BigInt(v);
          state.partialSums[name] = (state.partialSums[name] as bigint) + bigV;
        } else {
          state.partialSums[name] = (state.partialSums[name] as number) + Number(v);
        }
      }
    }

    out.emit(emptyBatch(params.outputSchema));
  },
  finalize: (params: TableInOutProcessParams, states: SimpleDistributedState[]) => {
    // Merge partial sums from all workers (framework auto-collected from SQLite)
    const merged: Record<string, bigint | number> = {};
    for (const field of params.outputSchema.fields) {
      merged[field.name] = DataType.isInt(field.type) ? BigInt(0) : 0;
    }
    for (const s of states) {
      if (!s?.partialSums) continue;
      for (const field of params.outputSchema.fields) {
        const v = s.partialSums[field.name];
        if (v === null || v === undefined) continue;
        if (typeof merged[field.name] === "bigint") {
          merged[field.name] = (merged[field.name] as bigint) + (typeof v === "bigint" ? v : BigInt(v));
        } else {
          merged[field.name] = (merged[field.name] as number) + Number(v);
        }
      }
    }
    const columns: Record<string, any[]> = {};
    for (const field of params.outputSchema.fields) {
      columns[field.name] = [merged[field.name] ?? (DataType.isInt(field.type) ? BigInt(0) : 0)];
    }
    return [batchFromColumns(columns, params.outputSchema)];
  },
  examples: [
    { sql: "SELECT * FROM sum_all_columns_simple_distributed((SELECT * FROM input_table))", description: "Sum columns using distributed workers with callback API" },
  ],
  categories: ["aggregation", "numeric", "distributed"],
});

// ============================================================================
// 8. filter_by_setting - Filters rows where value >= threshold setting
// ============================================================================

const filter_by_setting = defineTableInOutFunction({
  name: "filter_by_setting",
  description: "Filter rows where value column >= threshold setting",
  requiredSettings: ["threshold"],
  onBind: (params: TableInOutBindParams) => {
    if (!params.bindCall.input_schema) {
      throw new Error("input_schema is required");
    }
    return { outputSchema: params.bindCall.input_schema };
  },
  process: (
    params: TableInOutProcessParams,
    _state: null,
    batch: RecordBatch,
    out: OutputCollector
  ) => {
    const rawThreshold = params.settings.threshold;
    const threshold = typeof rawThreshold === "bigint" ? rawThreshold : BigInt(Number(rawThreshold));

    const col = batch.getChild("value");
    if (!col) {
      out.emit(batch);
      return;
    }

    // Filter rows where value >= threshold
    const indices: number[] = [];
    for (let i = 0; i < col.length; i++) {
      const v = col.get(i);
      if (v !== null && v !== undefined) {
        const bv = typeof v === "bigint" ? v : BigInt(Number(v));
        if (bv >= threshold) {
          indices.push(i);
        }
      }
    }

    if (indices.length === 0) {
      out.emit(emptyBatch(params.outputSchema));
      return;
    }

    // Build filtered batch
    const columns: Record<string, any[]> = {};
    for (const field of params.outputSchema.fields) {
      const srcCol = batch.getChild(field.name);
      const values: any[] = [];
      if (srcCol) {
        for (const idx of indices) {
          values.push(srcCol.get(idx));
        }
      }
      columns[field.name] = values;
    }

    out.emit(batchFromColumns(columns, params.outputSchema));
  },
  examples: [
    { sql: "SELECT * FROM filter_by_setting((SELECT * FROM input_table))", description: "Filter rows using the threshold setting" },
  ],
  categories: ["transform", "settings"],
});

// ============================================================================
// Export all table-in-out functions
// ============================================================================

export const tableInOutFunctions: VgiFunction[] = [
  echo,
  buffer_input,
  repeat_inputs,
  sum_all_columns,
  exception_process,
  exception_finalize,
  sum_all_columns_simple_distributed,
  filter_by_setting,
];
