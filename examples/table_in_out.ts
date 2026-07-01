// Copyright 2025, 2026 Query Farm LLC - https://query.farm
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
  Struct,
} from "@query-farm/apache-arrow";
import {
  defineTableInOutFunction,
  batchFromColumns,
  emptyBatch,
  serializeBatch,
  deserializeBatch,
  secretsOfType,
  type TableInOutBindParams,
  type TableInOutProcessParams,
} from "../src/index.js";
import type { OutputCollector } from "@query-farm/vgi-rpc";
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

// sum_all_columns is now a TableBufferingFunction — see examples/table_buffering.ts.

// ============================================================================
// 5. exception_process - Throws on even batches
// ============================================================================

interface ExceptionProcessState {
  batchCount: number;
}

// exception_process and exception_finalize are now TableBufferingFunctions —
// see examples/table_buffering.ts.

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
// 9. slow_cancellable_inout — passthrough with optional per-batch sleep.
//    Registration-only stub for the function_registration test; the on_cancel
//    semantics the cancel_on_limit test exercises require framework-level
//    on_cancel hooks not yet wired through.
// ============================================================================

const slow_cancellable_inout = defineTableInOutFunction({
  name: "slow_cancellable_inout",
  description: "Slow table-in-out passthrough (test fixture)",
  namedArgs: { sleep_ms: new Int64() },
  argDefaults: { sleep_ms: 50 },
  args: { probe_path: new Utf8() },
  onBind: (params) => {
    if (!params.bindCall.input_schema) {
      throw new Error("slow_cancellable_inout: input_schema is required");
    }
    return { outputSchema: params.bindCall.input_schema };
  },
  process: async (params, _state, batch, out) => {
    const sleepMs = Number((params.args as any).sleep_ms ?? 0);
    if (sleepMs > 0) await new Promise((r) => setTimeout(r, sleepMs));
    out.emit(batch);
  },
  categories: ["test"],
});

// ============================================================================
// 10. unnest_tensor_rows — invert nest_tensor as a table-in-out. One input
//     column shaped {tensor: nested-list, axes: struct of axis lists} →
//     one output row per cell with (value, axes).
// ============================================================================

function unwrapList(v: any): any[] {
  if (v == null) return [];
  if (Array.isArray(v)) return v;
  if (typeof v[Symbol.iterator] === "function") return Array.from(v);
  return [];
}

function unwrapStructFields(v: any, fieldNames: string[]): Record<string, any> {
  if (!v) return {};
  const out: Record<string, any> = {};
  for (const n of fieldNames) {
    if (typeof v.get === "function") {
      out[n] = v.get(n);
    } else if (typeof v === "object") {
      out[n] = (v as any)[n];
    }
  }
  return out;
}

const unnest_tensor_rows = defineTableInOutFunction({
  name: "unnest_tensor_rows",
  description: "Invert nest_tensor, streaming one row per cell (LATERAL-friendly)",
  onBind: (params) => {
    const inputSchema = params.bindCall.input_schema;
    if (!inputSchema || inputSchema.fields.length !== 1) {
      throw new Error("unnest_tensor_rows: input must have exactly one column (the nest_tensor struct)");
    }
    const structType = inputSchema.fields[0].type;
    if (!DataType.isStruct(structType)) {
      throw new Error(`unnest_tensor_rows: input column must be a struct, got ${structType}`);
    }
    const structFields = (structType as any).children as Field[];
    const tensorField = structFields.find((f) => f.name === "tensor");
    const axesField = structFields.find((f) => f.name === "axes");
    if (!tensorField || !axesField) {
      throw new Error("unnest_tensor_rows: struct must have 'tensor' and 'axes' fields");
    }
    const axesType = axesField.type;
    if (!DataType.isStruct(axesType)) {
      throw new Error("unnest_tensor_rows: 'axes' field must be a struct");
    }

    // Unwrap the nest depth of List(List(... value_type)).
    let valueType: DataType = tensorField.type;
    while (DataType.isList(valueType)) {
      valueType = (valueType as any).children[0].type;
    }
    // Output axes struct: each axis is the bare coord type (not List).
    const axisOutFields = ((axesType as any).children as Field[]).map(
      (f) => new Field(f.name, (f.type as any).children?.[0]?.type ?? f.type, true)
    );
    const outputSchema = new Schema([
      new Field("value", valueType, true),
      new Field("axes", new Struct(axisOutFields), true),
    ]);
    return { outputSchema };
  },
  process: (params, _state, batch, out) => {
    const col = batch.getChildAt(0);
    if (!col) { out.emit(emptyBatch(params.outputSchema)); return; }
    const axesField = (params.outputSchema.fields[1].type as any).children as Field[];
    const axisNames = axesField.map((f: Field) => f.name);

    const valueRows: any[] = [];
    const axisRows: Record<string, any>[] = [];

    for (let i = 0; i < batch.numRows; i++) {
      if (!col.isValid(i)) continue;
      const row = col.get(i);
      if (!row) continue;
      const tensor = typeof row.get === "function" ? row.get("tensor") : row.tensor;
      const axesStruct = typeof row.get === "function" ? row.get("axes") : row.axes;
      const axesByName: Record<string, any[]> = {};
      const fields = unwrapStructFields(axesStruct, axisNames);
      for (const n of axisNames) axesByName[n] = unwrapList(fields[n]);

      // Walk the nested tensor list at each axis. Every level corresponds to
      // axisNames[level]; the leaf is the value.
      const walk = (node: any, level: number, indices: number[]) => {
        if (level === axisNames.length) {
          valueRows.push(node);
          const ax: Record<string, any> = {};
          for (let l = 0; l < axisNames.length; l++) {
            const coords = axesByName[axisNames[l]];
            ax[axisNames[l]] = coords[indices[l]] ?? null;
          }
          axisRows.push(ax);
          return;
        }
        const items = unwrapList(node);
        for (let k = 0; k < items.length; k++) {
          walk(items[k], level + 1, [...indices, k]);
        }
      };
      walk(tensor, 0, []);
    }

    if (valueRows.length === 0) {
      out.emit(emptyBatch(params.outputSchema));
      return;
    }
    out.emit(batchFromColumns({ value: valueRows, axes: axisRows }, params.outputSchema));
  },
  categories: ["transform", "tensor"],
});

// ============================================================================
// Export all table-in-out functions
// ============================================================================

// ============================================================================
// echo_witness — projection-pushdown probe. Each output row has every column
// set to len(observed output_schema), so a SELECT of one column reveals
// whether projection narrowed the schema reaching the worker.
// ============================================================================

const echo_witness = defineTableInOutFunction({
  name: "echo_witness",
  description: "Emits len(observed_output_schema) per column — projection probe",
  projectionPushdown: true,
  onBind: (params: TableInOutBindParams) => {
    if (!params.bindCall.input_schema) {
      throw new Error("echo_witness: input_schema is required");
    }
    return { outputSchema: params.bindCall.input_schema };
  },
  process: (
    params: TableInOutProcessParams,
    _state: null,
    batch: RecordBatch,
    out: OutputCollector,
  ) => {
    const observed = params.outputSchema.fields.length;
    const columns: Record<string, any[]> = {};
    for (const field of params.outputSchema.fields) {
      const isBig = DataType.isInt(field.type) && (field.type as any).bitWidth === 64;
      columns[field.name] = new Array(batch.numRows).fill(isBig ? BigInt(observed) : observed);
    }
    out.emit(batchFromColumns(columns, params.outputSchema));
  },
  categories: ["test", "pushdown"],
});

// ============================================================================
// secret_in_out - Append a resolved secret value to each input row
//
// Exercises secrets x table-in-out. Declares a static requiredSecrets so the
// extension pre-resolves the vgi_example secret and delivers its VALUES on the
// bind + init requests. onBind returns the input schema with a trailing
// `secret_string` Utf8 column; process() reads the resolved secret's
// secret_string and appends it (constant) to every input row (1:1).
//
// (The Python fixture calls params.secrets.get() in on_bind for a two-phase
// resolve; the TS table-in-out onBind has no two-phase path, so we use the
// static requiredSecrets approach which resolves the same secret values.)
// ============================================================================

const secret_in_out = defineTableInOutFunction({
  name: "secret_in_out",
  description: "Append a resolved secret value to each input row",
  requiredSecrets: ["vgi_example"],
  onBind: (params: TableInOutBindParams) => {
    if (!params.bindCall.input_schema) {
      throw new Error("input_schema is required");
    }
    const fields = [
      ...params.bindCall.input_schema.fields,
      new Field("secret_string", new Utf8(), true),
    ];
    return { outputSchema: new Schema(fields) };
  },
  process: (
    params: TableInOutProcessParams,
    _state: null,
    batch: RecordBatch,
    out: OutputCollector,
  ) => {
    const secret = secretsOfType(params.secrets, "vgi_example")[0];
    const value =
      secret && "secret_string" in secret ? (secret.secret_string as any) : null;
    const columns: Record<string, any[]> = {};
    for (const f of batch.schema.fields) {
      const col: any[] = [];
      const child = (batch as any).getChild(f.name);
      for (let i = 0; i < batch.numRows; i++) col.push(child.get(i));
      columns[f.name] = col;
    }
    columns["secret_string"] = new Array(batch.numRows).fill(
      value === null || value === undefined ? null : String(value),
    );
    out.emit(batchFromColumns(columns, params.outputSchema));
  },
  examples: [
    { sql: "SELECT * FROM secret_in_out((SELECT 1 AS n))", description: "Append the secret_string value to each input row" },
  ],
  categories: ["transform", "secret"],
});

export const tableInOutFunctions: VgiFunction[] = [
  echo,
  repeat_inputs,
  sum_all_columns_simple_distributed,
  filter_by_setting,
  slow_cancellable_inout,
  unnest_tensor_rows,
  echo_witness,
  secret_in_out,
];
