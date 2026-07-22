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
  defineRowTransformFunction,
  parentRowsMetadata,
  PARENT_ROW_METADATA_KEY,
  cacheControlMetadata,
  batchFromColumns,
  emptyBatch,
  serializeBatch,
  deserializeBatch,
  secretsOfType,
  type TableInOutBindParams,
  type TableInOutProcessParams,
} from "../src/index.js";
import { createHash } from "node:crypto";
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

// sum_all_columns is now a TableBufferingFunction — see examples/table_buffering.ts.

// ============================================================================
// 5. exception_process - Throws on even batches
// ============================================================================

// exception_process and exception_finalize are now TableBufferingFunctions —
// see examples/table_buffering.ts.

// sum_all_columns_simple_distributed — a *global* cross-substream reduction —
// is now ALSO a TableBufferingFunction (see examples/table_buffering.ts): a
// streaming table-in-out is a per-substream map, and under per-substream
// worker fan-out a streaming finish() that merged across substreams would
// produce a partial. Mirrors vgi-python's migration.

// ============================================================================
// 7. substream_partial_sum — per-substream partial sum emitted at finalize.
//    Proves parallel streaming FINALIZE (Phase A / A4): process() accumulates
//    only THIS substream's rows (emitting nothing but the lockstep empty
//    batch), and finalize() emits ONE row = this substream's partial sum.
//    DuckDB unions the substreams' finalize outputs, so the caller
//    re-aggregates with an outer SELECT sum() — correct no matter how the
//    rows were partitioned across substreams. This is NOT a global
//    cross-substream combine (that is a TableBufferingFunction; see
//    sum_all_columns_simple_distributed in table_buffering.ts). Mirrors
//    vgi-python's SubstreamPartialSumFunction.
// ============================================================================

interface SubstreamPartialSumState {
  total: number;
}

const substream_partial_sum = defineTableInOutFunction<Record<string, any>, SubstreamPartialSumState>({
  name: "substream_partial_sum",
  description: "Per-substream partial sum emitted at finalize (parallel streaming finalize)",
  onBind: (params: TableInOutBindParams) => {
    if (!params.bindCall.input_schema) {
      throw new Error("input_schema is required");
    }
    const first = params.bindCall.input_schema.fields[0];
    return { outputSchema: new Schema([new Field(first.name, new Int64(), true)]) };
  },
  initialState: () => ({ total: 0 }),
  process: (
    params: TableInOutProcessParams,
    state: SubstreamPartialSumState,
    batch: RecordBatch,
    out: OutputCollector,
  ) => {
    const col = batch.getChildAt(0);
    if (col) {
      for (let i = 0; i < col.length; i++) {
        const v = col.get(i);
        if (v === null || v === undefined) continue;
        state.total += Number(v);
      }
    }
    // Accumulate only; emit nothing during processing (lockstep empty batch).
    out.emit(emptyBatch(params.outputSchema));
  },
  finalize: (params: TableInOutProcessParams, states: SubstreamPartialSumState[]) => {
    // `states` are THIS substream's accumulated states (one per worker that
    // handled this substream's batches); their sum is this substream's partial.
    let total = 0;
    for (const s of states) total += Number(s?.total ?? 0);
    const name = params.outputSchema.fields[0].name;
    return [batchFromColumns({ [name]: [BigInt(total)] }, params.outputSchema)];
  },
  categories: ["aggregation", "numeric"],
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

// ============================================================================
// Blended ("UNNEST-style") fixtures — Phase B. A blended function's POSITIONAL
// args ARE its per-row input columns (real typed args, no synthetic TABLE
// placeholder), so ONE registration serves the literal call (f(52,13)), the
// column call (FROM t, f(t.x,t.y)), and LATERAL. Ports vgi-python's
// GeoEncodeFunction / GeoEncode3Function / RowSumFunction / BlendedDropFunction
// / BlendedExplodeFunction / ProjectableBlendedFunction /
// HostileProvenanceFunction from vgi/_test_fixtures/table_in_out.py.
// ============================================================================

// Python-float-repr formatting for a rounded double: integral values render
// with a trailing ".0" (round(52.0, 4) -> "52.0"), matching the Python
// fixture's f"{round(lat, p)}" output the shared blended.test asserts.
function fmtRounded(v: number, precision: number): string {
  const f = 10 ** precision;
  const r = Math.round(v * f) / f;
  return Number.isInteger(r) ? `${r.toFixed(1)}` : String(r);
}

const GEOHASH_SCHEMA = new Schema([new Field("geohash", new Utf8(), true)]);

interface GeoNamedArgs {
  precision: number;
}

// geo_encode(latitude, longitude) — the simple blended fixture: one
// registration serves every call shape. latitude/longitude are POSITIONAL
// args = the per-row input columns (read from `batch` by declared name — the
// C++ bind builds the input schema from the declared arg names and casts a
// literal call's constants to the declared types). `precision` is a named
// bind-time option surfaced on params.args. Emits one "<lat>:<lon>" string
// per input row, rounded to `precision` decimals — deterministic so tests
// assert exact values.
const geo_encode = defineRowTransformFunction<GeoNamedArgs>({
  name: "geo_encode",
  description: "Blended per-row geo encoder (lat, lon -> geohash)",
  args: { latitude: new Float64(), longitude: new Float64() },
  argDocs: {
    latitude: "Latitude input column",
    longitude: "Longitude input column",
    precision: "Rounding precision",
  },
  namedArgs: { precision: new Int64() },
  argDefaults: { precision: 4 },
  onBind: () => ({ outputSchema: GEOHASH_SCHEMA }),
  process: (params, batch, out) => {
    const precision = Number(params.args.precision ?? 4);
    const lats = batch.getChild("latitude");
    const lons = batch.getChild("longitude");
    const codes: (string | null)[] = [];
    for (let i = 0; i < batch.numRows; i++) {
      const lat = lats?.get(i);
      const lon = lons?.get(i);
      codes.push(
        lat === null || lat === undefined || lon === null || lon === undefined
          ? null
          : `${fmtRounded(Number(lat), precision)}:${fmtRounded(Number(lon), precision)}`,
      );
    }
    out.emit(batchFromColumns({ geohash: codes }, GEOHASH_SCHEMA));
  },
  categories: ["geo", "blended"],
});

// Arity-overloaded blended geo encoder — same name ("geo_encode"), 3
// positional input columns (lat, lon, alt). Proves same-name blended
// overloads resolve by INPUT-COLUMN arity: blended functions use REAL value
// types (no TABLE-typed arg), so DuckDB permits multiple overloads.
const geo_encode3 = defineRowTransformFunction<GeoNamedArgs>({
  name: "geo_encode",
  description: "Blended per-row geo encoder (lat, lon, alt -> geohash)",
  args: { latitude: new Float64(), longitude: new Float64(), altitude: new Float64() },
  argDocs: {
    latitude: "Latitude input column",
    longitude: "Longitude input column",
    altitude: "Altitude input column",
    precision: "Rounding precision",
  },
  namedArgs: { precision: new Int64() },
  argDefaults: { precision: 4 },
  onBind: () => ({ outputSchema: GEOHASH_SCHEMA }),
  process: (params, batch, out) => {
    const p = Number(params.args.precision ?? 4);
    const lats = batch.getChild("latitude");
    const lons = batch.getChild("longitude");
    const alts = batch.getChild("altitude");
    const codes: (string | null)[] = [];
    for (let i = 0; i < batch.numRows; i++) {
      const lat = lats?.get(i);
      const lon = lons?.get(i);
      const alt = alts?.get(i);
      codes.push(
        lat == null || lon == null || alt == null
          ? null
          : `${fmtRounded(Number(lat), p)}:${fmtRounded(Number(lon), p)}:${fmtRounded(Number(alt), p)}`,
      );
    }
    out.emit(batchFromColumns({ geohash: codes }, GEOHASH_SCHEMA));
  },
  categories: ["geo", "blended"],
});

// Blended VARARGS row-wise sum — proves the varargs input path. `values` is a
// varargs positional arg: the per-row input is N columns of the declared
// type. A varargs blended function has no per-column declared names (the C++
// bind names them col0..colN-1), so process() reads the columns POSITIONALLY
// off `batch`. row_sum(1,2,3) -> 6; FROM t, row_sum(t.a,t.b,t.c) sums each
// row's columns. The `absolute` named option is surfaced on params.args.
interface RowSumNamedArgs {
  absolute: boolean;
}

const ROW_SUM_SCHEMA = new Schema([new Field("row_sum", new Float64(), true)]);

const row_sum = defineRowTransformFunction<RowSumNamedArgs>({
  name: "row_sum",
  description: "Blended per-row varargs sum",
  varargs: { name: "values", type: new Float64(), doc: "Numeric input columns" },
  namedArgs: { absolute: new Bool() },
  argDocs: { absolute: "Sum absolute values" },
  argDefaults: { absolute: false },
  onBind: () => ({ outputSchema: ROW_SUM_SCHEMA }),
  process: (params, batch, out) => {
    const absolute = Boolean(params.args.absolute);
    const sums: number[] = new Array(batch.numRows).fill(0);
    for (let c = 0; c < batch.schema.fields.length; c++) {
      const col = batch.getChildAt(c);
      if (!col) continue;
      for (let i = 0; i < batch.numRows; i++) {
        const v = col.get(i);
        if (v === null || v === undefined) continue;
        const n = Number(v);
        sums[i] += absolute ? Math.abs(n) : n;
      }
    }
    out.emit(batchFromColumns({ row_sum: sums }, ROW_SUM_SCHEMA));
  },
  categories: ["numeric", "blended"],
});

// Blended 1->0 map: emits a single 0-row output batch for its input row.
// Exercises the literal scan-mode drain loop's "empty-but-not-EOS -> keep
// reading, finish only at true EOS" branch (no infinite loop).
const BLENDED_DROP_SCHEMA = new Schema([new Field("v", new Int64(), true)]);

const blended_drop = defineRowTransformFunction({
  name: "blended_drop",
  description: "Blended 1->0 map emitting a single 0-row batch (literal scan-mode)",
  args: { x: new Float64() },
  argDocs: { x: "Input column (ignored)" },
  onBind: () => ({ outputSchema: BLENDED_DROP_SCHEMA }),
  process: (_params, _batch, out) => {
    out.emit(batchFromColumns({ v: [] as bigint[] }, BLENDED_DROP_SCHEMA));
  },
  categories: ["blended", "test"],
});

// Blended 1->N fan-out map carrying per-output-row provenance. For each input
// row with count `n`, emits `n` output rows (the integers 0..n-1) and
// declares which input row produced each output row via parentRowsMetadata —
// that lets the batched correlated-LATERAL operator ship a whole input chunk
// in ONE exchange and still stamp each output row's outer columns from the
// right input row. n=0 -> 1->0 (filter), n=1 -> 1->1, n=3 -> 1->N.
const BLENDED_EXPLODE_SCHEMA = new Schema([new Field("i", new Int64(), true)]);

const blended_explode = defineRowTransformFunction({
  name: "blended_explode",
  description: "Blended 1->N fan-out (emit 0..n-1 per input row) with row provenance",
  args: { n: new Int64() },
  argDocs: { n: "Fan-out count: emit rows 0..n-1 for this input row" },
  onBind: () => ({ outputSchema: BLENDED_EXPLODE_SCHEMA }),
  process: (_params, batch, out) => {
    const counts = batch.getChild("n");
    const outVals: bigint[] = [];
    const parentRows: number[] = [];
    for (let rowIdx = 0; rowIdx < batch.numRows; rowIdx++) {
      const raw = counts?.get(rowIdx);
      const fan = raw === null || raw === undefined || Number(raw) < 0 ? 0 : Number(raw);
      for (let k = 0; k < fan; k++) {
        outVals.push(BigInt(k));
        parentRows.push(rowIdx);
      }
    }
    // Whole-chunk fan-out: one emit for the whole input batch, carrying the
    // per-output-row parent index. (Identity provenance is omitted for 1->1
    // maps — the extension assumes it — but here the row count changes.)
    out.emit(
      batchFromColumns({ i: outVals }, BLENDED_EXPLODE_SCHEMA),
      parentRowsMetadata(parentRows, outVals.length),
    );
  },
  categories: ["blended", "test"],
});

// Blended 1->1 map advertising projection_pushdown, with TWO output columns
// (a=x*10, b=x*100). Regression fixture for the batched correlated-LATERAL
// operator vs projection pushdown: a subset projection under correlated
// LATERAL must NOT read worker column 0 into the `b` slot.
const PROJECTABLE_SCHEMA = new Schema([
  new Field("a", new Int64(), true),
  new Field("b", new Int64(), true),
]);

const projectable_blended = defineRowTransformFunction({
  name: "projectable_blended",
  description: "Blended 1->1 map with projection_pushdown + two output columns",
  args: { x: new Int64() },
  argDocs: { x: "Input column" },
  projectionPushdown: true,
  onBind: () => ({ outputSchema: PROJECTABLE_SCHEMA }),
  process: (_params, batch, out) => {
    const xs = batch.getChild("x");
    const a: (bigint | null)[] = [];
    const b: (bigint | null)[] = [];
    for (let i = 0; i < batch.numRows; i++) {
      const v = xs?.get(i);
      if (v === null || v === undefined) {
        a.push(null);
        b.push(null);
      } else {
        const n = BigInt(v);
        a.push(n * 10n);
        b.push(n * 100n);
      }
    }
    // 1->1 identity map: no provenance needed (the operator assumes identity).
    // Emits the full declared schema; the framework projects when narrowed.
    out.emit(batchFromColumns({ a, b }, PROJECTABLE_SCHEMA));
  },
  categories: ["blended", "test"],
});

// Adversarial blended fixture: emits a MALFORMED vgi_rpc.parent_row payload
// per `mode`, simulating a buggy or hostile worker. The extension must reject
// each rather than use the integers as unchecked array indices; asserted on
// both transports so the subprocess/HTTP validate paths stay symmetric.
interface HostileNamedArgs {
  mode: string;
}

const HOSTILE_SCHEMA = new Schema([new Field("hv", new Int64(), true)]);

function b64encode(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString("base64");
}

const hostile_provenance = defineRowTransformFunction<HostileNamedArgs>({
  name: "hostile_provenance",
  description: "Adversarial blended fixture emitting malformed vgi_rpc.parent_row",
  args: { x: new Int64() },
  argDocs: { x: "Input column (echoed as output)", mode: "range | length | base64" },
  namedArgs: { mode: new Utf8() },
  argDefaults: { mode: "range" },
  onBind: () => ({ outputSchema: HOSTILE_SCHEMA }),
  process: (params, batch, out) => {
    const n = batch.numRows;
    const xs = batch.getChild("x");
    const hv: (bigint | null)[] = [];
    for (let i = 0; i < n; i++) {
      const v = xs?.get(i);
      hv.push(v === null || v === undefined ? null : BigInt(v));
    }
    const mode = String(params.args.mode ?? "range");
    let payload: string;
    if (mode === "base64") {
      payload = "@@@ this is not base64 @@@";
    } else if (mode === "length") {
      // One int32 too many for the emitted row count.
      payload = b64encode(new Uint8Array((n + 1) * 4));
    } else {
      // "range" — every parent index == n (one past the last valid index n-1)
      const raw = new Uint8Array(n * 4);
      const dv = new DataView(raw.buffer);
      for (let i = 0; i < n; i++) dv.setInt32(i * 4, n, true);
      payload = b64encode(raw);
    }
    out.emit(
      batchFromColumns({ hv }, HOSTILE_SCHEMA),
      new Map([[PARENT_ROW_METADATA_KEY, payload]]),
    );
  },
  categories: ["blended", "test", "adversarial"],
});

// ============================================================================
// Exchange-mode result-cache fixtures (M1/M2) + always-revalidate (304)
// fixtures. Port vgi-python's CachedDoubleFunction / CachedEchoFunction /
// CachedRevalidatingEchoFunction / CachedRevalidatingDoubleFunction.
// ============================================================================

const CACHED_DOUBLE_SCHEMA = new Schema([new Field("doubled", new Int64(), true)]);

function doubledColumn(batch: RecordBatch): (bigint | null)[] {
  const xs = batch.getChild("x");
  const doubled: (bigint | null)[] = [];
  for (let i = 0; i < batch.numRows; i++) {
    const v = xs?.get(i);
    doubled.push(v === null || v === undefined ? null : BigInt(v) * 2n);
  }
  return doubled;
}

// Cacheable blended 1->1 map (x -> x*2) advertising vgi.cache.*. Backs
// exchange-mode result-cache tests on BOTH call shapes served by the same
// registration: the streaming column form and the correlated LATERAL form.
//
// It also advertises `perValue`, which is what enables the extension's per-VALUE
// memo tier (off unless the worker asks for it), so the per_value_* tests have a
// blended-map fixture. As with the cached scalars this is a TEST choice: x*2 is
// far cheaper than a memo probe + decode, so a real worker this cheap should
// leave `perValue` off.
const cached_double = defineRowTransformFunction({
  name: "cached_double",
  description: "Cacheable blended map x -> x*2 (advertises vgi.cache.ttl + per_value)",
  args: { x: new Int64() },
  argDocs: { x: "Input column" },
  onBind: () => ({ outputSchema: CACHED_DOUBLE_SCHEMA }),
  process: (_params, batch, out) => {
    out.emit(
      batchFromColumns({ doubled: doubledColumn(batch) }, CACHED_DOUBLE_SCHEMA),
      cacheControlMetadata({ ttl: 300, perValue: true }),
    );
  },
  categories: ["blended", "cache", "test"],
});

// Cacheable CLASSIC (TABLE-input) streaming table-in-out passthrough. Called
// as FROM cached_echo((SELECT ...)) — routed through the streaming
// VgiTableInOutFunction exchange (M1 per-input-batch memoization). Advertises
// a ttl on each output batch.
const cached_echo = defineTableInOutFunction({
  name: "cached_echo",
  description: "Cacheable classic (TABLE-input) passthrough (advertises vgi.cache.ttl)",
  onBind: (params: TableInOutBindParams) => {
    if (!params.bindCall.input_schema) {
      throw new Error("input_schema is required");
    }
    return { outputSchema: params.bindCall.input_schema };
  },
  process: (
    _params: TableInOutProcessParams,
    _state: null,
    batch: RecordBatch,
    out: OutputCollector,
  ) => {
    out.emit(batch, cacheControlMetadata({ ttl: 300 }));
  },
  categories: ["cache", "test"],
});

// Stable etag from a batch's content (deterministic across runs for equal
// data). Only compared against etags this same worker minted earlier, so the
// exact digest formula need not match other SDKs.
function contentEtag(batch: RecordBatch): string {
  const h = createHash("sha256");
  for (let c = 0; c < batch.schema.fields.length; c++) {
    const col = batch.getChildAt(c);
    const vals: string[] = [];
    if (col) {
      for (let i = 0; i < batch.numRows; i++) {
        const v = col.get(i);
        vals.push(v === null || v === undefined ? "null" : String(v));
      }
    }
    h.update(`${batch.schema.fields[c].name}:[${vals.join(",")}];`);
  }
  return h.digest("hex").slice(0, 16);
}

// Classic (TABLE-input) passthrough with the always-revalidate (304)
// contract: CacheControl(ttl=0, etag, revalidatable) — stored but immediately
// stale, so every repeat sends a conditional request (vgi.cache.if_none_match
// on the input batch's metadata). On a matching validator the worker answers
// with a 0-row not_modified batch and the C++ side reuses the stored bytes.
const cached_reval_echo = defineTableInOutFunction({
  name: "cached_reval_echo",
  description: "Classic passthrough with always-revalidate (304 not_modified) contract",
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
    out: OutputCollector,
  ) => {
    const etag = contentEtag(batch);
    if (params.ifNoneMatch === etag) {
      // 304 Not Modified: the client's stored copy for this input is valid.
      out.emit(
        batch.slice(0, 0),
        cacheControlMetadata({ notModified: true, ttl: 0, etag, revalidatable: true }),
      );
      return;
    }
    out.emit(batch, cacheControlMetadata({ ttl: 0, etag, revalidatable: true }));
  },
  categories: ["cache", "test"],
});

// Blended map (x -> x*2) with the always-revalidate (304) contract —
// exercises the LATERAL exchange-cache revalidation path (M2).
const cached_reval_double = defineRowTransformFunction({
  name: "cached_reval_double",
  description: "Blended map x->x*2 with always-revalidate (304 not_modified) contract",
  args: { x: new Int64() },
  argDocs: { x: "Input column" },
  onBind: () => ({ outputSchema: CACHED_DOUBLE_SCHEMA }),
  process: (params, batch, out) => {
    const etag = contentEtag(batch);
    if (params.ifNoneMatch === etag) {
      out.emit(
        batchFromColumns({ doubled: [] as bigint[] }, CACHED_DOUBLE_SCHEMA),
        cacheControlMetadata({ notModified: true, ttl: 0, etag, revalidatable: true }),
      );
      return;
    }
    out.emit(
      batchFromColumns({ doubled: doubledColumn(batch) }, CACHED_DOUBLE_SCHEMA),
      cacheControlMetadata({ ttl: 0, etag, revalidatable: true }),
    );
  },
  categories: ["blended", "cache", "test"],
});

export const tableInOutFunctions: VgiFunction[] = [
  echo,
  repeat_inputs,
  substream_partial_sum,
  filter_by_setting,
  slow_cancellable_inout,
  unnest_tensor_rows,
  echo_witness,
  secret_in_out,
  geo_encode,
  geo_encode3,
  row_sum,
  blended_drop,
  blended_explode,
  projectable_blended,
  hostile_provenance,
  cached_double,
  cached_echo,
  cached_reval_echo,
  cached_reval_double,
];
