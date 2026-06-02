// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Example table_buffering (Sink+Source) function implementations.
// Ports the TableBufferingFunction fixtures from
// vgi-python/vgi/_test_fixtures/table_in_out.py.
//
// Pattern: process() appends per-batch state to an append-only log scoped by
// execution_id; combine() reduces/sorts and writes the finalize buffer;
// finalize() cursor-drains one batch per tick. State lives in cross-process
// BoundStorage so the Source phase can run on a different worker process.

import {
  Schema,
  Field,
  Int64,
  Float64,
  Bool,
  DataType,
  RecordBatch,
  Utf8,
} from "@query-farm/apache-arrow";
import {
  defineTableBufferingFunction,
  batchFromColumns,
  serializeBatch,
  deserializeBatch,
  type TableBufferingBindParams,
  type TableBufferingParams,
  type VgiFunction,
} from "../src/index.js";
import type { OutputCollector } from "@query-farm/vgi-rpc";

const TE = new TextEncoder();
const ns = (s: string) => TE.encode(s);

// Per-tick cursor over an append-only state_log namespace.
interface LogDrainState {
  ns: string;
  afterId: number;
}

function passthroughBind(params: TableBufferingBindParams) {
  if (!params.bindCall.input_schema) throw new Error("input_schema is required");
  return { outputSchema: params.bindCall.input_schema };
}

function buildNumericOutputSchema(inputSchema: Schema): Schema {
  const fields: Field[] = [];
  for (const f of inputSchema.fields) {
    if (DataType.isInt(f.type)) fields.push(new Field(f.name, new Int64(), true));
    else if (DataType.isFloat(f.type)) fields.push(new Field(f.name, new Float64(), true));
    // DECIMAL promotes to FLOAT64 in the summed output (matches Python).
    else if (DataType.isDecimal(f.type)) fields.push(new Field(f.name, new Float64(), true));
  }
  return new Schema(fields);
}

/** Decimal scale of an input column (raw integer values are scaled by 10^s). */
function decimalScale(inputSchema: Schema, name: string): number | null {
  const f = inputSchema.fields.find((x) => x.name === name);
  if (f && DataType.isDecimal(f.type)) return (f.type as any).scale ?? 0;
  return null;
}

// Shared LogDrain finalize: emit one buffered batch per tick from ns "buf".
async function logDrainFinalize(
  params: TableBufferingParams,
  _fid: Uint8Array,
  state: LogDrainState,
  out: OutputCollector,
): Promise<void> {
  const rows = await params.storage.stateLogScan(ns(state.ns), ns(""), state.afterId, 1);
  if (rows.length === 0) {
    out.finish();
    return;
  }
  const [logId, value] = rows[0];
  out.emit(deserializeBatch(value));
  state.afterId = logId;
}

const initBufDrain = (): LogDrainState => ({ ns: "buf", afterId: -1 });

// ============================================================================
// buffer_input — collect all input, emit on finalize (one bucket / execution)
// ============================================================================

const buffer_input = defineTableBufferingFunction<Record<string, any>, LogDrainState>({
  name: "buffer_input",
  description: "Collects all input batches and emits during finalization",
  onBind: passthroughBind,
  process: async (batch, params) => {
    await params.storage.stateAppend(ns("buf"), ns(""), serializeBatch(batch));
    return params.executionId;
  },
  combine: async (_stateIds, params) => [params.executionId],
  initialFinalizeState: () => initBufDrain(),
  finalize: logDrainFinalize,
  examples: [
    { sql: "SELECT * FROM buffer_input((SELECT * FROM input_table))", description: "Buffer all input and emit on finalize" },
  ],
  categories: ["utility", "buffer"],
});

// ============================================================================
// echo_buffering — buffered passthrough with projection + filter pushdown
// ============================================================================

const echo_buffering = defineTableBufferingFunction<Record<string, any>, LogDrainState>({
  name: "echo_buffering",
  description: "Buffered passthrough with projection + filter pushdown",
  projectionPushdown: true,
  filterPushdown: true,
  autoApplyFilters: true,
  onBind: passthroughBind,
  process: async (batch, params) => {
    await params.storage.stateAppend(ns("buf"), ns(""), serializeBatch(batch));
    return params.executionId;
  },
  combine: async (_stateIds, params) => [params.executionId],
  initialFinalizeState: () => initBufDrain(),
  finalize: logDrainFinalize,
  categories: ["test", "buffer", "pushdown"],
});

// ============================================================================
// sum_all_columns — column-wise sums across all batches (single output row)
// ============================================================================

interface SumArgs {
  logging: boolean;
}

function partialSumsBatch(
  batch: RecordBatch,
  outSchema: Schema,
): RecordBatch {
  const columns: Record<string, any[]> = {};
  for (const f of outSchema.fields) {
    const col = batch.getChild(f.name);
    const inField = batch.schema.fields.find((x) => x.name === f.name);
    const scale = inField && DataType.isDecimal(inField.type) ? ((inField.type as any).scale ?? 0) : null;
    let sum: bigint | number = DataType.isInt(f.type) ? 0n : 0;
    if (col) {
      for (let i = 0; i < col.length; i++) {
        const v = col.get(i);
        if (v === null || v === undefined) continue;
        if (scale != null) {
          // DECIMAL raw values are scaled integers; promote to float64.
          sum = (sum as number) + Number(v) / Math.pow(10, scale);
        } else if (typeof sum === "bigint") {
          sum += typeof v === "bigint" ? v : BigInt(v);
        } else {
          sum += Number(v);
        }
      }
    }
    columns[f.name] = [sum];
  }
  return batchFromColumns(columns, outSchema);
}

function sumNumericBind(params: TableBufferingBindParams<SumArgs>) {
  if (!params.bindCall.input_schema) throw new Error("input_schema is required");
  const out = buildNumericOutputSchema(params.bindCall.input_schema);
  if (out.fields.length === 0) {
    throw new Error("sum_all_columns requires at least one numeric input column");
  }
  return { outputSchema: out };
}

const sum_all_columns = defineTableBufferingFunction<SumArgs, LogDrainState>({
  name: "sum_all_columns",
  description: "Computes column-wise sums across all batches",
  namedArgs: { logging: new Bool() },
  argDefaults: { logging: false },
  cardinality: () => ({ estimate: 1n, max: 1n }),
  onBind: sumNumericBind,
  process: async (batch, params) => {
    if (params.args.logging) {
      params.clientLog("INFO", `Processing batch with ${batch.numRows} rows`);
    }
    const partial = partialSumsBatch(batch, params.outputSchema);
    await params.storage.stateAppend(ns("partial"), ns(""), serializeBatch(partial));
    return params.executionId;
  },
  combine: async (stateIds, params) => {
    if (params.args.logging) {
      params.clientLog("INFO", `Combining ${stateIds.length} state_ids`);
    }
    const merged: Record<string, bigint | number> = {};
    for (const f of params.outputSchema.fields) {
      merged[f.name] = DataType.isInt(f.type) ? 0n : 0;
    }
    const rows = await params.storage.stateLogScan(ns("partial"), ns(""));
    for (const [, blob] of rows) {
      const partial = deserializeBatch(blob);
      for (const f of params.outputSchema.fields) {
        const col = partial.getChild(f.name);
        if (!col) continue;
        const v = col.get(0);
        if (v === null || v === undefined) continue;
        if (typeof merged[f.name] === "bigint") {
          merged[f.name] = (merged[f.name] as bigint) + (typeof v === "bigint" ? v : BigInt(v));
        } else {
          merged[f.name] = (merged[f.name] as number) + Number(v);
        }
      }
    }
    const columns: Record<string, any[]> = {};
    for (const f of params.outputSchema.fields) columns[f.name] = [merged[f.name]];
    const mergedBatch = batchFromColumns(columns, params.outputSchema);
    await params.storage.stateAppend(ns("buf"), ns(""), serializeBatch(mergedBatch));
    return [params.executionId];
  },
  initialFinalizeState: () => initBufDrain(),
  finalize: logDrainFinalize,
  examples: [
    { sql: "SELECT * FROM sum_all_columns((SELECT * FROM input_table))", description: "Sum all numeric columns" },
  ],
  categories: ["aggregation", "numeric"],
});

// ============================================================================
// exception_process / exception_finalize — failure injection on sum shape
// ============================================================================

const exception_process = defineTableBufferingFunction<SumArgs, LogDrainState>({
  name: "exception_process",
  description: "Test function that raises exception during process",
  namedArgs: { logging: new Bool() },
  argDefaults: { logging: false },
  cardinality: () => ({ estimate: 1n, max: 1n }),
  onBind: sumNumericBind,
  process: async (_batch, params) => {
    // Race-safe counter via append-only log.
    await params.storage.stateAppend(ns("count"), ns(""), new Uint8Array(0));
    const rows = await params.storage.stateLogScan(ns("count"), ns(""));
    const count = rows.length;
    if (count % 2 === 0) {
      throw new Error(`Intentional exception on batch ${count}`);
    }
    return params.executionId;
  },
  combine: async (_stateIds, params) => {
    // Emit canonical zero row so a clean single-batch run still produces output.
    const columns: Record<string, any[]> = {};
    for (const f of params.outputSchema.fields) columns[f.name] = [DataType.isInt(f.type) ? 0n : 0];
    await params.storage.stateAppend(ns("buf"), ns(""), serializeBatch(batchFromColumns(columns, params.outputSchema)));
    return [params.executionId];
  },
  initialFinalizeState: () => initBufDrain(),
  finalize: logDrainFinalize,
  categories: ["test", "error"],
});

const exception_finalize = defineTableBufferingFunction<SumArgs, LogDrainState>({
  name: "exception_finalize",
  description: "Test function that raises exception during finalize",
  namedArgs: { logging: new Bool() },
  argDefaults: { logging: false },
  cardinality: () => ({ estimate: 1n, max: 1n }),
  onBind: sumNumericBind,
  process: async (batch, params) => {
    const partial = partialSumsBatch(batch, params.outputSchema);
    await params.storage.stateAppend(ns("partial"), ns(""), serializeBatch(partial));
    return params.executionId;
  },
  combine: async (_stateIds, params) => [params.executionId],
  initialFinalizeState: () => initBufDrain(),
  finalize: () => {
    throw new Error("Intentional exception during finalize()");
  },
  categories: ["test", "error"],
});

// ============================================================================
// crash_on_* / hang_on_process — operator failure-path fixtures
// ============================================================================

const crash_on_process = defineTableBufferingFunction<Record<string, any>, LogDrainState>({
  name: "crash_on_process",
  description: "Worker SIGKILLs itself during process (test)",
  onBind: passthroughBind,
  process: () => {
    process.kill(process.pid, "SIGKILL");
    return new Uint8Array(0); // unreachable
  },
  combine: async (_stateIds, params) => [params.executionId],
  initialFinalizeState: () => initBufDrain(),
  finalize: logDrainFinalize,
  categories: ["test", "crash"],
});

const crash_on_combine = defineTableBufferingFunction<Record<string, any>, LogDrainState>({
  name: "crash_on_combine",
  description: "Worker raises during combine (test)",
  onBind: passthroughBind,
  process: async (batch, params) => {
    await params.storage.stateAppend(ns("buf"), ns(""), serializeBatch(batch));
    return params.executionId;
  },
  combine: () => {
    throw new Error("Intentional exception during combine()");
  },
  initialFinalizeState: () => initBufDrain(),
  finalize: logDrainFinalize,
  categories: ["test", "crash"],
});

const crash_on_finalize = defineTableBufferingFunction<Record<string, any>, LogDrainState>({
  name: "crash_on_finalize",
  description: "Worker raises during finalize (test)",
  onBind: passthroughBind,
  process: async (batch, params) => {
    await params.storage.stateAppend(ns("buf"), ns(""), serializeBatch(batch));
    return params.executionId;
  },
  combine: async (_stateIds, params) => [params.executionId],
  initialFinalizeState: () => initBufDrain(),
  finalize: () => {
    throw new Error("Intentional exception during finalize()");
  },
  categories: ["test", "crash"],
});

const hang_on_process = defineTableBufferingFunction<Record<string, any>, LogDrainState>({
  name: "hang_on_process",
  description: "Worker sleeps in process (manual cancel test)",
  onBind: passthroughBind,
  process: async () => {
    await new Promise((r) => setTimeout(r, 3_600_000));
    return new Uint8Array(0); // unreachable
  },
  combine: async (_stateIds, params) => [params.executionId],
  initialFinalizeState: () => initBufDrain(),
  finalize: logDrainFinalize,
  categories: ["test", "hang"],
});

// ============================================================================
// large_state — ~1 MB per batch, exercises IPC chunking on combine/finalize
// ============================================================================

const large_state = defineTableBufferingFunction<Record<string, any>, LogDrainState>({
  name: "large_state",
  description: "Buffers ~1 MB per input batch into state (IPC test)",
  onBind: passthroughBind,
  process: async (_batch, params) => {
    await params.storage.stateAppend(ns("large"), ns(""), new Uint8Array(1024 * 1024));
    return params.executionId;
  },
  combine: async (_stateIds, params) => {
    const rows = await params.storage.stateLogScan(ns("large"), ns(""));
    let total = 0n;
    for (const [, blob] of rows) total += BigInt(blob.length);
    const columns: Record<string, any[]> = {};
    for (const f of params.outputSchema.fields) {
      columns[f.name] = [DataType.isInt(f.type) ? total : Number(total)];
    }
    await params.storage.stateAppend(ns("buf"), ns(""), serializeBatch(batchFromColumns(columns, params.outputSchema)));
    return [params.executionId];
  },
  initialFinalizeState: () => initBufDrain(),
  finalize: logDrainFinalize,
  categories: ["test", "memory"],
});

// ============================================================================
// ordered_buffer_input — sink_order_dependent=true
// ============================================================================

const ordered_buffer_input = defineTableBufferingFunction<Record<string, any>, LogDrainState>({
  name: "ordered_buffer_input",
  description: "buffer_input variant with sink_order_dependent=True",
  sinkOrderDependent: true,
  onBind: passthroughBind,
  process: async (batch, params) => {
    await params.storage.stateAppend(ns("buf"), ns(""), serializeBatch(batch));
    return params.executionId;
  },
  combine: async (_stateIds, params) => [params.executionId],
  initialFinalizeState: () => initBufDrain(),
  finalize: logDrainFinalize,
  categories: ["test", "ordering"],
});

// ============================================================================
// batch_index_buffer_input — requires_input_batch_index=true; sort by index
// ============================================================================

function packIndexed(batchIndex: number, batchBytes: Uint8Array): Uint8Array {
  const out = new Uint8Array(8 + batchBytes.length);
  new DataView(out.buffer).setBigInt64(0, BigInt(batchIndex), true);
  out.set(batchBytes, 8);
  return out;
}
function unpackIndexed(blob: Uint8Array): { index: number; bytes: Uint8Array } {
  const index = Number(new DataView(blob.buffer, blob.byteOffset, 8).getBigInt64(0, true));
  return { index, bytes: blob.subarray(8) };
}

const batch_index_buffer_input = defineTableBufferingFunction<Record<string, any>, LogDrainState>({
  name: "batch_index_buffer_input",
  description: "buffer_input variant using batch_index to reconstruct order",
  requiresInputBatchIndex: true,
  onBind: passthroughBind,
  process: async (batch, params) => {
    if (params.batchIndex == null) {
      throw new Error(
        "batch_index_buffer_input.process() received batch_index=None — " +
          "requiresInputBatchIndex plumbing is broken",
      );
    }
    await params.storage.stateAppend(
      ns("unsorted"),
      ns(""),
      packIndexed(params.batchIndex, serializeBatch(batch)),
    );
    return params.executionId;
  },
  combine: async (_stateIds, params) => {
    const rows = await params.storage.stateLogScan(ns("unsorted"), ns(""));
    const pairs = rows.map(([, v]) => unpackIndexed(v));
    pairs.sort((a, b) => a.index - b.index);
    for (const p of pairs) {
      await params.storage.stateAppend(ns("buf"), ns(""), p.bytes);
    }
    return [params.executionId];
  },
  initialFinalizeState: () => initBufDrain(),
  finalize: logDrainFinalize,
  categories: ["test", "ordering"],
});

// ============================================================================
// ordered_source — source_order_dependent=true; fixed 0..15 sequence
// ============================================================================

interface OneShotState {
  value: number;
  emitted: boolean;
}

const ORDERED_SOURCE_N = 16;
const ORDERED_SOURCE_SCHEMA = new Schema([new Field("v", new Int64(), true)]);

const ordered_source = defineTableBufferingFunction<Record<string, any>, OneShotState>({
  name: "ordered_source",
  description: "Emits a fixed 0..15 sequence via source_order_dependent=True; input is ignored",
  sourceOrderDependent: true,
  onBind: () => ({ outputSchema: ORDERED_SOURCE_SCHEMA }),
  process: async (_batch, params) => params.executionId,
  combine: async () => {
    const ids: Uint8Array[] = [];
    for (let i = 0; i < ORDERED_SOURCE_N; i++) {
      const b = new Uint8Array(4);
      new DataView(b.buffer).setUint32(0, i, false); // big-endian
      ids.push(b);
    }
    return ids;
  },
  initialFinalizeState: (fid) => {
    const value = new DataView(fid.buffer, fid.byteOffset, fid.byteLength).getUint32(0, false);
    return { value, emitted: false };
  },
  finalize: (params, _fid, state, out) => {
    if (state.emitted) {
      out.finish();
      return;
    }
    out.emit(batchFromColumns({ v: [BigInt(state.value)] }, params.outputSchema));
    state.emitted = true;
  },
  categories: ["test", "ordering"],
});

// ============================================================================
// slow_cancellable_buffering — slow buffered producer with on_cancel probe.
// Sink ignores input; finalize emits `count` rows with a per-row sleep so a
// LIMIT 1 query reliably triggers Source-side cancel before EOS.
// ============================================================================

interface SlowBufferingArgs {
  probe_path: string;
  count: number;
  sleep_ms: number;
}
interface SlowBufferingState {
  emitted: number;
  total: number;
  sleepMs: number;
}

const SLOW_BUFFERING_SCHEMA = new Schema([new Field("n", new Int64(), true)]);

const slow_cancellable_buffering = defineTableBufferingFunction<SlowBufferingArgs, SlowBufferingState>({
  name: "slow_cancellable_buffering",
  description: "Slow buffered table function with an on_cancel file probe (test fixture)",
  args: { probe_path: new Utf8() },
  namedArgs: { count: new Int64(), sleep_ms: new Int64() },
  argDefaults: { count: 1000, sleep_ms: 10 },
  onBind: () => ({ outputSchema: SLOW_BUFFERING_SCHEMA }),
  process: async (_batch, params) => params.executionId,
  combine: async (_stateIds, params) => [params.executionId],
  initialFinalizeState: (_fid, params) => ({
    emitted: 0,
    total: Number(params.args.count ?? 1000),
    sleepMs: Number(params.args.sleep_ms ?? 10),
  }),
  finalize: async (params, _fid, state, out) => {
    if (state.emitted >= state.total) {
      out.finish();
      return;
    }
    if (state.sleepMs > 0) await new Promise((r) => setTimeout(r, state.sleepMs));
    out.emit(batchFromColumns({ n: [BigInt(state.emitted)] }, params.outputSchema));
    state.emitted += 1;
  },
  categories: ["test"],
});

// ============================================================================
// Export
// ============================================================================

export const tableBufferingFunctions: VgiFunction[] = [
  buffer_input,
  echo_buffering,
  sum_all_columns,
  exception_process,
  exception_finalize,
  crash_on_process,
  crash_on_combine,
  crash_on_finalize,
  hang_on_process,
  large_state,
  ordered_buffer_input,
  batch_index_buffer_input,
  ordered_source,
  slow_cancellable_buffering,
];
