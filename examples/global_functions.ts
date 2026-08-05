// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Probe functions for global (`system.main`) registration.
//
// WARNING: EXAMPLE/TEST FUNCTIONS ONLY. These exist purely so a client can be
// observed publishing a worker's functions into its *global* function
// namespace, and are deliberately separate from every other fixture:
//
//  * They are additive for other language implementations. The example catalog
//    is a cross-language contract — Python, Go, and Java workers mirror it. If
//    global registration reused existing fixtures (`double`, `ten_thousand`,
//    `vgi_sum`, `echo_buffering`), every implementation would have to make the
//    same semantic change to functions it already ships.
//  * They document their own purpose. Nothing else depends on them, so
//    changing one cannot break an unrelated test.
//
// One per catalog function type, so the client's registration path is
// exercised for every function kind:
//
//   defineScalarFunction          global_scalar     vgi_example_global_scalar
//   defineTableFunction           global_table      vgi_example_global_table
//   defineAggregate               global_agg        vgi_example_global_agg
//   defineTableBufferingFunction  global_buffered   vgi_example_global_buffered
//
// Each returns a value tagged with its own name so a test can assert that the
// globally-published name reached the function it was supposed to, rather than
// some same-named function belonging to another catalog.
//
// Ported from vgi-python's `vgi/_test_fixtures/global_functions.py`.

import { Schema, Field, Int64, Utf8, RecordBatch } from "@query-farm/apache-arrow";
import {
  defineScalarFunction,
  defineTableFunction,
  defineAggregate,
  defineTableBufferingFunction,
  batchFromColumns,
  serializeBatch,
  deserializeBatch,
  type TableProcessParams,
  type TableBufferingBindParams,
  type TableBufferingParams,
  type VgiFunction,
} from "../src/index.js";
import type { OutputCollector } from "@query-farm/vgi-rpc";

const TE = new TextEncoder();
const ns = (s: string) => TE.encode(s);

// ============================================================================
// global_scalar — labels each input so the caller can prove which impl ran.
// SQL: SELECT vgi_example_global_scalar(7) -> 'global_scalar:7'
// ============================================================================

const global_scalar = defineScalarFunction({
  name: "global_scalar",
  description: "Global-registration probe (scalar)",
  params: { value: new Int64() },
  argDocs: { value: "Value to label" },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => {
    const col = batch.getChildAt(0);
    const out: (string | null)[] = [];
    for (let i = 0; i < batch.numRows; i++) {
      const v = col?.get(i);
      out.push(v === null || v === undefined ? null : `global_scalar:${v}`);
    }
    return out;
  },
  examples: [
    {
      sql: "SELECT vgi_example_global_scalar(7)",
      description: "Scalar probe published into system.main",
    },
  ],
  categories: ["test", "global"],
});

// ============================================================================
// global_table — three labelled rows, no arguments.
// SQL: SELECT * FROM vgi_example_global_table()
// ============================================================================

const GLOBAL_TABLE_SCHEMA = new Schema([
  new Field("n", new Int64(), true),
  new Field("label", new Utf8(), true),
]);

interface GlobalTableState {
  emitted: boolean;
}

const global_table = defineTableFunction<Record<string, any>, GlobalTableState>({
  name: "global_table",
  description: "Global-registration probe (table)",
  onBind: () => ({ outputSchema: GLOBAL_TABLE_SCHEMA }),
  cardinality: () => ({ estimate: 3, max: 3 }),
  initialState: () => ({ emitted: false }),
  process: (
    params: TableProcessParams<Record<string, any>>,
    state: GlobalTableState,
    out: OutputCollector,
  ) => {
    if (state.emitted) {
      out.finish();
      return;
    }
    out.emit(
      batchFromColumns(
        {
          n: [0n, 1n, 2n],
          label: ["global_table:0", "global_table:1", "global_table:2"],
        },
        params.outputSchema,
      ),
    );
    state.emitted = true;
  },
  examples: [
    {
      sql: "SELECT * FROM vgi_example_global_table()",
      description: "Table probe published into system.main",
    },
  ],
  categories: ["test", "global"],
});

// ============================================================================
// global_agg — sums int64 input. NullHandling.DEFAULT means DuckDB skips NULL
// inputs, so a group with no state finalizes to SQL NULL.
// SQL: SELECT vgi_example_global_agg(v) FROM t
// ============================================================================

interface GlobalAggState {
  total: bigint;
}

const global_agg = defineAggregate<{ value: number }, GlobalAggState>({
  name: "global_agg",
  description: "Global-registration probe (aggregate)",
  args: { value: new Int64() },
  outputType: new Int64(),
  nullHandling: "DEFAULT",
  initialState: () => ({ total: 0n }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    for (let i = 0; i < groupIds.length; i++) {
      if (valueCol != null && !valueCol.isValid(i)) continue;
      const v = valueCol?.get(i);
      if (v == null) continue;
      // Allocate lazily so a group with no values gets no state, and
      // finalize() returns SQL NULL for it.
      const s = ensureState(groupIds[i]);
      s.total += typeof v === "bigint" ? v : BigInt(v);
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (bigint | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.total : null;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["test", "global"],
});

// ============================================================================
// global_buffered — buffers all input, replays it on finalize.
// SQL: SELECT * FROM vgi_example_global_buffered((SELECT * FROM t))
// ============================================================================

interface GlobalBufferedState {
  afterId: number;
}

const GLOBAL_BUF_NS = "global_buf";

const global_buffered = defineTableBufferingFunction<Record<string, any>, GlobalBufferedState>({
  name: "global_buffered",
  description: "Global-registration probe (table-buffering)",
  onBind: (params: TableBufferingBindParams) => {
    if (!params.bindCall.input_schema) throw new Error("input_schema is required");
    // Output schema = input schema (passthrough).
    return { outputSchema: params.bindCall.input_schema };
  },
  process: async (batch, params) => {
    await params.storage.stateAppend(ns(GLOBAL_BUF_NS), ns(""), serializeBatch(batch));
    return params.executionId;
  },
  // Every state_id is the execution_id; collapse to one stream.
  combine: async (_stateIds, params) => [params.executionId],
  // Start the drain cursor before the first log entry.
  initialFinalizeState: () => ({ afterId: -1 }),
  finalize: async (
    params: TableBufferingParams,
    _finalizeStateId: Uint8Array,
    state: GlobalBufferedState,
    out: OutputCollector,
  ) => {
    const rows = await params.storage.stateLogScan(ns(GLOBAL_BUF_NS), ns(""), state.afterId, 1);
    if (rows.length === 0) {
      out.finish();
      return;
    }
    const [logId, value] = rows[0];
    out.emit(deserializeBatch(value));
    state.afterId = logId;
  },
  examples: [
    {
      sql: "SELECT * FROM vgi_example_global_buffered((SELECT 1 AS x))",
      description: "Buffering probe published into system.main",
    },
  ],
  categories: ["test", "global"],
});

/** The four probes, in registration order (scalar, table, aggregate, buffering). */
export const globalProbeFunctions: VgiFunction[] = [
  global_scalar,
  global_table,
  global_agg,
  global_buffered,
];

export { global_scalar, global_table, global_agg, global_buffered };
