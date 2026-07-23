// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Same-name-in-two-schemas *exchange-mode + aggregate* fixtures.
//
// The scalar analogue lives in `examples/same_name.ts` (`test_same_name_bind`,
// driven by scalar/same_name_schemas.test). This module covers the three
// remaining function shapes, which reach the worker through DIFFERENT
// resolution sites than a scalar:
//
//   * table-in-out (`test_same_name_transform`) — binds through the
//     VgiTableInOutBind path; its runtime process() rides the bound connection
//     the bind established, so schema disambiguation happens at bind.
//   * table-buffering (`test_same_name_buffered`) — shares that bind site, but
//     its SINK-phase process() runs a stateless pooled unary RPC that carries
//     its own schema_name (protocol 1.2.0). It tags in the SINK phase so a
//     mis-routed process() reads as the wrong tag.
//   * aggregate (`test_same_name_agg`) — every aggregate RPC (bind / update /
//     combine / finalize, and the window fallback that reuses them) resolves
//     through one by-name entry point that now names its schema. The tag is
//     stamped at finalize while accumulation happens in update, so a partial
//     mis-route (bind one, update/finalize another) is visible too.
//
// Each class registers under a name shared with its sibling, in the `main` and
// `data` schemas of the `example` catalog, and tags its output with its own
// schema, so a mis-routed call reads as the wrong tag rather than a plausible
// answer.
//
// Mirrors vgi-python's `vgi/_test_fixtures/table_in_out_same_name.py` and
// `vgi/_test_fixtures/aggregate/same_name.py`; driven by
// `test/sql/integration/{table_in_out,aggregate}/same_name_schemas.test`.

import { Schema, Field, Int64, Utf8, RecordBatch } from "@query-farm/apache-arrow";
import {
  defineTableInOutFunction,
  defineTableBufferingFunction,
  batchFromColumns,
  serializeBatch,
  deserializeBatch,
  type TableInOutBindParams,
  type TableInOutProcessParams,
  type TableBufferingBindParams,
  type TableBufferingParams,
  type VgiFunction,
} from "../src/index.js";
import { defineAggregate } from "../src/functions/aggregate.js";
import type { OutputCollector } from "@query-farm/vgi-rpc";

// Deliberately shared across the two schemas — the collision is the point.
const TRANSFORM_NAME = "test_same_name_transform";
const BUFFERED_NAME = "test_same_name_buffered";
const AGG_NAME = "test_same_name_agg";

// The single VARCHAR column every implementation here emits.
const TAG_SCHEMA = new Schema([new Field("tag", new Utf8(), true)]);

/** Render `<schemaName>:<value>` for every row of column 0, preserving nulls. */
function tagBatch(schemaName: string, batch: RecordBatch): RecordBatch {
  const col = batch.getChildAt(0);
  const tags: (string | null)[] = [];
  for (let i = 0; i < batch.numRows; i++) {
    const v = col?.get(i);
    tags.push(v === null || v === undefined ? null : `${schemaName}:${v}`);
  }
  return batchFromColumns({ tag: tags }, TAG_SCHEMA);
}

const TE = new TextEncoder();
const ns = (s: string) => TE.encode(s);

// ---------------------------------------------------------------------------
// Table-in-out (streaming) pair
// ---------------------------------------------------------------------------

function makeTransform(owningSchema: string): VgiFunction {
  return defineTableInOutFunction({
    name: TRANSFORM_NAME,
    description: `Schema-disambiguation probe; the ${owningSchema}-schema table-in-out`,
    onBind: (_params: TableInOutBindParams) => ({ outputSchema: TAG_SCHEMA }),
    process: (
      _params: TableInOutProcessParams,
      _state: null,
      batch: RecordBatch,
      out: OutputCollector,
    ) => {
      out.emit(tagBatch(owningSchema, batch));
    },
    examples: [
      {
        sql: `SELECT * FROM example.${owningSchema}.test_same_name_transform((SELECT 1 AS n))`,
        description: `Returns '${owningSchema}:1'`,
      },
    ],
    categories: ["test", "schema-disambiguation"],
  });
}

export const sameNameMainTransform = makeTransform("main");
export const sameNameDataTransform = makeTransform("data");

// ---------------------------------------------------------------------------
// Table-buffering pair. Tags in the SINK phase (process), buffers the tagged
// batch, and drains one batch per finalize tick — proving the sink-side worker
// resolved the right implementation, a distinct connection from the source.
// ---------------------------------------------------------------------------

interface DrainState {
  afterId: number;
}

function makeBuffered(owningSchema: string): VgiFunction {
  return defineTableBufferingFunction<Record<string, any>, DrainState>({
    name: BUFFERED_NAME,
    description: `Schema-disambiguation probe; the ${owningSchema}-schema buffered function`,
    onBind: (_params: TableBufferingBindParams) => ({ outputSchema: TAG_SCHEMA }),
    process: async (batch: RecordBatch, params: TableBufferingParams) => {
      const tagged = tagBatch(owningSchema, batch);
      await params.storage.stateAppend(ns("buf"), ns(""), serializeBatch(tagged));
      return params.executionId;
    },
    combine: async (_stateIds, params) => [params.executionId],
    initialFinalizeState: () => ({ afterId: -1 }),
    finalize: async (
      params: TableBufferingParams,
      _fid: Uint8Array,
      state: DrainState,
      out: OutputCollector,
    ) => {
      const rows = await params.storage.stateLogScan(ns("buf"), ns(""), state.afterId, 1);
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
        sql: `SELECT * FROM example.${owningSchema}.test_same_name_buffered((SELECT 1 AS n))`,
        description: `Returns '${owningSchema}:1'`,
      },
    ],
    categories: ["test", "schema-disambiguation"],
  });
}

export const sameNameMainBuffered = makeBuffered("main");
export const sameNameDataBuffered = makeBuffered("data");

// ---------------------------------------------------------------------------
// Aggregate pair. A running sum tagged with the owning schema at finalize.
// The window fallback (OVER without a window() callback) reuses this same
// update / combine / finalize path, so schema disambiguation there rides the
// aggregate RPCs' schema_name too.
// ---------------------------------------------------------------------------

interface AggState {
  total: bigint;
}

function makeAgg(owningSchema: string): VgiFunction {
  return defineAggregate<{ value: number }, AggState>({
    name: AGG_NAME,
    description: `Schema-disambiguation probe; the ${owningSchema}-schema aggregate`,
    args: { value: new Int64() },
    outputType: new Utf8(),
    nullHandling: "SPECIAL",
    initialState: () => ({ total: 0n }),
    update: ({ groupIds, columns, ensureState }) => {
      const valueCol = columns[0];
      const n = groupIds.length;
      for (let i = 0; i < n; i++) {
        if (valueCol != null && !valueCol.isValid(i)) continue;
        const v = valueCol?.get(i);
        if (v == null) continue;
        const s = ensureState(groupIds[i]);
        s.total += typeof v === "bigint" ? v : BigInt(v);
      }
    },
    combine: (src, tgt) => ({ total: src.total + tgt.total }),
    finalize: ({ groupIds, states, outputSchema }) => {
      const results = groupIds.map((gid) => {
        const s = states.get(gid);
        const total = s != null ? s.total : 0n;
        return `${owningSchema}:${total}`;
      });
      return batchFromColumns({ result: results }, outputSchema);
    },
    examples: [
      {
        sql: `SELECT example.${owningSchema}.test_same_name_agg(n) FROM range(3) t(n)`,
        description: `Returns '${owningSchema}:3'`,
      },
    ],
    categories: ["aggregate", "test", "schema-disambiguation"],
  });
}

export const sameNameMainAgg = makeAgg("main");
export const sameNameDataAgg = makeAgg("data");

// The `main`-schema half of each pair (advertised in `main`, registered on the
// worker) and the `data`-schema half (advertised in `data`). The worker
// registers all six; the catalog schemas below scope which surfaces where.
export const sameNameExchangeMainFunctions = [
  sameNameMainTransform,
  sameNameMainBuffered,
  sameNameMainAgg,
];
export const sameNameExchangeDataFunctions = [
  sameNameDataTransform,
  sameNameDataBuffered,
  sameNameDataAgg,
];
export const sameNameExchangeFunctions = [
  ...sameNameExchangeMainFunctions,
  ...sameNameExchangeDataFunctions,
];
