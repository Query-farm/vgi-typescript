// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// projection_repro fixture worker — TypeScript port of
// vgi-python/vgi/_test_fixtures/projection_repro/worker.py.
//
// Provides four functions that exercise projection pushdown end-to-end:
//   proj_repro_strict        — emits batches built against params.outputSchema
//   proj_repro_full_schema   — emits batches built against the full WIDE_SCHEMA
//   proj_repro_chunked       — multi-tick variant of full_schema
//   proj_repro_multi_worker  — multi-tick + multi-worker variant
//
// The reproducer's value: with projection pushdown DuckDB asks for a
// subset of columns. A regression in the C++ wire mapping shows up as
// `value_schema_id IS NOT NULL` returning rows even when the worker
// emitted None for that column on every row.

import {
  Schema,
  Field,
  Int32,
  Int64,
  Utf8,
  Binary,
  Struct,
  List,
  TimestampMillisecond,
  RecordBatch,
} from "@query-farm/apache-arrow";
import {
  defineTableFunction,
  batchFromRows,
  type CatalogDescriptor,
  type TableProcessParams,
} from "../src/index.js";
import type { OutputCollector } from "@query-farm/vgi-rpc";
import type { VgiFunction } from "../src/index.js";

const HEADERS_TYPE = new List(new Field("item", new Struct([
  new Field("k", new Utf8(), false),
  new Field("v", new Binary(), true),
]), false));

const WIDE_SCHEMA = new Schema([
  new Field("topic", new Utf8(), false),
  new Field("partition", new Int32(), false),
  new Field("offset", new Int64(), false),
  new Field("timestamp", new TimestampMillisecond("UTC" as any), true),
  new Field("timestamp_type", new Utf8(), true),
  new Field("key", new Binary(), true),
  new Field("key_string", new Utf8(), true),
  new Field("key_schema_id", new Int32(), true),
  new Field("value", new Binary(), true),
  new Field("value_string", new Utf8(), true),
  new Field("value_schema_id", new Int32(), true),
  new Field("headers", HEADERS_TYPE, false),
]);

interface ProjReproArgs {
  n: number;
}

function buildRow(i: number): Record<string, any> {
  const enc = new TextEncoder();
  return {
    topic: "demo_topic",
    partition: i % 4,
    offset: BigInt(i),
    timestamp: null,
    timestamp_type: null,
    key: enc.encode(`k${i}`),
    key_string: `k${i}`,
    key_schema_id: null,
    value: enc.encode(`v${i}`),
    value_string: `v${i}`,
    value_schema_id: null,
    headers: [],
  };
}

function projectRow(row: Record<string, any>, outputSchema: Schema): Record<string, any> {
  const out: Record<string, any> = {};
  for (const f of outputSchema.fields) out[f.name] = row[f.name] ?? null;
  return out;
}

const proj_repro_strict = defineTableFunction<ProjReproArgs, null>({
  name: "proj_repro_strict",
  description: "projection-pushdown reproducer (strict params.output_schema)",
  args: { n: new Int64() },
  projectionPushdown: true,
  onBind: () => ({ outputSchema: WIDE_SCHEMA }),
  process: (params: TableProcessParams<ProjReproArgs>, _state, out: OutputCollector) => {
    const n = Number(params.args.n);
    const rows = [];
    for (let i = 0; i < n; i++) rows.push(projectRow(buildRow(i), params.outputSchema));
    out.emit(batchFromRows(rows, params.outputSchema));
    out.finish();
  },
  categories: ["test", "projection"],
});

const proj_repro_full_schema = defineTableFunction<ProjReproArgs, null>({
  name: "proj_repro_full_schema",
  description: "projection-pushdown reproducer (emits full WIDE_SCHEMA)",
  args: { n: new Int64() },
  projectionPushdown: true,
  onBind: () => ({ outputSchema: WIDE_SCHEMA }),
  process: (params: TableProcessParams<ProjReproArgs>, _state, out: OutputCollector) => {
    const n = Number(params.args.n);
    const rows = [];
    for (let i = 0; i < n; i++) rows.push(buildRow(i));
    // Always emit against WIDE_SCHEMA — the framework projects down to
    // params.outputSchema as part of the auto-projection pass.
    out.emit(batchFromRows(rows, WIDE_SCHEMA));
    out.finish();
  },
  categories: ["test", "projection"],
});

interface ChunkedState { emitted: number }

const proj_repro_chunked = defineTableFunction<ProjReproArgs, ChunkedState>({
  name: "proj_repro_chunked",
  description: "projection-pushdown reproducer (multi-tick, full WIDE_SCHEMA)",
  args: { n: new Int64() },
  projectionPushdown: true,
  onBind: () => ({ outputSchema: WIDE_SCHEMA }),
  initialState: () => ({ emitted: 0 }),
  process: (params: TableProcessParams<ProjReproArgs>, state, out: OutputCollector) => {
    const n = Number(params.args.n);
    const chunk = 2;
    if (state.emitted >= n) { out.finish(); return; }
    const end = Math.min(state.emitted + chunk, n);
    const rows = [];
    for (let i = state.emitted; i < end; i++) rows.push(buildRow(i));
    state.emitted = end;
    out.emit(batchFromRows(rows, WIDE_SCHEMA));
    if (state.emitted >= n) out.finish();
  },
  categories: ["test", "projection"],
});

const proj_repro_multi_worker = defineTableFunction<ProjReproArgs, ChunkedState>({
  name: "proj_repro_multi_worker",
  description: "projection-pushdown reproducer (multi-tick, multi-worker)",
  args: { n: new Int64() },
  projectionPushdown: true,
  // Single-worker for now — true multi-worker partitioning needs shared
  // work-queue distribution. The test only checks correctness, not pid count.
  maxWorkers: 1,
  onBind: () => ({ outputSchema: WIDE_SCHEMA }),
  initialState: () => ({ emitted: 0 }),
  process: (params: TableProcessParams<ProjReproArgs>, state, out: OutputCollector) => {
    const n = Number(params.args.n);
    const chunk = 2;
    if (state.emitted >= n) { out.finish(); return; }
    const end = Math.min(state.emitted + chunk, n);
    const rows = [];
    for (let i = state.emitted; i < end; i++) rows.push(buildRow(i));
    state.emitted = end;
    out.emit(batchFromRows(rows, WIDE_SCHEMA));
    if (state.emitted >= n) out.finish();
  },
  categories: ["test", "projection"],
});

export const projectionReproFunctions: VgiFunction[] = [
  proj_repro_strict,
  proj_repro_full_schema,
  proj_repro_chunked,
  proj_repro_multi_worker,
];

export const projectionReproCatalog: CatalogDescriptor = {
  name: "projection_repro",
  defaultSchema: "main",
  comment: "Projection-pushdown reproducer fixture",
  schemas: [
    {
      name: "main",
      functions: projectionReproFunctions,
    },
  ],
};
