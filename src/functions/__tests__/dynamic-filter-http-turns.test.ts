// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Dynamic-filter state across HTTP turns.
//
// DuckDB tightens a Top-N (or join) filter mid-scan and ships the change as a
// v2 delta on the next tick's `vgi_pushdown_filters` metadata -- ONCE per
// stream, only when the value changes (vgi d160e2b). A byte-stream worker keeps
// the parsed filters in memory between ticks; an HTTP worker rebuilds every turn
// from its state token. The token used to carry no filter at all, so the turn
// after a delta scanned under the init snapshot again: results stayed correct
// (DuckDB re-applies its own filter) but the worker stopped pruning.
//
// The token now carries a COMPACTED delta history (filter-pushdown/history.ts,
// a port of vgi-python 0283898): for each live (id, revision), tombstones
// included, the first delta that carried it, plus the live predicate order.
// Carrying every delta instead would be vgi-python's quadratic-replay bug, so
// the cursor must also stay flat-sized as turns accumulate.
//
// Driven end to end over HTTP against the real `dynamic_filter_echo` and
// `echo` fixtures served in-process. The requests are hand-built -- this test
// plays the client, stamping delta metadata the way the C++ extension does --
// and every response is the worker's own.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { httpConnect, type RpcClient, STATE_KEY, type StreamSession } from "@query-farm/vgi-rpc";
import { tableFunctions } from "../../../examples/table.js";
import { tableInOutFunctions } from "../../../examples/table_in_out.js";
import { deserializeBatch, field, int64, schema, serializeBatch, utf8, type VgiBatch } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { unwrapResult, wrapRequest } from "../../client/protocol.js";
import { deserializeBindResponse, serializeBindRequest, serializeInitRequest } from "../../protocol/serialize.js";
import type { BindRequest, BindResponse, InitRequest } from "../../protocol/types.js";
import { SIGNING_KEY_BYTES, serveVgiWorker, type VgiHttpServer } from "../../serve-entry.js";
import { FunctionType, TableInOutPhase } from "../../types.js";
import { batchFromColumns, iterRows } from "../../util/arrow/index.js";
import { FunctionRegistry } from "../registry.js";
import { FunctionStorageSqlite, resolveStorageFromEnv, setStorage } from "../storage.js";

const DYNAMIC_FILTER_ECHO = tableFunctions.find((f) => f.meta.name === "dynamic_filter_echo")!;
const ECHO = tableInOutFunctions.find((f) => f.meta.name === "echo")!;

const FILTER_METADATA = new Map([
  ["vgi_filter_encoding", "vgi.filters.v2"],
  ["vgi_filter_version", "2"],
  ["vgi_evaluation_context", "vgi.none.v1"],
]);

/** A v2 filter document batch: `filter_spec` plus one int64 `value_<i>` per literal. */
function documentBatch(document: Record<string, unknown>, values: number[]): VgiBatch {
  const fields = [field("filter_spec", utf8(), false), ...values.map((_, i) => field(`value_${i}`, int64(), true))];
  const columns: Record<string, any[]> = { filter_spec: [JSON.stringify(document)] };
  values.forEach((value, i) => {
    columns[`value_${i}`] = [BigInt(value)];
  });
  return batchFromColumns(columns, schema(fields, FILTER_METADATA));
}

const EMPTY_SNAPSHOT = () =>
  documentBatch(
    { encoding: "vgi.filters.v2", semantics: "vgi.duckdb.standard.v1", kind: "snapshot", predicates: [] },
    [],
  );

function upsert(id: string, revision: number, op: string, valueRef: number): Record<string, unknown> {
  return {
    operation: "upsert",
    id,
    revision,
    mode: "advisory",
    source: "top_n",
    expression: {
      node: "comparison",
      op,
      left: { node: "column_ref", column_index: 0, column_name: "n" },
      right: { node: "literal", value_ref: valueRef },
    },
  };
}

function remove(id: string, revision: number): Record<string, unknown> {
  return { operation: "remove", id, revision };
}

/** Tick metadata carrying one delta, framed the way the C++ extension frames it. */
function deltaMetadata(updates: Record<string, unknown>[], values: number[] = []): Map<string, string> {
  const batch = documentBatch(
    { encoding: "vgi.filters.v2", semantics: "vgi.duckdb.standard.v1", kind: "delta", updates },
    values,
  );
  return new Map([["vgi_pushdown_filters", Buffer.from(serializeBatch(batch)).toString("base64")]]);
}

/** A producer tick: a zero-column batch; its metadata is what the worker reads. */
function tick(metadata: Map<string, string> = new Map()): { batch: VgiBatch; metadata: Map<string, string> } {
  return { batch: batchFromColumns({}, schema([])), metadata };
}

function rows(batch: VgiBatch | null | undefined): Record<string, any>[] {
  return batch ? [...iterRows(batch)] : [];
}

let dir: string;
let server: VgiHttpServer;
let rpc: RpcClient;
// The cursor each request echoed, in request order -- i.e. the size of the
// cursor the worker minted on the turn before. Read off the client's own
// request bodies, which this test builds.
const echoedCursorLengths: number[] = [];

beforeAll(() => {
  dir = mkdtempSync(join(tmpdir(), "vgi-dynamic-filter-turns-"));
  setStorage(new FunctionStorageSqlite(join(dir, "state.db")));
  const registry = new FunctionRegistry();
  registry.register(DYNAMIC_FILTER_ECHO);
  registry.register(ECHO);
  server = serveVgiWorker({
    name: "demo",
    doc: "Dynamic filter HTTP turns test worker.",
    version: "0.0.1",
    registry,
    catalogInterface: new ReadOnlyCatalogInterface(
      { name: "demo", schemas: [{ name: "main", functions: [DYNAMIC_FILTER_ECHO, ECHO] }] },
      registry,
    ),
    port: 0,
    signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(9),
    quiet: true,
    env: {},
  });
  const spy: typeof fetch = (async (input: any, init?: any) => {
    if (init?.body instanceof Uint8Array && String(input).endsWith("/exchange")) {
      const cursor = deserializeBatch(init.body).metadata?.get(STATE_KEY);
      if (cursor) echoedCursorLengths.push(cursor.length);
    }
    return fetch(input, init);
  }) as typeof fetch;
  rpc = httpConnect(`http://localhost:${server.port}`, { prefix: "", fetch: spy });
});

afterAll(() => {
  server?.stop(true);
  setStorage(resolveStorageFromEnv());
  rmSync(dir, { recursive: true, force: true });
});

async function bind(request: BindRequest): Promise<BindResponse> {
  const result = await rpc.call("bind", wrapRequest(serializeBindRequest(request)));
  return deserializeBindResponse(unwrapResult(result as Record<string, any>));
}

function initRequest(bindCall: BindRequest, bound: BindResponse, overrides: Partial<InitRequest> = {}): InitRequest {
  return {
    bind_call: bindCall,
    output_schema: bound.output_schema,
    bind_opaque_data: bound.opaque_data,
    split_tokens: null,
    row_limit: null,
    projection_ids: null,
    pushdown_filters: EMPTY_SNAPSHOT(),
    join_keys: [],
    phase: null,
    finalize_state_id: null,
    order_by_column_name: null,
    order_by_direction: null,
    order_by_null_order: null,
    order_by_limit: null,
    tablesample_percentage: null,
    tablesample_seed: null,
    execution_id: null,
    init_opaque_data: null,
    substream_id: null,
    ...overrides,
  };
}

/** Open `dynamic_filter_echo(count, batch_size := batchSize)`: descending n from count - 1. */
async function echoStream(count: number, batchSize: number): Promise<StreamSession> {
  const bindCall: BindRequest = {
    function_name: "dynamic_filter_echo",
    arguments: new Arguments([count], new Map([["batch_size", batchSize]])),
    function_type: FunctionType.TABLE,
    input_schema: null,
    settings: null,
    secrets: null,
    attach_opaque_data: null,
    transaction_opaque_data: null,
    resolved_secrets_provided: false,
  };
  const bound = await bind(bindCall);
  return rpc.stream("init", wrapRequest(serializeInitRequest(initRequest(bindCall, bound))));
}

/** One producer turn: the data batch this turn returned (possibly zero rows). */
async function turn(session: StreamSession, metadata?: Map<string, string>): Promise<Record<string, any>[]> {
  const reply = await (session as any).exchangeRaw(tick(metadata));
  return rows(reply?.batch);
}

describe("dynamic filters survive HTTP turns (table function)", () => {
  test("a tightened filter still prunes on later turns that carry no delta", async () => {
    const session = await echoStream(1000, 10);
    try {
      // The init turn's batch: 999..990, under the (empty) init snapshot.
      const first = rows((await (session as any).tickRaw())?.batch);
      expect(first.map((r) => Number(r.n))).toEqual([999, 998, 997, 996, 995, 994, 993, 992, 991, 990]);

      // The one delta this stream ever gets: n > 975. This turn's rows (989..980) all pass.
      const applied = await turn(session, deltaMetadata([upsert("top_n:0", 1, "gt", 0)], [975]));
      expect(applied.length).toBe(10);
      expect(applied[0].pushed_filters).toBe("PushdownFilters([ConstantFilter(n > 975)])");

      // No delta from here on. 979..970: only 979..976 pass -- and the worker
      // still sees the filter. Before the fix: all ten rows, filters "(none)".
      const later = await turn(session);
      expect(later.map((r) => Number(r.n))).toEqual([979, 978, 977, 976]);
      expect(later.every((r) => r.pushed_filters === "PushdownFilters([ConstantFilter(n > 975)])")).toBe(true);

      // Every row of the following turns is pruned by the worker.
      for (let i = 0; i < 5; i++) expect(await turn(session)).toEqual([]);
    } finally {
      session.close();
    }
  });

  test("the cursor stays flat-sized while a delta arrives every turn", async () => {
    const session = await echoStream(1000, 10);
    try {
      await (session as any).tickRaw();
      echoedCursorLengths.length = 0;
      const ticks = 60;
      for (let revision = 1; revision <= ticks; revision++) {
        // Rows descend in tens and every tick narrows n < bound, so every
        // delta changes the live predicate -- the case the extension resends.
        const bound = 1000 - 10 * revision; // this turn's rows are [bound - 10, bound - 1]
        const batch = await turn(session, deltaMetadata([upsert("top_n:0", revision, "lt", 0)], [bound]));
        expect(batch.map((r) => Number(r.n))).toEqual(Array.from({ length: 10 }, (_, i) => bound - 1 - i));
        expect(batch[0].pushed_filters).toBe(`PushdownFilters([ConstantFilter(n < ${bound})])`);
      }
      // One more turn with no delta: the latest bound is still in force.
      const rebuilt = await turn(session);
      expect(rebuilt[0].pushed_filters).toBe(`PushdownFilters([ConstantFilter(n < ${1000 - 10 * ticks})])`);

      // echoedCursorLengths[k] is the cursor minted by turn k. After the first
      // delta the history holds exactly one delta however many arrive: a
      // cursor that kept every delta would grow by one delta (~1 KB) per turn.
      const afterFirstDelta = echoedCursorLengths.slice(1);
      expect(afterFirstDelta.length).toBe(ticks);
      const spread = Math.max(...afterFirstDelta) - Math.min(...afterFirstDelta);
      expect(spread).toBeLessThan(64);
    } finally {
      session.close();
    }
  });

  test("a rebuilt turn keeps the predicate order the worker had (remove + re-add)", async () => {
    // Deltas: {a:1, b:1} -> [a, b]; {remove a:2} -> [b]; {a:3} -> [b, a]. The
    // compacted history is the deltas that first carried a:3 and b:1 -- the
    // third and the first -- and replaying those alone yields [a, b]. The turn
    // after the third delta must show the order the third turn itself had.
    const session = await echoStream(1000, 10);
    try {
      await (session as any).tickRaw();
      await turn(session, deltaMetadata([upsert("top_n:0", 1, "lt", 0), upsert("top_n:1", 1, "gt", 1)], [100000, 5]));
      await turn(session, deltaMetadata([remove("top_n:0", 2)]));
      const applied = await turn(session, deltaMetadata([upsert("top_n:0", 3, "lt", 0)], [99999]));
      const rebuilt = await turn(session); // no delta: filters come purely from the tokens
      expect(applied.length).toBe(10);
      expect(rebuilt.length).toBe(10);
      expect(applied[0].pushed_filters).toBe(
        "PushdownFilters([ConstantFilter(n > 5), ConstantFilter(n < 99999)])",
      );
      expect(rebuilt[0].pushed_filters).toBe(applied[0].pushed_filters);
    } finally {
      session.close();
    }
  });

  test("a tombstone survives compaction: a stale upsert cannot resurrect a removed predicate", async () => {
    const session = await echoStream(1000, 10);
    try {
      await (session as any).tickRaw();
      await turn(session, deltaMetadata([upsert("top_n:0", 1, "lt", 0)], [100000]));
      await turn(session, deltaMetadata([remove("top_n:0", 2)]));
      await turn(session); // a turn rebuilt from the compacted tokens
      const stale = await turn(session, deltaMetadata([upsert("top_n:0", 1, "lt", 0)], [5]));
      expect(stale.length).toBe(10);
      expect(stale[0].pushed_filters).toBe("(none)");
    } finally {
      session.close();
    }
  });
});

describe("dynamic filters survive HTTP turns (table-in-out exchange)", () => {
  test("a delta on one input batch still filters the next", async () => {
    const inputSchema = schema([field("n", int64(), true)]);
    const bindCall: BindRequest = {
      function_name: "echo",
      arguments: new Arguments(),
      function_type: FunctionType.TABLE,
      input_schema: inputSchema,
      settings: null,
      secrets: null,
      attach_opaque_data: null,
      transaction_opaque_data: null,
      resolved_secrets_provided: false,
    };
    const bound = await bind(bindCall);
    const session = await rpc.stream(
      "init",
      wrapRequest(serializeInitRequest(initRequest(bindCall, bound, { phase: TableInOutPhase.INPUT }))),
    );
    const input = (values: number[], metadata: Map<string, string> = new Map()) => ({
      batch: batchFromColumns({ n: values.map(BigInt) }, inputSchema),
      metadata,
    });
    const exchange = async (values: number[], metadata?: Map<string, string>) =>
      rows((await (session as any).exchangeRaw(input(values, metadata)))?.batch).map((r) => Number(r.n));
    try {
      const values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
      expect(await exchange(values)).toEqual(values);
      expect(await exchange(values, deltaMetadata([upsert("join:0", 1, "gt", 0)], [5]))).toEqual([6, 7, 8, 9, 10]);
      // No delta on this batch. Before the fix every row came back.
      expect(await exchange(values)).toEqual([6, 7, 8, 9, 10]);
      expect(await exchange(values)).toEqual([6, 7, 8, 9, 10]);
    } finally {
      session.close();
    }
  });
});
