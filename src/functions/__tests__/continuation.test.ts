// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Continued table-in-out output (functions/continuation.ts).
//
// A table-in-out function answers one input batch with several replies: each
// but the last says `vgi.status: HAVE_MORE_OUTPUT`, and the client asks for the
// next with a zero-row tick carrying `vgi.status: CONTINUE`, which the
// framework answers through the function's resume(). Driven here over HTTP --
// every turn rebuilt from its state token, so the resume position and the
// "more pending" flag must ride the token -- against the real blended_paged,
// blended_sliced and paged_explode fixtures served in-process. The test plays
// the client, stamping status metadata the way the C++ extension does.
//
// Also covers what a definition is refused for (resume() without a state,
// maxWorkers: 1, a bad inputRowsPerCall), the catalog's input_rows_per_call,
// and the auto-apply filter dropping provenance with the rows it drops.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { httpConnect, OutputCollector, type RpcClient, type StreamSession } from "@query-farm/vgi-rpc";
import { VgiClient } from "../../client/client.js";
import { tableInOutFunctions } from "../../../examples/table_in_out.js";
import { field, int64, schema, utf8, type VgiBatch, withBatchMetadata } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { CACHE_IF_NONE_MATCH_KEY, cacheControlMetadata } from "../../cache-control.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { unwrapResult, wrapRequest } from "../../client/protocol.js";
import { FilteringOutputCollector } from "../../filter-pushdown/index.js";
import { deserializeFilters } from "../../filter-pushdown/deserialize.js";
import { deserializeBindResponse, serializeBindRequest, serializeInitRequest } from "../../protocol/serialize.js";
import type { BindRequest, BindResponse, InitRequest } from "../../protocol/types.js";
import { SIGNING_KEY_BYTES, serveVgiWorker, type VgiHttpServer } from "../../serve-entry.js";
import { FunctionType, TableInOutPhase } from "../../types.js";
import { batchFromColumns, iterRows } from "../../util/arrow/index.js";
import { decodeParentRows, encodeParentRows, PARENT_ROW_METADATA_KEY } from "../../util/parent-rows.js";
import {
  CONTINUE,
  continuationTurn,
  HAVE_MORE_OUTPUT,
  moreOutputMetadata,
  refuseMoreOutput,
  STATUS_KEY,
} from "../continuation.js";
import { FunctionRegistry } from "../registry.js";
import { FunctionStorageSqlite, resolveStorageFromEnv, setStorage } from "../storage.js";
import { defineScalarFunction } from "../scalar.js";
import { defineTableFunction } from "../table.js";
import { defineRowTransformFunction, defineTableInOutFunction } from "../table-in-out.js";

const FIXTURES = ["blended_paged", "blended_sliced", "paged_explode", "blended_explode"].map(
  (name) => tableInOutFunctions.find((f) => f.meta.name === name)!,
);
const INPUT_SCHEMA = schema([field("n", int64(), true)]);
const I_SCHEMA = schema([field("i", int64(), true)]);
const page = (vals: number[]) => batchFromColumns({ i: vals.map(BigInt) }, I_SCHEMA);

// Functions that break the protocol, one way each, to reach the framework's
// refusals (and the dispatcher's, for streams that cannot continue).
const rowTransform = (name: string, rest: Record<string, unknown>) =>
  defineRowTransformFunction<Record<string, any>, { left: number }>({
    name,
    args: { n: int64() },
    onBind: () => ({ outputSchema: I_SCHEMA }),
    process: (_p, _b, out) => out.emit(page([1]), moreOutputMetadata(true)),
    ...rest,
  } as any);
const BROKEN = [
  // resume() answers its tick with cache control.
  rowTransform("tick_cache", {
    initialState: () => ({ left: 1 }),
    resume: (_p: unknown, _s: unknown, out: OutputCollector) => out.emit(page([2]), cacheControlMetadata({ ttl: 60 })),
  }),
  // resume() answers its tick with nothing.
  rowTransform("silent_resume", { initialState: () => ({ left: 1 }), resume: () => {} }),
  // Says more, but has no resume() to answer the tick.
  rowTransform("more_no_resume", {}),
  // process() answers real input with nothing.
  rowTransform("silent_process", { process: () => {} }),
  defineTableInOutFunction({ name: "silent_classic", process: () => {} }),
  // A scalar and a table function (producer) saying more: refused by the dispatcher.
  defineScalarFunction({
    name: "scalar_more",
    params: { n: int64() },
    returns: int64(),
    compute: (batch) =>
      withBatchMetadata(
        batchFromColumns({ result: Array.from({ length: batch.numRows }, () => 1n) }, schema([field("result", int64(), true)])),
        moreOutputMetadata(true),
      ),
  }),
  defineTableFunction({
    name: "table_more",
    onBind: () => ({ outputSchema: I_SCHEMA }),
    process: (_p, _s, out) => out.emit(page([1]), moreOutputMetadata(true)),
  }),
];

let dir: string;
let server: VgiHttpServer;
let rpc: RpcClient;
let catalog: ReadOnlyCatalogInterface;

beforeAll(() => {
  dir = mkdtempSync(join(tmpdir(), "vgi-continuation-"));
  setStorage(new FunctionStorageSqlite(join(dir, "state.db")));
  const registry = new FunctionRegistry();
  for (const f of [...FIXTURES, ...BROKEN]) registry.register(f);
  catalog = new ReadOnlyCatalogInterface(
    { name: "demo", schemas: [{ name: "main", functions: [...FIXTURES, ...BROKEN] }] },
    registry,
  );
  server = serveVgiWorker({
    name: "demo",
    doc: "Continued output test worker.",
    version: "0.0.1",
    registry,
    catalogInterface: catalog,
    port: 0,
    signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(7),
    quiet: true,
    env: {},
  });
  rpc = httpConnect(`http://localhost:${server.port}`, { prefix: "" });
});

afterAll(() => {
  server?.stop(true);
  setStorage(resolveStorageFromEnv());
  rmSync(dir, { recursive: true, force: true });
});

async function open(name: string, named: Record<string, unknown> = {}): Promise<StreamSession> {
  const bindCall: BindRequest = {
    function_name: name,
    arguments: new Arguments([], new Map(Object.entries(named))),
    function_type: FunctionType.TABLE,
    input_schema: INPUT_SCHEMA,
    settings: null,
    secrets: null,
    attach_opaque_data: null,
    transaction_opaque_data: null,
    resolved_secrets_provided: false,
  };
  const bound: BindResponse = deserializeBindResponse(
    unwrapResult((await rpc.call("bind", wrapRequest(serializeBindRequest(bindCall)))) as Record<string, any>),
  );
  const init: InitRequest = {
    bind_call: bindCall,
    output_schema: bound.output_schema,
    bind_opaque_data: bound.opaque_data,
    split_tokens: null,
    row_limit: null,
    projection_ids: null,
    pushdown_filters: null,
    join_keys: [],
    phase: TableInOutPhase.INPUT,
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
  };
  return rpc.stream("init", wrapRequest(serializeInitRequest(init)));
}

interface Reply {
  i: number[];
  parents: number[] | null;
  more: boolean;
  metadata: Map<string, string>;
}

async function send(session: StreamSession, values: (number | null)[], metadata = new Map<string, string>()): Promise<Reply> {
  const batch = batchFromColumns({ n: values.map((v) => (v == null ? null : BigInt(v))) }, INPUT_SCHEMA);
  return toReply(await (session as any).exchangeRaw({ batch, metadata }));
}

async function tick(session: StreamSession): Promise<Reply> {
  return send(session, [], new Map([[STATUS_KEY, CONTINUE]]));
}

function toReply(raw: { batch: VgiBatch; metadata?: Map<string, string> } | null): Reply {
  expect(raw).not.toBeNull();
  const metadata = new Map<string, string>([...((raw!.batch as any).metadata ?? []), ...(raw!.metadata ?? [])]);
  const encoded = metadata.get(PARENT_ROW_METADATA_KEY);
  return {
    i: [...iterRows(raw!.batch)].map((r) => Number(r.i)),
    parents: encoded ? [...decodeParentRows(encoded)] : null,
    more: metadata.get(STATUS_KEY) === HAVE_MORE_OUTPUT,
    metadata,
  };
}

describe("continued output over HTTP", () => {
  test("blended_paged pages one input across replies, with provenance on each", async () => {
    const s = await open("blended_paged", { page_size: 2 });
    try {
      const first = await send(s, [3, 0, null, 2]);
      expect([first.i, first.parents, first.more]).toEqual([[0, 1], [0, 0], true]);
      const second = await tick(s);
      expect([second.i, second.parents, second.more]).toEqual([[2, 0], [0, 3], true]);
      const last = await tick(s);
      expect([last.i, last.parents, last.more]).toEqual([[1], [3], false]);
      // The next input starts over; a batch with no output is one empty reply.
      const empty = await send(s, [0, -1]);
      expect([empty.i, empty.more]).toEqual([[], false]);
    } finally {
      s.close();
    }
  });

  test("cache control rides only an input's first reply", async () => {
    const s = await open("blended_paged", { page_size: 1, cache_ttl: 60 });
    try {
      const first = await send(s, [2]);
      expect(first.metadata.get("vgi.cache.ttl")).toBe("60");
      const next = await tick(s);
      expect(next.i).toEqual([1]);
      expect([...next.metadata.keys()].some((k) => k.startsWith("vgi.cache."))).toBe(false);
    } finally {
      s.close();
    }
  });

  test("blended_sliced reports its call size and refuses more than 3 rows", async () => {
    const s = await open("blended_sliced", { page_size: 2 });
    try {
      expect((await send(s, [1, 2])).i).toEqual([0, 0]);
      expect((await tick(s)).i).toEqual([1]);
      await expect(send(s, [1, 1, 1, 1])).rejects.toThrow(/inputRowsPerCall = 3/);
    } finally {
      s.close();
    }
  });

  test("paged_explode: revalidation answered with changed, continued output", async () => {
    const s = await open("paged_explode", { page_size: 2, revalidate: true });
    try {
      const plain = await send(s, [3]);
      expect([plain.i, plain.more, plain.metadata.get("vgi.cache.etag")]).toEqual([[0, 1, 2], false, "v1"]);
      const changed = await send(s, [3], new Map([[CACHE_IF_NONE_MATCH_KEY, "v1"]]));
      expect([changed.i, changed.more, changed.metadata.get("vgi.cache.etag")]).toEqual([[1000, 1001], true, "v2"]);
      // The validator belongs to that input: the tick's reply is a plain page.
      const rest = await tick(s);
      expect([rest.i, rest.more, rest.metadata.has("vgi.cache.etag")]).toEqual([[1002], false, false]);
    } finally {
      s.close();
    }
  });

  test("fail_on_page fails the reply it names", async () => {
    const s = await open("paged_explode", { page_size: 1, fail_on_page: 2 });
    try {
      await send(s, [5]);
      await expect(tick(s)).rejects.toThrow(/failing on page 2 as asked/);
    } finally {
      s.close();
    }
  });

  test("a tick with nothing pending is refused", async () => {
    const s = await open("blended_paged");
    try {
      await expect(tick(s)).rejects.toThrow(/no output is pending/);
    } finally {
      s.close();
    }
  });

  test("real input while output is pending is refused", async () => {
    const s = await open("blended_paged", { page_size: 1 });
    try {
      await send(s, [2]);
      await expect(send(s, [1])).rejects.toThrow(/expected a continuation tick/);
    } finally {
      s.close();
    }
  });

  test("a tick with rows is refused", async () => {
    const s = await open("blended_paged", { page_size: 1 });
    try {
      await send(s, [2]);
      await expect(send(s, [1], new Map([[STATUS_KEY, CONTINUE]]))).rejects.toThrow(/must have no rows/);
    } finally {
      s.close();
    }
  });

  test("a tick to a function that never says more is refused", async () => {
    const s = await open("blended_explode");
    try {
      // blended_explode never says more, so no output is ever pending.
      expect((await send(s, [2])).more).toBe(false);
      await expect(tick(s)).rejects.toThrow(/no output is pending/);
    } finally {
      s.close();
    }
  });

  test("a function without resume() may not say more", async () => {
    const s = await open("more_no_resume");
    try {
      await expect(send(s, [1])).rejects.toThrow(/emitted HAVE_MORE_OUTPUT but does not implement resume\(\)/);
    } finally {
      s.close();
    }
  });

  test("cache control on a tick's reply is refused", async () => {
    const s = await open("tick_cache");
    try {
      expect((await send(s, [1])).more).toBe(true);
      await expect(tick(s)).rejects.toThrow(/cache control is only valid on an input's first reply.*vgi\.cache\.ttl/);
    } finally {
      s.close();
    }
  });

  test("a tick resume() answers with nothing is refused", async () => {
    const s = await open("silent_resume");
    try {
      expect((await send(s, [1])).more).toBe(true);
      await expect(tick(s)).rejects.toThrow(/resume\(\) must emit a batch for every continuation tick/);
    } finally {
      s.close();
    }
  });

  test("real input process() answers with nothing is refused", async () => {
    for (const name of ["silent_process", "silent_classic"]) {
      const s = await open(name);
      try {
        await expect(send(s, [1])).rejects.toThrow(/No data batch was emitted/);
      } finally {
        s.close();
      }
    }
  });

  test("not_modified_more: an empty not_modified first reply that says more, then the changed pages", async () => {
    // The framework need not block it (only the client does); the empty first
    // reply is a page of its own.
    const s = await open("paged_explode", { page_size: 2, revalidate: true, not_modified_more: true });
    try {
      const first = await send(s, [3], new Map([[CACHE_IF_NONE_MATCH_KEY, "v1"]]));
      expect([first.i, first.more]).toEqual([[], true]);
      expect([first.metadata.get("vgi.cache.not_modified"), first.metadata.get("vgi.cache.etag")]).toEqual(["1", "v1"]);
      const second = await tick(s);
      expect([second.i, second.more, second.metadata.has("vgi.cache.not_modified")]).toEqual([[1000, 1001], true, false]);
      const last = await tick(s);
      expect([last.i, last.more]).toEqual([[1002], false]);
    } finally {
      s.close();
    }
  });

  test("not_modified_more: the empty first reply counts toward fail_on_page", async () => {
    const s = await open("paged_explode", { page_size: 2, revalidate: true, not_modified_more: true, fail_on_page: 2 });
    try {
      // Page 1 is the empty not_modified reply, so the first tick is page 2.
      expect((await send(s, [3], new Map([[CACHE_IF_NONE_MATCH_KEY, "v1"]]))).i).toEqual([]);
      await expect(tick(s)).rejects.toThrow(/failing on page 2 as asked/);
    } finally {
      s.close();
    }
  });
});

describe("HAVE_MORE_OUTPUT outside a table-in-out exchange (dispatcher)", () => {
  test("a scalar function's stream refuses it", async () => {
    const client = new VgiClient(httpConnect(`http://localhost:${server.port}`, { prefix: "" }));
    const run = async () => {
      for await (const _ of client.scalarFunction({
        functionName: "scalar_more",
        input: [batchFromColumns({ n: [1n, 2n] }, INPUT_SCHEMA)],
      })) {
        // drain
      }
    };
    await expect(run()).rejects.toThrow(/scalar_more: HAVE_MORE_OUTPUT .* only valid in a table-in-out/);
  });

  test("a table function's (producer) stream refuses it", async () => {
    const client = new VgiClient(httpConnect(`http://localhost:${server.port}`, { prefix: "" }));
    const run = async () => {
      for await (const _ of client.tableFunctionRows({ functionName: "table_more" })) {
        // drain
      }
    };
    await expect(run()).rejects.toThrow(/table_more: HAVE_MORE_OUTPUT .* only valid in a table-in-out/);
  });
});

describe("tick status off the collector (byte-stream transports)", () => {
  // vgi-rpc's stdio/pipe dispatch may hand the exchange an input batch rebuilt
  // to the declared input schema, and the status then reaches the turn only
  // through out.inputMetadata.
  test("a tick marked only on out.inputMetadata is a tick", async () => {
    const eState = { morePending: true };
    const bare = batchFromColumns({ n: [] }, INPUT_SCHEMA);
    const out = new OutputCollector(I_SCHEMA, false, "", null, undefined, undefined, undefined, {
      inputMetadata: new Map([[STATUS_KEY, CONTINUE]]),
    } as any);
    const seen: boolean[] = [];
    await continuationTurn("t", eState, bare, out, true, async (tick) => {
      seen.push(tick);
      out.emit(page([7]));
    });
    expect([seen, eState.morePending]).toEqual([[true], false]);
  });
});

describe("catalog", () => {
  test("input_rows_per_call is advertised for blended_sliced only", async () => {
    const infos = catalog.schemaContentsFunctions(null as any, ["main"], "table_function");
    const byName = new Map<string, any>(infos.map((f) => [f.name, f]));
    expect(byName.get("blended_sliced").input_rows_per_call).toBe(3);
    expect(byName.get("blended_paged").input_rows_per_call ?? null).toBeNull();
    expect(byName.get("paged_explode").input_rows_per_call ?? null).toBeNull();
  });
});

describe("definition-time refusals", () => {
  const onBind = () => ({ outputSchema: INPUT_SCHEMA });
  const process = () => {};

  test("resume() needs an initialState to resume from", () => {
    expect(() =>
      defineRowTransformFunction({ name: "f", args: { n: int64() }, onBind, process, resume: () => {} }),
    ).toThrow(/initialState/);
    expect(() => defineTableInOutFunction({ name: "g", resume: () => {} })).toThrow(/initialState/);
  });

  test("resume() is refused with maxWorkers: 1", () => {
    expect(() =>
      defineTableInOutFunction({ name: "g", maxWorkers: 1, initialState: () => ({ row: 0 }), resume: () => {} }),
    ).toThrow(/maxWorkers: 1/);
  });

  test("inputRowsPerCall must be a positive integer, on a blended function", () => {
    for (const bad of [0, -1, 1.5]) {
      expect(() =>
        defineRowTransformFunction({ name: "f", args: { n: int64() }, onBind, process, inputRowsPerCall: bad }),
      ).toThrow(/positive integer/);
    }
    expect(() => defineTableInOutFunction({ name: "g", inputRowsPerCall: 3 } as any)).toThrow(/blended/);
    const ok = defineRowTransformFunction({ name: "f", args: { n: int64() }, onBind, process, inputRowsPerCall: 1 });
    expect(ok.meta.inputRowsPerCall).toBe(1);
  });
});

describe("collector checks", () => {
  test("HAVE_MORE_OUTPUT is refused outside a table-in-out exchange", () => {
    const out = new OutputCollector(INPUT_SCHEMA, false);
    out.emit(batchFromColumns({ n: [1n] }, INPUT_SCHEMA), moreOutputMetadata(true));
    expect(() => refuseMoreOutput("t", out)).toThrow(/only valid in a table-in-out/);
    const fine = new OutputCollector(INPUT_SCHEMA, false);
    fine.emit(batchFromColumns({ n: [1n] }, INPUT_SCHEMA), moreOutputMetadata(false));
    expect(() => refuseMoreOutput("t", fine)).not.toThrow();
  });

  test("auto-applied filters drop provenance with the rows they drop", () => {
    const out = new OutputCollector(INPUT_SCHEMA, false);
    // n > 1, as a v2 snapshot.
    const document = {
      encoding: "vgi.filters.v2",
      semantics: "vgi.duckdb.standard.v1",
      kind: "snapshot",
      predicates: [
        {
          id: "p",
          revision: 0,
          mode: "required",
          source: "query",
          expression: {
            node: "comparison",
            op: "gt",
            left: { node: "column_ref", column_index: 0, column_name: "n" },
            right: { node: "literal", value_ref: 0 },
          },
        },
      ],
    };
    const snapshot = batchFromColumns(
      { filter_spec: [JSON.stringify(document)], value_0: [1n] },
      schema(
        [field("filter_spec", utf8(), false), field("value_0", int64(), true)],
        new Map([
          ["vgi_filter_encoding", "vgi.filters.v2"],
          ["vgi_filter_version", "2"],
          ["vgi_evaluation_context", "vgi.none.v1"],
        ]),
      ),
    );
    const filters = deserializeFilters(snapshot, { outputSchema: INPUT_SCHEMA });
    const filtering = new FilteringOutputCollector(out, filters);
    const batch = batchFromColumns({ n: [1n, 2n, 0n, 3n] }, INPUT_SCHEMA);
    filtering.emit(batch, new Map([[PARENT_ROW_METADATA_KEY, encodeParentRows([0, 0, 1, 2])]]));
    const emitted = out.batches[out.dataBatchIdx!];
    expect([...iterRows(emitted.batch)].map((r) => Number(r.n))).toEqual([2, 3]);
    expect([...decodeParentRows(emitted.metadata!.get(PARENT_ROW_METADATA_KEY)!)]).toEqual([0, 2]);
  });
});
