// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// A table-in-out function's accumulated state is keyed per SUBSTREAM, not per
// process.
//
// After every INPUT batch the framework upserts the function's state into
// storage scoped to the execution, and FINALIZE hands finish() every stored
// state. The execution is shared by every connection of a fanned-out scan, so
// the row key has to tell those connections apart. It used to be the process
// id, which only does when each connection is its own process: one process
// serving several connections -- the launcher, TCP, or HTTP, as here -- let
// them overwrite each other, and finish() undercounted (the vgi-python run that
// found it: substream_partial_sum returned 194850 for 1999000). The key is now
// `InitRequest.substream_id`, the random client-minted id the DuckDB extension
// already sends on every substream; the pid is only the fallback for a client
// that sends none. Mirrors vgi-python 072e543.
//
// Driven end to end over HTTP against a real in-process worker, which is one
// process serving every connection -- the exact condition. The requests are
// hand-built (two INPUT inits sharing one execution, which is what a fanned-out
// client sends); every response is the worker's own.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { httpConnect, type RpcClient, type StreamSession } from "@query-farm/vgi-rpc";
import { field, int64, schema, type VgiBatch } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { unwrapResult, wrapRequest } from "../../client/protocol.js";
import {
  deserializeBindResponse,
  deserializeGlobalInitResponse,
  serializeBindRequest,
  serializeInitRequest,
} from "../../protocol/serialize.js";
import type { BindRequest, BindResponse, InitRequest } from "../../protocol/types.js";
import { SIGNING_KEY_BYTES, serveVgiWorker, type VgiHttpServer } from "../../serve-entry.js";
import { FunctionType, TableInOutPhase } from "../../types.js";
import { batchFromColumns, emptyBatch, iterRows } from "../../util/arrow/index.js";
import { FunctionRegistry } from "../registry.js";
import { BoundStorage, FunctionStorageSqlite, resolveStorageFromEnv, setStorage } from "../storage.js";
import { defineTableInOutFunction } from "../table-in-out.js";

// What each finish() call was handed: one entry per call, the totals of the
// states it received. The worker runs in this process, so it can be read here.
const finishCalls: number[][] = [];

const FUNCTION_NAME = "substream_state_sum";

// Accumulates its substream's sum; finish() emits one row, the sum of every
// state it receives -- the shape of the examples' `substream_partial_sum`.
const partialSum = defineTableInOutFunction<Record<string, any>, { total: number }>({
  name: FUNCTION_NAME,
  description: "Accumulates a sum per substream; finish() sums the states it is handed",
  onBind: () => ({ outputSchema: schema([field("n", int64(), true)]) }),
  initialState: () => ({ total: 0 }),
  process: (params, state, batch, out) => {
    const col = batch.getChildAt(0);
    for (let i = 0; i < (col?.length ?? 0); i++) state.total += Number(col?.get(i) ?? 0);
    out.emit(emptyBatch(params.outputSchema));
  },
  finalize: (params, states) => {
    const totals = states.map((s) => Number(s?.total ?? 0));
    finishCalls.push(totals);
    const sum = totals.reduce((a, b) => a + b, 0);
    return [batchFromColumns({ n: [BigInt(sum)] }, params.outputSchema)];
  },
});

const INPUT_SCHEMA = schema([field("n", int64(), true)]);

function inputRows(values: number[]): Record<string, any>[] {
  return [...iterRows(batchFromColumns({ n: values.map(BigInt) }, INPUT_SCHEMA) as VgiBatch)];
}

function substreamId(): Uint8Array {
  return crypto.getRandomValues(new Uint8Array(16));
}

let dir: string;
let server: VgiHttpServer;
let rpc: RpcClient;

beforeAll(() => {
  dir = mkdtempSync(join(tmpdir(), "vgi-substream-state-"));
  // A file-backed store, like a real worker's, private to this test.
  setStorage(new FunctionStorageSqlite(join(dir, "state.db")));
  const registry = new FunctionRegistry();
  registry.register(partialSum);
  server = serveVgiWorker({
    name: "demo",
    doc: "Substream state test worker.",
    version: "0.0.1",
    registry,
    catalogInterface: new ReadOnlyCatalogInterface(
      { name: "demo", schemas: [{ name: "main", functions: [partialSum] }] },
      registry,
    ),
    port: 0,
    signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(7),
    quiet: true,
    env: {},
  });
  rpc = httpConnect(`http://localhost:${server.port}`, { prefix: "" });
});

afterAll(() => {
  server?.stop(true);
  // Back to the env-selected default for any test file that runs after this.
  setStorage(resolveStorageFromEnv());
  rmSync(dir, { recursive: true, force: true });
});

async function bind(): Promise<{ request: BindRequest; response: BindResponse }> {
  const request: BindRequest = {
    function_name: FUNCTION_NAME,
    arguments: new Arguments(),
    function_type: FunctionType.TABLE,
    input_schema: INPUT_SCHEMA,
    settings: null,
    secrets: null,
    attach_opaque_data: null,
    transaction_opaque_data: null,
    resolved_secrets_provided: false,
  };
  const result = await rpc.call("bind", wrapRequest(serializeBindRequest(request)));
  return { request, response: deserializeBindResponse(unwrapResult(result as Record<string, any>)) };
}

async function init(
  bound: { request: BindRequest; response: BindResponse },
  phase: TableInOutPhase,
  opts: { executionId?: Uint8Array | null; substreamId?: Uint8Array | null },
): Promise<{ session: StreamSession; executionId: Uint8Array }> {
  const request: InitRequest = {
    bind_call: bound.request,
    output_schema: bound.response.output_schema,
    bind_opaque_data: bound.response.opaque_data,
    split_tokens: null,
    row_limit: null,
    projection_ids: null,
    pushdown_filters: null,
    join_keys: [],
    phase,
    finalize_state_id: null,
    order_by_column_name: null,
    order_by_direction: null,
    order_by_null_order: null,
    order_by_limit: null,
    tablesample_percentage: null,
    tablesample_seed: null,
    execution_id: opts.executionId ?? null,
    init_opaque_data: null,
    substream_id: opts.substreamId ?? null,
  };
  const session = await rpc.stream("init", wrapRequest(serializeInitRequest(request)));
  const header = session.header;
  if (!header) throw new Error("init returned no header");
  return { session, executionId: deserializeGlobalInitResponse(header).execution_id };
}

async function finalize(
  bound: { request: BindRequest; response: BindResponse },
  executionId: Uint8Array,
  substream: Uint8Array | null,
): Promise<bigint[]> {
  const { session } = await init(bound, TableInOutPhase.FINALIZE, { executionId, substreamId: substream });
  const out: bigint[] = [];
  try {
    for await (const rows of session) for (const row of rows) out.push(BigInt(row.n));
  } finally {
    session.close();
  }
  return out;
}

describe("table-in-out state is keyed per substream, not per process", () => {
  test("two substreams of one execution served by one process both reach finish()", async () => {
    finishCalls.length = 0;
    const bound = await bind();
    const primarySubstream = substreamId();
    // The primary connection's INPUT stream opens the execution; the second
    // joins it, as a fanned-out client's other connections do.
    const a = await init(bound, TableInOutPhase.INPUT, { substreamId: primarySubstream });
    const b = await init(bound, TableInOutPhase.INPUT, { executionId: a.executionId, substreamId: substreamId() });
    try {
      // Interleaved, so each stream's last write lands after the other's.
      await a.session.exchange(inputRows([1, 2, 3]));
      await b.session.exchange(inputRows([10, 20, 30]));
      await a.session.exchange(inputRows([4]));
      await b.session.exchange(inputRows([40]));
    } finally {
      a.session.close();
      b.session.close();
    }
    const result = await finalize(bound, a.executionId, primarySubstream);
    // Keyed by process, the second stream's state overwrote the first's:
    // finish() got one state and returned 100 (or 10).
    expect(finishCalls).toEqual([expect.arrayContaining([10, 100])]);
    expect(finishCalls[0]).toHaveLength(2);
    expect(result).toEqual([110n]);
  }, 20000);

  test("a client that sends no substream_id is still summed, keyed by process", async () => {
    // The fallback, for an older client: one INPUT stream per execution with no
    // substream_id is keyed by the pid, as it always was.
    finishCalls.length = 0;
    const bound = await bind();
    const a = await init(bound, TableInOutPhase.INPUT, {});
    try {
      await a.session.exchange(inputRows([5, 6]));
      await a.session.exchange(inputRows([7]));
    } finally {
      a.session.close();
    }
    expect(await finalize(bound, a.executionId, null)).toEqual([18n]);
    expect(finishCalls).toEqual([[18]]);
  }, 20000);
});

describe("BoundStorage.put keys the worker-state slot", () => {
  test("by substream_id when there is one, by process id when there is not", async () => {
    const store = new FunctionStorageSqlite(":memory:");
    try {
      const exec = new TextEncoder().encode("exec-substream-keys");
      const first = substreamId();
      const second = substreamId();
      await new BoundStorage(store, exec, first).put(new TextEncoder().encode("first"));
      await new BoundStorage(store, exec, second).put(new TextEncoder().encode("second"));
      await new BoundStorage(store, exec).put(new TextEncoder().encode("no-substream"));
      // Upsert semantics hold per substream: a later put replaces its own slot only.
      await new BoundStorage(store, exec, first).put(new TextEncoder().encode("first-again"));

      const scanned = (await store.workerScan(exec)).map(([id, v]) => [id, new TextDecoder().decode(v)] as const);
      // A byte-keyed (substream) slot has no numeric worker id; the fallback
      // slot's is the process id.
      expect(scanned).toContainEqual([process.pid, "no-substream"]);
      expect(scanned.filter(([id]) => id === 0).map(([, v]) => v).sort()).toEqual(["first-again", "second"]);

      const collected = (await new BoundStorage(store, exec).collect()).map((v) => new TextDecoder().decode(v));
      expect(collected.sort()).toEqual(["first-again", "no-substream", "second"]);
    } finally {
      store.close();
    }
  });
});
