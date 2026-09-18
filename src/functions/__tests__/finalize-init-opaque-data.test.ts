// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// A FINALIZE init's `init_opaque_data` is not a state token.
//
// The HTTP init handler used to "recover FINALIZE state" by opening
// `init_opaque_data` as an exchange cursor, a leftover from a protocol in which
// the DuckDB extension passed its last INPUT cursor there. It no longer does:
// every transport sends null on FINALIZE, and the reference protocol
// (vgi-python's InitRequest, and its client) defines the field as the primary
// init response's opaque data, echoed back -- never a cursor. The accumulated
// states come from storage, where the INPUT phase persists one per substream.
//
// The recovery also never ran: the hook createVgiFetch installs was async and
// was called without being awaited. What it did do is fail -- a FINALIZE init
// carrying bytes that are not an anonymous cursor (the echoed opaque data a
// Python client sends, a cursor sealed for an authenticated caller) made the
// hook reject with nothing to catch it, and an unhandled rejection exits Bun:
// one request took the whole HTTP worker down.
//
// Driven over the real HTTP stack: in-process against a table-in-out function
// whose init returns opaque data, and against the example worker binary in its
// own process. The requests are hand-built (INPUT then FINALIZE, as a client
// sends them); every response is the worker's.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
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
import type { BindRequest, BindResponse, GlobalInitResponse, InitRequest } from "../../protocol/types.js";
import { SIGNING_KEY_BYTES, serveVgiWorker, type VgiHttpServer } from "../../serve-entry.js";
import { FunctionType, TableInOutPhase } from "../../types.js";
import { batchFromColumns, emptyBatch, iterRows } from "../../util/arrow/index.js";
import { FunctionRegistry } from "../registry.js";
import { FunctionStorageSqlite, resolveStorageFromEnv, setStorage } from "../storage.js";
import { defineTableInOutFunction } from "../table-in-out.js";

const INPUT_SCHEMA = schema([field("n", int64(), true)]);
/** What this function's primary init hands back as opaque data. */
const INIT_OPAQUE = new TextEncoder().encode("partition-plan:v1");

// Sums its substream's input; finalize() sums the states it is handed.
const partialSum = defineTableInOutFunction<Record<string, any>, { total: number }>({
  name: "partial_sum_with_init_data",
  description: "Accumulates a sum per substream; its init returns opaque data",
  onBind: () => ({ outputSchema: INPUT_SCHEMA }),
  onInit: ({ executionId }) => ({ max_workers: 1, execution_id: executionId, opaque_data: INIT_OPAQUE }),
  initialState: () => ({ total: 0 }),
  process: (params, state, batch, out) => {
    const col = batch.getChildAt(0);
    for (let i = 0; i < (col?.length ?? 0); i++) state.total += Number(col?.get(i) ?? 0);
    out.emit(emptyBatch(params.outputSchema));
  },
  finalize: (params, states) => {
    const sum = states.reduce((acc, s) => acc + Number(s?.total ?? 0), 0);
    return [batchFromColumns({ n: [BigInt(sum)] }, params.outputSchema)];
  },
});

function inputRows(values: number[]): Record<string, any>[] {
  return [...iterRows(batchFromColumns({ n: values.map(BigInt) }, INPUT_SCHEMA) as VgiBatch)];
}

function bindRequest(name: string): BindRequest {
  return {
    function_name: name,
    arguments: new Arguments(),
    function_type: FunctionType.TABLE,
    input_schema: INPUT_SCHEMA,
    settings: null,
    secrets: null,
    attach_opaque_data: null,
    transaction_opaque_data: null,
    resolved_secrets_provided: false,
  };
}

async function bind(rpc: RpcClient, name: string): Promise<{ request: BindRequest; response: BindResponse }> {
  const request = bindRequest(name);
  const result = await rpc.call("bind", wrapRequest(serializeBindRequest(request)));
  return { request, response: deserializeBindResponse(unwrapResult(result as Record<string, any>)) };
}

async function init(
  rpc: RpcClient,
  bound: { request: BindRequest; response: BindResponse },
  phase: TableInOutPhase,
  opts: { executionId?: Uint8Array | null; initOpaqueData?: Uint8Array | null; substreamId: Uint8Array },
): Promise<{ session: StreamSession; header: GlobalInitResponse }> {
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
    init_opaque_data: opts.initOpaqueData ?? null,
    substream_id: opts.substreamId,
  };
  const session = await rpc.stream("init", wrapRequest(serializeInitRequest(request)));
  if (!session.header) throw new Error("init returned no header");
  return { session, header: deserializeGlobalInitResponse(session.header) };
}

async function drain(session: StreamSession): Promise<bigint[]> {
  const out: bigint[] = [];
  try {
    for await (const rows of session) for (const row of rows) out.push(BigInt(row.n));
  } finally {
    session.close();
  }
  return out;
}

describe("in process", () => {
  let dir: string;
  let server: VgiHttpServer;
  let rpc: RpcClient;
  // An unhandled rejection exits a Bun worker; here it would only be reported
  // between tests, so record it and let the tests assert there was none.
  const rejections: unknown[] = [];
  const onRejection = (reason: unknown) => rejections.push(reason);

  beforeAll(() => {
    process.on("unhandledRejection", onRejection);
    dir = mkdtempSync(join(tmpdir(), "vgi-finalize-opaque-"));
    setStorage(new FunctionStorageSqlite(join(dir, "state.db")));
    const registry = new FunctionRegistry();
    registry.register(partialSum);
    server = serveVgiWorker({
      name: "demo",
      doc: "FINALIZE init_opaque_data test worker.",
      version: "0.0.1",
      registry,
      catalogInterface: new ReadOnlyCatalogInterface(
        { name: "demo", schemas: [{ name: "main", functions: [partialSum] }] },
        registry,
      ),
      port: 0,
      signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(4),
      quiet: true,
      env: {},
    });
    rpc = httpConnect(`http://localhost:${server.port}`, { prefix: "" });
  });

  afterAll(() => {
    server?.stop(true);
    setStorage(resolveStorageFromEnv());
    rmSync(dir, { recursive: true, force: true });
    process.off("unhandledRejection", onRejection);
  });

  test("FINALIZE echoing the primary init's opaque data (as vgi-python's client sends it) answers from storage", async () => {
    rejections.length = 0;
    const bound = await bind(rpc, "partial_sum_with_init_data");
    const substreamId = crypto.getRandomValues(new Uint8Array(16));
    const input = await init(rpc, bound, TableInOutPhase.INPUT, { substreamId });
    expect(input.header.opaque_data).toEqual(INIT_OPAQUE);
    try {
      await input.session.exchange(inputRows([1, 2, 3]));
      await input.session.exchange(inputRows([4]));
    } finally {
      input.session.close();
    }
    const finalize = await init(rpc, bound, TableInOutPhase.FINALIZE, {
      executionId: input.header.execution_id,
      initOpaqueData: input.header.opaque_data,
      substreamId,
    });
    expect(await drain(finalize.session)).toEqual([10n]);
    // Before: the recovery hook rejected ("Malformed state token") with nothing
    // awaiting it -- in a real worker, the process exited.
    await Bun.sleep(20);
    expect(rejections).toEqual([]);
  });

  test("FINALIZE carrying the last INPUT cursor counts the substream's state once", async () => {
    // The shape the recovery path was written for. Over HTTP every INPUT turn
    // already persists its state to storage, so reading the cursor as well
    // would hand finalize() the last state twice (a naive `await` of the old
    // hook turns 10 into 20 here).
    rejections.length = 0;
    const bound = await bind(rpc, "partial_sum_with_init_data");
    const substreamId = crypto.getRandomValues(new Uint8Array(16));
    const input = await init(rpc, bound, TableInOutPhase.INPUT, { substreamId });
    let cursor: string;
    try {
      await input.session.exchange(inputRows([1, 2, 3]));
      await input.session.exchange(inputRows([4]));
      cursor = (input.session as any)._stateToken;
    } finally {
      input.session.close();
    }
    expect(typeof cursor).toBe("string");
    const finalize = await init(rpc, bound, TableInOutPhase.FINALIZE, {
      executionId: input.header.execution_id,
      initOpaqueData: new TextEncoder().encode(cursor),
      substreamId,
    });
    expect(await drain(finalize.session)).toEqual([10n]);
    await Bun.sleep(20);
    expect(rejections).toEqual([]);
  });
});

describe("the example HTTP worker, in its own process", () => {
  const repo = resolve(import.meta.dir, "../../..");
  let dir: string;
  let proc: ReturnType<typeof Bun.spawn> | undefined;
  let port = 0;

  beforeAll(async () => {
    dir = mkdtempSync(join(tmpdir(), "vgi-finalize-opaque-worker-"));
    proc = Bun.spawn([join(repo, "bin/vgi-example-http-worker")], {
      cwd: dir,
      env: { ...process.env, VGI_WORKER_SQLITE_PATH: join(dir, "state.db") },
      stdout: "pipe",
      stderr: "pipe",
    });
    const reader = (proc.stdout as ReadableStream<Uint8Array>).getReader();
    let seen = "";
    const deadline = Date.now() + 30000;
    while (!port && Date.now() < deadline) {
      const { value, done } = await reader.read();
      if (done) break;
      seen += new TextDecoder().decode(value);
      const match = /PORT:(\d+)/.exec(seen);
      if (match) port = Number(match[1]);
    }
    reader.releaseLock();
    if (!port) throw new Error(`example worker reported no port: ${seen}`);
  }, 40000);

  afterAll(() => {
    // The one process this test started, by its own handle.
    proc?.kill();
    rmSync(dir, { recursive: true, force: true });
  });

  test("a FINALIZE init with opaque data a client echoes back does not take the worker down", async () => {
    const rpc = httpConnect(`http://localhost:${port}`, { prefix: "" });
    const bound = await bind(rpc, "substream_partial_sum");
    const substreamId = crypto.getRandomValues(new Uint8Array(16));
    const input = await init(rpc, bound, TableInOutPhase.INPUT, { substreamId });
    try {
      await input.session.exchange(inputRows([5, 6, 7]));
    } finally {
      input.session.close();
    }
    const finalize = await init(rpc, bound, TableInOutPhase.FINALIZE, {
      executionId: input.header.execution_id,
      // Opaque bytes the client holds for this execution -- not a cursor.
      initOpaqueData: new TextEncoder().encode("opaque-per-init-state"),
      substreamId,
    });
    expect(await drain(finalize.session)).toEqual([18n]);
    // Before: the rejected recovery promise exited the worker right after it
    // answered, so the next request found nothing listening.
    await Bun.sleep(200);
    expect(proc!.exitCode).toBeNull();
    const health = await fetch(`http://localhost:${port}/health`);
    expect(health.ok).toBe(true);
  }, 30000);
});
