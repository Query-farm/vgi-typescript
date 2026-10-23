// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// VgiClient against a worker whose table-in-out output continues
// (docs/table_in_out.md "Continued Output").
//
// A reply saying `vgi.status: HAVE_MORE_OUTPUT` has more output for the same
// input; the client must fetch it with continuation ticks before it sends the
// next input or closes. The row-level `StreamSession.exchange` cannot see the
// status, so a client using it returned only each input's first page.
//
// Driven over HTTP (in-process worker) and over a pipe (the example worker as a
// subprocess, so the worker reads the tick's status the byte-stream way), plus
// fake sessions for the replies only a broken worker sends.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { httpConnect, subprocessConnect, type RpcClient, type StreamSession } from "@query-farm/vgi-rpc";
import { tableInOutFunctions } from "../../../examples/table_in_out.js";
import { batchFromColumns, emptyBatch, field, int64, schema, type VgiBatch } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { cacheControlMetadata } from "../../cache-control.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { CONTINUE, HAVE_MORE_OUTPUT, moreOutputMetadata, STATUS_KEY } from "../../functions/continuation.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { FunctionStorageSqlite, resolveStorageFromEnv, setStorage } from "../../functions/storage.js";
import { defineRowTransformFunction } from "../../functions/table-in-out.js";
import { SIGNING_KEY_BYTES, serveVgiWorker, type VgiHttpServer } from "../../serve-entry.js";
import { iterRows } from "../../util/arrow/index.js";
import { exchangeContinued, VgiClient } from "../client.js";

const N_SCHEMA = schema([field("n", int64(), true)]);
const I_SCHEMA = schema([field("i", int64(), true)]);
const nBatch = (vals: (number | null)[]) =>
  batchFromColumns({ n: vals.map((v) => (v == null ? null : BigInt(v))) }, N_SCHEMA);
const iOf = (rows: Record<string, any>[]) => rows.map((r) => Number(r.i));

// Answers its input with an empty not_modified reply that also says more: the
// client must refuse it (the framework need not).
const nmMore = defineRowTransformFunction<Record<string, any>, { left: number }>({
  name: "nm_more",
  args: { n: int64() },
  onBind: () => ({ outputSchema: I_SCHEMA }),
  initialState: () => ({ left: 1 }),
  process: (_p, _b, out) =>
    out.emit(emptyBatch(I_SCHEMA), moreOutputMetadata(true, cacheControlMetadata({ notModified: true, etag: "v1" }))),
  resume: (_p, _s, out) => out.emit(batchFromColumns({ i: [1n] }, I_SCHEMA)),
});

const FIXTURES = [
  ...["blended_paged", "paged_explode"].map((name) => tableInOutFunctions.find((f) => f.meta.name === name)!),
  nmMore,
];

/** Every reply's i values, one array per reply the client yielded. */
async function pagedReplies(client: VgiClient, inputs: VgiBatch[], pageSize: number): Promise<number[][]> {
  const replies: number[][] = [];
  for await (const rows of client.tableInOutFunctionRows({
    functionName: "blended_paged",
    input: inputs,
    arguments: new Arguments([], new Map([["page_size", pageSize]])),
    hasFinalize: false,
  })) {
    replies.push(iOf(rows));
  }
  return replies;
}

/** paged_explode through the RecordBatch API (the classic table-in-out path). */
async function explodeBatches(client: VgiClient, inputs: VgiBatch[], pageSize: number): Promise<number[][]> {
  const replies: number[][] = [];
  for await (const batch of client.tableInOutFunction({
    functionName: "paged_explode",
    input: inputs,
    arguments: new Arguments([], new Map([["page_size", pageSize]])),
    hasFinalize: false,
  })) {
    replies.push(iOf([...iterRows(batch)]));
  }
  return replies;
}

function transportSuite(name: string, connect: () => RpcClient) {
  describe(`continued output over ${name}`, () => {
    test("tableInOutFunctionRows ticks for every page before the next input", async () => {
      const client = new VgiClient(connect());
      try {
        // [3, 0, NULL, 2] fans out to 5 rows: pages of 2, 2, 1; then [1] is one page.
        expect(await pagedReplies(client, [nBatch([3, 0, null, 2]), nBatch([1])], 2)).toEqual([
          [0, 1],
          [2, 0],
          [1],
          [0],
        ]);
      } finally {
        client.close();
      }
    });

    test("tableInOutFunction ticks the classic path too", async () => {
      const client = new VgiClient(connect());
      try {
        expect(await explodeBatches(client, [nBatch([3]), nBatch([2])], 1)).toEqual([[0], [1], [2], [0], [1]]);
      } finally {
        client.close();
      }
    });

    test("stopping early closes with output pending, and the worker stays usable", async () => {
      const rpc = connect();
      const client = new VgiClient(rpc);
      try {
        for await (const rows of client.tableInOutFunctionRows({
          functionName: "blended_paged",
          input: [nBatch([10])],
          arguments: new Arguments([], new Map([["page_size", 2]])),
          hasFinalize: false,
        })) {
          expect(iOf(rows)).toEqual([0, 1]);
          break; // a LIMIT: no more ticks, the input closes
        }
        expect(await pagedReplies(client, [nBatch([3])], 2)).toEqual([[0, 1], [2]]);
      } finally {
        client.close();
      }
    });
  });
}

let dir: string;
let server: VgiHttpServer;

beforeAll(() => {
  dir = mkdtempSync(join(tmpdir(), "vgi-client-continuation-"));
  setStorage(new FunctionStorageSqlite(join(dir, "state.db")));
  const registry = new FunctionRegistry();
  for (const f of FIXTURES) registry.register(f);
  server = serveVgiWorker({
    name: "demo",
    doc: "Continued output client test worker.",
    version: "0.0.1",
    registry,
    catalogInterface: new ReadOnlyCatalogInterface({ name: "demo", schemas: [{ name: "main", functions: FIXTURES }] }, registry),
    port: 0,
    signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(5),
    quiet: true,
    env: {},
  });
});

afterAll(() => {
  server?.stop(true);
  setStorage(resolveStorageFromEnv());
  rmSync(dir, { recursive: true, force: true });
});

transportSuite("HTTP", () => httpConnect(`http://localhost:${server.port}`, { prefix: "" }));

// The example worker over stdin/stdout: the same fixtures, read off a pipe.
const WORKER = resolve(import.meta.dir, "../../../bin/vgi-example-worker");
transportSuite("a pipe (subprocess)", () =>
  subprocessConnect([WORKER], { cwd: dir, env: { VGI_WORKER_SQLITE_PATH: ":memory:" } }),
);

describe("replies the client refuses", () => {
  test("not_modified on a reply that says more (live worker)", async () => {
    const client = new VgiClient(httpConnect(`http://localhost:${server.port}`, { prefix: "" }));
    const run = async () => {
      for await (const _ of client.tableInOutFunctionRows({ functionName: "nm_more", input: [nBatch([1])], hasFinalize: false })) {
        // drain
      }
    };
    await expect(run()).rejects.toThrow(/not_modified on a continued reply \(one saying HAVE_MORE_OUTPUT\)/);
  });

  /** A session answering each exchangeRaw with the next scripted reply; records what was sent. */
  function scripted(replies: ({ rows: number[]; md: [string, string][] } | null)[]) {
    const sent: Map<string, string>[] = [];
    const session = {
      async exchangeRaw(input: { batch: VgiBatch; metadata: Map<string, string> }) {
        sent.push(input.metadata);
        const next = replies.shift();
        if (next === undefined) throw new Error("unexpected exchange");
        if (next === null) return null;
        return { batch: batchFromColumns({ i: next.rows.map(BigInt) }, I_SCHEMA), metadata: new Map(next.md) };
      },
    } as unknown as StreamSession;
    return { session, sent };
  }

  async function collect(session: StreamSession): Promise<number[][]> {
    const out: number[][] = [];
    for await (const rows of exchangeContinued(session, "f", nBatch([1]))) out.push(iOf(rows));
    return out;
  }

  test("ticks carry CONTINUE and no rows until a reply stops saying more", async () => {
    const more: [string, string][] = [[STATUS_KEY, HAVE_MORE_OUTPUT]];
    const { session, sent } = scripted([
      { rows: [1], md: more },
      { rows: [], md: more }, // an empty page that still says more
      { rows: [2], md: [] },
    ]);
    expect(await collect(session)).toEqual([[1], [2]]);
    expect(sent.map((m) => m.get(STATUS_KEY) ?? null)).toEqual([null, CONTINUE, CONTINUE]);
  });

  test("not_modified on a tick's reply", async () => {
    const { session } = scripted([
      { rows: [1], md: [[STATUS_KEY, HAVE_MORE_OUTPUT]] },
      { rows: [], md: [["vgi.cache.not_modified", "1"]] },
    ]);
    await expect(collect(session)).rejects.toThrow(/not_modified on a continued reply \(answering a continuation tick\)/);
  });

  test("end of stream while output is pending", async () => {
    const { session } = scripted([{ rows: [1], md: [[STATUS_KEY, HAVE_MORE_OUTPUT]] }, null]);
    await expect(collect(session)).rejects.toThrow(/ended the stream while output was pending/);
  });

  test("not_modified on an input's only reply is fine", async () => {
    const { session } = scripted([{ rows: [], md: [["vgi.cache.not_modified", "1"]] }]);
    expect(await collect(session)).toEqual([]);
  });
});
