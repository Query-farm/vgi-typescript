// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// VgiClient driving a blended row-transform function (defineRowTransformFunction).
//
// A row-transform function is a per-row map with NO finalize stage (DuckDB forbids
// FinalExecute under correlated LATERAL), advertised as has_finalize=false, and the
// worker REJECTS a FINALIZE-phase init for it. The client's table-in-out calls used to
// send FINALIZE unconditionally, so a client could only call one by abandoning the
// generator after the input phase. `hasFinalize: false` skips the FINALIZE init
// entirely, mirroring vgi-python's `table_in_out_function(has_finalize=False)`. The
// default stays `true` so existing callers see no change.
//
// Served in-process over HTTP (createVgiFetch + Bun.serve), so no external worker.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { httpConnect } from "@query-farm/vgi-rpc";
import { batchFromColumns, field, int64, schema, utf8 } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { defineRowTransformFunction, parentRowsMetadata } from "../../functions/table-in-out.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { createVgiFetch } from "../../http/fetch.js";
import { VgiClient } from "../client.js";

// repeat(word) -> `times` rows per input row (1->N, so provenance is required).
const OUT = schema([field("word", utf8(), true), field("i", int64(), true)]);
const repeat = defineRowTransformFunction<{ times: number }>({
  name: "repeat",
  args: { word: utf8() },
  namedArgs: { times: int64() },
  argDefaults: { times: 2 },
  onBind: () => ({ outputSchema: OUT }),
  process: (params, batch, out) => {
    const times = Number(params.args.times ?? 2);
    const words: (string | null)[] = [];
    const is: bigint[] = [];
    const parents: number[] = [];
    for (let row = 0; row < batch.numRows; row++) {
      for (let i = 0; i < times; i++) {
        words.push(batch.getChild("word")?.get(row) ?? null);
        is.push(BigInt(i));
        parents.push(row);
      }
    }
    out.emit(batchFromColumns({ word: words, i: is }, OUT), parentRowsMetadata(parents, parents.length));
  },
});

const INPUT = schema([field("word", utf8(), true)]);
const input = () => [batchFromColumns({ word: ["a", "b"] }, INPUT), batchFromColumns({ word: ["c"] }, INPUT)];

let server: ReturnType<typeof Bun.serve>;
let baseUrl: string;

beforeAll(() => {
  const registry = new FunctionRegistry();
  registry.register(repeat);
  const catalogInterface = new ReadOnlyCatalogInterface(
    { name: "rt", schemas: [{ name: "main", functions: [repeat] }] },
    registry,
  );
  const fetch = createVgiFetch({
    protocol: { registry, catalogInterface },
    signingKey: new Uint8Array(32).fill(3),
    prefix: "",
    landingInfo: { name: "rt", doc: "row-transform client test", version: "0.0.0" },
  });
  server = Bun.serve({ port: 0, fetch });
  baseUrl = `http://localhost:${server.port}`;
});

afterAll(() => {
  server?.stop(true);
});

async function withClient<T>(fn: (client: VgiClient, attach: Uint8Array) => Promise<T>): Promise<T> {
  const rpc = httpConnect(baseUrl, { prefix: "" });
  try {
    const client = new VgiClient(rpc);
    const { attach_opaque_data } = await client.catalogAttach("rt");
    return await fn(client, attach_opaque_data);
  } finally {
    rpc.close();
  }
}

describe("VgiClient — blended row-transform functions", () => {
  test("hasFinalize: false streams every input batch and completes (rows)", async () => {
    const rows = await withClient(async (client, attach) => {
      const out: Record<string, any>[] = [];
      for await (const batch of client.tableInOutFunctionRows({
        functionName: "repeat",
        input: input(),
        arguments: new Arguments([], new Map([["times", 3n]])),
        attachOpaqueData: attach,
        hasFinalize: false,
      })) {
        out.push(...batch);
      }
      return out;
    });
    expect(rows.map((r) => `${r.word}${r.i}`)).toEqual(["a0", "a1", "a2", "b0", "b1", "b2", "c0", "c1", "c2"]);
  });

  test("hasFinalize: false completes for the batch-yielding variant too", async () => {
    const total = await withClient(async (client, attach) => {
      let n = 0;
      for await (const batch of client.tableInOutFunction({
        functionName: "repeat",
        input: input(),
        attachOpaqueData: attach,
        hasFinalize: false,
      })) {
        n += batch.numRows;
      }
      return n;
    });
    expect(total).toBe(6); // 3 input rows x default times=2
  });

  test("the default still sends FINALIZE, which a row-transform worker rejects", async () => {
    await withClient(async (client, attach) => {
      const drain = async () => {
        for await (const _ of client.tableInOutFunctionRows({
          functionName: "repeat",
          input: input(),
          attachOpaqueData: attach,
        })) {
          // input-phase output arrives; the FINALIZE init after it is what throws
        }
      };
      await expect(drain()).rejects.toThrow(/no FINALIZE phase/);
    });
  });
});
