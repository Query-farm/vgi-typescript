// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Regression: an authenticated caller could not run a single function over
// HTTP.
//
// `catalog_attach` seals `attach_opaque_data` under `attachAad(auth)`, which
// binds the caller's identity. Every unary method opens it under the same
// identity because the dispatcher hands it a CallContext. Stream `init` is the
// exception — `@query-farm/vgi-rpc` types stream initialization as
// `(params) => state` — and it used to open the envelope with auth hard-coded
// to `undefined`. `identityTail(undefined)` is the anonymous tail, so the AAD
// matched for anonymous callers only: an authenticated one got
// `OpaqueDataRejectedError: attach_opaque_data not recognized` at `/init`.
//
// Nothing caught it because no example worker authenticated anybody, so every
// test caller was anonymous and the anonymous tail was the right answer.
//
// These tests drive a real listening server through the real client, because
// the fix is a property of the whole HTTP path (the request-scoped identity is
// published by the fetch wrapper and read inside the protocol handler) and a
// unit test of either half alone would keep passing if the plumbing between
// them were removed.

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { AuthContext, httpConnect } from "@query-farm/vgi-rpc";
import { Schema, Field, Int64 } from "@query-farm/apache-arrow";
import type { OutputCollector } from "@query-farm/vgi-rpc";
import { batchFromColumns } from "../../util/arrow/index.js";
import { defineTableFunction } from "../../functions/table.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { VgiClient } from "../../client/client.js";
import { createVgiFetch } from "../fetch.js";

const OUT = new Schema([new Field("n", new Int64(), true)]);

// A minimal producer: one row, then done. The row value does not matter — the
// assertion is that `init` ran at all under an authenticated caller.
const one = defineTableFunction<Record<string, never>, { done: boolean }>({
  name: "one",
  description: "Emits a single row",
  onBind: () => ({ outputSchema: OUT }),
  initialState: () => ({ done: false }),
  process: (params, state, out: OutputCollector) => {
    if (state.done) {
      out.finish();
      return;
    }
    out.emit(batchFromColumns({ n: [1n] }, params.outputSchema));
    state.done = true;
  },
});

const TOKENS = new Map([
  ["tok-alice", "alice"],
  ["tok-bob", "bob"],
]);

let server: { port: number; stop(force?: boolean): void };
let baseUrl: string;

beforeAll(() => {
  const registry = new FunctionRegistry();
  registry.register(one as any);
  const catalogInterface = new ReadOnlyCatalogInterface(
    { name: "demo", schemas: [{ name: "main", functions: [one as any] }] },
    registry,
  );
  const fetch = createVgiFetch({
    protocol: { registry, catalogInterface },
    // A signing key is what turns sealing on; without one every helper is a
    // pass-through and this bug cannot occur.
    signingKey: new Uint8Array(32).fill(9),
    prefix: "",
    serverId: "vgi-request-auth-test",
    landingInfo: { name: "demo", doc: "A demo worker.", version: "0" },
    authenticate: (req: Request) => {
      const m = /^Bearer\s+(.+)$/i.exec(req.headers.get("authorization") ?? "");
      const principal = m ? TOKENS.get(m[1]!.trim()) : undefined;
      return principal ? new AuthContext("bearer", true, principal) : AuthContext.anonymous();
    },
  });
  server = (globalThis as any).Bun.serve({ port: 0, fetch });
  baseUrl = `http://localhost:${server.port}`;
});

afterAll(() => server?.stop(true));

async function runAs(token: string | undefined): Promise<number> {
  const rpc = httpConnect(baseUrl, token ? { authorization: `Bearer ${token}` } : undefined);
  const client = new VgiClient(rpc);
  try {
    const attach = await client.catalogAttach("demo");
    let rows = 0;
    for await (const batch of client.tableFunction({
      functionName: "one",
      attachOpaqueData: attach.attach_opaque_data,
    })) {
      rows += batch.numRows;
    }
    return rows;
  } finally {
    client.close();
  }
}

describe("authenticated HTTP callers reach stream init", () => {
  test("an authenticated caller can scan (the attach envelope opens at /init)", async () => {
    expect(await runAs("tok-alice")).toBe(1);
  });

  test("an anonymous caller still works", async () => {
    expect(await runAs(undefined)).toBe(1);
  });

  test("two principals interleaved on one server do not cross identities", async () => {
    // Concurrency is the point: the identity travels on an ambient
    // AsyncLocalStorage scope, so a per-process (rather than per-request) cell
    // would pass the sequential cases above and fail here.
    const results = await Promise.all([
      runAs("tok-alice"),
      runAs("tok-bob"),
      runAs(undefined),
      runAs("tok-bob"),
      runAs("tok-alice"),
    ]);
    expect(results).toEqual([1, 1, 1, 1, 1]);
  });
});
