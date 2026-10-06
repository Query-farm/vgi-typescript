// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// End to end over a real listening HTTP server: a minted grant logs its owner
// in (WIRE_PROTOCOL.md §16, "Accepting identity credentials").
//
//   1. A caller who authenticated recently mints a grant through
//      vgi_rpc.Identity.v1's issue_grant -- sealed by the framework, because
//      the worker configured grant keys and no mintGrant hook.
//   2. Automation presents it as `Authorization: Bearer <grant>` to a vgi.v2
//      method (catalog_attach, then a table scan).
//   3. That call runs as the grant's owner: the attach envelope, sealed under
//      the caller's identity, opens at /init, and the function reads the owner
//      off the request scope.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Field, Schema, Utf8 } from "@query-farm/apache-arrow";
import { AuthContext, GrantKeys, httpConnect, type OutputCollector } from "@query-farm/vgi-rpc";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { VgiClient } from "../../client/client.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { defineTableFunction } from "../../functions/table.js";
import { currentRequestAuth } from "../../request-auth.js";
import { batchFromColumns, deserializeBatch } from "../../util/arrow/index.js";
import { createVgiFetch } from "../fetch.js";

const OUT = new Schema([new Field("principal", new Utf8(), true)]);

/** Emits one row: the principal the request is running as. */
const whoami = defineTableFunction<Record<string, never>, { done: boolean }>({
  name: "whoami",
  description: "Reports the calling principal",
  onBind: () => ({ outputSchema: OUT }),
  initialState: () => ({ done: false }),
  process: (params, state, out: OutputCollector) => {
    if (state.done) {
      out.finish();
      return;
    }
    out.emit(batchFromColumns({ principal: [currentRequestAuth()?.principal ?? ""] }, params.outputSchema));
    state.done = true;
  },
});

const KEYS = new GrantKeys([new Uint8Array(32).fill(5)], { audience: "sdk-test" });
const OWNER = "owner@example.com";

let server: { port: number; stop(force?: boolean): void };
let baseUrl: string;

beforeAll(() => {
  const registry = new FunctionRegistry();
  registry.register(whoami as any);
  const catalogInterface = new ReadOnlyCatalogInterface(
    { name: "demo", schemas: [{ name: "main", functions: [whoami as any] }] },
    registry,
  );
  const fetch = createVgiFetch({
    protocol: { registry, catalogInterface },
    signingKey: new Uint8Array(32).fill(9),
    prefix: "",
    serverId: "vgi-grant-bearer-test",
    landingInfo: { name: "demo", doc: "A demo worker.", version: "0" },
    // The deployment's own login: a header names the user and when they
    // authenticated (what a JWT's sub/auth_time would). Anything else -- a
    // grant included -- is "not mine" and moves on down the chain.
    authenticate: (req: Request) => {
      const user = req.headers.get("x-test-user");
      if (!user) {
        if (req.headers.get("authorization")) throw new Error("not mine");
        return AuthContext.anonymous();
      }
      return new AuthContext("test", true, user, { auth_time: Math.trunc(Date.now() / 1000) });
    },
    grantKeys: KEYS,
  });
  server = (globalThis as any).Bun.serve({ port: 0, fetch });
  baseUrl = `http://localhost:${server.port}`;
});

afterAll(() => server?.stop(true));

async function mintGrant(): Promise<string> {
  const rpc = httpConnect(baseUrl, {
    protocol: "vgi_rpc.Identity.v1",
    fetch: ((input: RequestInfo | URL, init?: RequestInit) => {
      const headers = new Headers(init?.headers);
      headers.set("x-test-user", OWNER);
      return fetch(input, { ...init, headers });
    }) as typeof fetch,
  });
  try {
    const out = (await rpc.call("issue_grant", { purpose: "nightly", scopes: ["read"], ttl_seconds: 600 })) as {
      result: Uint8Array;
    };
    return String((deserializeBatch(out.result) as any).getChild("token").get(0));
  } finally {
    rpc.close();
  }
}

async function scanAs(authorization: string | undefined): Promise<string[]> {
  const client = new VgiClient(httpConnect(baseUrl, authorization ? { authorization } : undefined));
  try {
    const attach = await client.catalogAttach("demo");
    const seen: string[] = [];
    for await (const batch of client.tableFunction({
      functionName: "whoami",
      attachOpaqueData: attach.attach_opaque_data,
    })) {
      for (let i = 0; i < batch.numRows; i++) seen.push(String((batch as any).getChild("principal").get(i)));
    }
    return seen;
  } finally {
    client.close();
  }
}

describe("a sealed grant authenticates a vgi.v2 call as its owner", () => {
  test("issue_grant, then Bearer <grant> on catalog_attach and a scan", async () => {
    const grant = await mintGrant();
    expect(grant.startsWith("vgig1.")).toBe(true);
    expect(await scanAs(`Bearer ${grant}`)).toEqual([OWNER]);
  });

  test("an anonymous caller still works and is nobody", async () => {
    expect(await scanAs(undefined)).toEqual([""]);
  });

  test("a tampered grant is refused, not downgraded to anonymous", async () => {
    const grant = await mintGrant();
    const tampered = grant.slice(0, -2) + (grant.endsWith("A") ? "BB" : "AA");
    await expect(scanAs(`Bearer ${tampered}`)).rejects.toThrow();
  });
});
