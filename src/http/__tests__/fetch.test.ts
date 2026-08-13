// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// createVgiFetch is the Cloudflare/workerd entry. Its landing surface is what
// a human sees when they open the worker's URL, and it used to be possible to
// ship a worker with that surface silently absent: `landingInfo` was optional,
// and omitting it downgraded GET / to vgi-rpc's generic "this is an RPC
// endpoint" placeholder while GET /vgi-client.js 404'd. Nothing threw, no test
// covered it, and vgi-open-meteo was deployed that way.
//
// These tests pin the surface itself rather than the option, so they fail the
// same way if a future refactor stops mounting it for some other reason.

import { describe, test, expect } from "bun:test";
import { int64 } from "../../arrow/index.js";
import { defineScalarFunction } from "../../functions/scalar.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { createVgiFetch, type VgiFetchOptions } from "../fetch.js";

const dbl = defineScalarFunction({
  name: "dbl",
  description: "Doubles its input",
  params: { x: int64() },
  outputType: () => int64(),
  compute: () => [],
});

function options(overrides: Partial<VgiFetchOptions> = {}): VgiFetchOptions {
  const registry = new FunctionRegistry();
  registry.register(dbl);
  const catalogInterface = new ReadOnlyCatalogInterface(
    { name: "demo", schemas: [{ name: "main", functions: [dbl] }] },
    registry,
  );
  return {
    protocol: { registry, catalogInterface },
    signingKey: new Uint8Array(32).fill(7),
    prefix: "",
    serverId: "vgi-test",
    landingInfo: { name: "demo", doc: "A demo worker.", version: "1.2.3" },
    ...overrides,
  };
}

const get = (handler: (r: Request) => Promise<Response>, path: string, accept = "text/html") =>
  handler(new Request(`http://worker.test${path}`, { headers: { Accept: accept } }));

describe("createVgiFetch landing surface", () => {
  test("GET / serves the shared landing page, not the vgi-rpc placeholder", async () => {
    const res = await get(createVgiFetch(options()), "/");
    expect(res.status).toBe(200);
    const body = await res.text();
    // The asset marker is the contract the cross-language landing checker
    // uses; the placeholder page carries no such marker and is ~2 KB.
    expect(body).toContain("vgi-landing-asset v");
    expect(body).not.toContain("This is a <code>vgi-rpc</code> service endpoint.");
    expect(body.length).toBeGreaterThan(100_000);
  });

  test("GET /vgi-client.js serves the browser client the page imports", async () => {
    const res = await get(createVgiFetch(options()), "/vgi-client.js", "*/*");
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toContain("javascript");
  });

  test("the status document carries the worker identity", async () => {
    const res = await get(createVgiFetch(options()), "/?format=json", "application/json");
    expect(res.status).toBe(200);
    const body = (await res.json()) as Record<string, unknown>;
    expect(body.worker).toBe("demo");
    expect(body.doc).toBe("A demo worker.");
    expect(body.version).toBe("1.2.3");
    expect(body.lang).toBe("typescript");
    // Drives the "Explore in Cupola" CTA; a missing value breaks the link.
    expect(body.cupola_base).toBeTruthy();
  });

  test("omitting landingInfo throws instead of silently serving the placeholder", () => {
    const { landingInfo: _drop, ...rest } = options();
    expect(() => createVgiFetch(rest as VgiFetchOptions)).toThrow(/landingInfo/);
  });
});
