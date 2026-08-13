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

  // Both assets sit at URLs that never change but change content on every
  // release, and they used to be served in the two ways that go wrong quietly:
  // the page with no Cache-Control and no validator at all (heuristic caching,
  // nothing to revalidate against, stale until a shift-reload), the bundle with
  // `max-age=3600` and no validator (a guaranteed hour in which a client cannot
  // learn a release happened — how a bundle that could not decode workerd's
  // compressed responses stayed live in a browser after being fixed).
  //
  // `no-cache` + a strong ETag costs the same bandwidth as a long TTL on repeat
  // visits, because they answer 304 with no body, and closes the staleness
  // window to zero.
  test.each([
    ["/", "text/html"],
    ["/vgi-client.js", "*/*"],
  ])("%s revalidates rather than going stale", async (path, accept) => {
    const res = await get(createVgiFetch(options()), path, accept);
    expect(res.status).toBe(200);
    expect(res.headers.get("cache-control")).toBe("public, no-cache");
    expect(res.headers.get("etag")).toMatch(/^"[0-9a-f]+-[0-9a-f]+"$/);
  });

  test.each([
    ["/", "text/html"],
    ["/vgi-client.js", "*/*"],
  ])("%s answers 304 with no body when the client already has it", async (path, accept) => {
    const handler = createVgiFetch(options());
    const first = await get(handler, path, accept);
    const etag = first.headers.get("etag")!;

    const second = await handler(
      new Request(`http://worker.test${path}`, { headers: { Accept: accept, "If-None-Match": etag } }),
    );
    expect(second.status).toBe(304);
    expect(await second.text()).toBe("");
    expect(second.headers.get("etag")).toBe(etag);

    // A cache is allowed to weaken the validator on the way back.
    const weak = await handler(
      new Request(`http://worker.test${path}`, {
        headers: { Accept: accept, "If-None-Match": `W/${etag}` },
      }),
    );
    expect(weak.status).toBe(304);

    // A stale validator must not be honoured.
    const stale = await handler(
      new Request(`http://worker.test${path}`, {
        headers: { Accept: accept, "If-None-Match": '"deadbeef-1"' },
      }),
    );
    expect(stale.status).toBe(200);
  });

  test("the two assets have different ETags", async () => {
    const handler = createVgiFetch(options());
    const page = await get(handler, "/", "text/html");
    const bundle = await get(handler, "/vgi-client.js", "*/*");
    expect(page.headers.get("etag")).not.toBe(bundle.headers.get("etag"));
  });

  test("omitting landingInfo throws instead of silently serving the placeholder", () => {
    const { landingInfo: _drop, ...rest } = options();
    expect(() => createVgiFetch(rest as VgiFetchOptions)).toThrow(/landingInfo/);
  });
});

// CORS was opt-in here while `serveVgiWorker` defaulted it to "*", so the two
// entries in this package disagreed and a CF worker that simply didn't mention
// CORS shipped with no Access-Control headers at all. That fails only in a
// browser, only cross-origin, and so only for someone else's page — never in
// the author's own curl or test run. vgi-open-meteo was deployed that way and
// its landing page linked to a Cupola it could not answer.
describe("createVgiFetch CORS", () => {
  const preflight = (handler: (r: Request) => Promise<Response>) =>
    handler(
      new Request("http://worker.test/__describe__", {
        method: "OPTIONS",
        headers: {
          Origin: "https://cupola.query-farm.services",
          "Access-Control-Request-Method": "POST",
        },
      }),
    );

  test("defaults to open, so a worker that never mentions CORS still answers a preflight", async () => {
    const { corsOrigins: _none, ...rest } = options();
    const res = await preflight(createVgiFetch(rest as VgiFetchOptions));
    expect(res.headers.get("Access-Control-Allow-Origin")).toBe("*");
    expect(res.headers.get("Access-Control-Allow-Methods")).toContain("POST");
  });

  test("an explicit origin is used verbatim", async () => {
    const res = await preflight(createVgiFetch(options({ corsOrigins: "https://only.test" })));
    expect(res.headers.get("Access-Control-Allow-Origin")).toBe("https://only.test");
  });

  // The opt-out is the fragile half: `null` and `undefined` now mean opposite
  // things, so a refactor that collapses them re-enables CORS for a worker that
  // deliberately turned it off.
  test("null disables CORS entirely", async () => {
    const res = await preflight(createVgiFetch(options({ corsOrigins: null })));
    expect(res.headers.get("Access-Control-Allow-Origin")).toBeNull();
  });
});
