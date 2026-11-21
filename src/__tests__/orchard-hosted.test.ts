// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Orchard hosted mode (VGI_ORCHARD_HOSTED=1), end to end.
//
// A port of vgi-python's tests/test_orchard_hosted.py, plus the JWKS cache
// rules it covers through vgi-rpc-python. Checks the contract in
// docs/orchard-hosted.md:
//
// - a valid Pass reaches function code, with claims in their JSON types;
// - no Pass, a tampered claim, the wrong aud/iss/typ/alg, an expired Pass and
//   so on all get 401 with `WWW-Authenticate: Bearer error="invalid_token"`,
//   `VGI-Auth-Reason`, the capability headers and no resource_metadata;
//   `HEAD /health` and CORS preflights alone are anonymous;
// - authentication runs before the body is read;
// - the JWKS cache honours max-age (capped at 10 minutes), never uses stale
//   keys (503 instead), and re-fetches for an unknown kid at most once a minute;
// - the Authorization header never reaches the logs, even with VGI_DEBUG;
// - every misconfiguration refuses to start, with a non-zero exit.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { exportJWK, generateKeyPair, SignJWT, type CryptoKey, type JWK } from "jose";
import { AuthContext, AuthFailure, AuthUnavailableError, GrantKeys, httpConnect } from "@query-farm/vgi-rpc";
import { batchFromColumns, field, int64, schema, utf8 } from "../arrow/index.js";
import { defineScalarFunction } from "../functions/scalar.js";
import { FunctionRegistry } from "../functions/registry.js";
import { ReadOnlyCatalogInterface } from "../catalog/read-only.js";
import { VgiClient } from "../client/client.js";
import { createVgiFetch, type VgiFetchOptions } from "../http/fetch.js";
import { createVgiWorkerFetch } from "../serve-entry.js";
import {
  checkHostedEnvironment,
  checkHostedSigningKey,
  hostedModeEnabled,
  OrchardHostedConfigError,
  resolveHostedConfig,
  type HostedConfig,
} from "../hosted.js";
import { cacheControlMaxAge, hostedAuthenticate, isHostedAuthenticate } from "../http/hosted-auth.js";

const ISSUER = "https://auth.orchard.test";
const AUDIENCE = "https://desk.orchard.test/d/dep_123";
const REPO = new URL("../..", import.meta.url).pathname;
const FIXTURE = "examples/orchard-hosted-worker.ts";

const ec = await generateKeyPair("ES256", { extractable: true });
const ecJwk: JWK = { ...(await exportJWK(ec.publicKey)), kid: "desk-es256-1", alg: "ES256", use: "sig" };
const rotated = await generateKeyPair("ES256", { extractable: true });
const rotatedJwk: JWK = { ...(await exportJWK(rotated.publicKey)), kid: "desk-es256-2", alg: "ES256", use: "sig" };
const rsa = await generateKeyPair("RS256", { extractable: true });
const rsaJwk: JWK = { ...(await exportJWK(rsa.publicKey)), kid: "desk-rs256-1", alg: "RS256", use: "sig" };

// ---------------------------------------------------------------------------
// Passes
// ---------------------------------------------------------------------------

interface PassOptions {
  key?: CryptoKey | Uint8Array;
  alg?: string;
  typ?: string | null;
  kid?: string;
  [claim: string]: unknown;
}

async function pass(options: PassOptions = {}): Promise<string> {
  const { key = ec.privateKey, alg = "ES256", typ = "at+jwt", kid = "desk-es256-1", ...overrides } = options;
  const now = Math.floor(Date.now() / 1000);
  const claims: Record<string, unknown> = {
    iss: ISSUER,
    aud: AUDIENCE,
    sub: "user_01M7KNJYC18T",
    kind: "user",
    email: "ada@example.com",
    email_verified: true,
    name: "Ada Lovelace",
    org_id: "org_01M7",
    org_name: "Acme Analytics",
    client_id: "vgi-duckdb",
    iat: now,
    nbf: now,
    exp: now + 900,
    jti: `jti-${crypto.randomUUID()}`,
    ...overrides,
  };
  for (const [name, value] of Object.entries(claims)) if (value === null || value === undefined) delete claims[name];
  const header: Record<string, string> = { alg, kid };
  if (typ !== null) header.typ = typ;
  return new SignJWT(claims).setProtectedHeader(header as { alg: string }).sign(key);
}

function b64urlJson(segment: string): Record<string, unknown> {
  const padded = segment.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - (segment.length % 4)) % 4);
  return JSON.parse(atob(padded));
}

function tamper(token: string, changes: Record<string, unknown>): string {
  const [header, payload, signature] = token.split(".");
  const claims = { ...b64urlJson(payload!), ...changes };
  const forged = btoa(JSON.stringify(claims)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  return `${header}.${forged}.${signature}`;
}

// ---------------------------------------------------------------------------
// A local JWKS endpoint
// ---------------------------------------------------------------------------

interface JwksControl {
  keys: JWK[];
  cacheControl: string | null;
  fail: boolean;
  hits: number;
}

function startJwks(control: JwksControl) {
  return Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    fetch() {
      control.hits++;
      if (control.fail) return new Response("down", { status: 503 });
      const headers = new Headers({ "Content-Type": "application/json" });
      if (control.cacheControl) headers.set("Cache-Control", control.cacheControl);
      return new Response(JSON.stringify({ keys: control.keys }), { headers });
    },
  });
}

// ---------------------------------------------------------------------------
// A hosted worker, in process
// ---------------------------------------------------------------------------

const PASS_CLAIM_NAMES = ["sub", "kind", "email", "email_verified", "name", "org_id", "org_name", "client_id", "exp"];

const passClaims = defineScalarFunction({
  name: "pass_claims",
  description: "Return the caller's verified Pass claims as JSON.",
  params: { x: int64() },
  returns: utf8(),
  compute: (batch, _consts, info) => {
    const claims: Record<string, unknown> = {};
    for (const name of PASS_CLAIM_NAMES) if (name in info.auth.claims) claims[name] = info.auth.claims[name];
    const payload = JSON.stringify({ principal: info.auth.principal, domain: info.auth.domain, claims });
    return Array.from({ length: batch.numRows }, () => payload);
  },
});

function fetchOptions(config: HostedConfig | undefined, overrides: Partial<VgiFetchOptions> = {}): VgiFetchOptions {
  const registry = new FunctionRegistry();
  registry.register(passClaims);
  const catalogInterface = new ReadOnlyCatalogInterface(
    { name: "orchard", schemas: [{ name: "main", functions: [passClaims] }] },
    registry,
  );
  return {
    protocol: { registry, catalogInterface },
    signingKey: new Uint8Array(32).fill(7),
    prefix: "",
    serverId: "vgi-hosted-test",
    landingInfo: { name: "orchard", doc: "Hosted test worker.", version: "0.0.1" },
    orchardHosted: config,
    ...overrides,
  };
}

function hostedConfig(jwksUri: string, algorithms: readonly string[] = ["ES256"]): HostedConfig {
  return { issuer: ISSUER, audience: AUDIENCE, jwksUri, algorithms };
}

async function callPassClaims(base: string, token: string): Promise<Record<string, any>> {
  const rpc = httpConnect(base, { prefix: "", authorization: `Bearer ${token}` });
  try {
    const client = new VgiClient(rpc);
    const { attach_opaque_data } = await client.catalogAttach("orchard");
    const out: string[] = [];
    for await (const rows of client.scalarFunctionRows({
      functionName: "pass_claims",
      input: [batchFromColumns({ x: [1n] }, schema([field("x", int64(), true)]))],
      attachOpaqueData: attach_opaque_data,
    })) {
      for (const row of rows) out.push(String(Object.values(row)[0]));
    }
    expect(out.length).toBe(1);
    return JSON.parse(out[0]!);
  } finally {
    rpc.close();
  }
}

function expect401(res: Response, reason?: string): void {
  expect(res.status).toBe(401);
  expect(res.headers.get("www-authenticate")).toBe('Bearer error="invalid_token"');
  expect(res.headers.get("www-authenticate") ?? "").not.toContain("resource_metadata");
  expect(res.headers.has("vgi-supported-encodings")).toBe(true);
  expect(
    res.headers.has("vgi-accept-max-response-bytes-support") || res.headers.has("vgi-externalization-enabled"),
  ).toBe(true);
  expect(res.headers.get("access-control-allow-origin")).toBe("*");
  const got = res.headers.get("vgi-auth-reason");
  expect(["missing_credential", "invalid_credential", "expired_credential"]).toContain(got!);
  if (reason) expect(got).toBe(reason);
}

describe("hosted worker over HTTP", () => {
  const control: JwksControl = { keys: [ecJwk, rsaJwk], cacheControl: null, fail: false, hits: 0 };
  let jwks: ReturnType<typeof startJwks>;
  let server: ReturnType<typeof Bun.serve>;
  let base: string;
  let handler: (req: Request) => Promise<Response>;

  beforeAll(() => {
    jwks = startJwks(control);
    // CORS restricted on purpose: hosted mode must open it regardless.
    handler = createVgiFetch(
      fetchOptions(hostedConfig(`http://127.0.0.1:${jwks.port}/jwks`), { corsOrigins: "https://only.example" }),
    );
    server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch: handler });
    base = `http://127.0.0.1:${server.port}`;
  });
  afterAll(() => {
    server.stop(true);
    jwks.stop(true);
  });

  const send = (method: string, path: string, token?: string, headers: Record<string, string> = {}) =>
    fetch(base + path, {
      method,
      headers: { ...headers, ...(token !== undefined ? { Authorization: `Bearer ${token}` } : {}) },
      ...(method === "POST" ? { body: new Uint8Array(0) } : {}),
    });

  test("a valid Pass reaches function code, with claims in their JSON types", async () => {
    const result = await callPassClaims(base, await pass());
    expect(result.principal).toBe("user_01M7KNJYC18T");
    expect(result.domain).toBe("orchard");
    const claims = result.claims;
    expect(claims.email_verified).toBe(true);
    expect(Number.isInteger(claims.exp)).toBe(true);
    expect(claims.kind).toBe("user");
    expect(claims.email).toBe("ada@example.com");
    expect(claims.name).toBe("Ada Lovelace");
    expect(claims.org_id).toBe("org_01M7");
    expect(claims.org_name).toBe("Acme Analytics");
  });

  test("HEAD /health is anonymous and carries the capability headers", async () => {
    const res = await send("HEAD", "/health");
    expect(res.status).toBe(200);
    expect(res.headers.has("vgi-supported-encodings")).toBe(true);
  });

  for (const [method, path] of [
    ["GET", "/health"],
    ["OPTIONS", "/health"],
    ["GET", "/"],
    ["GET", "/describe"],
    ["GET", "/vgi-client.js"],
    ["GET", "/no/such/path"],
    ["POST", "/vgi.v3/catalog_attach"],
    ["DELETE", "/__session__"],
    ["PUT", "/"],
    ["HEAD", "/"],
  ] as const) {
    test(`no Pass: ${method} ${path} is 401`, async () => {
      expect401(await send(method, path, undefined, { Origin: "https://shell.example.com" }), "missing_credential");
    });
  }

  test("a valid Pass opens the pages and GET /health", async () => {
    const token = await pass();
    expect((await send("GET", "/health", token)).status).toBe(200);
    expect((await send("GET", "/", token, { Accept: "text/html" })).status).toBe(200);
  });

  const now = () => Math.floor(Date.now() / 1000);
  const bad: Record<string, [() => Promise<string>, string]> = {
    garbage: [async () => "not-a-jwt", "invalid_credential"],
    tampered: [async () => tamper(await pass(), { org_id: "org_EVIL" }), "invalid_credential"],
    wrong_aud: [() => pass({ aud: "https://desk.orchard.test/d/dep_other" }), "invalid_credential"],
    multi_aud: [() => pass({ aud: [AUDIENCE, "https://desk.orchard.test/d/dep_other"] }), "invalid_credential"],
    expired: [() => pass({ iat: now() - 1200, nbf: now() - 1200, exp: now() - 120 }), "expired_credential"],
    wrong_typ: [() => pass({ typ: "JWT" }), "invalid_credential"],
    missing_typ: [() => pass({ typ: null }), "invalid_credential"],
    rs256: [() => pass({ key: rsa.privateKey, alg: "RS256", kid: "desk-rs256-1" }), "invalid_credential"],
    hs256: [() => pass({ key: new TextEncoder().encode("k".repeat(32)), alg: "HS256" }), "invalid_credential"],
    wrong_iss: [() => pass({ iss: "https://evil.test" }), "invalid_credential"],
    no_exp: [() => pass({ exp: null }), "invalid_credential"],
    not_yet_valid: [() => pass({ nbf: now() + 600 }), "invalid_credential"],
    unknown_kid: [() => pass({ key: rotated.privateKey, kid: "desk-es256-9" }), "invalid_credential"],
  };
  for (const [name, [make, reason]] of Object.entries(bad)) {
    test(`bad Pass (${name}) is 401 ${reason}`, async () => {
      const res = await send("POST", "/vgi.v3/catalog_attach", await make(), { Origin: "https://shell.example.com" });
      expect401(res, reason);
    });
  }

  test("a non-Bearer Authorization is 401", async () => {
    const res = await fetch(base + "/health", { headers: { Authorization: "Basic dXNlcjpwYXNz" } });
    expect401(res, "invalid_credential");
  });

  test("accepted variants: typ case and prefix, a one-element aud list, exp within the leeway, lowercase scheme", async () => {
    const tokens = [
      await pass({ typ: "application/AT+JWT" }),
      await pass({ aud: [AUDIENCE] }),
      await pass({ exp: now() - 30 }),
    ];
    for (const token of tokens) expect((await send("GET", "/health", token)).status).toBe(200);
    const lower = await fetch(base + "/health", { headers: { Authorization: `bearer ${await pass()}` } });
    expect(lower.status).toBe(200);
  });

  test("a CORS preflight needs no Pass, and CORS is open", async () => {
    const res = await send("OPTIONS", "/vgi.v3/catalog_attach", undefined, {
      Origin: "https://shell.example.com",
      "Access-Control-Request-Method": "POST",
      "Access-Control-Request-Headers": "authorization",
    });
    expect(res.status).toBeGreaterThanOrEqual(200);
    expect(res.status).toBeLessThan(300);
    expect(res.headers.get("access-control-allow-origin")).toBe("*");
  });

  test("authentication runs before the body is read", async () => {
    let pulled = false;
    // highWaterMark 0: nothing is pulled until someone reads.
    const body = new ReadableStream<Uint8Array>(
      {
        pull(controller) {
          pulled = true;
          controller.enqueue(new Uint8Array(1 << 20));
        },
      },
      { highWaterMark: 0 },
    );
    const req = new Request(`${base}/vgi.v3/catalog_attach`, {
      method: "POST",
      headers: { "Content-Type": "application/vnd.apache.arrow.stream", "Content-Encoding": "zstd" },
      body,
      // @ts-expect-error -- Bun/undici need duplex for a stream body
      duplex: "half",
    });
    expect401(await handler(req), "missing_credential");
    expect(pulled).toBe(false);
  });
});

describe("JWKS unavailable", () => {
  test("a fetch failure is 503 with Retry-After, never 401", async () => {
    const control: JwksControl = { keys: [ecJwk], cacheControl: null, fail: true, hits: 0 };
    const jwks = startJwks(control);
    try {
      const handler = createVgiFetch(fetchOptions(hostedConfig(`http://127.0.0.1:${jwks.port}/jwks`)));
      const res = await handler(
        new Request("http://worker.test/health", { headers: { Authorization: `Bearer ${await pass()}` } }),
      );
      expect(res.status).toBe(503);
      expect(Number(res.headers.get("retry-after"))).toBeGreaterThan(0);
      expect(res.headers.has("www-authenticate")).toBe(false);
      // Recovers once the endpoint does.
      control.fail = false;
      const ok = await handler(
        new Request("http://worker.test/health", { headers: { Authorization: `Bearer ${await pass()}` } }),
      );
      expect(ok.status).toBe(200);
    } finally {
      jwks.stop(true);
    }
  });
});

// ---------------------------------------------------------------------------
// The verifier and its JWKS cache, with a fake clock and fetch
// ---------------------------------------------------------------------------

describe("Pass verifier", () => {
  function harness(initial: { keys?: JWK[]; cacheControl?: string | null } = {}) {
    const state = {
      keys: initial.keys ?? [ecJwk],
      cacheControl: initial.cacheControl ?? null,
      fail: false,
      fetches: 0,
      clock: Date.now(),
    };
    const fakeFetch = (async () => {
      state.fetches++;
      if (state.fail) throw new TypeError("connection refused");
      const headers = new Headers({ "Content-Type": "application/json" });
      if (state.cacheControl) headers.set("Cache-Control", state.cacheControl);
      return new Response(JSON.stringify({ keys: state.keys }), { headers });
    });
    const verify = hostedAuthenticate(hostedConfig("https://auth.orchard.test/jwks"), {
      fetch: fakeFetch,
      now: () => state.clock,
      warn: () => {},
    });
    const call = async (token: string) =>
      verify(new Request("http://worker.test/", { headers: { Authorization: `Bearer ${token}` } }));
    return { state, verify, call };
  }

  // Tokens are minted against real time; the fake clock stays near it.
  const advance = (state: { clock: number }, seconds: number) => {
    state.clock += seconds * 1000;
  };
  const longLived = () => pass({ exp: Math.floor(Date.now() / 1000) + 7200 });

  test("claims keep their JSON types; principal is sub, domain orchard", async () => {
    const { call } = harness();
    const auth = await call(await pass());
    expect(auth).toBeInstanceOf(AuthContext);
    expect(auth.authenticated).toBe(true);
    expect(auth.principal).toBe("user_01M7KNJYC18T");
    expect(auth.domain).toBe("orchard");
    expect(auth.claims.email_verified).toBe(true);
    expect(typeof auth.claims.exp).toBe("number");
    expect(typeof auth.claims.iat).toBe("number");
    expect(typeof auth.claims.jti).toBe("string");
  });

  test("the verifier is recognisable as the hosted authenticate", () => {
    expect(isHostedAuthenticate(harness().verify)).toBe(true);
    expect(isHostedAuthenticate(() => AuthContext.anonymous())).toBe(false);
  });

  test("keys are cached for 10 minutes without Cache-Control", async () => {
    const { state, call } = harness();
    const token = await longLived();
    await call(token);
    advance(state, 599);
    await call(token);
    expect(state.fetches).toBe(1);
    advance(state, 2);
    await call(token);
    expect(state.fetches).toBe(2);
  });

  test("a shorter Cache-Control max-age wins; a longer one is capped at 10 minutes", async () => {
    const short = harness({ cacheControl: "public, max-age=30" });
    const token = await longLived();
    await short.call(token);
    advance(short.state, 29);
    await short.call(token);
    expect(short.state.fetches).toBe(1);
    advance(short.state, 2);
    await short.call(token);
    expect(short.state.fetches).toBe(2);

    const long = harness({ cacheControl: "max-age=86400" });
    await long.call(token);
    advance(long.state, 601);
    await long.call(token);
    expect(long.state.fetches).toBe(2);
  });

  test("expired keys are discarded: a failed re-fetch is 503 until it succeeds, never stale keys", async () => {
    const { state, call } = harness();
    const token = await longLived();
    await call(token);
    advance(state, 601);
    state.fail = true;
    await expect(call(token)).rejects.toBeInstanceOf(AuthUnavailableError);
    await expect(call(token)).rejects.toBeInstanceOf(AuthUnavailableError);
    state.fail = false;
    expect((await call(token)).principal).toBe("user_01M7KNJYC18T");
  });

  test("an unknown kid re-fetches at most once per 60 s, then picks up a rotated key", async () => {
    const { state, call } = harness();
    await call(await longLived());
    expect(state.fetches).toBe(1);
    const rotatedToken = await pass({ key: rotated.privateKey, kid: "desk-es256-2", exp: Math.floor(Date.now() / 1000) + 7200 });
    // Within 60 s of the last fetch: rejected without a fetch.
    advance(state, 10);
    await expect(call(rotatedToken)).rejects.toBeInstanceOf(AuthFailure);
    expect(state.fetches).toBe(1);
    // The Front Desk rotates; past the interval one re-fetch finds the key.
    state.keys = [ecJwk, rotatedJwk];
    advance(state, 51);
    expect((await call(rotatedToken)).principal).toBe("user_01M7KNJYC18T");
    expect(state.fetches).toBe(2);
    // A still-unknown kid right after: rejected, no fetch.
    const unknown = await pass({ key: rotated.privateKey, kid: "desk-es256-404" });
    await expect(call(unknown)).rejects.toBeInstanceOf(AuthFailure);
    expect(state.fetches).toBe(2);
  });

  test("concurrent first requests share one JWKS fetch", async () => {
    const { state, call } = harness();
    const token = await pass();
    await Promise.all(Array.from({ length: 8 }, () => call(token)));
    expect(state.fetches).toBe(1);
  });

  test("a missing Authorization is missing_credential; an expired Pass expired_credential", async () => {
    const { verify, call } = harness();
    await expect(verify(new Request("http://worker.test/"))).rejects.toMatchObject({ reason: "missing_credential" });
    const t = Math.floor(Date.now() / 1000);
    await expect(call(await pass({ iat: t - 1200, nbf: t - 1200, exp: t - 120 }))).rejects.toMatchObject({
      reason: "expired_credential",
    });
  });

  test("VGI_JWT_ALGORITHMS can add an algorithm", async () => {
    const verify = hostedAuthenticate(
      { issuer: ISSUER, audience: AUDIENCE, jwksUri: "https://x.test/jwks", algorithms: ["ES256", "RS256"] },
      {
        fetch: (async () => new Response(JSON.stringify({ keys: [ecJwk, rsaJwk] }))),
        warn: () => {},
      },
    );
    const token = await pass({ key: rsa.privateKey, alg: "RS256", kid: "desk-rs256-1" });
    const auth = await verify(new Request("http://w.test/", { headers: { Authorization: `Bearer ${token}` } }));
    expect(auth.principal).toBe("user_01M7KNJYC18T");
  });

  test("Cache-Control max-age parsing", () => {
    expect(cacheControlMaxAge("public, max-age=30")).toBe(30);
    expect(cacheControlMaxAge('max-age="45", must-revalidate')).toBe(45);
    expect(cacheControlMaxAge("s-maxage=10")).toBeNull();
    expect(cacheControlMaxAge(null)).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const JWKS = "http://127.0.0.1:9/jwks"; // never contacted: startup refuses first
const KEY = "orchard-hosted-test-signing-key-0001";

function hostedEnv(overrides: Record<string, string | undefined> = {}): Record<string, string | undefined> {
  const env: Record<string, string | undefined> = {
    VGI_ORCHARD_HOSTED: "1",
    VGI_JWT_ISSUER: ISSUER,
    VGI_JWT_AUDIENCE: AUDIENCE,
    VGI_JWT_JWKS_URI: JWKS,
    VGI_SIGNING_KEY: KEY,
    ...overrides,
  };
  for (const [k, v] of Object.entries(env)) if (v === undefined) delete env[k];
  return env;
}

function refusal(fn: () => unknown): string {
  try {
    fn();
  } catch (error) {
    expect(error).toBeInstanceOf(OrchardHostedConfigError);
    return (error as Error).message;
  }
  throw new Error("expected a hosted-mode refusal");
}

describe("hosted-mode configuration", () => {
  test("on and off values", () => {
    for (const v of ["1", "true", "YES", "on", " On "]) expect(hostedModeEnabled({ VGI_ORCHARD_HOSTED: v })).toBe(true);
    for (const v of ["0", "false", "", "off", "No"]) expect(hostedModeEnabled({ VGI_ORCHARD_HOSTED: v })).toBe(false);
    expect(hostedModeEnabled({})).toBe(false);
  });

  test("an unrecognised value refuses instead of meaning off", () => {
    expect(refusal(() => hostedModeEnabled({ VGI_ORCHARD_HOSTED: "yes please" }))).toContain("VGI_ORCHARD_HOSTED");
  });

  for (const name of ["VGI_JWT_ISSUER", "VGI_JWT_AUDIENCE", "VGI_JWT_JWKS_URI"]) {
    test(`${name} unset, empty or a list refuses`, () => {
      expect(refusal(() => resolveHostedConfig(hostedEnv({ [name]: undefined })))).toContain(name);
      expect(refusal(() => resolveHostedConfig(hostedEnv({ [name]: "  " })))).toContain(name);
      const listed = refusal(() => resolveHostedConfig(hostedEnv({ [name]: "https://a.test,https://b.test" })));
      expect(listed).toContain(name);
      expect(listed).toContain("exactly one");
    });
  }

  test("VGI_JWT_ALGORITHMS: never none or HS*; default ES256", () => {
    for (const v of ["HS256", "ES256,HS256", "none", "hs512", " , "]) {
      expect(refusal(() => resolveHostedConfig(hostedEnv({ VGI_JWT_ALGORITHMS: v })))).toContain("VGI_JWT_ALGORITHMS");
    }
    expect(resolveHostedConfig(hostedEnv()).algorithms).toEqual(["ES256"]);
    expect(resolveHostedConfig(hostedEnv({ VGI_JWT_ALGORITHMS: " " })).algorithms).toEqual(["ES256"]);
    expect(resolveHostedConfig(hostedEnv({ VGI_JWT_ALGORITHMS: "ES256, ES384" })).algorithms).toEqual(["ES256", "ES384"]);
  });

  test("other credentials in the environment refuse", () => {
    const key = btoa(String.fromCharCode(...Array.from({ length: 32 }, (_, i) => i)));
    for (const [name, value] of [
      ["VGI_BEARER_TOKENS", "tok=alice"],
      ["VGI_RPC_GRANT_KEYS", key],
      ["VGI_OAUTH_RESOURCE", "https://x.test"],
      ["VGI_PROXY_PROOF_MODE", "required"],
    ] as const) {
      expect(refusal(() => checkHostedEnvironment(hostedEnv({ [name]: value })))).toContain(name);
    }
    checkHostedEnvironment(hostedEnv({ VGI_PROXY_PROOF_MODE: "off" }));
  });

  test("the signing key must be configured, not minted, and at least 32 bytes", () => {
    expect(refusal(() => checkHostedSigningKey(hostedEnv({ VGI_SIGNING_KEY: undefined })))).toContain("VGI_SIGNING_KEY");
    const short = refusal(() => checkHostedSigningKey(hostedEnv({ VGI_SIGNING_KEY: "short-key" })));
    expect(short).toContain("VGI_SIGNING_KEY");
    expect(short).toContain("32 bytes");
    expect(
      refusal(() => checkHostedSigningKey(hostedEnv({ VGI_SIGNING_KEY_MINTED: "1" }))),
    ).toContain("VGI_SIGNING_KEY");
    checkHostedSigningKey(hostedEnv());
    checkHostedSigningKey(hostedEnv({ VGI_SIGNING_KEY: undefined }), new Uint8Array(32));
    expect(refusal(() => checkHostedSigningKey(hostedEnv({ VGI_SIGNING_KEY: undefined }), new Uint8Array(16)))).toContain(
      "32 bytes",
    );
  });

  test("createVgiFetch refuses every other way in", () => {
    const config = hostedConfig(JWKS);
    const grantKeys = GrantKeys.fromEnv({ VGI_RPC_GRANT_KEYS: btoa("k".repeat(32)) })!;
    const cases: Array<[Partial<VgiFetchOptions>, string]> = [
      [{ authenticate: () => AuthContext.anonymous() }, "authenticate"],
      [{ grantKeys }, "grant"],
      [{ resolveToken: () => null }, "resolve_token"],
      [{ mintGrant: () => null as never }, "mint_grant"],
      [{ peerAuthenticationPolicy: async (_e, a) => a }, "peer identity"],
      [{ signingKeyConfigured: false }, "VGI_SIGNING_KEY"],
      [{ signingKey: new Uint8Array(16) }, "32 bytes"],
    ];
    for (const [overrides, needle] of cases) {
      expect(refusal(() => createVgiFetch(fetchOptions(config, overrides)))).toContain(needle);
    }
    // The hosted verifier itself is not "another way in".
    createVgiFetch(fetchOptions(config, { authenticate: hostedAuthenticate(config) }));
  });

  test("createVgiWorkerFetch reads hosted mode from its environment", async () => {
    const base = {
      name: "orchard",
      doc: "d",
      version: "0",
      registry: new FunctionRegistry(),
      catalogInterface: new ReadOnlyCatalogInterface({ name: "orchard", schemas: [] }, new FunctionRegistry()),
      argv: [] as string[],
    };
    expect(refusal(() => createVgiWorkerFetch({ ...base, env: hostedEnv({ VGI_JWT_ISSUER: undefined }) }))).toContain(
      "VGI_JWT_ISSUER",
    );
    expect(refusal(() => createVgiWorkerFetch({ ...base, env: hostedEnv({ VGI_SIGNING_KEY: undefined }) }))).toContain(
      "VGI_SIGNING_KEY",
    );
    const hexKey = "ab".repeat(32);
    expect(
      refusal(() =>
        createVgiWorkerFetch({ ...base, env: hostedEnv({ VGI_SIGNING_KEY: hexKey }), argv: ["--grant-key", "x"] }),
      ),
    ).toContain("--grant-key");
    expect(
      refusal(() =>
        createVgiWorkerFetch({
          ...base,
          env: hostedEnv({ VGI_SIGNING_KEY: hexKey }),
          irohBridge: { issuer: "iroh", peerResolutionContext: () => ({}) },
        }),
      ),
    ).toContain("Iroh");
    const handler = createVgiWorkerFetch({ ...base, env: hostedEnv({ VGI_SIGNING_KEY: hexKey }) });
    expect401(await handler(new Request("http://worker.test/health")), "missing_credential");
    expect((await handler(new Request("http://worker.test/health", { method: "HEAD" }))).status).toBe(200);
  });

  test("the hosted modules import nothing from node:*", async () => {
    for (const file of ["../hosted.ts", "../http/hosted-auth.ts"]) {
      const source = await Bun.file(new URL(file, import.meta.url)).text();
      expect(source).not.toMatch(/from\s+["']node:/);
      expect(source).not.toMatch(/import\(\s*["']node:/);
    }
  });
});

// ---------------------------------------------------------------------------
// The fixture worker, as a real process
// ---------------------------------------------------------------------------

const AUTH_ENV = [
  "VGI_SIGNING_KEY_MINTED",
  "VGI_SIGNING_KEY",
  "VGI_BEARER_TOKENS",
  "VGI_JWT_ISSUER",
  "VGI_JWT_AUDIENCE",
  "VGI_JWT_JWKS_URI",
  "VGI_JWT_ALGORITHMS",
  "VGI_RPC_GRANT_KEYS",
  "VGI_OAUTH_RESOURCE",
  "VGI_PROXY_PROOF_MODE",
  "VGI_ORCHARD_HOSTED",
  "VGI_DEBUG",
  "PORT",
];

function processEnv(overrides: Record<string, string | undefined> = {}): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [k, v] of Object.entries(process.env)) if (v !== undefined && !AUTH_ENV.includes(k)) env[k] = v;
  for (const [k, v] of Object.entries(hostedEnv(overrides))) if (v !== undefined) env[k] = v;
  return env;
}

async function refusedProcess(env: Record<string, string>, args: string[] = []): Promise<string> {
  const proc = Bun.spawn(["bun", "run", FIXTURE, ...args], { cwd: REPO, env, stdout: "pipe", stderr: "pipe" });
  const timer = setTimeout(() => proc.kill(), 30_000);
  const [stdout, stderr, code] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  clearTimeout(timer);
  expect(code).not.toBe(0);
  expect(stdout).not.toContain("PORT:");
  return stderr;
}

describe("fixture worker startup refusals", () => {
  const key = btoa(String.fromCharCode(...Array.from({ length: 32 }, (_, i) => i)));
  const cases: Array<[string, Record<string, string | undefined>, string[], string]> = [
    ["issuer unset", { VGI_JWT_ISSUER: undefined }, [], "VGI_JWT_ISSUER"],
    ["audience empty", { VGI_JWT_AUDIENCE: "  " }, [], "VGI_JWT_AUDIENCE"],
    ["jwks unset", { VGI_JWT_JWKS_URI: undefined }, [], "VGI_JWT_JWKS_URI"],
    ["comma audience", { VGI_JWT_AUDIENCE: `${AUDIENCE},https://desk.orchard.test/d/other` }, [], "exactly one"],
    ["signing key unset", { VGI_SIGNING_KEY: undefined }, [], "VGI_SIGNING_KEY"],
    ["signing key short", { VGI_SIGNING_KEY: "short-key" }, [], "32 bytes"],
    ["signing key minted", { VGI_SIGNING_KEY_MINTED: "1" }, [], "VGI_SIGNING_KEY"],
    ["bearer tokens", { VGI_BEARER_TOKENS: "tok=alice" }, [], "VGI_BEARER_TOKENS"],
    ["grant keys env", { VGI_RPC_GRANT_KEYS: key }, [], "VGI_RPC_GRANT_KEYS"],
    ["grant key flag", {}, ["--grant-key", key], "grant"],
    ["resolve token", {}, ["--with-resolve-token"], "resolve_token"],
    ["custom authenticate", {}, ["--with-authenticate"], "authenticate"],
    ["oauth resource", { VGI_OAUTH_RESOURCE: "https://x.test" }, [], "VGI_OAUTH_RESOURCE"],
    ["hs algorithm", { VGI_JWT_ALGORITHMS: "ES256,HS256" }, [], "VGI_JWT_ALGORITHMS"],
    ["unrecognised flag value", { VGI_ORCHARD_HOSTED: "yes please" }, [], "VGI_ORCHARD_HOSTED"],
    ["stdio transport", {}, ["--worker"], "only HTTP"],
    ["unix transport", {}, ["--worker", "--unix", "/tmp/vgi-orchard-hosted-refuse.sock"], "only HTTP"],
    ["tcp transport", {}, ["--worker", "--tcp", "127.0.0.1:0"], "only HTTP"],
  ];
  for (const [name, overrides, args, needle] of cases) {
    test(name, async () => {
      expect(await refusedProcess(processEnv(overrides), args)).toContain(needle);
    }, 40_000);
  }
});

describe("fixture worker in hosted mode", () => {
  test("serves Passes only, and never logs the Authorization header (VGI_DEBUG=1)", async () => {
    const control: JwksControl = { keys: [ecJwk, rsaJwk], cacheControl: null, fail: false, hits: 0 };
    const jwks = startJwks(control);
    const env = processEnv({ VGI_JWT_JWKS_URI: `http://127.0.0.1:${jwks.port}/jwks`, VGI_DEBUG: "1" });
    const proc = Bun.spawn(["bun", "run", FIXTURE], { cwd: REPO, env, stdout: "pipe", stderr: "pipe" });
    const stderrText = new Response(proc.stderr).text();
    const reader = proc.stdout.getReader();
    let stdout = "";
    const used: string[] = [];
    try {
      while (!stdout.includes("\n")) {
        const { value, done } = await reader.read();
        if (done) throw new Error(`fixture exited: ${stdout}\n${await stderrText}`);
        stdout += new TextDecoder().decode(value);
      }
      const port = /PORT:(\d+)/.exec(stdout)?.[1];
      expect(port).toBeDefined();
      const url = `http://127.0.0.1:${port}`;

      expect((await fetch(`${url}/health`, { method: "HEAD" })).status).toBe(200);
      expect401(await fetch(`${url}/health`), "missing_credential");

      const good = await pass();
      used.push(good);
      const result = await callPassClaims(url, good);
      expect(result).toMatchObject({ principal: "user_01M7KNJYC18T", domain: "orchard" });
      expect(result.claims.email_verified).toBe(true);
      expect(Number.isInteger(result.claims.exp)).toBe(true);

      for (const bad of [
        tamper(await pass(), { sub: "mallory" }),
        await pass({ aud: "https://other.test" }),
        await pass({ typ: "JWT" }),
      ]) {
        used.push(bad);
        const res = await fetch(`${url}/vgi.v3/catalog_attach`, {
          method: "POST",
          headers: { Authorization: `Bearer ${bad}` },
          body: new Uint8Array(0),
        });
        expect401(res);
      }
    } finally {
      proc.kill();
      await proc.exited;
      jwks.stop(true);
    }
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      stdout += new TextDecoder().decode(value);
    }
    const text = stdout + (await stderrText);
    expect(text).toContain("Pass rejected");
    for (const token of used) {
      expect(text).not.toContain(token);
      expect(text).not.toContain(token.split(".")[2]!);
    }
    expect(text).not.toContain("Bearer ");
  }, 60_000);
});
