// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// serveVgiWorker is the standardized HTTP entrypoint every TypeScript worker
// repo mounts instead of hand-rolling its own scripts/serve.ts. Two things it
// owns are worth pinning down:
//
//   1. The signing-key policy. A silently-truncated or silently-weak key is
//      worse than a loud failure, and the no-key fallback must warn rather
//      than quietly mint tokens nobody can validate after a restart.
//   2. That the assembled server actually speaks VGI at the root prefix, with
//      the landing surface attached — i.e. the same thing DuckDB ATTACHes to.

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { httpConnect } from "@query-farm/vgi-rpc";
import { int64 } from "../arrow/index.js";
import { defineScalarFunction } from "../functions/scalar.js";
import { FunctionRegistry } from "../functions/registry.js";
import { ReadOnlyCatalogInterface } from "../catalog/read-only.js";
import { VgiClient } from "../index.core.js";
import {
  parseSigningKeyHex,
  resolveSigningKey,
  serveVgiWorker,
  SIGNING_KEY_BYTES,
  type ServeEnv,
  type VgiHttpServer,
} from "../serve-entry.js";

const HEX_64 = "a".repeat(64);

describe("parseSigningKeyHex", () => {
  test("decodes 64 hex characters to 32 bytes", () => {
    const key = parseSigningKeyHex("00ff".repeat(16));
    expect(key.length).toBe(SIGNING_KEY_BYTES);
    expect(key[0]).toBe(0x00);
    expect(key[1]).toBe(0xff);
  });

  test("tolerates surrounding whitespace", () => {
    expect(parseSigningKeyHex(`  ${HEX_64}\n`).length).toBe(SIGNING_KEY_BYTES);
  });

  // The bug this replaces: Buffer.from("abcd", "hex").subarray(0, 32) yields a
  // 2-byte key and no complaint whatsoever.
  test("rejects a short key rather than silently accepting it", () => {
    expect(() => parseSigningKeyHex("abcd")).toThrow(/exactly 64 hex characters/);
  });

  test("rejects an over-long key rather than truncating it", () => {
    expect(() => parseSigningKeyHex("a".repeat(66))).toThrow(/exactly 64 hex characters/);
  });

  test("rejects non-hex characters", () => {
    expect(() => parseSigningKeyHex("z".repeat(64))).toThrow(/exactly 64 hex characters/);
  });
});

describe("resolveSigningKey", () => {
  const noWarn = () => {};

  test("prefers the explicit key over the environment", () => {
    const explicit = new Uint8Array(SIGNING_KEY_BYTES).fill(3);
    const key = resolveSigningKey(explicit, { VGI_SIGNING_KEY: HEX_64 }, noWarn);
    expect(key).toBe(explicit);
  });

  test("rejects an explicit key of the wrong length", () => {
    expect(() => resolveSigningKey(new Uint8Array(8), {}, noWarn)).toThrow(/must be 32 bytes/);
  });

  test("reads VGI_SIGNING_KEY when no explicit key is given", () => {
    const key = resolveSigningKey(undefined, { VGI_SIGNING_KEY: `  ${HEX_64}  ` }, noWarn);
    expect(key.length).toBe(SIGNING_KEY_BYTES);
    expect(key[0]).toBe(0xaa);
  });

  test("propagates a malformed VGI_SIGNING_KEY instead of falling back to random", () => {
    expect(() => resolveSigningKey(undefined, { VGI_SIGNING_KEY: "nope" }, noWarn)).toThrow();
  });

  test("falls back to a random key and warns loudly when unset", () => {
    const warnings: string[] = [];
    const key = resolveSigningKey(undefined, {}, (m) => warnings.push(m));
    expect(key.length).toBe(SIGNING_KEY_BYTES);
    expect(warnings.length).toBe(1);
    expect(warnings[0]).toContain("VGI_SIGNING_KEY is not set");
    expect(warnings[0]).toContain("will not survive a restart");
  });

  test("an empty VGI_SIGNING_KEY is treated as unset, not as a zero-length key", () => {
    const warnings: string[] = [];
    const key = resolveSigningKey(undefined, { VGI_SIGNING_KEY: "   " }, (m) => warnings.push(m));
    expect(key.length).toBe(SIGNING_KEY_BYTES);
    expect(warnings.length).toBe(1);
  });

  test("successive random fallbacks differ", () => {
    const a = resolveSigningKey(undefined, {}, noWarn);
    const b = resolveSigningKey(undefined, {}, noWarn);
    expect(Buffer.from(a).equals(Buffer.from(b))).toBe(false);
  });
});

// --- End-to-end: a served worker DuckDB could ATTACH ------------------------

const dbl = defineScalarFunction({
  name: "dbl",
  description: "Doubles its input",
  params: { x: int64() },
  outputType: () => int64(),
  compute: () => [],
});

function buildParts() {
  const registry = new FunctionRegistry();
  registry.register(dbl);
  const catalogInterface = new ReadOnlyCatalogInterface(
    { name: "demo", schemas: [{ name: "main", functions: [dbl] }] },
    registry,
  );
  return { registry, catalogInterface };
}

function serve(env: ServeEnv = {}): VgiHttpServer {
  const { registry, catalogInterface } = buildParts();
  return serveVgiWorker({
    name: "demo",
    doc: "Serve-entry test worker.",
    version: "0.0.1",
    repositoryUrl: "https://github.com/Query-farm/vgi-typescript",
    registry,
    catalogInterface,
    port: 0,
    signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(9),
    quiet: true,
    env,
  });
}

describe("serveVgiWorker", () => {
  let server: VgiHttpServer;
  let baseUrl: string;

  beforeAll(() => {
    server = serve();
    baseUrl = `http://localhost:${server.port}`;
  });

  afterAll(() => {
    server?.stop(true);
  });

  test("binds the requested ephemeral port", () => {
    expect(server.port).toBeGreaterThan(0);
  });

  test("serves /health", async () => {
    const res = await fetch(`${baseUrl}/health`);
    expect(res.status).toBe(200);
  });

  test("serves the landing describe.json contract at the root prefix", async () => {
    const res = await fetch(`${baseUrl}/describe.json`);
    expect(res.status).toBe(200);
    const body = (await res.json()) as { worker?: { name?: string; version?: string } };
    expect(body.worker?.name).toBe("demo");
    expect(body.worker?.version).toBe("0.0.1");
  });

  test("speaks VGI at the origin root, so LOCATION is the bare URL", async () => {
    const client = new VgiClient(httpConnect(baseUrl, { prefix: "" }));
    expect(await client.catalogs()).toContain("demo");
  });

  test("exposes the registered function over HTTP", async () => {
    const client = new VgiClient(httpConnect(baseUrl, { prefix: "" }));
    const attach = await client.catalogAttach("demo");
    const fns = await client.schemaContentsFunctions(attach.attach_opaque_data, "main", "scalar");
    expect(fns.map((f) => f.name)).toContain("dbl");
  });

  test("answers the CORS preflight the hosted Cupola UI sends", async () => {
    const res = await fetch(`${baseUrl}/__describe__`, {
      method: "OPTIONS",
      headers: { Origin: "https://cupola.query.farm", "Access-Control-Request-Method": "POST" },
    });
    expect(res.status).toBeLessThan(400);
    expect(res.headers.get("access-control-allow-origin")).toBe("*");
  });
});

describe("serveVgiWorker environment handling", () => {
  test("rejects a non-numeric PORT rather than binding something surprising", () => {
    const { registry, catalogInterface } = buildParts();
    expect(() =>
      serveVgiWorker({
        name: "demo",
        doc: "d",
        version: "0",
        registry,
        catalogInterface,
        quiet: true,
        signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(9),
        env: { PORT: "http-please" },
      }),
    ).toThrow(/PORT must be an integer/);
  });

  test("CORS_ORIGINS narrows the allowed origin", async () => {
    const { registry, catalogInterface } = buildParts();
    const server = serveVgiWorker({
      name: "demo",
      doc: "d",
      version: "0",
      registry,
      catalogInterface,
      port: 0,
      quiet: true,
      signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(9),
      env: { CORS_ORIGINS: "https://cupola.query.farm" },
    });
    try {
      const res = await fetch(`http://localhost:${server.port}/health`, {
        headers: { Origin: "https://cupola.query.farm" },
      });
      expect(res.headers.get("access-control-allow-origin")).toBe("https://cupola.query.farm");
    } finally {
      server.stop(true);
    }
  });
});
