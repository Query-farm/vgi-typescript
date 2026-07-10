// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Bun HTTP entrypoint for a VGI worker: `import { serveVgiWorker } from "@query-farm/vgi/serve"`.
//
// A worker's HTTP server is the same nine moving parts every time (protocol,
// state-token key, TTL, CORS, landing surface, port, Bun.serve, startup log).
// This collapses them to one call so a worker repo's serve script is its
// registry + catalog and nothing else:
//
//   serveVgiWorker({
//     name: "ishares",
//     doc: "iShares (BlackRock) US fund data.",
//     version: "0.1.0",
//     repositoryUrl: "https://github.com/Query-farm/vgi-etf-ishares",
//     registry,
//     catalogInterface,
//   });
//
// Mounted at the ROOT prefix, so DuckDB attaches the bare origin:
//   ATTACH 'ishares' AS ishares (TYPE vgi, LOCATION 'http://localhost:8787');
//
// Bun-only — it calls Bun.serve. Cloudflare Workers use `vgi/worker-cf`'s
// createVgiFetch instead; both share the handler factory in http/fetch.ts.

import type { CatalogInterface } from "./catalog/interface.js";
import type { FunctionRegistry } from "./functions/registry.js";
import { createLandingDescribe } from "./http/describe-json.js";
import { createVgiFetch } from "./http/fetch.js";

/** Environment variables `serveVgiWorker` reads. Injectable for testing. */
export interface ServeEnv {
  PORT?: string;
  VGI_SIGNING_KEY?: string;
  VGI_TOKEN_TTL?: string;
  CORS_ORIGINS?: string;
}

export const DEFAULT_PORT = 8787;
export const DEFAULT_TOKEN_TTL = 3600;
/** Bytes of HMAC key `createHttpHandler` expects; 64 hex characters. */
export const SIGNING_KEY_BYTES = 32;

export interface ServeVgiWorkerOptions {
  /** Short worker name shown on the landing page, e.g. "ishares". */
  name: string;
  /** One-line description of what the worker serves. */
  doc: string;
  /** Worker version string shown on the landing page. */
  version: string;
  /** Functions the worker serves, already registered. */
  registry: FunctionRegistry;
  /** Catalog DuckDB attaches to. */
  catalogInterface: CatalogInterface;

  /** Public source-repository URL, linked from the landing page. */
  repositoryUrl?: string;
  /** Overrides `$PORT` (default 8787). Pass 0 to bind an ephemeral port. */
  port?: number;
  /** Overrides `$VGI_SIGNING_KEY`. 32 bytes. */
  signingKey?: Uint8Array;
  /** Overrides `$VGI_TOKEN_TTL` (default 3600 seconds). */
  tokenTtl?: number;
  /** Overrides `$CORS_ORIGINS` (default "*"). Pass `null` to disable CORS. */
  corsOrigins?: string | null;
  /** Server ID for state-token attribution (default: `name`). */
  serverId?: string;
  /** Mount point for the VGI endpoints (default "", the origin root). */
  prefix?: string;
  /** Suppress the startup banner on stdout. */
  quiet?: boolean;
  /** Environment source (default `process.env`). */
  env?: ServeEnv;
}

/** The slice of `Bun.serve`'s return value this module uses. */
export interface VgiHttpServer {
  readonly port: number;
  readonly url: URL;
  stop(closeActiveConnections?: boolean): void;
}

interface BunLike {
  serve(options: {
    port: number;
    fetch: (req: Request) => Promise<Response>;
  }): VgiHttpServer;
}

/**
 * Decode a 32-byte signing key from its hex representation.
 *
 * Throws on anything that is not exactly 64 hex characters. A silent truncate
 * or zero-pad here would produce a worker that starts happily with a key far
 * weaker than the operator believed they supplied.
 */
export function parseSigningKeyHex(hex: string): Uint8Array {
  const cleaned = hex.trim();
  const expected = SIGNING_KEY_BYTES * 2;
  if (cleaned.length !== expected || !/^[0-9a-fA-F]+$/.test(cleaned)) {
    throw new Error(
      `VGI_SIGNING_KEY must be exactly ${expected} hex characters ` +
        `(${SIGNING_KEY_BYTES} bytes); got ${cleaned.length}. ` +
        `Generate one with: openssl rand -hex ${SIGNING_KEY_BYTES}`,
    );
  }
  const key = new Uint8Array(SIGNING_KEY_BYTES);
  for (let i = 0; i < SIGNING_KEY_BYTES; i++) {
    key[i] = Number.parseInt(cleaned.slice(i * 2, i * 2 + 2), 16);
  }
  return key;
}

/**
 * Resolve the state-token signing key: explicit option, then `$VGI_SIGNING_KEY`,
 * then a fresh random key.
 *
 * The random fallback is safe but ephemeral, and that tradeoff is why it warns:
 * state tokens minted under it stop validating after a restart, and never
 * validate across a second instance behind a load balancer. It exists so
 * `bun run scripts/serve.ts` works with no setup, not so deployments can skip
 * the key.
 */
export function resolveSigningKey(
  explicit: Uint8Array | undefined,
  env: ServeEnv,
  warn: (msg: string) => void = console.error,
): Uint8Array {
  if (explicit) {
    if (explicit.length !== SIGNING_KEY_BYTES) {
      throw new Error(
        `signingKey must be ${SIGNING_KEY_BYTES} bytes; got ${explicit.length}.`,
      );
    }
    return explicit;
  }
  const fromEnv = env.VGI_SIGNING_KEY?.trim();
  if (fromEnv) return parseSigningKeyHex(fromEnv);

  warn(
    "WARNING: VGI_SIGNING_KEY is not set; generated a random state-token key.\n" +
      "         State tokens will not survive a restart, and will not validate\n" +
      "         across multiple instances behind a load balancer.\n" +
      `         Set it for any real deployment: openssl rand -hex ${SIGNING_KEY_BYTES}`,
  );
  return crypto.getRandomValues(new Uint8Array(SIGNING_KEY_BYTES));
}

function resolvePort(explicit: number | undefined, env: ServeEnv): number {
  if (explicit !== undefined) return explicit;
  const raw = env.PORT?.trim();
  if (!raw) return DEFAULT_PORT;
  const port = Number(raw);
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error(`PORT must be an integer between 0 and 65535; got ${JSON.stringify(raw)}.`);
  }
  return port;
}

function resolveTokenTtl(explicit: number | undefined, env: ServeEnv): number {
  if (explicit !== undefined) return explicit;
  const raw = env.VGI_TOKEN_TTL?.trim();
  if (!raw) return DEFAULT_TOKEN_TTL;
  const ttl = Number(raw);
  if (!Number.isInteger(ttl) || ttl < 0) {
    throw new Error(`VGI_TOKEN_TTL must be a non-negative integer; got ${JSON.stringify(raw)}.`);
  }
  return ttl;
}

/**
 * Build the fetch handler a VGI worker serves over HTTP, with the standardized
 * landing surface mounted at `prefix`.
 *
 * Exposed separately from {@link serveVgiWorker} so a worker can put the VGI
 * routes inside its own Bun/Hono/Elysia server, or drive them from a test
 * without binding a port.
 */
export function createVgiWorkerFetch(
  opts: ServeVgiWorkerOptions,
): (req: Request) => Promise<Response> {
  const env = opts.env ?? (process.env as ServeEnv);
  const signingKey = resolveSigningKey(opts.signingKey, env);
  const tokenTtl = resolveTokenTtl(opts.tokenTtl, env);
  // `null` disables CORS; `undefined` falls through to the env var, then "*".
  const corsOrigins =
    opts.corsOrigins === null ? undefined : (opts.corsOrigins ?? env.CORS_ORIGINS ?? "*");

  return createVgiFetch({
    protocol: { registry: opts.registry, catalogInterface: opts.catalogInterface },
    signingKey,
    tokenTtl,
    prefix: opts.prefix ?? "",
    serverId: opts.serverId ?? opts.name,
    corsOrigins,
    repositoryUrl: opts.repositoryUrl,
    landingDescribe: createLandingDescribe(opts.catalogInterface, {
      name: opts.name,
      doc: opts.doc,
      version: opts.version,
    }),
  });
}

/**
 * Serve a VGI worker over HTTP on Bun and return the running server.
 *
 * Reads `PORT`, `VGI_SIGNING_KEY`, `VGI_TOKEN_TTL`, and `CORS_ORIGINS` from the
 * environment; every one has an explicit option that takes precedence.
 */
export function serveVgiWorker(opts: ServeVgiWorkerOptions): VgiHttpServer {
  const runtime = (globalThis as { Bun?: BunLike }).Bun;
  if (!runtime) {
    throw new Error(
      '"@query-farm/vgi/serve" requires the Bun runtime (it calls Bun.serve). ' +
        'On Cloudflare Workers use createVgiFetch from "@query-farm/vgi/worker-cf"; ' +
        "on other runtimes use createVgiWorkerFetch and bind the port yourself.",
    );
  }

  const env = opts.env ?? (process.env as ServeEnv);
  const port = resolvePort(opts.port, env);
  const fetch = createVgiWorkerFetch(opts);
  const server = runtime.serve({ port, fetch });

  if (!opts.quiet) {
    const base = `http://localhost:${server.port}`;
    console.log(`${opts.name} VGI HTTP worker listening on ${base}`);
    console.log(`  landing page   ${base}/`);
    console.log(`  describe.json  ${base}/describe.json`);
    console.log(`  health         ${base}/health`);
    console.log(`  attach         ATTACH '${opts.name}' AS ${opts.name} (TYPE vgi, LOCATION '${base}');`);
  }
  return server;
}
