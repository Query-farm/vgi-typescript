// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Shared HTTP fetch-handler factory for VGI workers.
//
// Both HTTP entrypoints funnel through here:
//   - `vgi/worker-cf`  -> createVgiFetch, exported as `default.fetch` for workerd
//   - `vgi/serve`      -> serveVgiWorker, which hands the handler to Bun.serve
//
// Centralising the wiring means the state-token key reaches `createHttpHandler`
// as `tokenKey` (the option the handler actually reads) and reaches
// `buildVgiProtocol` as `signingKey`, from one source. Passing different keys to
// those two seams produces a worker that mints tokens it cannot recover.

import { createHttpHandler, unpackStateToken, type Protocol } from "@query-farm/vgi-rpc";
import { arrowStateSerializer } from "../protocol/state-serializer.js";
import { buildVgiProtocol, type ProtocolConfig } from "../protocol/dispatch.js";
import type { LandingDescribeProvider } from "./describe-json.js";

export interface VgiFetchOptions {
  /** Wire-protocol config (registry + catalogInterface). */
  protocol: Omit<ProtocolConfig, "recoverExchangeState">;
  /** HMAC key for state-token signing. Pass a stable, secret 32-byte key
   *  (e.g. derived from a Wrangler secret). Required because Workers don't
   *  preserve in-memory state across requests/instances. */
  signingKey: Uint8Array;
  /** State-token TTL in seconds (default 3600). */
  tokenTtl?: number;
  /** URL path prefix for VGI requests (default "/vgi"). Pass "" to mount at
   *  the root, which is what a dedicated single-worker HTTP server wants. */
  prefix?: string;
  /** Server ID for state-token attribution (default "vgi-cf"). */
  serverId?: string;
  /** CORS allowed origins. When set, CORS headers are added to all responses,
   *  and the preflight `OPTIONS` that browser clients (e.g. the hosted Cupola
   *  UI) send before `__describe__` will succeed. Omit to disable CORS. */
  corsOrigins?: string;
  /** Public source-repository URL, surfaced on the landing page. */
  repositoryUrl?: string;
  /** Enables the standardized VGI landing surface: `GET /` serves the shared
   *  landing.html, `GET /describe.json` the catalog contract, and
   *  `GET /describe/{catalog}/{schema}/{table}.json` lazy per-object columns.
   *  Build one with `createLandingDescribe(catalogInterface, workerInfo)`. */
  landingDescribe?: LandingDescribeProvider;
}

/**
 * Build a fetch handler suitable for `export default { fetch }` in a CF
 * Worker module, or for `Bun.serve({ fetch })`. The returned handler is
 * async-safe across Workers' isolate-per-request execution model — all state
 * round-trips through the signed state token created here.
 */
export function createVgiFetch(opts: VgiFetchOptions): (req: Request) => Promise<Response> {
  const tokenTtl = opts.tokenTtl ?? 3600;
  // `?? "/vgi"` and not `|| "/vgi"`: an explicit "" means "mount at root".
  const prefix = opts.prefix ?? "/vgi";
  const serverId = opts.serverId ?? "vgi-cf";

  const protocol: Protocol = buildVgiProtocol({
    signingKey: opts.signingKey,
    ...opts.protocol,
    recoverExchangeState: async (opaqueData: Uint8Array) => {
      const tokenString = new TextDecoder().decode(opaqueData);
      // principal binding is enforced by the HTTP handler on the request that
      // carried this token; the recovery path itself is not principal-scoped.
      const unpacked = await unpackStateToken(tokenString, opts.signingKey, tokenTtl, undefined);
      return arrowStateSerializer.deserialize(unpacked.stateBytes);
    },
  });

  const handler = createHttpHandler(protocol, {
    prefix,
    serverId,
    tokenKey: opts.signingKey,
    tokenTtl,
    stateSerializer: arrowStateSerializer,
    corsOrigins: opts.corsOrigins,
    repositoryUrl: opts.repositoryUrl,
    landingDescribe: opts.landingDescribe,
  });
  return async (req: Request) => handler(req);
}
