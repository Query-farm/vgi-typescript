// Cloudflare Workers entrypoint for the VGI worker.
//
// Imports the same building blocks as the Bun HTTP worker (Protocol, registry,
// catalog interface, state serializer) but exposes the handler as `default.fetch`
// — the shape `wrangler dev` and the Workers runtime expect.
//
// Build picks the flechette Arrow backend automatically because this entry is
// referenced under the `workerd`/`worker`/`browser` conditional export, which
// resolves `#arrow-impl` -> impl-flechette.
//
// Consumers wire their own catalog and function registry; the helper here is
// `createVgiFetch` rather than a hard-coded singleton, so a CF Worker module
// can compose multiple registries the same way the Bun worker does.

import { createHttpHandler, unpackStateToken, type Protocol } from "vgi-rpc";
import { arrowStateSerializer } from "./protocol/state-serializer.js";
import { buildVgiProtocol, type ProtocolConfig } from "./protocol/dispatch.js";

export interface VgiFetchOptions {
  /** Wire-protocol config (registry + catalogInterface). */
  protocol: Omit<ProtocolConfig, "recoverExchangeState">;
  /** HMAC key for state-token signing. Pass a stable, secret 32-byte key
   *  (e.g. derived from a Wrangler secret). Required because Workers don't
   *  preserve in-memory state across requests/instances. */
  signingKey: Uint8Array;
  /** State-token TTL in seconds (default 3600). */
  tokenTtl?: number;
  /** URL path prefix for VGI requests (default "/vgi"). */
  prefix?: string;
  /** Server ID for state-token attribution (default "vgi-cf"). */
  serverId?: string;
}

/**
 * Build a fetch handler suitable for `export default { fetch }` in a CF
 * Worker module. The returned handler is async-safe across Workers'
 * isolate-per-request execution model — all state round-trips through the
 * signed state token created here.
 */
export function createVgiFetch(opts: VgiFetchOptions): (req: Request) => Promise<Response> {
  const tokenTtl = opts.tokenTtl ?? 3600;
  const prefix = opts.prefix ?? "/vgi";
  const serverId = opts.serverId ?? "vgi-cf";

  const protocol: Protocol = buildVgiProtocol({
    ...opts.protocol,
    recoverExchangeState: async (opaqueData: Uint8Array) => {
      const tokenString = new TextDecoder().decode(opaqueData);
      const unpacked = await unpackStateToken(tokenString, opts.signingKey, tokenTtl);
      return arrowStateSerializer.deserialize(unpacked.stateBytes);
    },
  });

  const handler = createHttpHandler(protocol, {
    prefix,
    serverId,
    signingKey: opts.signingKey,
    tokenTtl,
    stateSerializer: arrowStateSerializer,
  });
  return async (req: Request) => handler(req);
}

// Re-export the public types CF Worker authors need to wire a registry +
// catalog. They import from "vgi/worker-cf" rather than from "vgi" directly
// so the build-condition resolution picks the flechette Arrow backend.
export {
  defineScalarFunction,
  defineTableFunction,
  defineAggregate,
  defineTableInOutFunction,
  FunctionRegistry,
  ReadOnlyCatalogInterface,
  CompositeCatalogInterface,
  CatalogInterface,
} from "./index.js";
export type { ProtocolConfig } from "./protocol/dispatch.js";
