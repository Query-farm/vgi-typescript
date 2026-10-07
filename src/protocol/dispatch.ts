// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Build VGI protocol from worker implementation.
// The method table, its schemas and the UNIMPLEMENTED defaults are generated
// (src/generated/vgi-service.ts, from vgi-python's VgiProtocol); this module
// composes the handler groups in handlers/ over those defaults.

import type { Protocol } from "@query-farm/vgi-rpc";

import type { FunctionRegistry } from "../functions/registry.js";
import type { CatalogInterface } from "../catalog/interface.js";
import {
  createVgiProtocol,
  UNIMPLEMENTED_VGI_SERVICE,
  VGI_PROTOCOL_NAME,
  type VgiService,
} from "../generated/vgi-service.js";
import { functionHandlers } from "./handlers/function.js";
import { aggregateHandlers } from "./handlers/aggregate.js";
import { tableBufferingHandlers } from "./handlers/table-buffering.js";
import { catalogHandlers } from "./handlers/catalog/index.js";

export interface ProtocolConfig {
  registry: FunctionRegistry;
  catalogInterface?: CatalogInterface;
  catalogName?: string;
  /**
   * AEAD signing key for sealing catalog opaque-data envelopes. Pass the same
   * 32-byte key used for HTTP state tokens. When omitted (subprocess / unix
   * transports) attach_opaque_data / transaction_opaque_data pass through
   * unsealed — OS process ownership already enforces identity there.
   */
  signingKey?: Uint8Array;
}

/**
 * Wire name of the VGI protocol: the `vgi_rpc.protocol` routing key and the
 * `{protocol}` HTTP path segment.
 *
 * The name carries the major version, so an incompatible major is a
 * *different* protocol and therefore a 404 — an answer every proxy, WAF and
 * load balancer understands without an Arrow parser, and one that lets
 * `vgi.v2` and a future `vgi.v3` be served side by side while clients migrate.
 * That matters for this consumer specifically: the DuckDB extension ships to
 * users and cannot be flag-dayed.
 *
 * Declared rather than derived, and now generated with the method table
 * (src/generated/vgi-service.ts). Until the transports made the routing key
 * required, each implementation's wire name defaulted to whatever its local
 * type was called, which left the six ports disagreeing four ways — Python
 * `VgiProtocol`, Java and C# `VgiService`, Go the framework default `Service`,
 * this port `vgi` — so no client could address them all. The canonical name is
 * decided in vgi-python and emitted by the DuckDB extension; this is that same
 * string, not an independent choice.
 *
 * Since `@query-farm/vgi-rpc` 0.24.0 the name is also bound into the AEAD
 * associated data of every state and call token (`TokenScope.protocol`), so a
 * caller that opens a token minted by this protocol has to name it. Exported
 * so the HTTP entry points build that scope from the same constant the
 * protocol is registered under rather than a second copy of the literal.
 */
export { VGI_PROTOCOL_NAME };

/**
 * The `vgi.v2` protocol hash `vgi_rpc.Reflection.v1` must report for this
 * worker: the reference's (vgi-python 0.43.0, 72 methods). Every SDK hosts
 * every `vgi.v2` method with the reference's schemas, so the hash is one value
 * across the ports; a method this SDK does not implement is still registered
 * with the generated UNIMPLEMENTED default (src/generated/vgi-service.ts). The
 * hash covers method names, types and the params/result/header schemas only
 * (WIRE_PROTOCOL.md §14).
 *
 * It changes only with vgi.v2's protocol version (currently 2.1.0). Asserted
 * by src/protocol/__tests__/vgi-v2-hash.test.ts; if that test fails, the
 * surface drifted from the reference -- fix the schemas, do not bump this.
 */
export const VGI_V2_PROTOCOL_HASH = "774cb80090d71ea76d09aa311b9cda4ca4c33c3bf72c43242eb6dc87b6f79ce5";

/**
 * This worker's `vgi.v2` implementation: the generated UNIMPLEMENTED default
 * for every method, overridden by the handler groups for what this SDK serves.
 */
export function buildVgiService(config: ProtocolConfig): VgiService {
  return {
    ...UNIMPLEMENTED_VGI_SERVICE,
    ...functionHandlers({
      registry: config.registry,
      signingKey: config.signingKey,
      catalogInterface: config.catalogInterface,
    }),
    ...aggregateHandlers(config.registry),
    ...tableBufferingHandlers(config.registry, config.signingKey),
    ...catalogHandlers(config.catalogInterface, config.signingKey),
  };
}

/** The `vgi.v2` protocol, registered from the generated table with {@link buildVgiService}. */
export function buildVgiProtocol(config: ProtocolConfig): Protocol {
  return createVgiProtocol(buildVgiService(config));
}
