// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Build VGI protocol from worker implementation.
// Orchestrator that registers function, aggregate, and catalog handlers
// against vgi-rpc Protocol. The handler implementations live in handlers/.

import { Protocol } from "@query-farm/vgi-rpc";

// NOTE: We must NOT use vgi-rpc's str/bytes/int/etc. singletons in Schema objects
// because Bun loads apache-arrow as separate module instances for our code vs vgi-rpc's
// compiled dist. Instead, we pre-build Schema objects and pass them directly to Protocol
// methods (toSchema() passes Schema instances through without instanceof checks).
import type { FunctionRegistry } from "../functions/registry.js";
import type { CatalogInterface } from "../catalog/interface.js";
import { registerFunctionMethods } from "./handlers/function.js";
import { registerAggregateMethods } from "./handlers/aggregate.js";
import { registerTableBufferingMethods } from "./handlers/table-buffering.js";
import { registerCatalogMethods } from "./handlers/catalog/index.js";
import { registerUnimplementedMethods } from "./handlers/unimplemented.js";

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
 * Declared rather than derived. Until the transports made the routing key
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
export const VGI_PROTOCOL_NAME = "vgi.v2";

/**
 * The `vgi.v2` protocol hash `vgi_rpc.Reflection.v1` must report for this
 * worker: the reference's (vgi-python 0.43.0, 72 methods). Every SDK hosts
 * every `vgi.v2` method with the reference's schemas, so the hash is one value
 * across the ports; a method this SDK does not implement is still registered
 * (handlers/unimplemented.ts). The hash covers method names, types and the
 * params/result/header schemas only (WIRE_PROTOCOL.md §14).
 *
 * It changes only with vgi.v2's protocol version (currently 2.1.0). Asserted
 * by src/protocol/__tests__/vgi-v2-hash.test.ts; if that test fails, the
 * surface drifted from the reference -- fix the schemas, do not bump this.
 */
export const VGI_V2_PROTOCOL_HASH = "774cb80090d71ea76d09aa311b9cda4ca4c33c3bf72c43242eb6dc87b6f79ce5";

export function buildVgiProtocol(config: ProtocolConfig): Protocol {
  const protocol = new Protocol(VGI_PROTOCOL_NAME, { protocolVersion: "2.1.0" });

  registerFunctionMethods(protocol, {
    registry: config.registry,
    signingKey: config.signingKey,
    catalogInterface: config.catalogInterface,
  });
  registerAggregateMethods(protocol, config.registry);
  registerTableBufferingMethods(protocol, config.registry, config.signingKey);
  registerCatalogMethods(protocol, config.catalogInterface, config.catalogName, config.signingKey);
  registerUnimplementedMethods(protocol);

  return protocol;
}
