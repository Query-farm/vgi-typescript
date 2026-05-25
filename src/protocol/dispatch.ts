// Build VGI protocol from worker implementation.
// Orchestrator that registers function, aggregate, and catalog handlers
// against vgi-rpc Protocol. The handler implementations live in handlers/.

import { Protocol } from "vgi-rpc";

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

export interface ProtocolConfig {
  registry: FunctionRegistry;
  catalogInterface?: CatalogInterface;
  catalogName?: string;
  /**
   * Recover accumulated exchange state from FINALIZE init_opaque_data.
   * For HTTP transport, this unpacks the state token that the C++ extension
   * passes from the last INPUT exchange to the FINALIZE init request.
   * Returns the deserialized VGI dispatch state object (with userState field).
   */
  recoverExchangeState?: (opaqueData: Uint8Array) => any;
  /**
   * AEAD signing key for sealing catalog opaque-data envelopes. Pass the same
   * 32-byte key used for HTTP state tokens. When omitted (subprocess / unix
   * transports) attach_opaque_data / transaction_opaque_data pass through
   * unsealed — OS process ownership already enforces identity there.
   */
  signingKey?: Uint8Array;
}

export function buildVgiProtocol(config: ProtocolConfig): Protocol {
  const protocol = new Protocol("vgi");

  registerFunctionMethods(protocol, {
    registry: config.registry,
    recoverExchangeState: config.recoverExchangeState,
    signingKey: config.signingKey,
  });
  registerAggregateMethods(protocol, config.registry);
  registerTableBufferingMethods(protocol, config.registry);
  registerCatalogMethods(protocol, config.catalogInterface, config.catalogName, config.signingKey);

  return protocol;
}
