// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// HTTP variant of the versioned-tables fixture worker.
// Prints PORT:<n> to stdout for test discovery.

import { createHttpHandler, unpackStateToken } from "@query-farm/vgi-rpc";
import { arrowStateSerializer } from "../src/protocol/state-serializer.js";
import { FunctionRegistry } from "../src/functions/registry.js";
import { buildVgiProtocol, VGI_PROTOCOL_NAME } from "../src/protocol/dispatch.js";
import { VersionedTablesCatalog, CATALOG_NAME, versionedTablesFunctions } from "./versioned-tables-common.js";

const registry = new FunctionRegistry();
for (const fn of versionedTablesFunctions) registry.register(fn);

const signingKey = crypto.getRandomValues(new Uint8Array(32));
const tokenTtl = 3600;

const protocol = buildVgiProtocol({
  registry,
  catalogInterface: new VersionedTablesCatalog(),
  catalogName: CATALOG_NAME,
  recoverExchangeState: (opaqueData: Uint8Array) => {
    const tokenString = new TextDecoder().decode(opaqueData);
    // The token scope's `protocol` is required since vgi-rpc 0.24.0 -- it is
    // bound into the token's AEAD associated data, so a cursor minted under
    // one hosted protocol cannot be opened under another. These workers serve
    // anonymously, so the identity half of the scope stays empty.
    const unpacked = unpackStateToken(tokenString, signingKey, tokenTtl, {
      protocol: VGI_PROTOCOL_NAME,
      principal: undefined,
    });
    return arrowStateSerializer.deserialize(unpacked.stateBytes);
  },
});

const handler = createHttpHandler(protocol, {
  prefix: "",
  serverId: "vgi-example-versioned-tables-http",
  signingKey,
  tokenTtl,
  stateSerializer: arrowStateSerializer,
});

const server = Bun.serve({ port: 0, fetch: handler });
console.log(`PORT:${server.port}`);
