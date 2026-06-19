// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Example HTTP worker entry point.
// Serves the same functions as worker.ts over HTTP transport.
// Prints PORT:<n> to stdout for test discovery.

import { createHttpHandler, unpackStateToken } from "@query-farm/vgi-rpc";
import { arrowStateSerializer } from "../src/protocol/state-serializer.js";
import { FunctionRegistry } from "../src/functions/registry.js";
import { buildVgiProtocol } from "../src/protocol/dispatch.js";
import { ReadOnlyCatalogInterface } from "../src/catalog/read-only.js";
import { CompositeCatalogInterface } from "../src/catalog/composite.js";
import { allFunctions, catalog, createExampleCatalog } from "./common.js";
import { projectionReproCatalog, projectionReproFunctions } from "./projection_repro.js";
import { accumulateFunctions, createAccumulateCatalog } from "./accumulate.js";
import { narrowBindCatalog, narrowBindFunctions } from "./narrow_bind.js";

const registry = new FunctionRegistry();
for (const func of [
  ...allFunctions,
  ...projectionReproFunctions,
  ...accumulateFunctions,
  ...narrowBindFunctions,
]) {
  registry.register(func);
}

const exampleBase = new ReadOnlyCatalogInterface(catalog, registry);
const exampleCatalog = createExampleCatalog(exampleBase);
const projectionRepro = new ReadOnlyCatalogInterface(projectionReproCatalog, registry);
const accumulate = createAccumulateCatalog(registry);
const narrowBind = new ReadOnlyCatalogInterface(narrowBindCatalog, registry);
const catalogInterface = new CompositeCatalogInterface([
  exampleCatalog,
  projectionRepro,
  accumulate,
  narrowBind,
]);

// Shared signing key so buildVgiProtocol can decode state tokens created by createHttpHandler
const signingKey = crypto.getRandomValues(new Uint8Array(32));
const tokenTtl = 3600;

const protocol = buildVgiProtocol({
  registry,
  catalogInterface,
  recoverExchangeState: (opaqueData: Uint8Array) => {
    const tokenString = new TextDecoder().decode(opaqueData);
    const unpacked = unpackStateToken(tokenString, signingKey, tokenTtl);
    return arrowStateSerializer.deserialize(unpacked.stateBytes);
  },
});

const handler = createHttpHandler(protocol, {
  prefix: "/vgi",
  serverId: "vgi-example-http",
  signingKey,
  tokenTtl,
  stateSerializer: arrowStateSerializer,
});

const server = Bun.serve({ port: 0, fetch: handler });
console.log(`PORT:${server.port}`);
