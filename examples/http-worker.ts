// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Example HTTP worker entry point.
// Serves the same functions as worker.ts over HTTP transport.
// Prints PORT:<n> to stdout for test discovery.

import { serveVgiWorker } from "../src/serve-entry.js";
import { FunctionRegistry } from "../src/functions/registry.js";
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

// The `signingKey` this example used to pass to createHttpHandler was never read
// — the handler's option is `tokenKey`, so it minted tokens under a random key
// while the protocol tried to recover them under this one. serveVgiWorker feeds
// both seams from a single key. Left unset here: the helper generates a random
// one and warns, which is exactly right for an ephemeral test fixture.
const server = serveVgiWorker({
  name: "VgiExampleWorker",
  doc: "Example VGI TypeScript worker.",
  version: "0.12.0",
  registry,
  catalogInterface,
  // The integration harness attaches to http://localhost:$PORT/vgi.
  prefix: "/vgi",
  serverId: "vgi-example-http",
  port: 0,
  quiet: true,
});

// The Makefile's test-http target reads this line off stdout to discover the port.
console.log(`PORT:${server.port}`);
