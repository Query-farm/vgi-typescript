// Example HTTP worker entry point.
// Serves the same functions as worker.ts over HTTP transport.
// Prints PORT:<n> to stdout for test discovery.

import { createHttpHandler } from "vgi-rpc";
import { FunctionRegistry } from "../src/functions/registry.js";
import { buildVgiProtocol } from "../src/protocol/dispatch.js";
import { ReadOnlyCatalogInterface } from "../src/catalog/read-only.js";
import { allFunctions, catalog } from "./common.js";

const registry = new FunctionRegistry();
for (const func of allFunctions) {
  registry.register(func);
}

const catalogInterface = new ReadOnlyCatalogInterface(catalog, registry);

const protocol = buildVgiProtocol({
  registry,
  catalogInterface,
});

const handler = createHttpHandler(protocol, {
  prefix: "/vgi",
  serverId: "vgi-example-http",
});

const server = Bun.serve({ port: 0, fetch: handler });
console.log(`PORT:${server.port}`);
