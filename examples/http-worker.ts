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
import { twinACatalog, twinBCatalog, twinCatalogFunctions } from "./twin_catalogs.js";
import { createCatalogContentsCatalogs } from "./catalog_contents.js";
import { optionalTestBearerAuthenticate } from "./optional-bearer.js";
import {
  buildSecondaryProtocol,
  conformanceAuthenticate,
  conformanceMintGrant,
  conformanceResolveToken,
  INTROSPECTOR_PRINCIPAL,
  MAX_AUTH_AGE,
} from "@query-farm/vgi-rpc/conformance";


const registry = new FunctionRegistry();
for (const func of [
  ...allFunctions,
  ...projectionReproFunctions,
  ...accumulateFunctions,
  ...narrowBindFunctions,
  ...twinCatalogFunctions,
]) {
  registry.register(func);
}

const exampleBase = new ReadOnlyCatalogInterface(catalog, registry);
const exampleCatalog = createExampleCatalog(exampleBase);
const projectionRepro = new ReadOnlyCatalogInterface(projectionReproCatalog, registry);
const accumulate = createAccumulateCatalog(registry);
const narrowBind = new ReadOnlyCatalogInterface(narrowBindCatalog, registry);
// Two catalogs whose `main` schemas both declare `test_same_name_catalog` —
// only the attached catalog tells them apart.
const twinA = new ReadOnlyCatalogInterface(twinACatalog, registry);
const twinB = new ReadOnlyCatalogInterface(twinBCatalog, registry);
// The catalogs above are ReadOnlyCatalogInterfaces, so each advertises
// supports_catalog_contents and serves catalog_contents (the whole catalog in
// one RPC). VGI_EXAMPLE_NO_CATALOG_CONTENTS=1 turns that off, keeping DuckDB on
// the per-schema catalog_schema_contents_* RPCs -- for running the
// conformance suite both ways. (The contents_* fixtures below keep their own,
// fixed behaviour: their tests switch modes with SET vgi_catalog_contents.)
if (process.env.VGI_EXAMPLE_NO_CATALOG_CONTENTS) {
  for (const c of [exampleCatalog, projectionRepro, accumulate, narrowBind, twinA, twinB]) {
    c.supportsCatalogContents = false;
  }
}

const catalogInterface = new CompositeCatalogInterface([
  exampleCatalog,
  projectionRepro,
  accumulate,
  narrowBind,
  twinA,
  twinB,
  // contents_probe / _broken / _legacy / _memory / _reval / _hash:
  // catalog_contents fixtures.
  ...createCatalogContentsCatalogs(registry),
]);

// The `signingKey` this example used to pass to createHttpHandler was never read
// — the handler's option is `tokenKey`, so it minted tokens under a random key
// while the protocol tried to recover them under this one. serveVgiWorker feeds
// both seams from a single key. Left unset here: the helper generates a random
// one and warns, which is exactly right for an ephemeral test fixture.
// Identity's guards read an authenticated caller. The conformance fixture names
// its caller in X-Conformance-Principal (TEST ONLY: trivially spoofable); a
// request without that header falls through to the optional-bearer fixture the
// DuckDB suite relies on, so nothing the suite sends changes meaning.
const bearer = optionalTestBearerAuthenticate();
const fixtureAuthenticate = (request: Request) =>
  request.headers.has("X-Conformance-Principal") ? conformanceAuthenticate(request) : bearer(request);

const server = serveVgiWorker({
  name: "VgiExampleWorker",
  doc: "Example VGI TypeScript worker.",
  version: "0.12.0",
  registry,
  catalogInterface,
  // The integration harness attaches to http://localhost:$PORT (root, like the other SDK workers).
  prefix: "",
  serverId: "vgi-example-http",
  port: 0,
  quiet: true,
  // OPTIONAL bearer identity: `vgi-test-alice` -> alice, `vgi-test-bob` -> bob,
  // anything else (including no header) -> anonymous, never a 401. The whole
  // HTTP suite shares this one server, so it has to stay anonymous by default;
  // cache/identity_isolation.test is the test that needs real principals. Same
  // fixture the Python, Go and Rust example workers ship.
  authenticate: fixtureAuthenticate,
  // The cross-SDK fixture protocol, hosted beside vgi.v2 through the public hook.
  hostedProtocols: () => [buildSecondaryProtocol()],
  // vgi_rpc.Identity.v1 under the pinned IDENTITY_CONFORMANCE_FIXTURE.md
  // policy -- both hooks, the one-principal allowlist, the 900 s auth age --
  // so `vgi-rpc-test-hosted --url ... --identity` can assert against it.
  resolveToken: conformanceResolveToken,
  mintGrant: conformanceMintGrant,
  introspectPrincipals: [INTROSPECTOR_PRINCIPAL],
  maxAuthAge: MAX_AUTH_AGE,
});

// The Makefile's test-http target reads this line off stdout to discover the port.
console.log(`PORT:${server.port}`);
