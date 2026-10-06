// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Example worker entry point.
// Registers ALL example functions and the catalog, matching the Python vgi-example-worker.
//
// Multi-catalog: composes the main `example` catalog with the
// `projection_repro` reproducer fixture so a single LOCATION can serve
// both. Mirrors vgi-python's MetaWorker.

import {
  Worker,
  ReadOnlyCatalogInterface,
  CompositeCatalogInterface,
  FunctionRegistry,
} from "../src/index.js";
import { allFunctions, catalog, createExampleCatalog } from "./common.js";
import { projectionReproCatalog, projectionReproFunctions } from "./projection_repro.js";
import { accumulateFunctions, createAccumulateCatalog } from "./accumulate.js";
import { narrowBindCatalog, narrowBindFunctions } from "./narrow_bind.js";
import { twinACatalog, twinBCatalog, twinCatalogFunctions } from "./twin_catalogs.js";
import { buildSecondaryProtocol } from "@query-farm/vgi-rpc/conformance";
import { createCatalogContentsCatalogs } from "./catalog_contents.js";

// Build registry up front so all functions across catalogs are routable.
const registry = new FunctionRegistry();
for (const f of [
  ...allFunctions,
  ...projectionReproFunctions,
  ...accumulateFunctions,
  ...narrowBindFunctions,
  ...twinCatalogFunctions,
])
  registry.register(f);

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

const composite = new CompositeCatalogInterface([
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

const worker = new Worker({
  functions: [
    ...allFunctions,
    ...projectionReproFunctions,
    ...accumulateFunctions,
    ...narrowBindFunctions,
    ...twinCatalogFunctions,
  ],
  catalogInterface: composite,
  // Same instance the catalogs above indexed into, so schema-qualified and
  // catalog-qualified resolution both work at dispatch.
  registry,
  // The cross-SDK fixture protocol, hosted beside vgi.v2 on every transport
  // through the public hook -- what vgi-rpc's hosted-protocols conformance
  // group checks (`vgi-rpc-test-hosted --expect vgi.v2,conformance.Secondary.v1`).
  hostedProtocols: () => [buildSecondaryProtocol()],
});

worker.run();
