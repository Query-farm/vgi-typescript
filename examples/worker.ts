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

// Build registry up front so all functions across catalogs are routable.
const registry = new FunctionRegistry();
for (const f of [...allFunctions, ...projectionReproFunctions]) registry.register(f);

const exampleBase = new ReadOnlyCatalogInterface(catalog, registry);
const exampleCatalog = createExampleCatalog(exampleBase);
const projectionRepro = new ReadOnlyCatalogInterface(projectionReproCatalog, registry);

const composite = new CompositeCatalogInterface([exampleCatalog, projectionRepro]);

const worker = new Worker({
  functions: [...allFunctions, ...projectionReproFunctions],
  catalogInterface: composite,
});

worker.run();
