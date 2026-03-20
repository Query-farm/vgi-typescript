// Example worker entry point.
// Registers ALL example functions and the catalog, matching the Python vgi-example-worker.

import { Worker } from "../src/index.js";
import { allFunctions, catalog, createExampleCatalog } from "./common.js";

const worker = new Worker({
  functions: allFunctions,
  catalog,
  catalogInterfaceFactory: createExampleCatalog,
});

worker.run();
