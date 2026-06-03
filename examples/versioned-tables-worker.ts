// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Subprocess entry for the versioned-tables fixture worker.
// Registered via bin/vgi-example-versioned-tables-worker.

import { Worker } from "../src/index.js";
import { VersionedTablesCatalog, CATALOG_NAME, versionedTablesFunctions } from "./versioned-tables-common.js";

new Worker({
  catalogInterface: new VersionedTablesCatalog(),
  catalogName: CATALOG_NAME,
  functions: versionedTablesFunctions,
}).run();
