// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Catalog handler orchestrator: every catalog_* `vgi.v2` handler.

import type { CatalogInterface } from "../../../catalog/interface.js";
import type { VgiService } from "../../../generated/vgi-service.js";
import { makeGetCatalog } from "./shared.js";
import { catalogAdminHandlers } from "./admin.js";
import { catalogTableHandlers } from "./table.js";
import { catalogViewHandlers } from "./view.js";
import { catalogMacroHandlers } from "./macro.js";
import { catalogIndexHandlers } from "./index_methods.js";

/** The catalog_* `vgi.v2` handlers, over *catalog* (or `NoCatalogError` without one). */
export function catalogHandlers(
  catalog: CatalogInterface | undefined,
  signingKey?: Uint8Array,
): Partial<VgiService> {
  const getCatalog = makeGetCatalog(catalog);
  return {
    ...catalogAdminHandlers(getCatalog, signingKey),
    ...catalogTableHandlers(getCatalog, signingKey),
    ...catalogViewHandlers(getCatalog, signingKey),
    ...catalogMacroHandlers(getCatalog, signingKey),
    ...catalogIndexHandlers(getCatalog, signingKey),
  };
}
