// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Catalog handler orchestrator: registers all catalog_* RPC methods.

import { Protocol } from "@query-farm/vgi-rpc";
import type { CatalogInterface } from "../../../catalog/interface.js";
import { makeGetCatalog } from "./shared.js";
import { registerCatalogAdminMethods } from "./admin.js";
import { registerCatalogTableMethods } from "./table.js";
import { registerCatalogViewMethods } from "./view.js";
import { registerCatalogMacroMethods } from "./macro.js";
import { registerCatalogIndexMethods } from "./index_methods.js";

export function registerCatalogMethods(
  protocol: Protocol,
  catalog: CatalogInterface | undefined,
  _catalogName: string | undefined,
  signingKey?: Uint8Array,
): void {
  const getCatalog = makeGetCatalog(catalog);
  registerCatalogAdminMethods(protocol, getCatalog, signingKey);
  registerCatalogTableMethods(protocol, getCatalog, signingKey);
  registerCatalogViewMethods(protocol, getCatalog, signingKey);
  registerCatalogMacroMethods(protocol, getCatalog, signingKey);
  registerCatalogIndexMethods(protocol, getCatalog, signingKey);
}
