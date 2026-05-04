// Catalog handler orchestrator: registers all catalog_* RPC methods.

import { Protocol } from "vgi-rpc";
import type { CatalogInterface } from "../../../catalog/interface.js";
import { makeGetCatalog } from "./shared.js";
import { registerCatalogAdminMethods } from "./admin.js";
import { registerCatalogTableMethods } from "./table.js";
import { registerCatalogViewMethods } from "./view.js";
import { registerCatalogMacroMethods } from "./macro.js";

export function registerCatalogMethods(
  protocol: Protocol,
  catalog: CatalogInterface | undefined,
  _catalogName: string | undefined,
): void {
  const getCatalog = makeGetCatalog(catalog);
  registerCatalogAdminMethods(protocol, getCatalog);
  registerCatalogTableMethods(protocol, getCatalog);
  registerCatalogViewMethods(protocol, getCatalog);
  registerCatalogMacroMethods(protocol, getCatalog);
}
