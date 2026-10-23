// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Catalog macro handlers: macro_get/create/drop, schema_contents_macros.

import type { VgiService } from "../../../generated/vgi-service.js";
import { encodeMacroInfo } from "../../../generated/vgi-client.js";
import {
  CatalogMacroGetResultSchema,
  MacroCreateRequestSchema,
  CatalogSchemaContentsMacrosResultSchema,
} from "../../../generated/vgi-protocol-schemas.js";
import type { MacroType } from "../../../catalog/interface.js";
import { toUint8Array } from "../../../util/bytes.js";
import { decodeDictValue } from "../../../util/arrow/index.js";
import { decodeASD, encodeASD } from "../../../codec/asd.js";
import type { MacroCreateRequest } from "../../../generated/vgi-client.js";
import {
  wrapResult,
} from "../shared.js";
import {
  type GetCatalog,
  catalogHandler,
} from "./shared.js";

export function catalogMacroHandlers(getCatalog: GetCatalog, signingKey?: Uint8Array): Partial<VgiService> {
  // catalog_macro_get
  const catalogMacroGet = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const info = await cat.macroGet(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return wrapResult({
      items: info ? [encodeMacroInfo(info)] : [],
    }, CatalogMacroGetResultSchema);
  });

  // catalog_macro_create
  const catalogMacroCreate = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    // catalogHandler already flattened the wrapped request.
    const innerParams = decodeASD<MacroCreateRequest>(MacroCreateRequestSchema, encodeASD(MacroCreateRequestSchema, params));
    if (!innerParams.overloads?.length) throw new Error("macro overload family must be nonempty");
    await cat.macroCreate(
      toUint8Array(innerParams.attach_opaque_data),
      innerParams.schema_path,
      innerParams.name,
      innerParams.macro_type as MacroType,
      innerParams.overloads,
      innerParams.on_conflict,
      innerParams.transaction_opaque_data ? toUint8Array(innerParams.transaction_opaque_data) : undefined
    );
    return {};
  });

  // catalog_macro_drop
  const catalogMacroDrop = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    await cat.macroDrop(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.ignore_not_found,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });

  // catalog_schema_contents_macros
  const catalogSchemaContentsMacros = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const macros = await cat.schemaContentsMacros(
      toUint8Array(params.attach_opaque_data),
      params.path,
      decodeDictValue(params.type),
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return wrapResult({
      items: macros.map((m) => encodeMacroInfo(m)),
    }, CatalogSchemaContentsMacrosResultSchema);
  });

  return { catalogMacroGet, catalogMacroCreate, catalogMacroDrop, catalogSchemaContentsMacros };
}
