// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Catalog macro handlers: macro_get/create/drop, schema_contents_macros.

import { type VgiSchema, schema, type VgiField, field, type VgiDataType, binary, utf8, bool } from "../../../arrow/index.js";
import { Protocol } from "@query-farm/vgi-rpc";
import { encodeMacroInfo } from "../../../generated/vgi-client.js";
import {
  CatalogMacroGetResultSchema,
  CatalogSchemaContentsMacrosResultSchema,
} from "../../../generated/vgi-protocol-schemas.js";
import type { MacroType } from "../../../catalog/interface.js";
import { toUint8Array } from "../../../util/bytes.js";
import { decodeDictValue } from "../../../util/arrow/index.js";
import {
  REQUEST_PARAMS_SCHEMA,
  RESULT_BINARY_SCHEMA,
  unwrapRequest,
  wrapResult,
} from "../shared.js";
import {
  type GetCatalog,
  emptyResultSchema,
  attachOpaqueDataSchemaNameTxnParams,
  catalogUnary,
} from "./shared.js";

export function registerCatalogMacroMethods(protocol: Protocol, getCatalog: GetCatalog, signingKey?: Uint8Array): void {
  // catalog_macro_get
  catalogUnary(protocol, signingKey, "catalog_macro_get", {
    params: attachOpaqueDataSchemaNameTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const info = await cat.macroGet(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return wrapResult({
        items: info ? [encodeMacroInfo(info)] : [],
      }, CatalogMacroGetResultSchema);
    },
  });

  // catalog_macro_create
  catalogUnary(protocol, signingKey, "catalog_macro_create", {
    params: REQUEST_PARAMS_SCHEMA,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      const innerParams = unwrapRequest(params.request);
      await cat.macroCreate(
        toUint8Array(innerParams.attach_opaque_data),
        innerParams.schema_name,
        innerParams.name,
        innerParams.macro_type as MacroType,
        innerParams.parameters ? (Array.isArray(innerParams.parameters) ? innerParams.parameters : [...innerParams.parameters]) : [],
        innerParams.definition,
        innerParams.on_conflict,
        innerParams.parameter_default_values ? toUint8Array(innerParams.parameter_default_values) : null,
        innerParams.transaction_opaque_data ? toUint8Array(innerParams.transaction_opaque_data) : undefined
      );
      return {};
    },
  });

  // catalog_macro_drop
  catalogUnary(protocol, signingKey, "catalog_macro_drop", {
    params: schema([
      field("attach_opaque_data", binary(), true),
      field("schema_name", utf8(), false),
      field("name", utf8(), false),
      field("ignore_not_found", bool(), true),
      field("transaction_opaque_data", binary(), true),
    ]),
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.macroDrop(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.ignore_not_found,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return {};
    },
  });

  // catalog_schema_contents_macros
  catalogUnary(protocol, signingKey, "catalog_schema_contents_macros", {
    params: schema([
      field("attach_opaque_data", binary(), true),
      field("name", utf8(), false),
      field("type", utf8(), false),
      field("transaction_opaque_data", binary(), true),
    ]),
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const macros = await cat.schemaContentsMacros(
        toUint8Array(params.attach_opaque_data),
        params.name,
        decodeDictValue(params.type),
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return wrapResult({
        items: macros.map((m) => encodeMacroInfo(m)),
      }, CatalogSchemaContentsMacrosResultSchema);
    },
  });
}
