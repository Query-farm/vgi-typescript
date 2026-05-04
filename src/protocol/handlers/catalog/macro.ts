// Catalog macro handlers: macro_get/create/drop, schema_contents_macros.

import { Schema, Field, Binary, Utf8, Bool } from "@query-farm/apache-arrow";
import { Protocol } from "vgi-rpc";
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
  attachIdSchemaNameTxnParams,
} from "./shared.js";

export function registerCatalogMacroMethods(protocol: Protocol, getCatalog: GetCatalog): void {
  // catalog_macro_get
  protocol.unary("catalog_macro_get", {
    params: attachIdSchemaNameTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const cat = getCatalog();
      const info = cat.macroGet(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: info ? [encodeMacroInfo(info)] : [],
      }, CatalogMacroGetResultSchema);
    },
  });

  // catalog_macro_create
  protocol.unary("catalog_macro_create", {
    params: REQUEST_PARAMS_SCHEMA,
    result: emptyResultSchema,
    handler: (params) => {
      const cat = getCatalog();
      const innerParams = unwrapRequest(params.request);
      cat.macroCreate(
        toUint8Array(innerParams.attach_id),
        innerParams.schema_name,
        innerParams.name,
        innerParams.macro_type as MacroType,
        innerParams.parameters ? (Array.isArray(innerParams.parameters) ? innerParams.parameters : [...innerParams.parameters]) : [],
        innerParams.definition,
        innerParams.on_conflict,
        innerParams.parameter_default_values ? toUint8Array(innerParams.parameter_default_values) : null,
        innerParams.transaction_id ? toUint8Array(innerParams.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_macro_drop
  protocol.unary("catalog_macro_drop", {
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("schema_name", new Utf8(), false),
      new Field("name", new Utf8(), false),
      new Field("ignore_not_found", new Bool(), true),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: emptyResultSchema,
    handler: (params) => {
      const cat = getCatalog();
      cat.macroDrop(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.ignore_not_found,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_schema_contents_macros
  protocol.unary("catalog_schema_contents_macros", {
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("name", new Utf8(), false),
      new Field("type", new Utf8(), false),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const cat = getCatalog();
      const macros = cat.schemaContentsMacros(
        toUint8Array(params.attach_id),
        params.name,
        decodeDictValue(params.type),
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: macros.map((m) => encodeMacroInfo(m)),
      }, CatalogSchemaContentsMacrosResultSchema);
    },
  });
}
