// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Catalog index handlers: index_get, schema_contents_indexes.

import { type VgiSchema, schema, type VgiField, field, type VgiDataType, binary, utf8 } from "../../../arrow/index.js";
import { Protocol } from "@query-farm/vgi-rpc";
import { encodeIndexInfo } from "../../../generated/vgi-client.js";
import {
  CatalogIndexGetParamsSchema,
  CatalogIndexGetResultSchema,
  CatalogSchemaContentsIndexesParamsSchema,
  CatalogSchemaContentsIndexesResultSchema,
} from "../../../generated/vgi-protocol-schemas.js";
import { toUint8Array } from "../../../util/bytes.js";
import {
  RESULT_BINARY_SCHEMA,
  wrapResult,
} from "../shared.js";
import {
  type GetCatalog,
  catalogUnary,
} from "./shared.js";

export function registerCatalogIndexMethods(protocol: Protocol, getCatalog: GetCatalog, signingKey?: Uint8Array): void {
  // catalog_schema_contents_indexes
  catalogUnary(protocol, signingKey, "catalog_schema_contents_indexes", {
    params: CatalogSchemaContentsIndexesParamsSchema,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const indexes = await cat.schemaContentsIndexes(
        toUint8Array(params.attach_opaque_data),
        params.name,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined,
      );
      return wrapResult({
        items: indexes.map((i) => encodeIndexInfo(i)),
      }, CatalogSchemaContentsIndexesResultSchema);
    },
  });

  // catalog_index_get
  catalogUnary(protocol, signingKey, "catalog_index_get", {
    params: CatalogIndexGetParamsSchema,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const info = await cat.indexGet(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined,
      );
      return wrapResult({
        items: info ? [encodeIndexInfo(info)] : [],
      }, CatalogIndexGetResultSchema);
    },
  });
}
