// Catalog index handlers: index_get, schema_contents_indexes.

import { type VgiSchema, schema, type VgiField, field, type VgiDataType, binary, utf8 } from "../../../arrow/index.js";
import { Protocol } from "vgi-rpc";
import { encodeIndexInfo } from "../../../generated/vgi-client.js";
import {
  CatalogIndexGetResultSchema,
  CatalogSchemaContentsIndexesResultSchema,
} from "../../../generated/vgi-protocol-schemas.js";
import { toUint8Array } from "../../../util/bytes.js";
import {
  RESULT_BINARY_SCHEMA,
  wrapResult,
} from "../shared.js";
import {
  type GetCatalog,
} from "./shared.js";

export function registerCatalogIndexMethods(protocol: Protocol, getCatalog: GetCatalog): void {
  // catalog_schema_contents_indexes
  protocol.unary("catalog_schema_contents_indexes", {
    params: schema([
      field("attach_id", binary(), true),
      field("name", utf8(), false),
      field("transaction_id", binary(), true),
    ]),
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const indexes = await cat.schemaContentsIndexes(
        toUint8Array(params.attach_id),
        params.name,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined,
      );
      return wrapResult({
        items: indexes.map((i) => encodeIndexInfo(i)),
      }, CatalogSchemaContentsIndexesResultSchema);
    },
  });

  // catalog_index_get
  protocol.unary("catalog_index_get", {
    params: schema([
      field("attach_id", binary(), true),
      field("schema_name", utf8(), false),
      field("name", utf8(), false),
      field("transaction_id", binary(), true),
    ]),
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const info = await cat.indexGet(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined,
      );
      return wrapResult({
        items: info ? [encodeIndexInfo(info)] : [],
      }, CatalogIndexGetResultSchema);
    },
  });
}
