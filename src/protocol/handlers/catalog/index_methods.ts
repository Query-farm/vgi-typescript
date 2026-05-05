// Catalog index handlers: index_get, schema_contents_indexes.

import { Schema, Field, Binary, Utf8 } from "@query-farm/apache-arrow";
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
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("name", new Utf8(), false),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const cat = getCatalog();
      const indexes = cat.schemaContentsIndexes(
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
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("schema_name", new Utf8(), false),
      new Field("name", new Utf8(), false),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const cat = getCatalog();
      const info = cat.indexGet(
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
