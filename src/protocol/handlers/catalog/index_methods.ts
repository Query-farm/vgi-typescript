// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Catalog index handlers: index_get, schema_contents_indexes.

import type { VgiService } from "../../../generated/vgi-service.js";
import { encodeIndexInfo } from "../../../generated/vgi-client.js";
import {
  CatalogIndexGetResultSchema,
  CatalogSchemaContentsIndexesResultSchema,
} from "../../../generated/vgi-protocol-schemas.js";
import { toUint8Array } from "../../../util/bytes.js";
import {
  wrapResult,
} from "../shared.js";
import {
  type GetCatalog,
  catalogHandler,
} from "./shared.js";

export function catalogIndexHandlers(getCatalog: GetCatalog, signingKey?: Uint8Array): Partial<VgiService> {
  // catalog_schema_contents_indexes
  const catalogSchemaContentsIndexes = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const indexes = await cat.schemaContentsIndexes(
      toUint8Array(params.attach_opaque_data),
      params.path,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined,
    );
    return wrapResult({
      items: indexes.map((i) => encodeIndexInfo(i)),
    }, CatalogSchemaContentsIndexesResultSchema);
  });

  // catalog_index_get
  const catalogIndexGet = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const info = await cat.indexGet(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined,
    );
    return wrapResult({
      items: info ? [encodeIndexInfo(info)] : [],
    }, CatalogIndexGetResultSchema);
  });

  return { catalogSchemaContentsIndexes, catalogIndexGet };
}
