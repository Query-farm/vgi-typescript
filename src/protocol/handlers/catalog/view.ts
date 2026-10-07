// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Catalog view handlers: view_get/create/drop/rename/comment_set.

import type { VgiService } from "../../../generated/vgi-service.js";
import { encodeViewInfo } from "../../../generated/vgi-client.js";
import {
  CatalogViewGetResultSchema,
} from "../../../generated/vgi-protocol-schemas.js";
import { toUint8Array } from "../../../util/bytes.js";
import {
  wrapResult,
} from "../shared.js";
import {
  type GetCatalog,
  catalogHandler,
} from "./shared.js";

export function catalogViewHandlers(getCatalog: GetCatalog, signingKey?: Uint8Array): Partial<VgiService> {
  // catalog_view_get
  const catalogViewGet = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const info = await cat.viewGet(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return wrapResult({
      items: info ? [encodeViewInfo(info)] : [],
    }, CatalogViewGetResultSchema);
  });

  // catalog_view_create
  const catalogViewCreate = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    await cat.viewCreate(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.definition,
      params.on_conflict,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });

  // catalog_view_drop
  const catalogViewDrop = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    await cat.viewDrop(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.ignore_not_found,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });

  // catalog_view_rename
  const catalogViewRename = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    await cat.viewRename(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.new_name,
      params.ignore_not_found,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });

  // catalog_view_comment_set
  const catalogViewCommentSet = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    await cat.viewCommentSet(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.comment,
      params.ignore_not_found,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });

  return { catalogViewGet, catalogViewCreate, catalogViewDrop, catalogViewRename, catalogViewCommentSet };
}
