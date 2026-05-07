// Catalog view handlers: view_get/create/drop/rename/comment_set.

import { type VgiSchema, schema, type VgiField, field, type VgiDataType, binary, utf8 } from "../../../arrow/index.js";
import { Protocol } from "vgi-rpc";
import { encodeViewInfo } from "../../../generated/vgi-client.js";
import { CatalogViewGetResultSchema } from "../../../generated/vgi-protocol-schemas.js";
import { toUint8Array } from "../../../util/bytes.js";
import {
  RESULT_BINARY_SCHEMA,
  wrapResult,
} from "../shared.js";
import {
  type GetCatalog,
  emptyResultSchema,
  attachIdSchemaNameTxnParams,
  schemaNameIgnoreNotFoundTxnParams,
  schemaNameRenameParams,
  schemaNameCommentParams,
} from "./shared.js";

export function registerCatalogViewMethods(protocol: Protocol, getCatalog: GetCatalog): void {
  // catalog_view_get
  protocol.unary("catalog_view_get", {
    params: attachIdSchemaNameTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const info = await cat.viewGet(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: info ? [encodeViewInfo(info)] : [],
      }, CatalogViewGetResultSchema);
    },
  });

  // catalog_view_create
  protocol.unary("catalog_view_create", {
    params: schema([
      field("attach_id", binary(), true),
      field("schema_name", utf8(), false),
      field("name", utf8(), false),
      field("definition", utf8(), false),
      field("on_conflict", utf8(), false),
      field("transaction_id", binary(), true),
    ]),
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.viewCreate(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.definition,
        params.on_conflict,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_view_drop
  protocol.unary("catalog_view_drop", {
    params: schemaNameIgnoreNotFoundTxnParams,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.viewDrop(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.ignore_not_found,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_view_rename
  protocol.unary("catalog_view_rename", {
    params: schemaNameRenameParams,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.viewRename(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.new_name,
        params.ignore_not_found,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_view_comment_set
  protocol.unary("catalog_view_comment_set", {
    params: schemaNameCommentParams,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.viewCommentSet(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.comment,
        params.ignore_not_found,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });
}
