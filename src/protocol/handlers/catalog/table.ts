// Catalog table handlers: table_get/create/drop, scan_function_get,
// column_statistics_get, comment_set, rename, plus all column_* mutations.

import { type VgiSchema, schema, type VgiField, field, type VgiDataType, binary, utf8, bool, int32, list } from "../../../arrow/index.js";
import { Protocol } from "vgi-rpc";
import { encodeTableInfo } from "../../../generated/vgi-client.js";
import {
  CatalogTableGetResultSchema,
  ScanFunctionResultSchema,
} from "../../../generated/vgi-protocol-schemas.js";
import { toUint8Array } from "../../../util/bytes.js";
import {
  RESULT_BINARY_SCHEMA,
  RESULT_BINARY_NULLABLE_SCHEMA,
  wrapResult,
} from "../shared.js";
import {
  type GetCatalog,
  emptyResultSchema,
  schemaNameIgnoreNotFoundTxnParams,
  schemaNameCommentParams,
  schemaNameRenameParams,
  columnOpParams,
  catalogUnary,
} from "./shared.js";

export function registerCatalogTableMethods(protocol: Protocol, getCatalog: GetCatalog, signingKey?: Uint8Array): void {
  // catalog_table_get
  catalogUnary(protocol, signingKey, "catalog_table_get", {
    params: schema([
      field("attach_opaque_data", binary(), true),
      field("schema_name", utf8(), false),
      field("name", utf8(), false),
      field("at_unit", utf8(), true),
      field("at_value", utf8(), true),
      field("transaction_opaque_data", binary(), true),
    ]),
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const info = await cat.tableGet(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.at_unit,
        params.at_value,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return wrapResult({
        items: info ? [encodeTableInfo(info)] : [],
      }, CatalogTableGetResultSchema);
    },
  });

  // catalog_table_create
  catalogUnary(protocol, signingKey, "catalog_table_create", {
    params: schema([
      field("attach_opaque_data", binary(), true),
      field("schema_name", utf8(), false),
      field("name", utf8(), false),
      field("columns", binary(), false),
      field("on_conflict", utf8(), false),
      field("not_null_constraints", list(field("item", int32(), false)), true),
      field("unique_constraints", list(field("item", list(field("item", int32(), false)), false)), true),
      field("check_constraints", list(field("item", utf8(), false)), true),
      field("transaction_opaque_data", binary(), true),
    ]),
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.tableCreate(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        toUint8Array(params.columns),
        params.on_conflict,
        params.not_null_constraints ?? [],
        params.unique_constraints ?? [],
        params.check_constraints ?? [],
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return {};
    },
  });

  // catalog_table_drop
  catalogUnary(protocol, signingKey, "catalog_table_drop", {
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
      await cat.tableDrop(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.ignore_not_found,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return {};
    },
  });

  // catalog_table_column_statistics_get
  catalogUnary(protocol, signingKey, "catalog_table_column_statistics_get", {
    params: schema([
      field("attach_opaque_data", binary(), true),
      field("schema_name", utf8(), false),
      field("name", utf8(), false),
      field("transaction_opaque_data", binary(), true),
    ]),
    result: RESULT_BINARY_NULLABLE_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const stats = await cat.tableColumnStatisticsGet(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined,
      );
      return { result: stats?.bytes ?? null };
    },
  });

  // catalog_table_scan_function_get
  catalogUnary(protocol, signingKey, "catalog_table_scan_function_get", {
    params: schema([
      field("attach_opaque_data", binary(), true),
      field("schema_name", utf8(), false),
      field("name", utf8(), false),
      field("at_unit", utf8(), true),
      field("at_value", utf8(), true),
      field("transaction_opaque_data", binary(), true),
    ]),
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const scanResult = await cat.tableScanFunctionGet(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.at_unit,
        params.at_value,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return wrapResult(scanResult, ScanFunctionResultSchema);
    },
  });

  // catalog_table_comment_set
  catalogUnary(protocol, signingKey, "catalog_table_comment_set", {
    params: schemaNameCommentParams,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.tableCommentSet(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.comment,
        params.ignore_not_found,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return {};
    },
  });

  // catalog_table_rename
  catalogUnary(protocol, signingKey, "catalog_table_rename", {
    params: schemaNameRenameParams,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.tableRename(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.new_name,
        params.ignore_not_found,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return {};
    },
  });

  // catalog_table_column_add
  catalogUnary(protocol, signingKey, "catalog_table_column_add", {
    params: schema([
      field("attach_opaque_data", binary(), true),
      field("schema_name", utf8(), false),
      field("name", utf8(), false),
      field("column_name", utf8(), false),
      field("column_type", utf8(), false),
      field("default_value", utf8(), true),
      field("ignore_not_found", bool(), true),
      field("transaction_opaque_data", binary(), true),
    ]),
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.tableColumnAdd(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.column_name,
        params.column_type,
        params.default_value,
        params.ignore_not_found,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return {};
    },
  });

  // catalog_table_column_drop
  catalogUnary(protocol, signingKey, "catalog_table_column_drop", {
    params: columnOpParams,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.tableColumnDrop(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.column_name,
        params.ignore_not_found,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return {};
    },
  });

  // catalog_table_column_rename
  catalogUnary(protocol, signingKey, "catalog_table_column_rename", {
    params: schema([
      field("attach_opaque_data", binary(), true),
      field("schema_name", utf8(), false),
      field("name", utf8(), false),
      field("column_name", utf8(), false),
      field("new_name", utf8(), false),
      field("ignore_not_found", bool(), true),
      field("transaction_opaque_data", binary(), true),
    ]),
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.tableColumnRename(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.column_name,
        params.new_name,
        params.ignore_not_found,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return {};
    },
  });

  // catalog_table_column_default_set
  catalogUnary(protocol, signingKey, "catalog_table_column_default_set", {
    params: schema([
      field("attach_opaque_data", binary(), true),
      field("schema_name", utf8(), false),
      field("name", utf8(), false),
      field("column_name", utf8(), false),
      field("default_value", utf8(), false),
      field("ignore_not_found", bool(), true),
      field("transaction_opaque_data", binary(), true),
    ]),
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.tableColumnDefaultSet(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.column_name,
        params.default_value,
        params.ignore_not_found,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return {};
    },
  });

  // catalog_table_column_default_drop
  catalogUnary(protocol, signingKey, "catalog_table_column_default_drop", {
    params: columnOpParams,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.tableColumnDefaultDrop(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.column_name,
        params.ignore_not_found,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return {};
    },
  });

  // catalog_table_column_type_change
  catalogUnary(protocol, signingKey, "catalog_table_column_type_change", {
    params: schema([
      field("attach_opaque_data", binary(), true),
      field("schema_name", utf8(), false),
      field("name", utf8(), false),
      field("column_name", utf8(), false),
      field("new_type", utf8(), false),
      field("ignore_not_found", bool(), true),
      field("transaction_opaque_data", binary(), true),
    ]),
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.tableColumnTypeChange(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.column_name,
        params.new_type,
        params.ignore_not_found,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return {};
    },
  });

  // catalog_table_not_null_set
  catalogUnary(protocol, signingKey, "catalog_table_not_null_set", {
    params: columnOpParams,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.tableNotNullSet(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.column_name,
        params.ignore_not_found,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return {};
    },
  });

  // catalog_table_not_null_drop
  catalogUnary(protocol, signingKey, "catalog_table_not_null_drop", {
    params: columnOpParams,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.tableNotNullDrop(
        toUint8Array(params.attach_opaque_data),
        params.schema_name,
        params.name,
        params.column_name,
        params.ignore_not_found,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return {};
    },
  });
}
