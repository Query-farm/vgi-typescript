// Catalog table handlers: table_get/create/drop, scan_function_get,
// column_statistics_get, comment_set, rename, plus all column_* mutations.

import { Schema, Field, Binary, Utf8, Bool, Int32, List } from "@query-farm/apache-arrow";
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
} from "./shared.js";

export function registerCatalogTableMethods(protocol: Protocol, getCatalog: GetCatalog): void {
  // catalog_table_get
  protocol.unary("catalog_table_get", {
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("schema_name", new Utf8(), false),
      new Field("name", new Utf8(), false),
      new Field("at_unit", new Utf8(), true),
      new Field("at_value", new Utf8(), true),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const cat = getCatalog();
      const info = cat.tableGet(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.at_unit,
        params.at_value,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: info ? [encodeTableInfo(info)] : [],
      }, CatalogTableGetResultSchema);
    },
  });

  // catalog_table_create
  protocol.unary("catalog_table_create", {
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("schema_name", new Utf8(), false),
      new Field("name", new Utf8(), false),
      new Field("columns", new Binary(), false),
      new Field("on_conflict", new Utf8(), false),
      new Field("not_null_constraints", new List(new Field("item", new Int32(), false)), true),
      new Field("unique_constraints", new List(new Field("item", new List(new Field("item", new Int32(), false)), false)), true),
      new Field("check_constraints", new List(new Field("item", new Utf8(), false)), true),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: emptyResultSchema,
    handler: (params) => {
      const cat = getCatalog();
      cat.tableCreate(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        toUint8Array(params.columns),
        params.on_conflict,
        params.not_null_constraints ?? [],
        params.unique_constraints ?? [],
        params.check_constraints ?? [],
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_table_drop
  protocol.unary("catalog_table_drop", {
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
      cat.tableDrop(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.ignore_not_found,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_table_column_statistics_get — returns serialized ColumnStatistics
  // (IPC bytes of the sparse-union batch) for a specific table, or null when
  // the table has no declared stats. The cache TTL is attached via schema
  // metadata on the returned batch's stream header (see Python's
  // serialize_column_statistics for the wire detail); here we currently just
  // return the raw bytes — DuckDB reads `cache_max_age_seconds` out of the
  // IPC batch's custom_metadata if the serializer wrote it. For now the TTL
  // is attached only via the top-level result wrapper (cache behavior
  // intentionally conservative).
  protocol.unary("catalog_table_column_statistics_get", {
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("schema_name", new Utf8(), false),
      new Field("name", new Utf8(), false),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: RESULT_BINARY_NULLABLE_SCHEMA,
    handler: (params) => {
      const cat = getCatalog();
      const stats = cat.tableColumnStatisticsGet(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined,
      );
      return { result: stats?.bytes ?? null };
    },
  });

  // catalog_table_scan_function_get
  protocol.unary("catalog_table_scan_function_get", {
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("schema_name", new Utf8(), false),
      new Field("name", new Utf8(), false),
      new Field("at_unit", new Utf8(), true),
      new Field("at_value", new Utf8(), true),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const cat = getCatalog();
      const scanResult = cat.tableScanFunctionGet(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.at_unit,
        params.at_value,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult(scanResult, ScanFunctionResultSchema);
    },
  });

  // catalog_table_comment_set
  protocol.unary("catalog_table_comment_set", {
    params: schemaNameCommentParams,
    result: emptyResultSchema,
    handler: (params) => {
      const cat = getCatalog();
      cat.tableCommentSet(
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

  // catalog_table_rename
  protocol.unary("catalog_table_rename", {
    params: schemaNameRenameParams,
    result: emptyResultSchema,
    handler: (params) => {
      const cat = getCatalog();
      cat.tableRename(
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

  // catalog_table_column_add
  protocol.unary("catalog_table_column_add", {
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("schema_name", new Utf8(), false),
      new Field("name", new Utf8(), false),
      new Field("column_name", new Utf8(), false),
      new Field("column_type", new Utf8(), false),
      new Field("default_value", new Utf8(), true),
      new Field("ignore_not_found", new Bool(), true),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: emptyResultSchema,
    handler: (params) => {
      const cat = getCatalog();
      cat.tableColumnAdd(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.column_name,
        params.column_type,
        params.default_value,
        params.ignore_not_found,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_table_column_drop
  protocol.unary("catalog_table_column_drop", {
    params: columnOpParams,
    result: emptyResultSchema,
    handler: (params) => {
      const cat = getCatalog();
      cat.tableColumnDrop(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.column_name,
        params.ignore_not_found,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_table_column_rename
  protocol.unary("catalog_table_column_rename", {
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("schema_name", new Utf8(), false),
      new Field("name", new Utf8(), false),
      new Field("column_name", new Utf8(), false),
      new Field("new_name", new Utf8(), false),
      new Field("ignore_not_found", new Bool(), true),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: emptyResultSchema,
    handler: (params) => {
      const cat = getCatalog();
      cat.tableColumnRename(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.column_name,
        params.new_name,
        params.ignore_not_found,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_table_column_default_set
  protocol.unary("catalog_table_column_default_set", {
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("schema_name", new Utf8(), false),
      new Field("name", new Utf8(), false),
      new Field("column_name", new Utf8(), false),
      new Field("default_value", new Utf8(), false),
      new Field("ignore_not_found", new Bool(), true),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: emptyResultSchema,
    handler: (params) => {
      const cat = getCatalog();
      cat.tableColumnDefaultSet(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.column_name,
        params.default_value,
        params.ignore_not_found,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_table_column_default_drop
  protocol.unary("catalog_table_column_default_drop", {
    params: columnOpParams,
    result: emptyResultSchema,
    handler: (params) => {
      const cat = getCatalog();
      cat.tableColumnDefaultDrop(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.column_name,
        params.ignore_not_found,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_table_column_type_change
  protocol.unary("catalog_table_column_type_change", {
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("schema_name", new Utf8(), false),
      new Field("name", new Utf8(), false),
      new Field("column_name", new Utf8(), false),
      new Field("new_type", new Utf8(), false),
      new Field("ignore_not_found", new Bool(), true),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: emptyResultSchema,
    handler: (params) => {
      const cat = getCatalog();
      cat.tableColumnTypeChange(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.column_name,
        params.new_type,
        params.ignore_not_found,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_table_not_null_set
  protocol.unary("catalog_table_not_null_set", {
    params: columnOpParams,
    result: emptyResultSchema,
    handler: (params) => {
      const cat = getCatalog();
      cat.tableNotNullSet(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.column_name,
        params.ignore_not_found,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_table_not_null_drop
  protocol.unary("catalog_table_not_null_drop", {
    params: columnOpParams,
    result: emptyResultSchema,
    handler: (params) => {
      const cat = getCatalog();
      cat.tableNotNullDrop(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.column_name,
        params.ignore_not_found,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });
}
