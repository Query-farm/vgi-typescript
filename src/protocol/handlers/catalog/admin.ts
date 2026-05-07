// Catalog admin handlers: attach/detach/create/drop, version, transactions,
// schemas (list, get, create, drop), schema contents listings.

import { type VgiSchema, schema, type VgiField, field, type VgiDataType, binary, utf8, bool } from "../../../arrow/index.js";
import { Protocol } from "vgi-rpc";
import { encodeSchemaInfo, encodeTableInfo, encodeViewInfo, encodeFunctionInfo, encodeCatalogInfo } from "../../../generated/vgi-client.js";
import {
  CatalogCatalogsResultSchema,
  CatalogAttachResultSchema,
  CatalogVersionResultSchema,
  CatalogTransactionBeginResultSchema,
  CatalogSchemasResultSchema,
  CatalogSchemaGetResultSchema,
  CatalogSchemaContentsTablesResultSchema,
  CatalogSchemaContentsViewsResultSchema,
  CatalogSchemaContentsFunctionsResultSchema,
} from "../../../generated/vgi-protocol-schemas.js";
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
  decodeOptionsBatch,
  emptyResultSchema,
  attachIdParam,
  attachIdTxnParams,
  attachIdNameTxnParams,
} from "./shared.js";

export function registerCatalogAdminMethods(protocol: Protocol, getCatalog: GetCatalog): void {
  // catalog_catalogs
  protocol.unary("catalog_catalogs", {
    params: emptyResultSchema,
    result: RESULT_BINARY_SCHEMA,
    handler: async () => {
      const cat = getCatalog();
      // Each catalog advertised as an IPC-serialized CatalogInfo
      // {name, implementation_version?, data_version_spec?}. Versioned workers
      // override catalogsInfo() to supply real values; otherwise both version
      // fields default to null.
      const infos = cat.catalogsInfo
        ? await cat.catalogsInfo()
        : cat.catalogs().map((name) => ({
            name,
            implementation_version: null,
            data_version_spec: null,
          }));
      const items = infos.map((info) => encodeCatalogInfo(info));
      return wrapResult({ items }, CatalogCatalogsResultSchema);
    },
  });

  // catalog_attach (params wrapped in request: Binary like bind/init)
  protocol.unary("catalog_attach", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const innerParams = unwrapRequest(params.request);
      const cat = getCatalog();
      // The extension sends user-supplied ATTACH options as an IPC-serialized
      // RecordBatch of typed columns — one column per option. Deserialize
      // once here so workers see an ergonomic {name: value} dict instead of
      // raw bytes. Nullable / absent → {}.
      const optionsDict = decodeOptionsBatch(innerParams.options);
      const result = await cat.attach(
        innerParams.name,
        optionsDict,
        innerParams.data_version_spec ?? null,
        innerParams.implementation_version ?? null,
      );
      return wrapResult({
        attach_id: result.attach_id,
        supports_transactions: result.supports_transactions,
        supports_time_travel: result.supports_time_travel,
        catalog_version_frozen: result.catalog_version_frozen,
        catalog_version: result.catalog_version,
        attach_id_required: result.attach_id_required ?? true,
        default_schema: result.default_schema ?? "main",
        settings: result.settings ?? [],
        secret_types: result.secret_types ?? [],
        comment: result.comment ?? null,
        tags: result.tags ?? {},
        // True so DuckDB will route catalog_table_column_statistics_get RPCs
        // to our handler for tables whose TableInfo.supports_column_statistics
        // is also true. Catalogs that never serve column stats can override
        // this in attach() via CatalogAttachResult.supports_column_statistics.
        supports_column_statistics: result.supports_column_statistics ?? true,
        resolved_data_version: result.resolved_data_version ?? null,
        resolved_implementation_version: result.resolved_implementation_version ?? null,
      }, CatalogAttachResultSchema);
    },
  });

  // catalog_detach
  protocol.unary("catalog_detach", {
    params: attachIdParam,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.detach(toUint8Array(params.attach_id));
      return {};
    },
  });

  // catalog_create
  protocol.unary("catalog_create", {
    params: schema([
      field("name", utf8(), false),
      field("on_conflict", utf8(), false),
      field("options", binary(), true),
    ]),
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.create(params.name, params.on_conflict, params.options);
      return {};
    },
  });

  // catalog_drop
  protocol.unary("catalog_drop", {
    params: schema([field("name", utf8(), false)]),
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.drop(params.name);
      return {};
    },
  });

  // catalog_version
  protocol.unary("catalog_version", {
    params: attachIdTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const version = await cat.version(
        toUint8Array(params.attach_id),
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({ version }, CatalogVersionResultSchema);
    },
  });

  // catalog_transaction_begin
  protocol.unary("catalog_transaction_begin", {
    params: attachIdParam,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const transaction_id = await cat.transactionBegin(toUint8Array(params.attach_id));
      return wrapResult({ transaction_id }, CatalogTransactionBeginResultSchema);
    },
  });

  // catalog_transaction_commit
  protocol.unary("catalog_transaction_commit", {
    params: attachIdTxnParams,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.transactionCommit(
        toUint8Array(params.attach_id),
        toUint8Array(params.transaction_id)
      );
      return {};
    },
  });

  // catalog_transaction_rollback
  protocol.unary("catalog_transaction_rollback", {
    params: attachIdTxnParams,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.transactionRollback(
        toUint8Array(params.attach_id),
        toUint8Array(params.transaction_id)
      );
      return {};
    },
  });

  // catalog_schemas
  protocol.unary("catalog_schemas", {
    params: attachIdTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const schemas = await cat.schemas(
        toUint8Array(params.attach_id),
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: schemas.map((s) => encodeSchemaInfo(s)),
      }, CatalogSchemasResultSchema);
    },
  });

  // catalog_schema_get
  protocol.unary("catalog_schema_get", {
    params: attachIdNameTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const info = await cat.schemaGet(
        toUint8Array(params.attach_id),
        params.name,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: info ? [encodeSchemaInfo(info)] : [],
      }, CatalogSchemaGetResultSchema);
    },
  });

  // catalog_schema_create
  protocol.unary("catalog_schema_create", {
    params: schema([
      field("attach_id", binary(), true),
      field("name", utf8(), false),
      field("comment", utf8(), true),
      field("tags", binary(), true),
      field("transaction_id", binary(), true),
    ]),
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.schemaCreate(
        toUint8Array(params.attach_id),
        params.name,
        params.comment,
        null, // tags
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_schema_drop
  protocol.unary("catalog_schema_drop", {
    params: schema([
      field("attach_id", binary(), true),
      field("name", utf8(), false),
      field("ignore_not_found", bool(), true),
      field("cascade", bool(), true),
      field("transaction_id", binary(), true),
    ]),
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.schemaDrop(
        toUint8Array(params.attach_id),
        params.name,
        params.ignore_not_found,
        params.cascade,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_schema_contents_tables
  protocol.unary("catalog_schema_contents_tables", {
    params: attachIdNameTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const tables = await cat.schemaContentsTables(
        toUint8Array(params.attach_id),
        params.name,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: tables.map((t) => encodeTableInfo(t)),
      }, CatalogSchemaContentsTablesResultSchema);
    },
  });

  // catalog_schema_contents_views
  protocol.unary("catalog_schema_contents_views", {
    params: attachIdNameTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const views = await cat.schemaContentsViews(
        toUint8Array(params.attach_id),
        params.name,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: views.map((v) => encodeViewInfo(v)),
      }, CatalogSchemaContentsViewsResultSchema);
    },
  });

  // catalog_schema_contents_functions
  protocol.unary("catalog_schema_contents_functions", {
    params: schema([
      field("attach_id", binary(), true),
      field("name", utf8(), false),
      field("type", utf8(), false),
      field("transaction_id", binary(), true),
    ]),
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const funcs = await cat.schemaContentsFunctions(
        toUint8Array(params.attach_id),
        params.name,
        decodeDictValue(params.type),
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: funcs.map((f) => encodeFunctionInfo(f)),
      }, CatalogSchemaContentsFunctionsResultSchema);
    },
  });
}
