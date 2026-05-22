// Catalog admin handlers: attach/detach/create/drop, version, transactions,
// schemas (list, get, create, drop), schema contents listings.

import { type VgiSchema, schema, type VgiField, field, type VgiDataType, binary, utf8, bool } from "../../../arrow/index.js";
import { Protocol, type CallContext } from "vgi-rpc";
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
  attachOpaqueDataParam,
  attachOpaqueDataTxnParams,
  attachOpaqueDataNameTxnParams,
  catalogUnary,
  sealAttach,
  openAttach,
  sealTransaction,
} from "./shared.js";

export function registerCatalogAdminMethods(protocol: Protocol, getCatalog: GetCatalog, signingKey?: Uint8Array): void {
  // catalog_catalogs
  catalogUnary(protocol, signingKey, "catalog_catalogs", {
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
            attach_option_specs: [],
            releases: [],
          }));
      const items = infos.map((info) => encodeCatalogInfo(info));
      return wrapResult({ items }, CatalogCatalogsResultSchema);
    },
  });

  // catalog_attach (params wrapped in request: Binary like bind/init)
  protocol.unary("catalog_attach", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params, ctx) => {
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
      // Seal the attach value into an AEAD envelope bound to the caller's
      // identity before it leaves the worker (HTTP transport; pass-through
      // when there is no signing key).
      const sealedAttach = await sealAttach(
        toUint8Array(result.attach_opaque_data),
        (ctx as CallContext | undefined)?.auth,
        signingKey,
      );
      return wrapResult({
        attach_opaque_data: sealedAttach,
        supports_transactions: result.supports_transactions,
        supports_time_travel: result.supports_time_travel,
        catalog_version_frozen: result.catalog_version_frozen,
        catalog_version: result.catalog_version,
        attach_opaque_data_required: result.attach_opaque_data_required ?? true,
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
  catalogUnary(protocol, signingKey, "catalog_detach", {
    params: attachOpaqueDataParam,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.detach(toUint8Array(params.attach_opaque_data));
      return {};
    },
  });

  // catalog_create
  catalogUnary(protocol, signingKey, "catalog_create", {
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
  catalogUnary(protocol, signingKey, "catalog_drop", {
    params: schema([field("name", utf8(), false)]),
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.drop(params.name);
      return {};
    },
  });

  // catalog_version
  catalogUnary(protocol, signingKey, "catalog_version", {
    params: attachOpaqueDataTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const version = await cat.version(
        toUint8Array(params.attach_opaque_data),
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return wrapResult({ version }, CatalogVersionResultSchema);
    },
  });

  // catalog_transaction_begin — bespoke: it must keep the *sealed* attach
  // envelope to bind the transaction envelope's AAD, so it cannot route
  // through catalogUnary (which would replace it with plaintext).
  protocol.unary("catalog_transaction_begin", {
    params: attachOpaqueDataParam,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params, ctx) => {
      const auth = (ctx as CallContext | undefined)?.auth;
      const sealedAttach =
        params.attach_opaque_data != null ? toUint8Array(params.attach_opaque_data) : new Uint8Array(0);
      const attachPlain = await openAttach(sealedAttach, auth, signingKey);
      const cat = getCatalog();
      const txPlain = await cat.transactionBegin(attachPlain);
      // Seal the transaction value, binding it to the caller's identity and
      // the parent attach envelope it was minted under. null → null.
      const transaction_opaque_data =
        txPlain != null ? await sealTransaction(toUint8Array(txPlain), sealedAttach, auth, signingKey) : null;
      return wrapResult({ transaction_opaque_data }, CatalogTransactionBeginResultSchema);
    },
  });

  // catalog_transaction_commit
  catalogUnary(protocol, signingKey, "catalog_transaction_commit", {
    params: attachOpaqueDataTxnParams,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.transactionCommit(
        toUint8Array(params.attach_opaque_data),
        toUint8Array(params.transaction_opaque_data)
      );
      return {};
    },
  });

  // catalog_transaction_rollback
  catalogUnary(protocol, signingKey, "catalog_transaction_rollback", {
    params: attachOpaqueDataTxnParams,
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.transactionRollback(
        toUint8Array(params.attach_opaque_data),
        toUint8Array(params.transaction_opaque_data)
      );
      return {};
    },
  });

  // catalog_schemas
  catalogUnary(protocol, signingKey, "catalog_schemas", {
    params: attachOpaqueDataTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const schemas = await cat.schemas(
        toUint8Array(params.attach_opaque_data),
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return wrapResult({
        items: schemas.map((s) => encodeSchemaInfo(s)),
      }, CatalogSchemasResultSchema);
    },
  });

  // catalog_schema_get
  catalogUnary(protocol, signingKey, "catalog_schema_get", {
    params: attachOpaqueDataNameTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const info = await cat.schemaGet(
        toUint8Array(params.attach_opaque_data),
        params.name,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return wrapResult({
        items: info ? [encodeSchemaInfo(info)] : [],
      }, CatalogSchemaGetResultSchema);
    },
  });

  // catalog_schema_create
  catalogUnary(protocol, signingKey, "catalog_schema_create", {
    params: schema([
      field("attach_opaque_data", binary(), true),
      field("name", utf8(), false),
      field("comment", utf8(), true),
      field("tags", binary(), true),
      field("transaction_opaque_data", binary(), true),
    ]),
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.schemaCreate(
        toUint8Array(params.attach_opaque_data),
        params.name,
        params.comment,
        null, // tags
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return {};
    },
  });

  // catalog_schema_drop
  catalogUnary(protocol, signingKey, "catalog_schema_drop", {
    params: schema([
      field("attach_opaque_data", binary(), true),
      field("name", utf8(), false),
      field("ignore_not_found", bool(), true),
      field("cascade", bool(), true),
      field("transaction_opaque_data", binary(), true),
    ]),
    result: emptyResultSchema,
    handler: async (params) => {
      const cat = getCatalog();
      await cat.schemaDrop(
        toUint8Array(params.attach_opaque_data),
        params.name,
        params.ignore_not_found,
        params.cascade,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return {};
    },
  });

  // catalog_schema_contents_tables
  catalogUnary(protocol, signingKey, "catalog_schema_contents_tables", {
    params: attachOpaqueDataNameTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const tables = await cat.schemaContentsTables(
        toUint8Array(params.attach_opaque_data),
        params.name,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return wrapResult({
        items: tables.map((t) => encodeTableInfo(t)),
      }, CatalogSchemaContentsTablesResultSchema);
    },
  });

  // catalog_schema_contents_views
  catalogUnary(protocol, signingKey, "catalog_schema_contents_views", {
    params: attachOpaqueDataNameTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const views = await cat.schemaContentsViews(
        toUint8Array(params.attach_opaque_data),
        params.name,
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return wrapResult({
        items: views.map((v) => encodeViewInfo(v)),
      }, CatalogSchemaContentsViewsResultSchema);
    },
  });

  // catalog_schema_contents_functions
  catalogUnary(protocol, signingKey, "catalog_schema_contents_functions", {
    params: schema([
      field("attach_opaque_data", binary(), true),
      field("name", utf8(), false),
      field("type", utf8(), false),
      field("transaction_opaque_data", binary(), true),
    ]),
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const cat = getCatalog();
      const funcs = await cat.schemaContentsFunctions(
        toUint8Array(params.attach_opaque_data),
        params.name,
        decodeDictValue(params.type),
        params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
      );
      return wrapResult({
        items: funcs.map((f) => encodeFunctionInfo(f)),
      }, CatalogSchemaContentsFunctionsResultSchema);
    },
  });
}
