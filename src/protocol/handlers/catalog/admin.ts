// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Catalog admin handlers: attach/detach/create/drop, version, transactions,
// schemas (list, get, create, drop), schema contents listings.

import { schema } from "../../../arrow/index.js";
import type { CallContext } from "@query-farm/vgi-rpc";
import type { VgiService } from "../../../generated/vgi-service.js";
import {
  encodeSchemaInfo,
  encodeTableInfo,
  encodeViewInfo,
  encodeCatalogInfo,
} from "../../../generated/vgi-client.js";
import { buildCatalogAttachResult } from "../../../generated/vgi-protocol-types.js";
import { serveCatalogContents } from "./contents.js";
import { encodeFunctionInfoOnce } from "../../../catalog/item-encoding.js";
import { encodeCopyFromFormatInfo } from "../../../catalog/interface.js";
import {
  CatalogAttachResultSchema,
  CatalogCatalogsResultSchema,
  CatalogCopyFromFormatsResultSchema,
  CatalogSchemaContentsFunctionsResultSchema,
  CatalogSchemaContentsTablesResultSchema,
  CatalogSchemaContentsViewsResultSchema,
  CatalogSchemaGetResultSchema,
  CatalogSchemasResultSchema,
  CatalogTransactionBeginResultSchema,
  CatalogVersionResultSchema,
} from "../../../generated/vgi-protocol-schemas.js";
import { toUint8Array } from "../../../util/bytes.js";
import { redeemAttachTicket } from "../../../attach-ticket.js";
import { currentRequestAuth } from "../../../request-auth.js";
import { decodeDictValue } from "../../../util/arrow/index.js";
import {
  unwrapRequest,
  wrapResult,
} from "../shared.js";
import {
  type GetCatalog,
  decodeOptionsBatch,
  catalogHandler,
  sealAttach,
  openAttach,
  sealTransaction,
} from "./shared.js";

export function catalogAdminHandlers(getCatalog: GetCatalog, signingKey?: Uint8Array): Partial<VgiService> {
  // catalog_catalogs
  const catalogCatalogs = catalogHandler(signingKey, async () => {
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
  });

  // catalog_attach (params wrapped in request: Binary like bind/init)
  const catalogAttach: VgiService["catalogAttach"] = async (params, ctx) => {
    const innerParams = unwrapRequest(params.request);
    const cat = getCatalog();
    // The extension sends user-supplied ATTACH options as an IPC-serialized
    // RecordBatch of typed columns — one column per option. Deserialize
    // once here so workers see an ergonomic {name: value} dict instead of
    // raw bytes. Nullable / absent → {}.
    let optionsDict = decodeOptionsBatch(innerParams.options);
    // A `vgi_attach_ticket` is redeemed here, before any catalog code runs:
    // the request becomes the attach the ticket seals (catalog name,
    // options, version specs). A composite catalog therefore routes on the
    // sealed name, not the request's. The ticket opens only under the
    // caller's principal and this worker's signing key; neither it nor a
    // restored option is ever logged.
    const restored = await redeemAttachTicket(
      optionsDict,
      signingKey,
      (ctx as CallContext | undefined)?.auth ?? currentRequestAuth(),
    );
    if (restored !== null) {
      innerParams.name = restored.name;
      innerParams.options = restored.optionsIpc;
      innerParams.data_version_spec = restored.dataVersionSpec;
      innerParams.implementation_version = restored.implementationVersion;
      optionsDict = restored.options;
    }
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
    // The generated builder emits every column of the pinned
    // CatalogAttachResult schema, filling vgi-python's defaults for any a
    // catalog left out (supports_catalog_contents=false: the client then
    // keeps to the per-schema RPCs).
    return wrapResult(buildCatalogAttachResult({
      ...result,
      attach_opaque_data: sealedAttach,
      // SDK default, unlike vgi-python's false: true so DuckDB will route
      // catalog_table_column_statistics_get RPCs to our handler for tables
      // whose TableInfo.supports_column_statistics is also true. Catalogs
      // that never serve column stats set it false in attach().
      supports_column_statistics: result.supports_column_statistics ?? true,
    }), CatalogAttachResultSchema);
  };

  // catalog_detach
  const catalogDetach = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    await cat.detach(toUint8Array(params.attach_opaque_data));
    return {};
  });

  // catalog_create
  const catalogCreate = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    await cat.create(params.name, params.on_conflict, params.options);
    return {};
  });

  // catalog_drop
  const catalogDrop = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    await cat.drop(params.name);
    return {};
  });

  // catalog_version
  const catalogVersion = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const version = await cat.version(
      toUint8Array(params.attach_opaque_data),
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return wrapResult({ version }, CatalogVersionResultSchema);
  });

  // catalog_transaction_begin — bespoke: it must keep the *sealed* attach
  // envelope to bind the transaction envelope's AAD, so it cannot route
  // through catalogHandler (which would replace it with plaintext).
  const catalogTransactionBegin: VgiService["catalogTransactionBegin"] = async (params, ctx) => {
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
  };

  // catalog_transaction_commit
  const catalogTransactionCommit = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    await cat.transactionCommit(
      toUint8Array(params.attach_opaque_data),
      toUint8Array(params.transaction_opaque_data)
    );
    return {};
  });

  // catalog_transaction_rollback
  const catalogTransactionRollback = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    await cat.transactionRollback(
      toUint8Array(params.attach_opaque_data),
      toUint8Array(params.transaction_opaque_data)
    );
    return {};
  });

  // catalog_schemas
  const catalogSchemas = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const schemas = await cat.schemas(
      toUint8Array(params.attach_opaque_data),
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return wrapResult({
      items: schemas.map((s) => encodeSchemaInfo(s)),
    }, CatalogSchemasResultSchema);
  });

  // catalog_contents — every schema and all of its contents in one result,
  // for a client whose attach result set supports_catalog_contents. Takes no
  // transaction: the client caches the answer for the whole attach, so it is
  // the committed catalog at catalog_version. `if_none_match` revalidates a
  // snapshot the client holds (etag / not_modified); see ./contents.ts for the
  // rules, the content-hash etag and the frozen-catalog cache.
  const catalogContents = catalogHandler(signingKey, async (params) => {
    const ifNoneMatch = params.if_none_match == null ? null : String(params.if_none_match);
    return serveCatalogContents(getCatalog(), toUint8Array(params.attach_opaque_data), ifNoneMatch);
  });

  // catalog_copy_from_formats — catalog-level (not schema-scoped). Lists the
  // custom COPY ... FROM formats this catalog advertises; empty list when none.
  const catalogCopyFromFormats = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const formats = await cat.copyFromFormats(
      toUint8Array(params.attach_opaque_data),
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined,
    );
    return wrapResult({
      items: formats.map((f) => encodeCopyFromFormatInfo(f)),
    }, CatalogCopyFromFormatsResultSchema);
  });

  // catalog_schema_get
  const catalogSchemaGet = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const info = await cat.schemaGet(
      toUint8Array(params.attach_opaque_data),
      params.path,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return wrapResult({
      items: info ? [encodeSchemaInfo(info)] : [],
    }, CatalogSchemaGetResultSchema);
  });

  // catalog_schema_create
  const catalogSchemaCreate = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    await cat.schemaCreate(
      toUint8Array(params.attach_opaque_data),
      params.path,
      params.comment,
      null, // tags
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });

  // catalog_schema_drop
  const catalogSchemaDrop = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    await cat.schemaDrop(
      toUint8Array(params.attach_opaque_data),
      params.path,
      params.ignore_not_found,
      params.cascade,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });

  // catalog_schema_contents_tables
  const catalogSchemaContentsTables = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const tables = await cat.schemaContentsTables(
      toUint8Array(params.attach_opaque_data),
      params.path,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return wrapResult({
      items: tables.map((t) => encodeTableInfo(t)),
    }, CatalogSchemaContentsTablesResultSchema);
  });

  // catalog_schema_contents_views
  const catalogSchemaContentsViews = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const views = await cat.schemaContentsViews(
      toUint8Array(params.attach_opaque_data),
      params.path,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return wrapResult({
      items: views.map((v) => encodeViewInfo(v)),
    }, CatalogSchemaContentsViewsResultSchema);
  });

  // catalog_schema_contents_functions
  const catalogSchemaContentsFunctions = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const funcs = await cat.schemaContentsFunctions(
      toUint8Array(params.attach_opaque_data),
      params.path,
      decodeDictValue(params.type),
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    // A frozen item (a descriptor catalog's cached listing) is encoded once;
    // anything else is encoded per call, as before (see item-encoding.ts).
    return wrapResult({
      items: funcs.map((f) => encodeFunctionInfoOnce(f)),
    }, CatalogSchemaContentsFunctionsResultSchema);
  });

  return {
    catalogCatalogs,
    catalogAttach,
    catalogDetach,
    catalogCreate,
    catalogDrop,
    catalogVersion,
    catalogTransactionBegin,
    catalogTransactionCommit,
    catalogTransactionRollback,
    catalogSchemas,
    catalogContents,
    catalogCopyFromFormats,
    catalogSchemaGet,
    catalogSchemaCreate,
    catalogSchemaDrop,
    catalogSchemaContentsTables,
    catalogSchemaContentsViews,
    catalogSchemaContentsFunctions,
  };
}
