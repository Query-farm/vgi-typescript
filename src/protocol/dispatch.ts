// Build VGI protocol from worker implementation.
// Registers bind, init, table_function_cardinality, and catalog methods on vgi-rpc Protocol.

import { Schema, Field, Binary, Utf8, Int64, Int32, Bool, List, Map_, Struct, RecordBatch } from "@query-farm/apache-arrow";
import { Protocol, type OutputCollector } from "vgi-rpc";

// NOTE: We must NOT use vgi-rpc's str/bytes/int/etc. singletons in Schema objects
// because Bun loads apache-arrow as separate module instances for our code vs vgi-rpc's
// compiled dist. Instead, we pre-build Schema objects and pass them directly to Protocol
// methods (toSchema() passes Schema instances through without instanceof checks).
import type { FunctionRegistry, OverloadContext } from "../functions/registry.js";
import type { VgiFunction, StreamHandlers, HandlerState } from "../functions/types.js";
import {
  deserializeBindRequest,
  serializeBindResponse,
  deserializeInitRequest,
  serializeGlobalInitResponse,
  deserializeCardinalityRequest,
  serializeTableCardinality,
} from "./serialize.js";
import type {
  BindRequest,
  BindResponse,
  InitRequest,
  GlobalInitResponse,
} from "./types.js";
import { TableInOutPhase } from "../types.js";
import { resolveMetadata } from "../metadata/resolve.js";
import { metadatasToArrow, METADATA_SCHEMA } from "../metadata/serialize.js";
import { batchToScalarDict, serializeBatch, deserializeBatch, serializeSchema, emptyBatch, batchFromColumns } from "../util/arrow.js";
import { toUint8Array } from "../util/bytes.js";
import type { CatalogInterface } from "../catalog/interface.js";
import { MacroType } from "../catalog/interface.js";
import { NoCatalogError } from "../errors.js";
import {
  BindResultSchema,
  TableFunctionCardinalityResultSchema,
  CatalogInfoSchema,
  CatalogCatalogsResultSchema,
  CatalogAttachResultSchema,
  CatalogVersionResultSchema,
  CatalogTransactionBeginResultSchema,
  CatalogSchemasResultSchema,
  CatalogSchemaGetResultSchema,
  CatalogSchemaContentsTablesResultSchema,
  CatalogSchemaContentsViewsResultSchema,
  CatalogSchemaContentsFunctionsResultSchema,
  CatalogSchemaContentsMacrosResultSchema,
  CatalogTableGetResultSchema,
  CatalogViewGetResultSchema,
  CatalogMacroGetResultSchema,
  ScanFunctionResultSchema,
} from "../generated/vgi-protocol-schemas.js";

function overloadContext(req: { functionName: string; arguments: any; inputSchema: any; functionType: any }): OverloadContext {
  return {
    arguments: req.arguments,
    inputSchema: req.inputSchema,
    isScalar: String(req.functionType).toLowerCase() === "scalar",
  };
}

// ============================================================================
// Protocol building
// ============================================================================

export interface ProtocolConfig {
  registry: FunctionRegistry;
  catalogInterface?: CatalogInterface;
  catalogName?: string;
  /**
   * Recover accumulated exchange state from FINALIZE init_opaque_data.
   * For HTTP transport, this unpacks the state token that the C++ extension
   * passes from the last INPUT exchange to the FINALIZE init request.
   * Returns the deserialized VGI dispatch state object (with userState field).
   */
  recoverExchangeState?: (opaqueData: Uint8Array) => any;
}

// The Python vgi-rpc framework wraps ALL non-void unary results in a single
// "result" column. For ArrowSerializableDataclass types, the result is serialized
// as Arrow IPC bytes in a Binary column. DuckDB's VGI extension expects this format.
const RESULT_BINARY_SCHEMA = new Schema([
  new Field("result", new Binary(), false),
]);

// DuckDB wraps ArrowSerializableDataclass parameters in a single "request" Binary column.
const REQUEST_PARAMS_SCHEMA = new Schema([
  new Field("request", new Binary(), false),
]);

const RESULT_BINARY_NULLABLE_SCHEMA = new Schema([
  new Field("result", new Binary(), true),
]);

/**
 * Unwrap a "request" Binary column: deserialize the inner Arrow IPC batch
 * and return flat columns as a dict (row 0).
 */
function unwrapRequest(requestBytes: any): Record<string, any> {
  const bytes = toUint8Array(requestBytes);
  const innerBatch = deserializeBatch(bytes);
  return batchToScalarDict(innerBatch);
}

/**
 * Wrap a dict of values into a single "result" Binary column.
 * Builds a 1-row batch from the values using the given schema,
 * serializes it to IPC bytes, and returns { result: bytes }.
 */
function wrapResult(
  values: Record<string, any>,
  innerSchema: Schema,
): { result: Uint8Array } {
  const batch = batchFromColumns(
    Object.fromEntries(innerSchema.fields.map(f => [f.name, [values[f.name] ?? null]])),
    innerSchema,
  );
  return { result: serializeBatch(batch) };
}

/**
 * Recover accumulated exchange state from a FINALIZE init request.
 * Used by both init and exchange handlers to avoid duplicating the try/catch.
 */
function recoverFinalizeState(request: InitRequest, config: ProtocolConfig): any {
  if (request.phase === TableInOutPhase.FINALIZE && request.initOpaqueData && config.recoverExchangeState) {
    try {
      const recovered = config.recoverExchangeState(request.initOpaqueData);
      return recovered?.userState;
    } catch (e: any) {
      throw new Error(`Failed to recover FINALIZE state from init_opaque_data: ${e.message}`);
    }
  }
  return undefined;
}

export function buildVgiProtocol(config: ProtocolConfig): Protocol {
  const { registry, catalogInterface } = config;
  const protocol = new Protocol("vgi");

  // Response schemas sourced from generated/vgi-protocol-schemas.ts; these
  // local aliases keep the rest of the function body stable.
  const bindResponseInnerSchema = BindResultSchema;
  const cardinalityInnerSchema = TableFunctionCardinalityResultSchema;

  const initHeaderSchema = new Schema([
    new Field("execution_id", new Binary(), false),
    new Field("opaque_data", new Binary(), true),
    new Field("max_workers", new Int64(), false),
  ]);

  const emptySchema = new Schema([]);

  // Dummy non-empty schema for the init exchange registration.
  // The dispatch determines producer vs exchange based on inputSchema emptiness.
  // We need exchange mode so that input batches are passed to our callback.
  // The actual input schema comes from the IPC stream, not this registration.
  const dummyInputSchema = new Schema([
    new Field("_tick", new Binary(), true),
  ]);

  // --------------------------------------------------------------------------
  // bind (unary)
  // --------------------------------------------------------------------------
  protocol.unary("bind", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const innerParams = unwrapRequest(params.request);
      const request = deserializeBindRequest(innerParams);
      const func = registry.get(request.functionName, overloadContext(request));
      const response = func.bind(request);
      const serialized = serializeBindResponse(response);
      return wrapResult(serialized, bindResponseInnerSchema);
    },
  });

  // --------------------------------------------------------------------------
  // init (streaming) - dynamically produces either producer or exchange streams
  // --------------------------------------------------------------------------
  protocol.exchange("init", {
    params: REQUEST_PARAMS_SCHEMA,
    inputSchema: dummyInputSchema,
    outputSchema: emptySchema,
    init: (params) => {
      // Preserve raw request IPC bytes for exchange reconstruction
      const requestIpcBytes = toUint8Array(params.request);
      const innerParams = unwrapRequest(params.request);
      const request = deserializeInitRequest(innerParams);
      const func = registry.get(request.bindCall.functionName, overloadContext(request.bindCall));

      const initResponse = func.globalInit(request);

      // For FINALIZE over HTTP, recover accumulated INPUT state from init_opaque_data.
      // The C++ extension passes the last exchange state token as init_opaque_data.
      const accumulatedState = recoverFinalizeState(request, config);

      const handlers = func.createStreamHandlers(request, initResponse, accumulatedState);

      // Initialize the appropriate handler
      let handlerState: HandlerState | undefined;
      if (handlers.producerInit) {
        handlerState = handlers.producerInit();
      } else if (handlers.exchangeInit) {
        handlerState = handlers.exchangeInit();
      }

      const isProducer = !!handlers.producerFn;

      // Extract mutable user state from handler (e.g. { remaining, currentIndex })
      // for serialization across HTTP exchanges. The processParams/infrastructure
      // is reconstructed fresh; only user state needs to persist.
      const userState = handlerState?.state ?? null;

      // Build state object with raw binary data (no base64/hex encoding).
      // The Arrow state serializer picks fields by name; live objects
      // (_handlers, _handlerState, _initResponse, __outputSchema) are ignored.
      const state: any = {
        functionName: request.bindCall.functionName,
        initRequestIpc: requestIpcBytes,
        executionId: initResponse.executionId,
        maxWorkers: Number(initResponse.maxWorkers),
        opaqueData: initResponse.opaqueData ?? null,
        isProducer,
        userState,
        __isProducer: isProducer,
        // Live Schemas for vgi-rpc to read during init (not serializable).
        // __inputSchema overrides dispatchStream's method.inputSchema per call
        // — the TS worker registers `init` as exchange with the permissive
        // `dummyInputSchema` sentinel; the real per-function input shape comes
        // from the bound handlers here.
        __outputSchema: handlers.outputSchema ?? emptySchema,
        __inputSchema: handlers.inputSchema ?? emptySchema,
        // Live objects for immediate use during init (producer mode).
        _handlers: handlers,
        _handlerState: handlerState,
        _initResponse: initResponse,
      };
      return state;
    },
    exchange: async (state, input, out) => {
      // Reconstruct live objects from serializable state.
      // This handles both immediate use (producer during init, where _handlers
      // is still present) and deserialized exchange (where we reconstruct).
      let handlers: StreamHandlers;
      let handlerState: HandlerState | undefined;

      if (state._handlers) {
        // Immediate use (same request, state still in memory)
        handlers = state._handlers;
        handlerState = state._handlerState;
      } else {
        // Deserialized from token — reconstruct from serializable refs.
        // Infrastructure (processParams, BoundStorage) is recreated fresh.
        // Mutable user state is merged from state.userState.
        const initRequestBatch = deserializeBatch(state.initRequestIpc);
        const initRequestDict = batchToScalarDict(initRequestBatch);
        const request = deserializeInitRequest(initRequestDict);
        const func = registry.get(state.functionName, overloadContext(request.bindCall));
        const executionId = state.executionId;
        const opaqueData = state.opaqueData ?? null;
        const initResponse: GlobalInitResponse = {
          executionId,
          maxWorkers: Number(state.maxWorkers ?? 1),
          opaqueData,
        };

        // Recover accumulated state for FINALIZE phase from initOpaqueData
        const recoveredState = recoverFinalizeState(request, config);

        handlers = func.createStreamHandlers(request, initResponse, recoveredState);
        if (handlers.producerInit) {
          handlerState = handlers.producerInit();
        } else if (handlers.exchangeInit) {
          handlerState = handlers.exchangeInit();
        }
        // Merge preserved user state (e.g. { remaining, currentIndex })
        if (state.userState != null && handlerState?.state !== undefined) {
          handlerState!.state = state.userState;
        }
      }

      if (state.isProducer && handlers.producerFn) {
        // Producer mode inside an exchange dispatch: patch finish() to bypass
        // OutputCollector's producerMode check (since dispatch created it
        // in exchange mode but we're actually producing).
        out.finish = () => { (out as any)._finished = true; };
        // Let the handler observe tick-batch metadata before producing. Used
        // by table functions to apply dynamic-filter updates DuckDB attaches
        // as `vgi_pushdown_filters` on each tick (Top-N heap tightening).
        if (handlers.onTick) {
          await handlers.onTick(handlerState, (input as any)?.metadata);
        }
        await handlers.producerFn(handlerState, out);
      } else if (handlers.exchangeFn) {
        await handlers.exchangeFn(handlerState, input, out);
      }

      // Save mutated user state for the next exchange round
      if (handlerState?.state !== undefined) {
        state.userState = handlerState!.state;
      }
    },
    headerSchema: initHeaderSchema,
    headerInit: (params: any, state: any, ctx: any) => {
      // During init, _initResponse is still in memory.
      // For exchange, this is never called (headers are only in init response).
      if (!state._initResponse) {
        throw new Error("headerInit called on deserialized state: _initResponse not available");
      }
      return serializeGlobalInitResponse(state._initResponse);
    },
  });

  // --------------------------------------------------------------------------
  // table_function_cardinality (unary)
  // --------------------------------------------------------------------------
  protocol.unary("table_function_cardinality", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const innerParams = unwrapRequest(params.request);
      const request = deserializeCardinalityRequest(innerParams);
      const func = registry.get(request.bindCall.functionName, overloadContext(request.bindCall));
      let cardResult: Record<string, any>;
      if (func.cardinality) {
        cardResult = serializeTableCardinality(func.cardinality(request));
      } else {
        cardResult = { estimate: null, max: null };
      }
      return wrapResult(cardResult, cardinalityInnerSchema);
    },
  });

  // --------------------------------------------------------------------------
  // Catalog methods
  // --------------------------------------------------------------------------
  registerCatalogMethods(protocol, catalogInterface, config.catalogName);

  return protocol;
}

// ============================================================================
// Catalog method registration
// ============================================================================

function registerCatalogMethods(
  protocol: Protocol,
  catalog: CatalogInterface | undefined,
  catalogName: string | undefined,
): void {
  function getCatalog(): CatalogInterface {
    if (!catalog) throw new NoCatalogError();
    return catalog;
  }

  // Response-schema aliases — sourced from generated/vgi-protocol-schemas.ts.
  // Note: the hand-written `catalogsResponseSchema` was List<Utf8>, which was
  // the original drift that motivated this codegen pipeline. The generated
  // schema is List<Binary> matching vgi-python's Protocol.
  const catalogsResponseSchema = CatalogCatalogsResultSchema;
  const attachResultInnerSchema = CatalogAttachResultSchema;
  const versionResponseSchema = CatalogVersionResultSchema;
  const transactionBeginResponseSchema = CatalogTransactionBeginResultSchema;
  const scanFunctionResponseSchema = ScanFunctionResultSchema;

  const emptyResult = new Schema([]);

  // Common param schemas
  const attachIdParam = new Schema([
    new Field("attach_id", new Binary(), true),
  ]);

  const attachIdTxnParams = new Schema([
    new Field("attach_id", new Binary(), true),
    new Field("transaction_id", new Binary(), true),
  ]);

  const attachIdNameTxnParams = new Schema([
    new Field("attach_id", new Binary(), true),
    new Field("name", new Utf8(), false),
    new Field("transaction_id", new Binary(), true),
  ]);

  const attachIdSchemaNameTxnParams = new Schema([
    new Field("attach_id", new Binary(), true),
    new Field("schema_name", new Utf8(), false),
    new Field("name", new Utf8(), false),
    new Field("transaction_id", new Binary(), true),
  ]);

  // catalog_catalogs
  protocol.unary("catalog_catalogs", {
    params: emptyResult,
    result: RESULT_BINARY_SCHEMA,
    handler: () => {
      const cat = getCatalog();
      // Each catalog name becomes an IPC-serialized CatalogInfo batch
      // {name, implementation_version?, data_version_spec?}. Matches the
      // vgi-python Protocol's CatalogsResponse wire shape.
      const items = cat.catalogs().map((name) => {
        const infoBatch = batchFromColumns(
          { name: [name], implementation_version: [null], data_version_spec: [null] },
          CatalogInfoSchema,
        );
        return serializeBatch(infoBatch);
      });
      return wrapResult({ items }, catalogsResponseSchema);
    },
  });

  // catalog_attach (params wrapped in request: Binary like bind/init)
  protocol.unary("catalog_attach", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const innerParams = unwrapRequest(params.request);
      const cat = getCatalog();
      const result = cat.attach(innerParams.name, innerParams.options);
      return wrapResult({
        attach_id: result.attachId,
        supports_transactions: result.supportsTransactions ?? false,
        supports_time_travel: result.supportsTimeTravel ?? false,
        catalog_version_frozen: result.catalogVersionFrozen ?? false,
        catalog_version: result.catalogVersion ?? 0,
        attach_id_required: result.attachIdRequired ?? true,
        default_schema: result.defaultSchema ?? "main",
        settings: result.settings ?? [],
        secret_types: result.secretTypes ?? [],
        comment: result.comment ?? null,
        tags: result.tags ? Object.entries(result.tags).map(([k, v]) => [k, v]) : [],
      }, attachResultInnerSchema);
    },
  });

  // catalog_detach
  protocol.unary("catalog_detach", {
    params: attachIdParam,
    result: emptyResult,
    handler: (params) => {
      const cat = getCatalog();
      cat.detach(toUint8Array(params.attach_id));
      return {};
    },
  });

  // catalog_create
  protocol.unary("catalog_create", {
    params: new Schema([
      new Field("name", new Utf8(), false),
      new Field("on_conflict", new Utf8(), false),
      new Field("options", new Binary(), true),
    ]),
    result: emptyResult,
    handler: (params) => {
      const cat = getCatalog();
      cat.create(params.name, params.on_conflict, params.options);
      return {};
    },
  });

  // catalog_drop
  protocol.unary("catalog_drop", {
    params: new Schema([new Field("name", new Utf8(), false)]),
    result: emptyResult,
    handler: (params) => {
      const cat = getCatalog();
      cat.drop(params.name);
      return {};
    },
  });

  // catalog_version
  protocol.unary("catalog_version", {
    params: attachIdTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const cat = getCatalog();
      return wrapResult({
        version: cat.version(
          toUint8Array(params.attach_id),
          params.transaction_id ? toUint8Array(params.transaction_id) : undefined
        ),
      }, versionResponseSchema);
    },
  });

  // catalog_transaction_begin
  protocol.unary("catalog_transaction_begin", {
    params: attachIdParam,
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const cat = getCatalog();
      return wrapResult({
        transaction_id: cat.transactionBegin(toUint8Array(params.attach_id)),
      }, transactionBeginResponseSchema);
    },
  });

  // catalog_transaction_commit
  protocol.unary("catalog_transaction_commit", {
    params: attachIdTxnParams,
    result: emptyResult,
    handler: (params) => {
      const cat = getCatalog();
      cat.transactionCommit(
        toUint8Array(params.attach_id),
        toUint8Array(params.transaction_id)
      );
      return {};
    },
  });

  // catalog_transaction_rollback
  protocol.unary("catalog_transaction_rollback", {
    params: attachIdTxnParams,
    result: emptyResult,
    handler: (params) => {
      const cat = getCatalog();
      cat.transactionRollback(
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
    handler: (params) => {
      const cat = getCatalog();
      const schemas = cat.schemas(
        toUint8Array(params.attach_id),
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: schemas.map((s) => s.serialize()),
      }, CatalogSchemasResultSchema);
    },
  });

  // catalog_schema_get
  protocol.unary("catalog_schema_get", {
    params: attachIdNameTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const cat = getCatalog();
      const info = cat.schemaGet(
        toUint8Array(params.attach_id),
        params.name,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: info ? [info.serialize()] : [],
      }, CatalogSchemaGetResultSchema);
    },
  });

  // catalog_schema_create
  protocol.unary("catalog_schema_create", {
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("name", new Utf8(), false),
      new Field("comment", new Utf8(), true),
      new Field("tags", new Binary(), true),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: emptyResult,
    handler: (params) => {
      const cat = getCatalog();
      cat.schemaCreate(
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
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("name", new Utf8(), false),
      new Field("ignore_not_found", new Bool(), true),
      new Field("cascade", new Bool(), true),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: emptyResult,
    handler: (params) => {
      const cat = getCatalog();
      cat.schemaDrop(
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
    handler: (params) => {
      const cat = getCatalog();
      const tables = cat.schemaContentsTables(
        toUint8Array(params.attach_id),
        params.name,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: tables.map((t) => t.serialize()),
      }, CatalogSchemaContentsTablesResultSchema);
    },
  });

  // catalog_schema_contents_views
  protocol.unary("catalog_schema_contents_views", {
    params: attachIdNameTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const cat = getCatalog();
      const views = cat.schemaContentsViews(
        toUint8Array(params.attach_id),
        params.name,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: views.map((v) => v.serialize()),
      }, CatalogSchemaContentsViewsResultSchema);
    },
  });

  // catalog_schema_contents_functions
  protocol.unary("catalog_schema_contents_functions", {
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("name", new Utf8(), false),
      new Field("type", new Utf8(), false),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const cat = getCatalog();
      const funcs = cat.schemaContentsFunctions(
        toUint8Array(params.attach_id),
        params.name,
        params.type,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: funcs.map((f) => f.serialize()),
      }, CatalogSchemaContentsFunctionsResultSchema);
    },
  });

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
        items: info ? [info.serialize()] : [],
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
    result: emptyResult,
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
    result: emptyResult,
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
      return wrapResult(scanResult, scanFunctionResponseSchema);
    },
  });

  // Helper schemas for table/view mutations with common patterns
  const schemaNameIgnoreNotFoundTxnParams = new Schema([
    new Field("attach_id", new Binary(), true),
    new Field("schema_name", new Utf8(), false),
    new Field("name", new Utf8(), false),
    new Field("ignore_not_found", new Bool(), true),
    new Field("transaction_id", new Binary(), true),
  ]);

  const schemaNameCommentParams = new Schema([
    new Field("attach_id", new Binary(), true),
    new Field("schema_name", new Utf8(), false),
    new Field("name", new Utf8(), false),
    new Field("comment", new Utf8(), true),
    new Field("ignore_not_found", new Bool(), true),
    new Field("transaction_id", new Binary(), true),
  ]);

  const schemaNameRenameParams = new Schema([
    new Field("attach_id", new Binary(), true),
    new Field("schema_name", new Utf8(), false),
    new Field("name", new Utf8(), false),
    new Field("new_name", new Utf8(), false),
    new Field("ignore_not_found", new Bool(), true),
    new Field("transaction_id", new Binary(), true),
  ]);

  const columnOpParams = new Schema([
    new Field("attach_id", new Binary(), true),
    new Field("schema_name", new Utf8(), false),
    new Field("name", new Utf8(), false),
    new Field("column_name", new Utf8(), false),
    new Field("ignore_not_found", new Bool(), true),
    new Field("transaction_id", new Binary(), true),
  ]);

  // catalog_table_comment_set
  protocol.unary("catalog_table_comment_set", {
    params: schemaNameCommentParams,
    result: emptyResult,
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
    result: emptyResult,
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
    result: emptyResult,
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
    result: emptyResult,
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
    result: emptyResult,
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
    result: emptyResult,
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
    result: emptyResult,
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
    result: emptyResult,
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
    result: emptyResult,
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
    result: emptyResult,
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

  // catalog_view_get
  protocol.unary("catalog_view_get", {
    params: attachIdSchemaNameTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const cat = getCatalog();
      const info = cat.viewGet(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: info ? [info.serialize()] : [],
      }, CatalogViewGetResultSchema);
    },
  });

  // catalog_view_create
  protocol.unary("catalog_view_create", {
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("schema_name", new Utf8(), false),
      new Field("name", new Utf8(), false),
      new Field("definition", new Utf8(), false),
      new Field("on_conflict", new Utf8(), false),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: emptyResult,
    handler: (params) => {
      const cat = getCatalog();
      cat.viewCreate(
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
    result: emptyResult,
    handler: (params) => {
      const cat = getCatalog();
      cat.viewDrop(
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
    result: emptyResult,
    handler: (params) => {
      const cat = getCatalog();
      cat.viewRename(
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
    result: emptyResult,
    handler: (params) => {
      const cat = getCatalog();
      cat.viewCommentSet(
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

  // catalog_macro_get
  protocol.unary("catalog_macro_get", {
    params: attachIdSchemaNameTxnParams,
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const cat = getCatalog();
      const info = cat.macroGet(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: info ? [info.serialize()] : [],
      }, CatalogMacroGetResultSchema);
    },
  });

  // catalog_macro_create
  protocol.unary("catalog_macro_create", {
    params: REQUEST_PARAMS_SCHEMA,
    result: emptyResult,
    handler: (params) => {
      const cat = getCatalog();
      const innerParams = unwrapRequest(params.request);
      cat.macroCreate(
        toUint8Array(innerParams.attach_id),
        innerParams.schema_name,
        innerParams.name,
        innerParams.macro_type as MacroType,
        innerParams.parameters ? (Array.isArray(innerParams.parameters) ? innerParams.parameters : [...innerParams.parameters]) : [],
        innerParams.definition,
        innerParams.on_conflict,
        innerParams.parameter_default_values ? toUint8Array(innerParams.parameter_default_values) : null,
        innerParams.transaction_id ? toUint8Array(innerParams.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_macro_drop
  protocol.unary("catalog_macro_drop", {
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("schema_name", new Utf8(), false),
      new Field("name", new Utf8(), false),
      new Field("ignore_not_found", new Bool(), true),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: emptyResult,
    handler: (params) => {
      const cat = getCatalog();
      cat.macroDrop(
        toUint8Array(params.attach_id),
        params.schema_name,
        params.name,
        params.ignore_not_found,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return {};
    },
  });

  // catalog_schema_contents_macros
  protocol.unary("catalog_schema_contents_macros", {
    params: new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("name", new Utf8(), false),
      new Field("type", new Utf8(), false),
      new Field("transaction_id", new Binary(), true),
    ]),
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const cat = getCatalog();
      const macros = cat.schemaContentsMacros(
        toUint8Array(params.attach_id),
        params.name,
        params.type,
        params.transaction_id ? toUint8Array(params.transaction_id) : undefined
      );
      return wrapResult({
        items: macros.map((m) => m.serialize()),
      }, CatalogSchemaContentsMacrosResultSchema);
    },
  });
}

