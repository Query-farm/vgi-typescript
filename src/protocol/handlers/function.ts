// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Handlers for the per-function lifecycle: bind, init (exchange), cardinality,
// statistics. Generic over scalar/table/table-in-out — each function kind
// implements VgiFunction.bind/globalInit/createStreamHandlers.

import { type VgiSchema, schema, type VgiField, field, type VgiDataType, binary, int64 } from "../../arrow/index.js";
import { Protocol } from "@query-farm/vgi-rpc";
import type { FunctionRegistry } from "../../functions/registry.js";
import type { StreamHandlers, HandlerState } from "../../functions/types.js";
import {
  deserializeBindRequest,
  serializeBindResponse,
  deserializeInitRequest,
  serializeGlobalInitResponse,
  deserializeCardinalityRequest,
  serializeTableCardinality,
} from "../serialize.js";
import type { GlobalInitResponse } from "../types.js";
import { batchToScalarDict, deserializeBatch, adoptArrowJsShape } from "../../util/arrow/index.js";
import { toUint8Array } from "../../util/bytes.js";
import { serializeColumnStatistics } from "../../util/statistics.js";
import { BindResultSchema, TableFunctionCardinalityResultSchema, TableFunctionDynamicToStringResultSchema } from "../../generated/vgi-protocol-schemas.js";
import {
  REQUEST_PARAMS_SCHEMA,
  RESULT_BINARY_SCHEMA,
  RESULT_BINARY_NULLABLE_SCHEMA,
  unwrapRequest,
  wrapResult,
  overloadContext,
  recoverFinalizeState,
} from "./shared.js";
import { openAttach } from "./catalog/shared.js";

export interface FunctionHandlerConfig {
  registry: FunctionRegistry;
  recoverExchangeState?: (opaqueData: Uint8Array) => any;
  signingKey?: Uint8Array;
  /**
   * Used to map a bind's `attach_opaque_data` to its catalog, so resolution can
   * be scoped when two catalogs declare the same schema and function name.
   * Optional — without it, resolution falls back to (schema, name).
   */
  catalogInterface?: { catalogNameForAttach(a: Uint8Array): string | null };
}

export function registerFunctionMethods(protocol: Protocol, config: FunctionHandlerConfig): void {
  const { registry, signingKey, catalogInterface } = config;

  // The framework mints every attach as uuid(16) || catalog_bytes (sealed on
  // HTTP, plaintext on subprocess). Function bodies — like catalog bodies —
  // must see the catalog's own bytes, so unseal (when keyed) and strip the
  // framework UUID prefix before the user function reads attach_opaque_data.
  async function stripAttach(attach: any, ctx: any): Promise<Uint8Array | null> {
    if (attach == null) return null;
    const env = toUint8Array(attach);
    if (env.length === 0) return env;
    return openAttach(env, ctx?.auth, signingKey);
  }

  // Build an overload context whose attach has been unsealed, so catalog
  // resolution sees the real route byte. The bind/init handlers strip their
  // request in place and can call overloadContext directly; every other site
  // still holds the sealed envelope (73 bytes on HTTP vs 16 plaintext), whose
  // leading byte would otherwise route to an arbitrary catalog.
  async function strippedContext(bindCall: any, ctx?: any) {
    let attach: Uint8Array | null = null;
    try {
      attach = await stripAttach(bindCall.attach_opaque_data, ctx);
    } catch {
      attach = null; // Unopenable (wrong key / not sealed) — resolve unscoped.
    }
    return overloadContext({ ...bindCall, attach_opaque_data: attach }, catalogInterface);
  }

  const initHeaderSchema = schema([
    field("execution_id", binary(), false),
    field("opaque_data", binary(), true),
    field("max_workers", int64(), false),
  ]);

  const emptySchema = schema([]);

  // Dummy non-empty schema for the init exchange registration.
  // The dispatch determines producer vs exchange based on inputSchema emptiness.
  // We need exchange mode so that input batches are passed to our callback.
  // The actual input schema comes from the IPC stream, not this registration.
  const dummyInputSchema = schema([
    field("_tick", binary(), true),
  ]);

  // --------------------------------------------------------------------------
  // bind (unary)
  // --------------------------------------------------------------------------
  protocol.unary("bind", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params, ctx) => {
      const innerParams = unwrapRequest(params.request);
      const request = deserializeBindRequest(innerParams);
      request.attach_opaque_data = await stripAttach(request.attach_opaque_data, ctx);
      const func = registry.get(request.function_name, overloadContext(request, catalogInterface));
      const response = await func.bind(request);
      const serialized = serializeBindResponse(response);
      return wrapResult(serialized, BindResultSchema);
    },
  });

  // --------------------------------------------------------------------------
  // init (streaming) - dynamically produces either producer or exchange streams
  // --------------------------------------------------------------------------
  protocol.exchange("init", {
    params: REQUEST_PARAMS_SCHEMA,
    inputSchema: dummyInputSchema,
    outputSchema: emptySchema,
    init: async (params) => {
      // Preserve raw request IPC bytes for exchange reconstruction
      const requestIpcBytes = toUint8Array(params.request);
      const innerParams = unwrapRequest(params.request);
      const request = deserializeInitRequest(innerParams);
      // Exchange init has no ctx; on subprocess (no signing key) openAttach is a
      // pass-through strip that needs no auth. HTTP carries auth via the bind path.
      request.bind_call.attach_opaque_data = await stripAttach(request.bind_call.attach_opaque_data, undefined);
      const func = registry.get(request.bind_call.function_name, overloadContext(request.bind_call, catalogInterface));

      // globalInit is async — table function onInit may touch HTTP-backed
      // FunctionStorage (e.g. Cloudflare DO).
      const initResponse = await func.globalInit(request);

      // For FINALIZE over HTTP, recover accumulated INPUT state from init_opaque_data.
      // The C++ extension passes the last exchange state token as init_opaque_data.
      const accumulatedState = recoverFinalizeState(request, config.recoverExchangeState);

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
        functionName: request.bind_call.function_name,
        initRequestIpc: requestIpcBytes,
        executionId: initResponse.execution_id,
        maxWorkers: Number(initResponse.max_workers),
        opaqueData: initResponse.opaque_data ?? null,
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
      // `input` is the one batch worker code sees that this package did not
      // decode — vgi-rpc read it off the wire with its own Arrow backend. If
      // that resolved to a second copy of @query-farm/flechette (nested under
      // vgi-rpc whenever the version ranges disagree, even transiently), the
      // facade's prototype patches never touched its classes and user code
      // gets `col.isValid is not a function` on the first exchange round.
      // Adopting here is a WeakSet probe per call and a no-op on arrow-js.
      input = adoptArrowJsShape(input);
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
        const func = registry.get(state.functionName, await strippedContext(request.bind_call));
        const executionId = state.executionId;
        const opaqueData = state.opaqueData ?? null;
        const initResponse: GlobalInitResponse = {
          execution_id: executionId,
          max_workers: Number(state.maxWorkers ?? 1),
          opaque_data: opaqueData,
        };

        // Recover accumulated state for FINALIZE phase from initOpaqueData
        const recoveredState = recoverFinalizeState(request, config.recoverExchangeState);

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
    handler: async (params) => {
      const innerParams = unwrapRequest(params.request);
      const request = deserializeCardinalityRequest(innerParams);
      const func = registry.get(request.bind_call.function_name, await strippedContext(request.bind_call));
      let cardResult: Record<string, any>;
      if (func.cardinality) {
        cardResult = serializeTableCardinality(await func.cardinality(request));
      } else {
        cardResult = { estimate: null, max: null };
      }
      return wrapResult(cardResult, TableFunctionCardinalityResultSchema);
    },
  });

  // --------------------------------------------------------------------------
  // table_function_statistics (unary)
  // --------------------------------------------------------------------------
  // Returns bytes-or-null result: serialized ColumnStatistics RecordBatch when
  // the function declared a statistics() hook and it produced a non-empty
  // list, else null. DuckDB uses the bounds for plan-time filter elimination
  // (folds impossible filters to EMPTY_RESULT).
  protocol.unary("table_function_statistics", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_NULLABLE_SCHEMA,
    handler: async (params) => {
      const innerParams = unwrapRequest(params.request);
      const request = deserializeCardinalityRequest(innerParams);
      const func = registry.get(request.bind_call.function_name, await strippedContext(request.bind_call));
      if (!func.statistics) return { result: null };
      const stats = func.statistics(request);
      if (!stats || stats.length === 0) return { result: null };
      return { result: serializeColumnStatistics(stats) };
    },
  });

  // --------------------------------------------------------------------------
  // table_function_dynamic_to_string (unary)
  // --------------------------------------------------------------------------
  // DuckDB calls this once per parallel scan thread at FinishSource. The
  // result is a List<Utf8>/List<Utf8> pair carrying ordered key→value
  // diagnostics that surface under EXPLAIN ANALYZE alongside the
  // intrinsics (Function, Rows Read, Threads). When the function doesn't
  // declare dynamicToString, return empty maps so the C++ side falls back
  // to intrinsics only.
  protocol.unary("table_function_dynamic_to_string", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const innerParams = unwrapRequest(params.request);
      const bindCallBytes = toUint8Array(innerParams.bind_call);
      const bindCallBatch = deserializeBatch(bindCallBytes);
      // Single-row bind_call batch -> dict via the codec/canonical path.
      const bindParams = batchToScalarDict(bindCallBatch);
      const bindCall = deserializeBindRequest(bindParams);
      const globalExecutionId = toUint8Array(innerParams.global_execution_id);
      const bindOpaqueData = innerParams.bind_opaque_data
        ? toUint8Array(innerParams.bind_opaque_data)
        : null;
      const func = registry.get(bindCall.function_name, await strippedContext(bindCall));
      const map = func.dynamicToString
        ? await func.dynamicToString({ bindCall, bindOpaqueData, globalExecutionId })
        : {};
      const keys = Object.keys(map);
      const values = keys.map((k) => map[k] ?? "");
      return wrapResult({ keys, values }, TableFunctionDynamicToStringResultSchema);
    },
  });
}
