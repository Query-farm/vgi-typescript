// Handlers for the per-function lifecycle: bind, init (exchange), cardinality,
// statistics. Generic over scalar/table/table-in-out — each function kind
// implements VgiFunction.bind/globalInit/createStreamHandlers.

import { Schema, Field, Binary, Int64 } from "@query-farm/apache-arrow";
import { Protocol } from "vgi-rpc";
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
import { batchToScalarDict, deserializeBatch } from "../../util/arrow/index.js";
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

export interface FunctionHandlerConfig {
  registry: FunctionRegistry;
  recoverExchangeState?: (opaqueData: Uint8Array) => any;
}

export function registerFunctionMethods(protocol: Protocol, config: FunctionHandlerConfig): void {
  const { registry } = config;

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
      const func = registry.get(request.function_name, overloadContext(request));
      const response = func.bind(request);
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
    init: (params) => {
      // Preserve raw request IPC bytes for exchange reconstruction
      const requestIpcBytes = toUint8Array(params.request);
      const innerParams = unwrapRequest(params.request);
      const request = deserializeInitRequest(innerParams);
      const func = registry.get(request.bind_call.function_name, overloadContext(request.bind_call));

      const initResponse = func.globalInit(request);

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
        const func = registry.get(state.functionName, overloadContext(request.bind_call));
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
    handler: (params) => {
      const innerParams = unwrapRequest(params.request);
      const request = deserializeCardinalityRequest(innerParams);
      const func = registry.get(request.bind_call.function_name, overloadContext(request.bind_call));
      let cardResult: Record<string, any>;
      if (func.cardinality) {
        cardResult = serializeTableCardinality(func.cardinality(request));
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
    handler: (params) => {
      const innerParams = unwrapRequest(params.request);
      const request = deserializeCardinalityRequest(innerParams);
      const func = registry.get(request.bind_call.function_name, overloadContext(request.bind_call));
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
    handler: (params) => {
      const innerParams = unwrapRequest(params.request);
      const bindCallBytes = toUint8Array(innerParams.bind_call);
      const bindCallBatch = deserializeBatch(bindCallBytes);
      const bindParams: Record<string, any> = {};
      for (const field of bindCallBatch.schema.fields) {
        const col = bindCallBatch.getChild(field.name);
        bindParams[field.name] = col ? col.get(0) : null;
      }
      const bindCall = deserializeBindRequest(bindParams);
      const globalExecutionId = toUint8Array(innerParams.global_execution_id);
      const bindOpaqueData = innerParams.bind_opaque_data
        ? toUint8Array(innerParams.bind_opaque_data)
        : null;
      const func = registry.get(bindCall.function_name, overloadContext(bindCall));
      const map = func.dynamicToString
        ? func.dynamicToString({ bindCall, bindOpaqueData, globalExecutionId })
        : {};
      const keys = Object.keys(map);
      const values = keys.map((k) => map[k] ?? "");
      return wrapResult({ keys, values }, TableFunctionDynamicToStringResultSchema);
    },
  });
}
