// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Handlers for the per-function lifecycle: bind, init (exchange), cardinality,
// statistics. Generic over scalar/table/table-in-out — each function kind
// implements VgiFunction.bind/globalInit/createStreamHandlers.

import { type VgiSchema, schema, type VgiField, field, type VgiDataType, binary, int64 } from "../../arrow/index.js";
import { Protocol, type AuthContext } from "@query-farm/vgi-rpc";
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
import {
  BindParamsSchema,
  BindResultSchema,
  InitParamsSchema,
  TableFunctionCardinalityParamsSchema,
  TableFunctionCardinalityResultSchema,
  TableFunctionDynamicToStringParamsSchema,
  TableFunctionDynamicToStringResultSchema,
  TableFunctionPlanParamsSchema,
  TableFunctionStatisticsParamsSchema,
} from "../../generated/vgi-protocol-schemas.js";
import {
  REQUEST_PARAMS_SCHEMA,
  RESULT_BINARY_SCHEMA,
  RESULT_BINARY_NULLABLE_SCHEMA,
  unwrapRequest,
  wrapResult,
  overloadContext,
  recoverFinalizeState,
} from "./shared.js";
import { GLOBAL_INIT_RESPONSE_SCHEMA } from "../serializers/init.js";
import { openAttach } from "./catalog/shared.js";
import { currentRequestAuth } from "../../request-auth.js";
import { batchFromColumns, serializeBatch } from "../../util/arrow/index.js";
import {
  deserializePlanRequest,
  fingerprintInputs,
  planResponseRow,
  scanSplitRow,
  PLAN_RESPONSE_SCHEMA,
  SCAN_SPLIT_SCHEMA,
  type PlanResult,
} from "../serializers/splits.js";
import {
  bindFingerprint,
  buildSplitToken,
  openSplitToken,
  splitAnchor,
} from "../../split-token.js";

export interface FunctionHandlerConfig {
  registry: FunctionRegistry;
  recoverExchangeState?: (opaqueData: Uint8Array) => any;
  signingKey?: Uint8Array;
  /**
   * Used to map a bind's `attach_opaque_data` to its catalog, so resolution can
   * be scoped when two catalogs declare the same schema and function name.
   * Optional — without it, resolution falls back to (schema, name).
   */
  catalogInterface?: {
    catalogNameForAttach(a: Uint8Array): string | null;
    /**
     * The catalog's current version, which is the consistency anchor every split
     * token is stamped with AND checked against.
     *
     * Both sides read it from here on purpose. Minting from a different value
     * than redemption compares against is not a subtle bug: it refuses every
     * token, and the documented response to SPLIT_SNAPSHOT_EXPIRED is "re-run
     * the query", which re-plans, mints the same mismatch and fails again — a
     * livelock returning no rows, blaming the data for moving when it has not.
     */
    version?(attach: Uint8Array, txn?: Uint8Array): number | Promise<number>;
  };
}

/**
 * The live anchor a split token must still name, or undefined when this worker's
 * catalog reports no version.
 *
 * Undefined means "skip the staleness check" rather than "fail" — a check that
 * cannot be made must not become a check that always fails, which would refuse
 * every split token on every unversioned catalog.
 */
async function liveSplitAnchor(
  catalogInterface: FunctionHandlerConfig["catalogInterface"],
  bindCall: { attach_opaque_data?: Uint8Array | null; transaction_opaque_data?: Uint8Array | null },
  auth: AuthContext | undefined,
  signingKey: Uint8Array | undefined,
): Promise<Uint8Array | undefined> {
  if (!catalogInterface?.version || bindCall.attach_opaque_data == null) return undefined;
  const raw = toUint8Array(bindCall.attach_opaque_data);
  const txn = bindCall.transaction_opaque_data
    ? toUint8Array(bindCall.transaction_opaque_data)
    : undefined;

  // The two call sites see the attach envelope at DIFFERENT processing stages —
  // `plan` receives it with the framework's 16-byte UUID prefix still attached,
  // `init` receives it already stripped — so neither form alone works for both.
  // Try each. Getting this wrong is not a degraded check but a total one: if
  // mint resolves a version and redemption does not (or vice versa) then every
  // token fails SPLIT_SNAPSHOT_EXPIRED, and the documented response to that is
  // "re-run the query", which re-plans and reproduces the same mismatch.
  // Each form is produced LAZILY, inside the try. Built as an array literal —
  // `[raw, await openAttach(...)]` — the unseal is evaluated before the loop
  // begins, so it throws from OUTSIDE the try and escapes the function instead
  // of falling through to the next candidate. `init` hands this function
  // already-stripped plaintext, which openAttach rejects, so the first form
  // never got a turn: every split scan died with OpaqueDataRejectedError, on
  // HTTP only (sealing is a pass-through when there is no signing key, which is
  // every non-HTTP transport).
  const forms: Array<() => Promise<Uint8Array>> = [
    async () => raw,
    async () => openAttach(raw, auth, signingKey),
  ];
  for (const form of forms) {
    try {
      return splitAnchor(await catalogInterface.version(await form(), txn));
    } catch {
      // try the next form
    }
  }
  // A catalog that cannot report a version means "skip the staleness check",
  // never "fail it" — a check that cannot be made must not become a check that
  // always fails, which would refuse every split token on every unversioned
  // catalog.
  return undefined;
}

/**
 * Open this request's split envelopes into `request.split_payloads`.
 *
 * `split_payloads` is DERIVED, not wire state: the tokens ride the request, the
 * payloads are what falls out of opening them. Only this layer holds the
 * signing key — a function does not — so redemption happens here and only the
 * payloads are handed down.
 *
 * It must run on continuation turns too. The HTTP exchange path rebuilds the
 * *wire* request from the cursor, which carries `split_tokens` but of course no
 * derived field, so a split-capable function used to see `splitPayloads:
 * undefined` on every turn after the first and could not tell that from "the
 * planner gave me no splits". Over HTTP that made every split scan fail on its
 * second turn; subprocess never noticed because it holds the handlers in memory
 * and never rebuilds anything.
 *
 * `checkAnchor` is false on a continuation, deliberately. The consistency
 * anchor answers "is this plan still redeemable", which is a question about
 * REDEMPTION — asked once, when the split is admitted. Re-asking it mid-stream
 * would let a catalog version bump kill an in-flight scan that had already been
 * accepted, turning a successful query into SPLIT_SNAPSHOT_EXPIRED halfway
 * through its rows.
 */
async function resolveSplitPayloads(
  request: any,
  innerParams: any,
  opts: {
    auth: any;
    signingKey: Uint8Array | undefined;
    catalogInterface: any;
    checkAnchor: boolean;
  },
): Promise<void> {
  if (!request.split_tokens) return;
  const fp = fingerprintInputs(toUint8Array(innerParams.bind_call));
  const expected = await bindFingerprint(
    fp.schemaName,
    fp.functionName,
    fp.args,
    fp.settings,
    new Uint8Array(0),
  );
  const currentAnchor = opts.checkAnchor
    ? await liveSplitAnchor(opts.catalogInterface, request.bind_call as any, opts.auth, opts.signingKey)
    : undefined;
  request.split_payloads = [];
  for (const token of request.split_tokens) {
    request.split_payloads.push(
      await openSplitToken(token, {
        signingKey: opts.signingKey,
        // Same principal `table_function_plan` sealed under. Omitting it opens
        // under the anonymous tail, so an authenticated caller's own tokens
        // fail authentication.
        auth: opts.auth,
        expectedFingerprint: expected,
        currentAnchor,
      }),
    );
  }
}

export function registerFunctionMethods(protocol: Protocol, config: FunctionHandlerConfig): void {
  const { registry, signingKey, catalogInterface } = config;

  // The framework mints every attach as uuid(16) || catalog_bytes (sealed on
  // HTTP, plaintext on subprocess). Function bodies — like catalog bodies —
  // must see the catalog's own bytes, so unseal (when keyed) and strip the
  // framework UUID prefix before the user function reads attach_opaque_data.
  // `ctx` is the CallContext when the dispatcher supplies one (every unary
  // method) and absent otherwise (stream init); `currentRequestAuth()` is the
  // ambient per-request fallback the HTTP entry point publishes, so both reach
  // the same principal. Getting an authenticated caller's identity wrong here
  // is not a degraded lookup but a hard `OpaqueDataRejectedError`: the envelope
  // is sealed under `attachAad(auth)`.
  async function stripAttach(attach: any, ctx: any): Promise<Uint8Array | null> {
    if (attach == null) return null;
    const env = toUint8Array(attach);
    if (env.length === 0) return env;
    return openAttach(env, ctx?.auth ?? currentRequestAuth(), signingKey);
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

  // Shared with the serializer that actually emits the header, so the
  // advertised shape and the emitted shape cannot drift apart.
  const initHeaderSchema = GLOBAL_INIT_RESPONSE_SCHEMA;

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
    params: BindParamsSchema,
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
    params: InitParamsSchema,
    inputSchema: dummyInputSchema,
    outputSchema: emptySchema,
    init: async (params, ctx?: any) => {
      // Preserve raw request IPC bytes for exchange reconstruction
      const requestIpcBytes = toUint8Array(params.request);
      const innerParams = unwrapRequest(params.request);
      const request = deserializeInitRequest(innerParams);
      // The caller's identity, and the load-bearing line of this handler.
      //
      // vgi-rpc types stream init as `(params) => state`, so unlike every unary
      // method there is no CallContext to read `auth` off — `ctx` is only ever
      // populated by a caller that has one (the in-process client). The HTTP
      // entry point therefore publishes the dispatching principal on an ambient
      // per-request scope, the same shape vgi-python's `current_auth()`
      // ContextVar and vgi-go's `handleInit(callCtx)` give those ports.
      //
      // It has to be the REAL principal, not `undefined`: `attach_opaque_data`
      // is sealed by `catalog_attach` under `attachAad(auth)` and every split
      // token by `table_function_plan` under `splitTokenAad(body, auth)`, both
      // of which are unary and so seal under the authenticated caller. Opening
      // them here under the anonymous tail matches only for an anonymous
      // caller; an authenticated one got `OpaqueDataRejectedError:
      // attach_opaque_data not recognized` on its first scan, i.e. this worker
      // could not serve authenticated traffic at all.
      const auth = ctx?.auth ?? currentRequestAuth();
      request.bind_call.attach_opaque_data = await stripAttach(
        request.bind_call.attach_opaque_data,
        ctx,
      );

      // Verify and strip the split envelopes BEFORE any user code runs, so an
      // unverified token can never be acted on.
      await resolveSplitPayloads(request, innerParams, {
        auth,
        signingKey,
        catalogInterface,
        checkAnchor: true,
      });

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
        // Re-derive the split payloads. The cursor carries the wire request,
        // which has `split_tokens` but not the opened `split_payloads` — that
        // field only ever existed in memory on the init turn. Without this a
        // split-capable function sees `splitPayloads: undefined` from the
        // second turn onward, indistinguishable from "the planner gave me no
        // splits at all". checkAnchor is false: redemption was already decided
        // on the init turn, and re-checking here would let a mid-scan catalog
        // bump fail a query that had already been admitted.
        // fingerprintInputs hashes the RAW bind_call bytes, so pass the
        // pre-deserialization blob from the cursor — handing it the decoded
        // object hashes a different shape and every token fails as
        // SPLIT_TOKEN_INVALID "minted for a different bind".
        await resolveSplitPayloads(request, { bind_call: initRequestDict.bind_call }, {
          // Same principal the tokens were sealed under. The exchange dispatch
          // gets no ctx (see stripAttach above), so the request-scoped identity
          // is the only source — opening under the anonymous tail would fail
          // authentication for every authenticated caller's own tokens.
          auth: currentRequestAuth(),
          signingKey,
          catalogInterface,
          checkAnchor: false,
        });
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
    params: TableFunctionCardinalityParamsSchema,
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
  // table_function_plan (unary)
  // --------------------------------------------------------------------------
  // Divide a scan into named, independently redeemable splits, so a distributed
  // engine can retry a task without re-reading or skipping rows.
  //
  // A function that declares no `plan` hook gets the framework default: a SINGLE
  // empty-payload split, which is what "not split-capable" means — the whole scan
  // is one unit of work. That keeps every existing worker serving unchanged under
  // protocol 1.4.0, so splits stay opt-in rather than something every worker must
  // now implement.
  protocol.unary("table_function_plan", {
    params: TableFunctionPlanParamsSchema,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params, ctx?: any) => {
      const innerParams = unwrapRequest(params.request);
      const request = deserializePlanRequest(innerParams);
      const func = registry.get(
        request.bind_call.function_name,
        await strippedContext(request.bind_call),
      );

      const plan: PlanResult = func.plan
        ? await func.plan(request)
        : { splits: [{ payload: new Uint8Array(0) }] };

      // The framework stamps every token: an author cannot forget the
      // consistency anchor, cannot mis-bind the fingerprint, and never writes
      // crypto — and the envelope stays a private implementation detail whose
      // layout can change without touching worker code in five languages.
      const fp = fingerprintInputs(toUint8Array(innerParams.bind_call));
      const fingerprint = await bindFingerprint(
        fp.schemaName,
        fp.functionName,
        fp.args,
        fp.settings,
        // projection_ids is not a bind-call field (it rides the init request),
        // so it feeds in empty — matching the reference implementation, which
        // reads it off the bind call and likewise finds nothing.
        new Uint8Array(0),
      );
      // A worker that names its version is taken at its word — it knows which
      // snapshot it planned against. One that leaves it unset gets the LIVE
      // version, never 0: minting 0 while redemption compares against the live
      // counter refuses every token, and is invisible on a catalog whose version
      // happens to be 0, which is most fixtures.
      const anchor =
        plan.catalogVersion != null
          ? splitAnchor(plan.catalogVersion)
          : ((await liveSplitAnchor(catalogInterface, request.bind_call as any, ctx?.auth, signingKey)) ??
            splitAnchor(0));

      const blobs: Uint8Array[] = [];
      for (const split of plan.splits) {
        const token = await buildSplitToken({
          payload: split.payload,
          fingerprint,
          anchor,
          signingKey,
          // Bind the caller. Without it every token seals under the anonymous
          // identity, so one tenant's splits are replayable by another — the
          // exact replay the AAD exists to stop.
          auth: ctx?.auth,
        });
        blobs.push(
          serializeBatch(
            batchFromColumns(
              Object.fromEntries(
                SCAN_SPLIT_SCHEMA.fields.map((f) => [
                  f.name,
                  [scanSplitRow(split, token)[f.name] ?? null],
                ]),
              ),
              SCAN_SPLIT_SCHEMA,
            ),
          ),
        );
      }

      return wrapResult(planResponseRow(plan, blobs), PLAN_RESPONSE_SCHEMA);
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
    params: TableFunctionStatisticsParamsSchema,
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
    params: TableFunctionDynamicToStringParamsSchema,
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
