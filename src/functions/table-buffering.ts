// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Table sink+source ("table_buffering") function implementation.
//
// Three phases, mirroring the C++ PhysicalVgiTableBuffering Sink+Source
// operator:
//   1. Sink   — process(batch, params) -> state_id (bytes), one per input
//               batch. Driven by the unary `table_buffering_process` RPC.
//   2. Combine — combine(state_ids, params) -> finalize_state_ids (bytes[]),
//               once on the coordinator. Driven by `table_buffering_combine`.
//   3. Source  — finalize(params, fid, state, out) producer-mode streaming,
//               one batch per tick. Driven by an init() stream with
//               phase=TABLE_BUFFERING_FINALIZE.
//
// CROSS-PROCESS INVARIANT: state written in process() that finalize() reads
// MUST live in cross-process storage scoped by execution_id (BoundStorage).
// The Source phase may route a finalize_state_id to a worker process that
// did NOT run the corresponding process() calls.

import {
  type VgiSchema,
  schema,
  type VgiDataType,
  type VgiBatch,
  nullType,
} from "../arrow/index.js";
import type { OutputCollector } from "@query-farm/vgi-rpc";
import { DEFAULT_MAX_WORKERS, TableInOutPhase } from "../types.js";
import type {
  BindRequest,
  BindResponse,
  InitRequest,
  GlobalInitResponse,
  TableFunctionCardinalityRequest,
} from "../protocol/types.js";
import type { TableCardinality } from "../types.js";
import type {
  VgiFunction,
  FunctionMeta,
  StreamHandlers,
  FunctionExample,
} from "./types.js";
import type { ArgumentSpec } from "../arguments/argument-spec.js";
import { batchToScalarDict, batchToSecretDict, projectSchema } from "../util/arrow/index.js";
import { batchFromColumns, readCanonicalValue } from "../arrow/index.js";
import { codecFor } from "../arrow/codec/registry.js";
import {
  buildJoinKeysLookup,
  deserializeFilters,
  FilteringOutputCollector,
  type PushdownFilters,
} from "../filter-pushdown/index.js";
import { FunctionStability } from "../types.js";
import { BoundStorage, storage as defaultStorage, FrameworkNS } from "./storage.js";
import {
  serializeInitRequest,
  deserializeInitRequest,
  serializeGlobalInitResponse,
} from "../protocol/serialize.js";
import { serializeBatch, deserializeBatch } from "../util/arrow/index.js";

// ============================================================================
// Parameter bundles
// ============================================================================

export interface TableBufferingBindParams<TArgs = Record<string, any>> {
  args: TArgs;
  bindCall: BindRequest;
  settings: Record<string, any>;
  secrets: Record<string, Record<string, any>>;
}

export interface TableBufferingParams<TArgs = Record<string, any>> {
  args: TArgs;
  initCall: InitRequest;
  outputSchema: VgiSchema;
  settings: Record<string, any>;
  secrets: Record<string, Record<string, any>>;
  /** Shared cross-process storage scoped by execution_id. */
  storage: BoundStorage;
  /** Stable across coordinator + secondary workers for one DuckDB execution. */
  executionId: Uint8Array;
  /** Catalog attach identity (plaintext bytes). */
  attachId: Uint8Array;
  /** Hex/raw VGI transaction id, or null. */
  transactionId: Uint8Array | null;
  function_name: string;
  /** Per-chunk batch_index when requiresInputBatchIndex=true; else null. */
  batchIndex: number | null;
  /** In-band log sink for the unary process()/combine() RPCs. */
  clientLog: (level: string, message: string) => void;
}

// ============================================================================
// Functional API config
// ============================================================================

export interface TableBufferingConfig<
  TArgs = Record<string, any>,
  TState = any,
> {
  name: string;
  description?: string;
  args?: Record<string, VgiDataType>;
  namedArgs?: Record<string, VgiDataType>;
  argDefaults?: Record<string, any>;
  /** Bind: default passes through input schema. May be async. */
  onBind?: (params: TableBufferingBindParams<TArgs>) =>
    | { outputSchema: VgiSchema; opaqueData?: Uint8Array }
    | Promise<{ outputSchema: VgiSchema; opaqueData?: Uint8Array }>;
  /** Sink: ingest one batch, return an opaque state_id. */
  process: (
    batch: VgiBatch,
    params: TableBufferingParams<TArgs>,
  ) => Uint8Array | Promise<Uint8Array>;
  /** Combine: group/merge state_ids, return finalize_state_ids. */
  combine: (
    stateIds: Uint8Array[],
    params: TableBufferingParams<TArgs>,
  ) => Uint8Array[] | Promise<Uint8Array[]>;
  /** Build the initial per-tick finalize state for a finalize_state_id. */
  initialFinalizeState?: (
    finalizeStateId: Uint8Array,
    params: TableBufferingParams<TArgs>,
  ) => TState | Promise<TState>;
  /** Source tick: emit one batch via out.emit / signal EOS via out.finish. */
  finalize: (
    params: TableBufferingParams<TArgs>,
    finalizeStateId: Uint8Array,
    state: TState,
    out: OutputCollector,
  ) => void | Promise<void>;
  cardinality?: (
    params: TableBufferingBindParams<TArgs>,
  ) => TableCardinality | Promise<TableCardinality>;
  // Metadata
  projectionPushdown?: boolean;
  filterPushdown?: boolean;
  autoApplyFilters?: boolean;
  /** Force ParallelSink=false in the C++ operator (single-thread ingest). */
  sinkOrderDependent?: boolean;
  /** Force serial Source drain in finalize_queue order. */
  sourceOrderDependent?: boolean;
  /** Thread DuckDB's per-chunk batch_index into every process() call. */
  requiresInputBatchIndex?: boolean;
  stability?: FunctionStability;
  examples?: FunctionExample[];
  categories?: string[];
  tags?: Record<string, string>;
  maxWorkers?: number;
  requiredSettings?: string[];
  requiredSecrets?: string[];
}

/**
 * A table_buffering VgiFunction also carries its callback config so the
 * protocol's unary handlers (process/combine/destructor) can reach it.
 */
export interface TableBufferingVgiFunction extends VgiFunction {
  bufferingConfig: TableBufferingConfig<any, any>;
  bufferingExtractArgs: (request: BindRequest) => any;
}

export function defineTableBufferingFunction<
  TArgs = Record<string, any>,
  TState = any,
>(config: TableBufferingConfig<TArgs, TState>): TableBufferingVgiFunction {
  if (config.sinkOrderDependent && config.requiresInputBatchIndex) {
    throw new Error(
      `${config.name}: sinkOrderDependent and requiresInputBatchIndex are ` +
        `mutually exclusive — single-thread sink already orders input.`,
    );
  }

  const specs: ArgumentSpec[] = [];
  let posIdx = 0;
  if (config.args) {
    for (const [name, type] of Object.entries(config.args)) {
      specs.push({ name, position: posIdx++, arrowType: type });
    }
  }
  specs.push({
    name: "data",
    position: posIdx++,
    arrowType: nullType(),
    isTableInput: true,
  });
  if (config.namedArgs) {
    for (const [name, type] of Object.entries(config.namedArgs)) {
      specs.push({ name, position: name, arrowType: type });
    }
  }

  const meta: FunctionMeta = {
    name: config.name,
    description: config.description,
    stability: config.stability,
    projectionPushdown: config.projectionPushdown,
    filterPushdown: config.filterPushdown,
    autoApplyFilters: config.autoApplyFilters,
    examples: config.examples,
    categories: config.categories,
    tags: config.tags,
    maxWorkers: config.maxWorkers,
    requiredSettings: config.requiredSettings,
    requiredSecrets: config.requiredSecrets,
    // The Source phase always runs (it's how output is produced), so a
    // table_buffering function always advertises has_finalize.
    hasFinalize: true,
    sinkOrderDependent: config.sinkOrderDependent,
    sourceOrderDependent: config.sourceOrderDependent,
    requiresInputBatchIndex: config.requiresInputBatchIndex,
  };

  function extractArgs(request: BindRequest): TArgs {
    const args: Record<string, any> = {};
    for (const spec of specs) {
      if (spec.isTableInput) continue;
      const defaultVal =
        config.argDefaults?.[spec.name] !== undefined
          ? config.argDefaults[spec.name]
          : undefined;
      let val = request.arguments.get(spec.position, defaultVal);
      if (typeof val === "bigint") val = Number(val);
      args[spec.name] = val;
    }
    return args as TArgs;
  }

  const func: TableBufferingVgiFunction = {
    kind: "table_buffering" as any,
    meta,
    argumentSpecs: specs,
    bufferingConfig: config as TableBufferingConfig<any, any>,
    bufferingExtractArgs: extractArgs as any,

    async bind(request: BindRequest): Promise<BindResponse> {
      if (config.onBind) {
        const args = extractArgs(request);
        const settings = batchToScalarDict(request.settings);
        const secrets = batchToSecretDict(request.secrets);
        const result = await config.onBind({ args, bindCall: request, settings, secrets });
        return { output_schema: result.outputSchema, opaque_data: result.opaqueData ?? null };
      }
      return {
        output_schema: request.input_schema ?? schema([]),
        opaque_data: null,
      };
    },

    async globalInit(request: InitRequest): Promise<GlobalInitResponse> {
      // Mint a stable execution_id for the primary; secondary inits reuse it.
      let executionId: Uint8Array;
      if (request.execution_id) {
        executionId = request.execution_id;
      } else {
        executionId = new Uint8Array(16);
        crypto.getRandomValues(executionId);
      }
      const response: GlobalInitResponse = {
        max_workers: config.maxWorkers ?? DEFAULT_MAX_WORKERS,
        execution_id: executionId,
        opaque_data: null,
      };

      // On the sink-side init, persist init metadata so any pool worker can
      // cold-load it when serving subsequent process()/combine() unary RPCs
      // (which carry no init context themselves). Mirrors Python's
      // FrameworkNS.BUFFERING_INIT state_put.
      if (request.phase === TableInOutPhase.TABLE_BUFFERING) {
        const bound = new BoundStorage(defaultStorage, executionId);
        await bound.statePut(
          FrameworkNS.BUFFERING_INIT,
          BoundStorage.packIntKey(-1),
          encodeBufferingInit(request, response),
        );
      }
      return response;
    },

    createStreamHandlers(
      request: InitRequest,
      response: GlobalInitResponse,
      _accumulatedState?: any,
    ): StreamHandlers {
      const args = extractArgs(request.bind_call);
      const settings = batchToScalarDict(request.bind_call.settings);
      const secrets = batchToSecretDict(request.bind_call.secrets);

      const projIds = request.projection_ids && meta.projectionPushdown
        ? request.projection_ids
        : null;
      const outputSchema = projIds
        ? projectSchema(projIds, request.output_schema)
        : request.output_schema;

      const joinKeysLookup = buildJoinKeysLookup(request.join_keys);
      const pushdownFilters = request.pushdown_filters
        ? deserializeFilters(request.pushdown_filters, joinKeysLookup)
        : undefined;

      const bound = new BoundStorage(
        defaultStorage,
        response.execution_id ?? new Uint8Array(16),
      );

      const params: TableBufferingParams<TArgs> = {
        args,
        initCall: request,
        outputSchema,
        settings,
        secrets,
        storage: bound,
        executionId: response.execution_id,
        attachId: request.bind_call.attach_opaque_data ?? new Uint8Array(0),
        transactionId: request.bind_call.transaction_opaque_data ?? null,
        function_name: request.bind_call.function_name,
        batchIndex: null,
        clientLog: () => {},
      };

      if (request.phase === TableInOutPhase.TABLE_BUFFERING_FINALIZE) {
        // SOURCE phase: producer-mode streaming, one batch per tick.
        const finalizeStateId = request.finalize_state_id ?? new Uint8Array(0);
        return {
          outputSchema,
          producerInit: () => ({ state: { __tbf_uninit: true } as any }),
          producerFn: async (
            pState: { state: any },
            out: OutputCollector,
          ) => {
            // Lazily build the user's per-tick state on the first tick.
            // pState.state round-trips through the HTTP exchange-state token
            // between ticks; `__tbf_uninit` marks "not yet initialized".
            let userState: any = pState.state;
            if (userState && userState.__tbf_uninit) {
              userState = config.initialFinalizeState
                ? await config.initialFinalizeState(finalizeStateId, params)
                : null;
              pState.state = userState ?? null;
            } else {
              userState = pState.state;
            }

            // Narrow each emitted (full-width) buffered batch to the projected
            // output schema, then auto-apply pushdown filters. Mirrors the
            // streaming TableFunctionGenerator path: project first, filter
            // second.
            let wrappedOut: OutputCollector = projIds
              ? makeProjectingCollector(out, outputSchema)
              : out;
            if (config.autoApplyFilters && pushdownFilters) {
              wrappedOut = new FilteringOutputCollector(wrappedOut, pushdownFilters) as unknown as OutputCollector;
            }

            await config.finalize(params, finalizeStateId, userState, wrappedOut);

            // Persist the (possibly mutated) user state for the next tick.
            pState.state = userState ?? null;
          },
        };
      }

      // SINK-init phase (TABLE_BUFFERING): the C++ side opens the init stream
      // in exchange mode, immediately sends EOS, and drains output to EOS.
      // We produce nothing — the real sink traffic is the separate unary
      // table_buffering_process / _combine RPCs.
      return {
        outputSchema,
        inputSchema: request.bind_call.input_schema ?? undefined,
        exchangeInit: () => ({ state: null }),
        exchangeFn: async (
          _state: any,
          _input: VgiBatch,
          _out: OutputCollector,
        ) => {
          // No-op: no per-batch output on the sink-init stream.
        },
      };
    },
  };

  if (config.cardinality) {
    func.cardinality = async (request: TableFunctionCardinalityRequest) => {
      const args = extractArgs(request.bind_call);
      const settings = batchToScalarDict(request.bind_call.settings);
      const secrets = batchToSecretDict(request.bind_call.secrets);
      return config.cardinality!({ args, bindCall: request.bind_call, settings, secrets });
    };
  }

  return func;
}

// Wrap an OutputCollector so each emitted batch is projected-by-name to the
// target (narrowed) output schema before forwarding. Mirrors table.ts's
// makeProjectingCollector — buffered batches are stored full-width, so the
// Source phase narrows them per-tick to the projected columns.
function makeProjectingCollector(
  inner: OutputCollector,
  targetSchema: VgiSchema,
): OutputCollector {
  function alreadyMatches(batch: VgiBatch): boolean {
    if (batch.schema.fields.length !== targetSchema.fields.length) return false;
    for (let i = 0; i < targetSchema.fields.length; i++) {
      if (batch.schema.fields[i].name !== targetSchema.fields[i].name) return false;
    }
    return true;
  }
  function project(batch: VgiBatch): VgiBatch {
    if (alreadyMatches(batch)) return batch;
    const cols: Record<string, any[]> = {};
    for (const f of targetSchema.fields) {
      const src = batch.getChild(f.name);
      if (src) {
        // Canonical read -> rich so the rebuild goes through the codec path
        // (backend-agnostic, lossless) rather than the raw `.get(i)`.
        const type = f.type as VgiDataType;
        const codec = codecFor(type);
        const arr: any[] = [];
        for (let i = 0; i < batch.numRows; i++) {
          arr.push(codec.canonicalToRich(readCanonicalValue(type, src, i)));
        }
        cols[f.name] = arr;
      } else {
        cols[f.name] = new Array(batch.numRows).fill(null);
      }
    }
    return batchFromColumns(cols, targetSchema);
  }
  return new Proxy(inner, {
    get(target, prop, receiver) {
      if (prop === "emit") {
        return function (
          this: OutputCollector,
          batchOrColumns: VgiBatch | Record<string, any[]>,
          metadata?: Map<string, string>,
        ) {
          if (batchOrColumns && typeof (batchOrColumns as any).getChild === "function") {
            return (target as any).emit(project(batchOrColumns as VgiBatch), metadata);
          }
          return (target as any).emit(batchOrColumns, metadata);
        };
      }
      const v = Reflect.get(target, prop, receiver);
      return typeof v === "function" ? v.bind(target) : v;
    },
  });
}

// ============================================================================
// Init-metadata persistence (mirrors Python _encode/_decode_table_buffering_init)
// ============================================================================

export function encodeBufferingInit(
  request: InitRequest,
  response: GlobalInitResponse,
): Uint8Array {
  // Serialize the InitRequest batch and the GlobalInitResponse so any pool
  // worker can cold-rebuild params when serving a process()/combine() RPC.
  const initBatch = serializeInitRequest(request);
  const initBytes = serializeBatch(initBatch);
  const respBatch = serializeGlobalInitResponse(response);
  // GlobalInitResponse serializer returns a plain dict; rebuild a 1-row batch.
  // Simpler: re-serialize as a length-prefixed pair of byte blobs.
  const respBytes = encodeGlobalInitResponse(respBatch);
  return concatLenPrefixed([initBytes, respBytes]);
}

export function decodeBufferingInit(
  payload: Uint8Array,
): { request: InitRequest; response: GlobalInitResponse } {
  const [initBytes, respBytes] = splitLenPrefixed(payload, 2);
  const initBatch = deserializeBatch(initBytes);
  const initDict = batchToScalarDict(initBatch);
  const request = deserializeInitRequest(initDict);
  const response = decodeGlobalInitResponse(respBytes);
  return { request, response };
}

function encodeGlobalInitResponse(dict: Record<string, any>): Uint8Array {
  // Encode { execution_id, opaque_data, max_workers } as length-prefixed blobs.
  const eid: Uint8Array = dict.execution_id ?? new Uint8Array(0);
  const opaque: Uint8Array | null = dict.opaque_data ?? null;
  const maxWorkers = Number(dict.max_workers ?? 1);
  const mwBuf = new Uint8Array(8);
  new DataView(mwBuf.buffer).setBigInt64(0, BigInt(maxWorkers), true);
  return concatLenPrefixed([eid, opaque ?? new Uint8Array(0), mwBuf]);
}

function decodeGlobalInitResponse(bytes: Uint8Array): GlobalInitResponse {
  const [eid, opaque, mwBuf] = splitLenPrefixed(bytes, 3);
  const maxWorkers = Number(new DataView(mwBuf.buffer, mwBuf.byteOffset, 8).getBigInt64(0, true));
  return {
    execution_id: eid,
    opaque_data: opaque.length > 0 ? opaque : null,
    max_workers: maxWorkers,
  };
}

function concatLenPrefixed(parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const p of parts) total += 4 + p.length;
  const out = new Uint8Array(total);
  const dv = new DataView(out.buffer);
  let off = 0;
  for (const p of parts) {
    dv.setUint32(off, p.length, true);
    off += 4;
    out.set(p, off);
    off += p.length;
  }
  return out;
}

function splitLenPrefixed(buf: Uint8Array, count: number): Uint8Array[] {
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const parts: Uint8Array[] = [];
  let off = 0;
  for (let i = 0; i < count; i++) {
    const len = dv.getUint32(off, true);
    off += 4;
    parts.push(buf.subarray(off, off + len));
    off += len;
  }
  return parts;
}
