// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Table In-Out function implementation.
// Two-phase: INPUT phase receives and transforms batches,
// FINALIZE phase emits final results.

import { type VgiSchema, schema, type VgiField, type VgiDataType, type VgiBatch, nullType } from "../arrow/index.js";
import type { OutputCollector } from "@query-farm/vgi-rpc";
import { DEFAULT_MAX_WORKERS, TableInOutPhase } from "../types.js";
import type {
  BindRequest,
  BindResponse,
  InitRequest,
  GlobalInitResponse,
} from "../protocol/types.js";
import type {
  VgiFunction,
  FunctionMeta,
  StreamHandlers,
  FunctionExample,
} from "./types.js";
import type { ArgumentSpec } from "../arguments/argument-spec.js";
import { batchToScalarDict, batchToSecretDict, projectSchema, projectBatch, emptyBatch } from "../util/arrow/index.js";
import {
  buildJoinKeysLookup,
  deserializeFilters,
  FilteringOutputCollector,
  type PushdownFilters,
} from "../filter-pushdown/index.js";
import { FunctionStability } from "../types.js";
import { BoundStorage, storage as defaultStorage } from "./storage.js";
import { serializeUserState, deserializeUserState } from "../protocol/state-serializer.js";

// ============================================================================
// Table In-Out parameter bundles (reuse table function's)
// ============================================================================

export interface TableInOutBindParams<TArgs = Record<string, any>> {
  args: TArgs;
  bindCall: BindRequest;
  settings: Record<string, any>;
  secrets: Record<string, Record<string, any>>;
}

export interface TableInOutProcessParams<TArgs = Record<string, any>> {
  args: TArgs;
  initCall: InitRequest;
  initResponse: GlobalInitResponse;
  outputSchema: VgiSchema;
  settings: Record<string, any>;
  secrets: Record<string, Record<string, any>>;
  pushdownFilters?: PushdownFilters;
  /** Shared storage for cross-phase and cross-worker data (SQLite-backed). */
  storage: BoundStorage;
  /**
   * Stable client-minted id for this streaming table-in-out substream.
   * Present (identical across init / every process() / finalize) when the
   * client fanned this function out across per-substream workers; use it to
   * key per-substream accumulated state in shared storage so a finalize()
   * that lands on a different HTTP backend than the process() calls still
   * finds it. `null`/`undefined` for the serial path or an old client.
   * Mirrors vgi-python's `ProcessParams.substream_id`.
   */
  substreamId?: Uint8Array | null;
}

// ============================================================================
// Functional API
// ============================================================================

export interface TableInOutConfig<
  TArgs = Record<string, any>,
  TState = null,
> {
  name: string;
  description?: string;
  args?: Record<string, VgiDataType>;
  /** Named arguments (optional, DuckDB passes by name) */
  namedArgs?: Record<string, VgiDataType>;
  /** Argument defaults */
  argDefaults?: Record<string, any>;
  /** Bind: default passes through input schema. May be async. */
  onBind?: (params: TableInOutBindParams<TArgs>) =>
    | { outputSchema: VgiSchema; opaqueData?: Uint8Array }
    | Promise<{ outputSchema: VgiSchema; opaqueData?: Uint8Array }>;
  onInit?: (params: {
    args: TArgs;
    initCall: InitRequest;
    outputSchema: VgiSchema;
    executionId: Uint8Array;
  }) => GlobalInitResponse | Promise<GlobalInitResponse>;
  initialState?: (params: TableInOutProcessParams<TArgs>) => TState;
  /** Process: transform input batch, emit output via out */
  process?: (
    params: TableInOutProcessParams<TArgs>,
    state: TState,
    batch: VgiBatch,
    out: OutputCollector
  ) => void | Promise<void>;
  /** Finalize: emit final batches after all input processed.
   *  Receives all worker states collected from storage (matches Python's finish(params, states)). */
  finalize?: (
    params: TableInOutProcessParams<TArgs>,
    states: TState[]
  ) => VgiBatch[] | Promise<VgiBatch[]>;
  // Metadata
  projectionPushdown?: boolean;
  filterPushdown?: boolean;
  autoApplyFilters?: boolean;
  stability?: FunctionStability;
  examples?: FunctionExample[];
  categories?: string[];
  tags?: Record<string, string>;
  maxWorkers?: number;
  requiredSettings?: string[];
  requiredSecrets?: string[];
}

export function defineTableInOutFunction<
  TArgs = Record<string, any>,
  TState = null,
>(config: TableInOutConfig<TArgs, TState>): VgiFunction {

  // Build argument specs
  const specs: ArgumentSpec[] = [];
  let posIdx = 0;

  // Positional args come before the table input
  if (config.args) {
    for (const [name, type] of Object.entries(config.args)) {
      specs.push({
        name,
        position: posIdx++,
        arrowType: type,
      });
    }
  }

  // Table-in-out always has a table input (after positional args)
  specs.push({
    name: "data",
    position: posIdx++,
    arrowType: nullType(),
    isTableInput: true,
  });

  if (config.namedArgs) {
    for (const [name, type] of Object.entries(config.namedArgs)) {
      specs.push({
        name,
        position: name, // string position = named arg
        arrowType: type,
      });
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
    hasFinalize: !!config.finalize,
  };

  function extractArgs(request: BindRequest): TArgs {
    const args: Record<string, any> = {};
    for (const spec of specs) {
      if (spec.isTableInput) continue;
      const defaultVal =
        config.argDefaults?.[spec.name] !== undefined
          ? config.argDefaults[spec.name]
          : undefined;
      // For named args, position is a string (the arg name)
      let val = request.arguments.get(spec.position, defaultVal);
      // Arrow Int64 values come through as BigInt — coerce to number
      if (typeof val === "bigint") val = Number(val);
      args[spec.name] = val;
    }
    return args as TArgs;
  }

  return {
    kind: "table_in_out",
    meta,
    argumentSpecs: specs,

    async bind(request: BindRequest): Promise<BindResponse> {
      if (config.onBind) {
        const args = extractArgs(request);
        const settings = batchToScalarDict(request.settings);
        const secrets = batchToSecretDict(request.secrets);
        const result = await config.onBind({
          args,
          bindCall: request,
          settings,
          secrets,
        });
        return {
          output_schema: result.outputSchema,
          opaque_data: result.opaqueData ?? null,
        };
      }
      // Default: pass through input schema
      return {
        output_schema: request.input_schema ?? schema([]),
        opaque_data: null,
      };
    },

    async globalInit(request: InitRequest): Promise<GlobalInitResponse> {
      const executionId = new Uint8Array(16);
      crypto.getRandomValues(executionId);

      if (request.execution_id) {
        return {
          max_workers: DEFAULT_MAX_WORKERS,
          execution_id: request.execution_id,
          opaque_data: null,
        };
      }

      if (config.onInit) {
        const args = extractArgs(request.bind_call);
        return await config.onInit({
          args,
          initCall: request,
          outputSchema: request.output_schema,
          executionId,
        });
      }

      return {
        max_workers: config.maxWorkers ?? 1,
        execution_id: executionId,
        opaque_data: null,
      };
    },

    createStreamHandlers(
      request: InitRequest,
      response: GlobalInitResponse,
      accumulatedState?: any,
    ): StreamHandlers {
      const args = extractArgs(request.bind_call);
      const settings = batchToScalarDict(request.bind_call.settings);
      const secrets = batchToSecretDict(request.bind_call.secrets);

      // Apply projection pushdown only if the function supports it
      const projIds = request.projection_ids && meta.projectionPushdown
        ? request.projection_ids
        : null;
      const outputSchema = projIds
        ? projectSchema(projIds, request.output_schema)
        : request.output_schema;

      // Deserialize pushdown filters. Pass a join-keys column lookup so that
      // filters DuckDB promoted to join_keys (IN/OR lists, etc.) are
      // materialized as InFilters rather than silently dropped.
      const joinKeysLookup = buildJoinKeysLookup(request.join_keys);
      const pushdownFilters = request.pushdown_filters
        ? deserializeFilters(request.pushdown_filters, joinKeysLookup)
        : undefined;

      // Create BoundStorage for cross-phase/cross-worker data sharing
      const boundStorage = new BoundStorage(
        defaultStorage,
        response.execution_id ?? new Uint8Array(16),
      );

      const processParams: TableInOutProcessParams<TArgs> = {
        args,
        initCall: request,
        initResponse: response,
        outputSchema,
        settings,
        secrets,
        pushdownFilters,
        storage: boundStorage,
        substreamId: request.substream_id ?? null,
      };

      const phase = request.phase;

      if (phase === TableInOutPhase.FINALIZE) {
        // FINALIZE phase: producer mode.
        // The actual collect+finalize work is deferred into the first
        // producerFn call so the (now-async) FunctionStorage can be awaited
        // there. Storing the materialized batches on the per-call state.
        return {
          outputSchema,
          producerInit: () => ({ state: { batchIdx: 0 }, batches: null as VgiBatch[] | null }),
          producerFn: async (
            pState: { state: { batchIdx: number }; batches: VgiBatch[] | null },
            out: OutputCollector
          ) => {
            if (pState.batches == null) {
              const finalizeStates: TState[] = [];
              if (accumulatedState != null) {
                finalizeStates.push(accumulatedState as TState);
              }
              // Collect any states persisted by INPUT exchanges. Subprocess
              // path reads from SQLite; HTTP path reads from CF DO / wherever
              // the configured storage backend lives.
              const stored = await boundStorage.collect();
              for (const bytes of stored) {
                finalizeStates.push(deserializeUserState(bytes) as TState);
              }
              if (finalizeStates.length === 0) {
                finalizeStates.push(
                  config.initialState
                    ? config.initialState(processParams)
                    : (null as TState),
                );
              }
              pState.batches = config.finalize
                ? await config.finalize(processParams, finalizeStates)
                : [];
            }
            if (pState.state.batchIdx >= pState.batches.length) {
              out.finish();
              return;
            }
            out.emit(pState.batches[pState.state.batchIdx]);
            pState.state.batchIdx++;
            if (pState.state.batchIdx >= pState.batches.length) {
              out.finish();
            }
          },
        };
      }

      // INPUT phase: exchange mode
      const state = config.initialState
        ? config.initialState(processParams)
        : (null as TState);

      const processFn =
        config.process ??
        ((_params: any, _state: any, batch: VgiBatch, out: OutputCollector) => {
          // Default passthrough. When DuckDB pushed a projection, the declared
          // output schema is narrowed to `projIds`, so the emitted batch must be
          // narrowed too — otherwise it carries columns (and their dictionaries)
          // the stream schema doesn't declare, and the C++ reader rejects the
          // batch (e.g. "No record of dictionary type with id 1" when the
          // dropped columns were dictionary-encoded ENUMs).
          out.emit(projIds ? projectBatch(projIds, batch) : batch);
        });

      return {
        outputSchema,
        inputSchema: request.bind_call.input_schema ?? undefined,
        exchangeInit: () => ({ state, processParams }),
        exchangeFn: async (
          eState: {
            state: TState;
            processParams: TableInOutProcessParams<TArgs>;
          },
          input: VgiBatch,
          out: OutputCollector
        ) => {
          // Reconcile emitted batches to the (possibly projected) output schema
          // by name — a process() may emit its full declared schema and let the
          // framework project (mirrors Python's OutputCollector.emit reconcile).
          let wrappedOut: OutputCollector = projIds
            ? makeSchemaReconcilingCollector(out, outputSchema)
            : out;
          if (config.autoApplyFilters && pushdownFilters) {
            wrappedOut = new FilteringOutputCollector(wrappedOut, pushdownFilters) as unknown as OutputCollector;
          }
          await processFn(eState.processParams, eState.state, input, wrappedOut);

          // Auto-persist accumulated state for FINALIZE recovery (matches Python/Go pattern)
          if (eState.state != null) {
            const serialized = serializeUserState(eState.state);
            if (serialized != null) {
              await boundStorage.put(serialized);
            }
          }
        },
      };
    },
  };
}

// ============================================================================
// Shared INPUT-phase helpers
// ============================================================================

/**
 * Wrap an OutputCollector so each emitted pre-built batch is reconciled to the
 * (possibly projected) target output schema by NAME before forwarding — a
 * process() may emit its full declared schema and let the framework project.
 * Mirrors vgi-rpc-python's `OutputCollector.emit` select step. A target column
 * missing from the emitted batch throws a clear error.
 */
function makeSchemaReconcilingCollector(
  inner: OutputCollector,
  targetSchema: VgiSchema,
): OutputCollector {
  function reconcile(batch: VgiBatch): VgiBatch {
    const batchFields = batch.schema.fields;
    if (
      batchFields.length === targetSchema.fields.length &&
      targetSchema.fields.every((f, i) => batchFields[i].name === f.name)
    ) {
      return batch;
    }
    const ids: number[] = [];
    for (const f of targetSchema.fields) {
      const idx = batchFields.findIndex((bf) => bf.name === f.name);
      if (idx < 0) {
        throw new Error(
          `emitted batch is missing projected output column '${f.name}' ` +
            `(emitted: [${batchFields.map((bf) => bf.name).join(", ")}])`,
        );
      }
      ids.push(idx);
    }
    return projectBatch(ids, batch) as unknown as VgiBatch;
  }
  return new Proxy(inner, {
    get(target, prop, receiver) {
      if (prop === "emit") {
        return function (
          batchOrColumns: VgiBatch | Record<string, any[]>,
          metadata?: Map<string, string>,
        ) {
          if (batchOrColumns && typeof (batchOrColumns as any).getChild === "function") {
            return (target as any).emit(reconcile(batchOrColumns as VgiBatch), metadata);
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
// Blended ("UNNEST-style") row-transform functions
// ============================================================================

/**
 * Process params for a blended row-transform function. `args` carries only the
 * NAMED (bind-time scalar) options — the positional args are the per-row input
 * columns, read from `batch` in process() (by declared name for fixed args,
 * positionally for varargs).
 */
export type RowTransformProcessParams<TArgs = Record<string, any>> =
  TableInOutProcessParams<TArgs>;

export interface RowTransformConfig<TArgs = Record<string, any>> {
  name: string;
  description?: string;
  /**
   * Positional args = the per-row INPUT COLUMNS (real typed args on the wire,
   * no synthetic TABLE placeholder). Read from `batch` by declared name in
   * process(); NOT surfaced on `params.args`.
   */
  args?: Record<string, VgiDataType>;
  /**
   * Trailing VARARGS input columns: the per-row input is N columns of the
   * declared type. A varargs blended function has no per-column declared
   * names (the C++ bind names them col0..colN-1), so process() reads the
   * columns POSITIONALLY off `batch`.
   */
  varargs?: { name: string; type: VgiDataType; doc?: string };
  /** Named (string-position) args stay bind-time scalars on `params.args`. */
  namedArgs?: Record<string, VgiDataType>;
  argDefaults?: Record<string, any>;
  /** Per-argument descriptions keyed by arg name (surfaced as `vgi_doc`). */
  argDocs?: Record<string, string>;
  /** Bind: return the output schema. The input schema (the declared per-row
   *  columns, typed by the C++ bind) is on `params.bindCall.input_schema`. */
  onBind: (params: TableInOutBindParams<TArgs>) =>
    | { outputSchema: VgiSchema; opaqueData?: Uint8Array }
    | Promise<{ outputSchema: VgiSchema; opaqueData?: Uint8Array }>;
  /**
   * Per-row map: transform one input batch, emit exactly one output batch via
   * `out`. 1->1, 1->N (with {@link parentRowsMetadata} provenance), and 1->0
   * (a 0-row emit) all work. There is NO finalize — a blended function is a
   * per-row map (DuckDB forbids FinalExecute under correlated LATERAL, one of
   * the call shapes blended must serve). Accumulating functions use a classic
   * TableInput table-in-out or a TableBufferingFunction.
   */
  process: (
    params: RowTransformProcessParams<TArgs>,
    batch: VgiBatch,
    out: OutputCollector,
  ) => void | Promise<void>;
  // Metadata
  projectionPushdown?: boolean;
  filterPushdown?: boolean;
  autoApplyFilters?: boolean;
  stability?: FunctionStability;
  examples?: FunctionExample[];
  categories?: string[];
  tags?: Record<string, string>;
  maxWorkers?: number;
  requiredSettings?: string[];
  requiredSecrets?: string[];
}

/**
 * Define a blended ("UNNEST-style") table-in-out function: its positional args
 * ARE its per-row input columns, so ONE registration serves every call shape —
 * `f(52, 13)` (literal -> one input row), `FROM t, f(t.x, t.y)` (columns ->
 * streaming), and `LATERAL f(t.x, t.y)`. Mirrors vgi-python's
 * `RowTransformFunction` (Phase B).
 *
 * Registers as a TABLE function with `FunctionInfo.input_from_args = true`;
 * the worker's overload resolution matches blended overloads by INPUT-COLUMN
 * count (the positional args are not on the wire). Map-shaped, no finalize.
 */
export function defineRowTransformFunction<
  TArgs = Record<string, any>,
>(config: RowTransformConfig<TArgs>): VgiFunction {
  // Foot-gun guard (mirrors Python's resolve_metadata check): a blended
  // function needs at least one positional Arg — its per-row input column.
  const numPositional = Object.keys(config.args ?? {}).length + (config.varargs ? 1 : 0);
  if (numPositional === 0) {
    throw new Error(
      `${config.name}: a blended row-transform function needs at least one ` +
        `positional arg (its per-row input column); found none.`,
    );
  }

  // Positional args are the per-row input columns — REAL typed args on the
  // wire (no TableInput placeholder). Named args follow.
  const specs: ArgumentSpec[] = [];
  let posIdx = 0;
  if (config.args) {
    for (const [name, type] of Object.entries(config.args)) {
      specs.push({
        name,
        position: posIdx++,
        arrowType: type,
        doc: config.argDocs?.[name],
      });
    }
  }
  if (config.varargs) {
    specs.push({
      name: config.varargs.name,
      position: posIdx++,
      arrowType: config.varargs.type,
      isVarargs: true,
      doc: config.varargs.doc ?? config.argDocs?.[config.varargs.name],
    });
  }
  if (config.namedArgs) {
    for (const [name, type] of Object.entries(config.namedArgs)) {
      specs.push({
        name,
        position: name,
        arrowType: type,
        doc: config.argDocs?.[name],
      });
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
    // Map-shaped, per-row: a blended function never has a finalize.
    hasFinalize: false,
    inputFromArgs: true,
  };

  // Only NAMED args are on the wire — positional args are the input columns,
  // absent from the wire arguments in every call shape.
  function extractArgs(request: BindRequest): TArgs {
    const args: Record<string, any> = {};
    for (const spec of specs) {
      if (typeof spec.position !== "string") continue;
      const defaultVal =
        config.argDefaults?.[spec.name] !== undefined
          ? config.argDefaults[spec.name]
          : undefined;
      let val = request.arguments.get(spec.position, defaultVal ?? null);
      if (typeof val === "bigint") val = Number(val);
      args[spec.name] = val;
    }
    return args as TArgs;
  }

  return {
    kind: "table_in_out",
    meta,
    argumentSpecs: specs,

    async bind(request: BindRequest): Promise<BindResponse> {
      const args = extractArgs(request);
      const settings = batchToScalarDict(request.settings);
      const secrets = batchToSecretDict(request.secrets);
      const result = await config.onBind({
        args,
        bindCall: request,
        settings,
        secrets,
      });
      return {
        output_schema: result.outputSchema,
        opaque_data: result.opaqueData ?? null,
      };
    },

    async globalInit(request: InitRequest): Promise<GlobalInitResponse> {
      if (request.execution_id) {
        return {
          max_workers: DEFAULT_MAX_WORKERS,
          execution_id: request.execution_id,
          opaque_data: null,
        };
      }
      const executionId = new Uint8Array(16);
      crypto.getRandomValues(executionId);
      return {
        max_workers: config.maxWorkers ?? 1,
        execution_id: executionId,
        opaque_data: null,
      };
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

      const boundStorage = new BoundStorage(
        defaultStorage,
        response.execution_id ?? new Uint8Array(16),
      );

      const processParams: RowTransformProcessParams<TArgs> = {
        args,
        initCall: request,
        initResponse: response,
        outputSchema,
        settings,
        secrets,
        pushdownFilters,
        storage: boundStorage,
        substreamId: request.substream_id ?? null,
      };

      if (request.phase === TableInOutPhase.FINALIZE) {
        throw new Error(
          `${config.name}: a blended row-transform function has no FINALIZE ` +
            `phase (it is a per-row map; has_finalize is advertised false).`,
        );
      }

      return {
        outputSchema,
        inputSchema: request.bind_call.input_schema ?? undefined,
        exchangeInit: () => ({ state: null, processParams }),
        exchangeFn: async (
          eState: { state: null; processParams: RowTransformProcessParams<TArgs> },
          input: VgiBatch,
          out: OutputCollector,
        ) => {
          let wrappedOut: OutputCollector = projIds
            ? makeSchemaReconcilingCollector(out, outputSchema)
            : out;
          if (config.autoApplyFilters && pushdownFilters) {
            wrappedOut = new FilteringOutputCollector(wrappedOut, pushdownFilters) as unknown as OutputCollector;
          }
          await config.process(eState.processParams, input, wrappedOut);
        },
      };
    },
  };
}
