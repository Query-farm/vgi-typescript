// Table In-Out function implementation.
// Two-phase: INPUT phase receives and transforms batches,
// FINALIZE phase emits final results.

import { Schema, Field, DataType, RecordBatch, Null } from "apache-arrow";
import type { OutputCollector } from "vgi-rpc";
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
import { batchToScalarDict, batchToSecretDict, projectSchema, emptyBatch } from "../util/arrow.js";
import { deserializeFilters, FilteringOutputCollector, type PushdownFilters } from "../util/filter-pushdown.js";
import { FunctionStability } from "../types.js";

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
  outputSchema: Schema;
  settings: Record<string, any>;
  secrets: Record<string, Record<string, any>>;
  pushdownFilters?: PushdownFilters;
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
  args?: Record<string, DataType>;
  /** Named arguments (optional, DuckDB passes by name) */
  namedArgs?: Record<string, DataType>;
  /** Argument defaults */
  argDefaults?: Record<string, any>;
  /** Bind: default passes through input schema */
  onBind?: (params: TableInOutBindParams<TArgs>) => {
    outputSchema: Schema;
    opaqueData?: Uint8Array;
  };
  onInit?: (params: {
    args: TArgs;
    initCall: InitRequest;
    outputSchema: Schema;
    executionId: Uint8Array;
  }) => GlobalInitResponse;
  initialState?: (params: TableInOutProcessParams<TArgs>) => TState;
  /** Process: transform input batch, emit output via out */
  process?: (
    params: TableInOutProcessParams<TArgs>,
    state: TState,
    batch: RecordBatch,
    out: OutputCollector
  ) => void | Promise<void>;
  /** Finalize: emit final batches after all input processed */
  finalize?: (
    params: TableInOutProcessParams<TArgs>,
    state: TState
  ) => RecordBatch[];
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

function executionIdToKey(id: Uint8Array): string {
  return Array.from(id).map(b => b.toString(16).padStart(2, "0")).join("");
}

export function defineTableInOutFunction<
  TArgs = Record<string, any>,
  TState = null,
>(config: TableInOutConfig<TArgs, TState>): VgiFunction {
  // State cache: stores INPUT phase state so FINALIZE phase can access it
  const stateCache = new Map<string, TState>();

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
    arrowType: new Null(),
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

    bind(request: BindRequest): BindResponse {
      if (config.onBind) {
        const args = extractArgs(request);
        const settings = batchToScalarDict(request.settings);
        const secrets = batchToSecretDict(request.secrets);
        const result = config.onBind({
          args,
          bindCall: request,
          settings,
          secrets,
        });
        return {
          outputSchema: result.outputSchema,
          opaqueData: result.opaqueData ?? null,
        };
      }
      // Default: pass through input schema
      return {
        outputSchema: request.inputSchema ?? new Schema([]),
        opaqueData: null,
      };
    },

    globalInit(request: InitRequest): GlobalInitResponse {
      const executionId = new Uint8Array(16);
      crypto.getRandomValues(executionId);

      if (request.executionId) {
        return {
          maxWorkers: DEFAULT_MAX_WORKERS,
          executionId: request.executionId,
          opaqueData: null,
        };
      }

      if (config.onInit) {
        const args = extractArgs(request.bindCall);
        return config.onInit({
          args,
          initCall: request,
          outputSchema: request.outputSchema,
          executionId,
        });
      }

      return {
        maxWorkers: config.maxWorkers ?? 1,
        executionId,
        opaqueData: null,
      };
    },

    createStreamHandlers(
      request: InitRequest,
      response: GlobalInitResponse
    ): StreamHandlers {
      const args = extractArgs(request.bindCall);
      const settings = batchToScalarDict(request.bindCall.settings);
      const secrets = batchToSecretDict(request.bindCall.secrets);

      const outputSchema = request.projectionIds
        ? projectSchema(request.projectionIds, request.outputSchema)
        : request.outputSchema;

      // Deserialize pushdown filters
      const pushdownFilters = request.pushdownFilters
        ? deserializeFilters(request.pushdownFilters)
        : undefined;

      const processParams: TableInOutProcessParams<TArgs> = {
        args,
        initCall: request,
        initResponse: response,
        outputSchema,
        settings,
        secrets,
        pushdownFilters,
      };

      const phase = request.phase;

      if (phase === TableInOutPhase.FINALIZE) {
        // FINALIZE phase: producer mode
        // Retrieve state accumulated during INPUT phase
        const execKey = response.executionId
          ? executionIdToKey(response.executionId)
          : null;
        const cachedState = execKey ? stateCache.get(execKey) : undefined;
        if (execKey) stateCache.delete(execKey); // Clean up

        const finalizeState = cachedState ?? (config.initialState
          ? config.initialState(processParams)
          : (null as TState));

        const batches = config.finalize
          ? config.finalize(processParams, finalizeState)
          : [];
        let batchIdx = 0;

        return {
          outputSchema,
          producerInit: () => ({ batches, batchIdx }),
          producerFn: (
            state: { batches: RecordBatch[]; batchIdx: number },
            out: OutputCollector
          ) => {
            if (state.batchIdx >= state.batches.length) {
              out.finish();
              return;
            }
            out.emit(state.batches[state.batchIdx]);
            state.batchIdx++;
            if (state.batchIdx >= state.batches.length) {
              out.finish();
            }
          },
        };
      }

      // INPUT phase: exchange mode
      const state = config.initialState
        ? config.initialState(processParams)
        : (null as TState);

      // Store state in cache so FINALIZE phase can access it
      const execKey = response.executionId
        ? executionIdToKey(response.executionId)
        : null;
      if (execKey) {
        stateCache.set(execKey, state);
      }

      const processFn =
        config.process ??
        ((_params: any, _state: any, batch: RecordBatch, out: OutputCollector) => {
          out.emit(batch);
        });

      return {
        outputSchema,
        inputSchema: request.bindCall.inputSchema ?? undefined,
        exchangeInit: () => ({ state, processParams }),
        exchangeFn: async (
          eState: {
            state: TState;
            processParams: TableInOutProcessParams<TArgs>;
          },
          input: RecordBatch,
          out: OutputCollector
        ) => {
          const wrappedOut = (config.autoApplyFilters && pushdownFilters)
            ? new FilteringOutputCollector(out, pushdownFilters) as unknown as OutputCollector
            : out;
          await processFn(eState.processParams, eState.state, input, wrappedOut);
        },
      };
    },
  };
}
