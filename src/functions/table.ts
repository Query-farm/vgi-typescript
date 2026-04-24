// Table function implementation.
// Table functions produce output batches from arguments (no streaming input).

import { Schema, Field, DataType, Null } from "@query-farm/apache-arrow";
import type { OutputCollector } from "vgi-rpc";
import { DEFAULT_MAX_WORKERS } from "../types.js";
import type {
  BindRequest,
  BindResponse,
  InitRequest,
  GlobalInitResponse,
  TableFunctionCardinalityRequest,
  TableCardinality,
} from "../protocol/types.js";
import type {
  VgiFunction,
  FunctionMeta,
  StreamHandlers,
  FunctionExample,
} from "./types.js";
import type { ArgumentSpec } from "../arguments/argument-spec.js";
import { batchToScalarDict, batchToSecretDict, projectSchema, safeNumber } from "../util/arrow.js";
import {
  buildJoinKeysLookup,
  deserializeFilters,
  FilteringOutputCollector,
  type PushdownFilters,
} from "../util/filter-pushdown.js";
import { FunctionStability } from "../types.js";
import { BoundStorage, storage as globalStorage } from "../storage/function-storage.js";

// ============================================================================
// Table function parameter bundles
// ============================================================================

export interface TableBindParams<TArgs = Record<string, any>> {
  args: TArgs;
  bindCall: BindRequest;
  settings: Record<string, any>;
  secrets: Record<string, Record<string, any>>;
  resolvedSecretsProvided: boolean;
}

export interface TableProcessParams<TArgs = Record<string, any>> {
  args: TArgs;
  initCall: InitRequest;
  initResponse: GlobalInitResponse;
  outputSchema: Schema;
  settings: Record<string, any>;
  secrets: Record<string, Record<string, any>>;
  pushdownFilters?: PushdownFilters;
  storage?: BoundStorage;
}

// ============================================================================
// Functional API
// ============================================================================

export interface TableFunctionConfig<
  TArgs = Record<string, any>,
  TState = null,
> {
  name: string;
  description?: string;
  /** Argument schema (positional args) */
  args?: Record<string, DataType>;
  /** Argument docs */
  argDocs?: Record<string, string>;
  /** Argument defaults */
  argDefaults?: Record<string, any>;
  /** Names of args that accept variable number of arguments */
  varargs?: string[];
  /** Bind: return output schema */
  onBind: (params: TableBindParams<TArgs>) => {
    outputSchema: Schema;
    opaqueData?: Uint8Array;
    lookupSecretTypes?: string[];
    lookupScopes?: string[];
    lookupNames?: string[];
  };
  /** Init (optional) */
  onInit?: (params: {
    args: TArgs;
    initCall: InitRequest;
    outputSchema: Schema;
    executionId: Uint8Array;
    storage: BoundStorage;
  }) => GlobalInitResponse;
  /** State factory */
  initialState?: (params: TableProcessParams<TArgs>) => TState;
  /** Process: emit batches via out, call out.finish() when done */
  process: (
    params: TableProcessParams<TArgs>,
    state: TState,
    out: OutputCollector
  ) => void | Promise<void>;
  /** Cardinality hints */
  cardinality?: (params: TableBindParams<TArgs>) => TableCardinality;
  // Metadata
  projectionPushdown?: boolean;
  filterPushdown?: boolean;
  samplingPushdown?: boolean;
  autoApplyFilters?: boolean;
  stability?: FunctionStability;
  examples?: FunctionExample[];
  categories?: string[];
  tags?: Record<string, string>;
  maxWorkers?: number;
  requiredSettings?: string[];
  requiredSecrets?: string[];
}

export function defineTableFunction<
  TArgs = Record<string, any>,
  TState = null,
>(config: TableFunctionConfig<TArgs, TState>): VgiFunction {
  // Build argument specs
  const specs: ArgumentSpec[] = [];
  let posIdx = 0;

  if (config.args) {
    const varargsSet = new Set(config.varargs ?? []);
    for (const [name, type] of Object.entries(config.args)) {
      const isAny = type instanceof Null;
      const hasDefault = config.argDefaults?.[name] !== undefined;
      specs.push({
        name,
        // Args with defaults are named (string position), others are positional
        position: hasDefault ? name : posIdx++,
        arrowType: isAny ? new Null() : type,
        isAnyType: isAny,
        isVarargs: varargsSet.has(name),
      });
    }
  }

  const meta: FunctionMeta = {
    name: config.name,
    description: config.description,
    stability: config.stability,
    projectionPushdown: config.projectionPushdown,
    filterPushdown: config.filterPushdown,
    samplingPushdown: config.samplingPushdown,
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
      const defaultVal =
        config.argDefaults?.[spec.name] !== undefined
          ? config.argDefaults[spec.name]
          : undefined;
      let val: any;
      try {
        val = request.arguments.get(spec.position, defaultVal);
      } catch {
        // Fallback: try by name (for scan function tables where DuckDB
        // converts positional args to named args)
        val = request.arguments.get(spec.name, defaultVal);
      }
      // Arrow Int64 values come through as BigInt — coerce to number
      if (typeof val === "bigint") val = safeNumber(val);
      args[spec.name] = val;
    }
    return args as TArgs;
  }

  return {
    kind: "table",
    meta,
    argumentSpecs: specs,

    bind(request: BindRequest): BindResponse {
      const args = extractArgs(request);
      const settings = batchToScalarDict(request.settings);
      const secrets = batchToSecretDict(request.secrets);
      const result = config.onBind({
        args, bindCall: request, settings, secrets,
        resolvedSecretsProvided: request.resolvedSecretsProvided ?? false,
      });
      return {
        outputSchema: result.outputSchema,
        opaqueData: result.opaqueData ?? null,
        lookupSecretTypes: result.lookupSecretTypes,
        lookupScopes: result.lookupScopes,
        lookupNames: result.lookupNames,
      };
    },

    globalInit(request: InitRequest): GlobalInitResponse {
      const executionId = new Uint8Array(16);
      crypto.getRandomValues(executionId);

      if (request.executionId) {
        // Secondary init - reuse execution ID
        return {
          maxWorkers: config.maxWorkers ?? 1,
          executionId: request.executionId,
          opaqueData: null,
        };
      }

      if (config.onInit) {
        const args = extractArgs(request.bindCall);
        const boundStorage = new BoundStorage(globalStorage, executionId);
        return config.onInit({
          args,
          initCall: request,
          outputSchema: request.outputSchema,
          executionId,
          storage: boundStorage,
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

      // Apply projection pushdown only if the function supports it
      const projIds = request.projectionIds && meta.projectionPushdown
        ? request.projectionIds
        : null;
      const outputSchema = projIds
        ? projectSchema(projIds, request.outputSchema)
        : request.outputSchema;

      // Deserialize pushdown filters. Pass a join-keys column lookup so that
      // filters DuckDB promoted to join_keys (IN/OR lists, etc.) are
      // materialized as InFilters rather than silently dropped.
      const joinKeysLookup = buildJoinKeysLookup(request.joinKeys);
      const pushdownFilters = request.pushdownFilters
        ? deserializeFilters(request.pushdownFilters, joinKeysLookup)
        : undefined;

      const boundStorage = new BoundStorage(globalStorage, response.executionId);

      const processParams: TableProcessParams<TArgs> = {
        args,
        initCall: request,
        initResponse: response,
        outputSchema,
        settings,
        secrets,
        pushdownFilters,
        storage: boundStorage,
      };

      const state = config.initialState
        ? config.initialState(processParams)
        : (null as TState);

      return {
        outputSchema,
        producerInit: () => ({ state, processParams }),
        producerFn: async (
          pState: { state: TState; processParams: TableProcessParams<TArgs> },
          out: OutputCollector
        ) => {
          const wrappedOut = (config.autoApplyFilters && pushdownFilters)
            ? new FilteringOutputCollector(out, pushdownFilters) as unknown as OutputCollector
            : out;
          await config.process(pState.processParams, pState.state, wrappedOut);
        },
      };
    },

    cardinality: config.cardinality
      ? (request: TableFunctionCardinalityRequest) => {
          const args = extractArgs(request.bindCall);
          const settings = batchToScalarDict(request.bindCall.settings);
          const secrets = batchToSecretDict(request.bindCall.secrets);
          return config.cardinality!({
            args,
            bindCall: request.bindCall,
            settings,
            secrets,
            resolvedSecretsProvided: request.bindCall.resolvedSecretsProvided ?? false,
          });
        }
      : undefined,
  };
}
