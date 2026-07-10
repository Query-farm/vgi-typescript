// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Table function implementation.
// Table functions produce output batches from arguments (no streaming input).

import {
  type VgiSchema,
  type VgiBatch,
  type VgiDataType,
  isBatch,
  isNull,
  nullType,
  deserializeBatch,
  readCanonicalValue,
} from "../arrow/index.js";
import { codecFor } from "../arrow/codec/registry.js";
import type { OutputCollector } from "@query-farm/vgi-rpc";
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
import {
  constraintSpecFields,
  validateConstConstraints,
  type ArgumentSpec,
  type ArgumentConstraints,
} from "../arguments/argument-spec.js";
import { batchToScalarDict, batchToSecretDict, projectSchema, safeNumber } from "../util/arrow/index.js";
import {
  buildJoinKeysLookup,
  deserializeFilters,
  FilteringOutputCollector,
  type PushdownFilters,
} from "../filter-pushdown/index.js";
import type { ColumnStatistics } from "../util/statistics.js";
import {
  FunctionStability,
  type NullHandling,
  type OrderPreservation,
  type OrderDependence,
  type DistinctDependence,
} from "../types.js";
import { BoundStorage, storage as globalStorage } from "./storage.js";
import { CACHE_IF_MODIFIED_SINCE_KEY, CACHE_IF_NONE_MATCH_KEY } from "../cache-control.js";

// Base64-decode a string into raw bytes. Used to unpack the dynamic filter
// update DuckDB attaches to each tick batch's custom metadata.
function base64Decode(s: string): Uint8Array {
  const bin = (globalThis as any).atob ? (globalThis as any).atob(s) : Buffer.from(s, "base64").toString("binary");
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// Read the first RecordBatch from an Arrow IPC stream buffer. Returns null if
// the stream has no batches.
function deserializeFilterBatch(bytes: Uint8Array): VgiBatch | null {
  if (!bytes || bytes.length === 0) return null;
  const batch = deserializeBatch(bytes);
  return batch.numRows === 0 && batch.schema.fields.length === 0 ? null : batch;
}

// Wrap an OutputCollector so each emitted RecordBatch is projected-by-name
// to the bound outputSchema before forwarding. Lenient: workers can emit
// over-wide batches (full declared schema) and the framework drops the
// columns DuckDB didn't ask for. Field name mismatches yield null columns
// rather than wrong-position reads. Field types must already match.
function makeProjectingCollector(inner: OutputCollector, targetSchema: VgiSchema): OutputCollector {
  // Single-pass guard: don't project when a batch already matches.
  function alreadyMatches(batch: VgiBatch): boolean {
    if (batch.schema.fields.length !== targetSchema.fields.length) return false;
    for (let i = 0; i < targetSchema.fields.length; i++) {
      if (batch.schema.fields[i].name !== targetSchema.fields[i].name) return false;
    }
    return true;
  }
  function project(batch: VgiBatch): VgiBatch {
    if (alreadyMatches(batch)) return batch;
    const { batchFromColumns } = require("../arrow/index.js");
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
        return function (this: OutputCollector, batchOrColumns: VgiBatch | Record<string, any[]>, metadata?: Map<string, string>) {
          if (isBatch(batchOrColumns)) {
            return (target as any).emit(project(batchOrColumns), metadata);
          }
          // Object form: vgi-rpc itself converts to a batch via outputSchema,
          // and the user passed columns by name — already aligned.
          return (target as any).emit(batchOrColumns, metadata);
        };
      }
      const v = Reflect.get(target, prop, receiver);
      return typeof v === "function" ? v.bind(target) : v;
    },
  });
}

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
  outputSchema: VgiSchema;
  settings: Record<string, any>;
  secrets: Record<string, Record<string, any>>;
  pushdownFilters?: PushdownFilters;
  storage?: BoundStorage;
  /**
   * AT (TIMESTAMP|VERSION) clause for this scan, or `undefined` when the scan
   * has no AT clause. Carried on the per-scan bind embedded in the init request
   * (`initCall.bind_call.at_unit` / `.at_value`), so function-backed tables can
   * read time travel at init alongside their pushdown filters. Mirrors
   * vgi-python's `ProcessParams.at_unit` / `.at_value`. See `BindRequest.at_unit`.
   */
  atUnit?: string;
  atValue?: string;
  /**
   * Conditional-revalidation validator (the client's stored ETag). Set when the
   * client holds a stale-but-revalidatable cached result and asks the worker to
   * confirm freshness cheaply; a worker that advertised `revalidatable` compares
   * it and, if unchanged, emits a 0-row batch tagged
   * `cacheControlMetadata({ notModified: true })` instead of re-streaming.
   * Undefined on a normal call.
   */
  ifNoneMatch?: string;
  /** Conditional-revalidation validator (the client's stored Last-Modified).
   *  Companion to {@link ifNoneMatch}. Undefined on a normal call. */
  ifModifiedSince?: string;
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
  args?: Record<string, VgiDataType>;
  /** Argument docs */
  argDocs?: Record<string, string>;
  /** Argument defaults */
  argDefaults?: Record<string, any>;
  /**
   * Per-argument discovery constraints (choices / ge / le / gt / lt / pattern),
   * keyed by argument name. Surfaced via `vgi_function_arguments()` for agent
   * discovery AND enforced at bind: a value violating a declared constraint
   * fails the bind with an ArgumentValidationError.
   */
  argConstraints?: Record<string, ArgumentConstraints>;
  /** Names of args that accept variable number of arguments */
  varargs?: string[];
  /** Bind: return output schema. May be async — handlers `await` the result. */
  onBind: (params: TableBindParams<TArgs>) =>
    | {
        outputSchema: VgiSchema;
        opaqueData?: Uint8Array;
        lookupSecretTypes?: string[];
        lookupScopes?: string[];
        lookupNames?: string[];
      }
    | Promise<{
        outputSchema: VgiSchema;
        opaqueData?: Uint8Array;
        lookupSecretTypes?: string[];
        lookupScopes?: string[];
        lookupNames?: string[];
      }>;
  /** Init (optional). May be async — common when storage is HTTP-backed. */
  onInit?: (params: {
    args: TArgs;
    initCall: InitRequest;
    outputSchema: VgiSchema;
    executionId: Uint8Array;
    storage: BoundStorage;
  }) => GlobalInitResponse | Promise<GlobalInitResponse>;
  /** State factory */
  initialState?: (params: TableProcessParams<TArgs>) => TState;
  /** Process: emit batches via out, call out.finish() when done */
  process: (
    params: TableProcessParams<TArgs>,
    state: TState,
    out: OutputCollector
  ) => void | Promise<void>;
  /** Cardinality hints */
  cardinality?: (params: TableBindParams<TArgs>) => TableCardinality | Promise<TableCardinality>;
  /**
   * Per-column statistics for the function's output. Returned to DuckDB via
   * the `table_function_statistics` RPC; the optimizer uses min/max to
   * eliminate impossible filters at plan time (folding scans to
   * EMPTY_RESULT). Return `null` or an empty array when bounds are unknown.
   */
  statistics?: (params: TableBindParams<TArgs>) => ColumnStatistics[] | null;
  /**
   * Per-execution diagnostics surfaced under EXPLAIN ANALYZE. DuckDB calls
   * this once per parallel scan thread at pipeline FinishSource via the
   * `table_function_dynamic_to_string` RPC. Return ordered key→value
   * strings; the C++ extension merges these with the intrinsic keys
   * (Function, Rows Read, Threads). The framework provides a BoundStorage
   * keyed by the global execution_id so process() can persist counters
   * that this callback then reads back — see profiling_demo for the
   * canonical pattern.
   */
  dynamicToString?: (
    params: TableBindParams<TArgs>,
    executionId: Uint8Array,
    storage: BoundStorage,
  ) => Record<string, string> | Promise<Record<string, string>>;
  // Metadata
  projectionPushdown?: boolean;
  filterPushdown?: boolean;
  samplingPushdown?: boolean;
  /** Opt in to DuckDB's late-materialization SEMI-join rewrite; requires a
   *  UNIQUE, snapshot-stable rowid column. FunctionInfo.late_materialization. */
  lateMaterialization?: boolean;
  supportedExpressionFilters?: string[];
  autoApplyFilters?: boolean;
  stability?: FunctionStability;
  examples?: FunctionExample[];
  categories?: string[];
  tags?: Record<string, string>;
  maxWorkers?: number;
  requiredSettings?: string[];
  requiredSecrets?: string[];
  /** Row-order preservation behavior; flows to DuckDB's
   *  TableFunction::order_preservation_type. */
  preservesOrder?: OrderPreservation;
  nullHandling?: NullHandling;
  orderDependent?: OrderDependence;
  distinctDependent?: DistinctDependence;
  /** Emit per-batch `vgi_batch_index`; FunctionInfo.supports_batch_index. */
  supportsBatchIndex?: boolean;
  /** Hive-style partition-columns mode; FunctionInfo.partition_kind. */
  partitionKind?: "NOT_PARTITIONED" | "SINGLE_VALUE_PARTITIONS" | "OVERLAPPING_PARTITIONS" | "DISJOINT_PARTITIONS";
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
      const isAny = isNull(type);
      const hasDefault = config.argDefaults?.[name] !== undefined;
      specs.push({
        name,
        // Args with defaults are named (string position), others are positional
        position: hasDefault ? name : posIdx++,
        arrowType: isAny ? nullType() : type,
        isAnyType: isAny,
        isVarargs: varargsSet.has(name),
        doc: config.argDocs?.[name],
        ...constraintSpecFields(config.argConstraints?.[name]),
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
    lateMaterialization: config.lateMaterialization,
    supportedExpressionFilters: config.supportedExpressionFilters,
    autoApplyFilters: config.autoApplyFilters,
    examples: config.examples,
    categories: config.categories,
    tags: config.tags,
    maxWorkers: config.maxWorkers,
    requiredSettings: config.requiredSettings,
    requiredSecrets: config.requiredSecrets,
    preservesOrder: config.preservesOrder,
    nullHandling: config.nullHandling,
    orderDependent: config.orderDependent,
    distinctDependent: config.distinctDependent,
    supportsBatchIndex: config.supportsBatchIndex,
    partitionKind: config.partitionKind,
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
      // Enforce declared constraints at bind (table args are all bind-time).
      const constraints = config.argConstraints?.[spec.name];
      if (constraints) validateConstConstraints(spec.name, constraints, val);
      args[spec.name] = val;
    }
    return args as TArgs;
  }

  return {
    kind: "table",
    meta,
    argumentSpecs: specs,

    async bind(request: BindRequest): Promise<BindResponse> {
      const args = extractArgs(request);
      const settings = batchToScalarDict(request.settings);
      const secrets = batchToSecretDict(request.secrets);
      const result = await config.onBind({
        args, bindCall: request, settings, secrets,
        resolvedSecretsProvided: request.resolved_secrets_provided ?? false,
      });
      return {
        output_schema: result.outputSchema,
        opaque_data: result.opaqueData ?? null,
        lookup_secret_types: result.lookupSecretTypes,
        lookup_scopes: result.lookupScopes,
        lookup_names: result.lookupNames,
      };
    },

    async globalInit(request: InitRequest): Promise<GlobalInitResponse> {
      const executionId = new Uint8Array(16);
      crypto.getRandomValues(executionId);

      if (request.execution_id) {
        // Secondary init - reuse execution ID
        return {
          max_workers: config.maxWorkers ?? 1,
          execution_id: request.execution_id,
          opaque_data: null,
        };
      }

      if (config.onInit) {
        const args = extractArgs(request.bind_call);
        const boundStorage = new BoundStorage(globalStorage, executionId);
        return await config.onInit({
          args,
          initCall: request,
          outputSchema: request.output_schema,
          executionId,
          storage: boundStorage,
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
      response: GlobalInitResponse
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

      const boundStorage = new BoundStorage(globalStorage, response.execution_id);

      const processParams: TableProcessParams<TArgs> = {
        args,
        initCall: request,
        initResponse: response,
        outputSchema,
        settings,
        secrets,
        pushdownFilters,
        storage: boundStorage,
        atUnit: request.bind_call.at_unit ?? undefined,
        atValue: request.bind_call.at_value ?? undefined,
      };

      const state = config.initialState
        ? config.initialState(processParams)
        : (null as TState);

      return {
        outputSchema,
        producerInit: () => ({ state, processParams }),
        onTick: (
          pState: { state: TState; processParams: TableProcessParams<TArgs> },
          tickMetadata: Map<string, string> | undefined,
        ) => {
          if (!tickMetadata) return;

          // Conditional-revalidation validators: the client holds a stale
          // cached result and asks the worker to confirm freshness cheaply.
          // Over subprocess these ride the first producer tick; over HTTP the
          // first tick folds into the /init request, whose metadata vgi-rpc
          // surfaces here.
          const ifNoneMatch = tickMetadata.get(CACHE_IF_NONE_MATCH_KEY);
          const ifModifiedSince = tickMetadata.get(CACHE_IF_MODIFIED_SINCE_KEY);
          if (ifNoneMatch !== undefined) pState.processParams.ifNoneMatch = ifNoneMatch;
          if (ifModifiedSince !== undefined) pState.processParams.ifModifiedSince = ifModifiedSince;

          // Dynamic filter pushdown: DuckDB's Top-N optimizer tightens filters
          // between ticks and serializes the current filter into the tick
          // batch's custom metadata under `vgi_pushdown_filters` (base64 of a
          // filter IPC stream). Decode and overwrite the current pushdown
          // filters so process() sees the updated value.
          const encoded = tickMetadata.get("vgi_pushdown_filters");
          if (!encoded) return;
          try {
            const bytes = base64Decode(encoded);
            const filterBatch = deserializeFilterBatch(bytes);
            if (filterBatch) {
              const updated = deserializeFilters(filterBatch, joinKeysLookup);
              pState.processParams.pushdownFilters = updated;
            }
          } catch {
            // Malformed dynamic-filter update: keep the previous filter. Not
            // fatal — this is a best-effort optimization hint from DuckDB.
          }
        },
        producerFn: async (
          pState: { state: TState; processParams: TableProcessParams<TArgs> },
          out: OutputCollector
        ) => {
          const current = pState.processParams.pushdownFilters;
          // Auto-project: workers may emit a batch with the function's full
          // declared schema even when DuckDB only requested a subset. Wrap
          // the OutputCollector so each emit() projects-by-name to the
          // bound outputSchema. Lenient: absent fields become null columns.
          let wrappedOut: OutputCollector =
            outputSchema && projIds
              ? makeProjectingCollector(out, outputSchema)
              : out;
          if (config.autoApplyFilters && current) {
            wrappedOut = new FilteringOutputCollector(wrappedOut, current) as unknown as OutputCollector;
          }
          await config.process(pState.processParams, pState.state, wrappedOut);
        },
      };
    },

    cardinality: config.cardinality
      ? (request: TableFunctionCardinalityRequest) => {
          const args = extractArgs(request.bind_call);
          const settings = batchToScalarDict(request.bind_call.settings);
          const secrets = batchToSecretDict(request.bind_call.secrets);
          return config.cardinality!({
            args,
            bindCall: request.bind_call,
            settings,
            secrets,
            resolvedSecretsProvided: request.bind_call.resolved_secrets_provided ?? false,
          });
        }
      : undefined,

    statistics: config.statistics
      ? (request: TableFunctionCardinalityRequest) => {
          const args = extractArgs(request.bind_call);
          const settings = batchToScalarDict(request.bind_call.settings);
          const secrets = batchToSecretDict(request.bind_call.secrets);
          return config.statistics!({
            args,
            bindCall: request.bind_call,
            settings,
            secrets,
            resolvedSecretsProvided: request.bind_call.resolved_secrets_provided ?? false,
          });
        }
      : undefined,

    dynamicToString: config.dynamicToString
      ? (request) => {
          const args = extractArgs(request.bindCall);
          const settings = batchToScalarDict(request.bindCall.settings);
          const secrets = batchToSecretDict(request.bindCall.secrets);
          const storage = new BoundStorage(globalStorage, request.globalExecutionId);
          return config.dynamicToString!(
            {
              args,
              bindCall: request.bindCall,
              settings,
              secrets,
              resolvedSecretsProvided: request.bindCall.resolved_secrets_provided ?? false,
            },
            request.globalExecutionId,
            storage,
          );
        }
      : undefined,
  };
}
