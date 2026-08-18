// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Function config interfaces and VgiFunction base type.

import type { VgiSchema, VgiBatch, VgiDataType } from "../arrow/index.js";
import type { OutputCollector } from "@query-farm/vgi-rpc";
import type {
  FunctionStability,
  NullHandling,
  OrderPreservation,
  OrderDependence,
  DistinctDependence,
  TableCardinality,
} from "../types.js";
import type {
  BindRequest,
  BindResponse,
  InitRequest,
  GlobalInitResponse,
  TableFunctionCardinalityRequest,
} from "../protocol/types.js";
import type { Arguments } from "../arguments/arguments.js";
import type { ArgumentSpec } from "../arguments/argument-spec.js";

/** Convention for handler state: mutable user state lives in `.state`. */
export interface HandlerState<T = any> {
  /** Serializable user state for HTTP exchange round-trips. */
  state: T;
  [key: string]: any;
}

export interface FunctionExample {
  sql: string;
  description: string;
  expectedOutput?: string;
}

export interface FunctionMeta {
  name: string;
  description?: string;
  stability?: FunctionStability;
  nullHandling?: NullHandling;
  examples?: FunctionExample[];
  categories?: string[];
  tags?: Record<string, string>;
  projectionPushdown?: boolean;
  filterPushdown?: boolean;
  samplingPushdown?: boolean;
  /**
   * table (generator): opt in to DuckDB's late-materialization rewrite. A
   * TOP_N/LIMIT/SAMPLE over a rowid-bearing table is rewritten into a SEMI
   * join — a narrow ordering scan selects survivors, then the wide scan
   * re-fetches their columns with the surviving rowids pushed down. Surfaces
   * as FunctionInfo `late_materialization`. Only honoured by the C++ extension
   * for tables whose worker also guarantees a UNIQUE, snapshot-stable rowid.
   */
  lateMaterialization?: boolean;
  supportedExpressionFilters?: string[];
  autoApplyFilters?: boolean;
  preservesOrder?: OrderPreservation;
  maxWorkers?: number;
  requiredSettings?: string[];
  requiredSecrets?: string[];
  orderDependent?: OrderDependence;
  distinctDependent?: DistinctDependence;
  /**
   * For table_in_out functions: whether the user defined a finalize callback.
   * DuckDB issues a separate FINALIZE init() phase only when this is true;
   * otherwise calling FinalExecute is unsupported and crashes the C++ side.
   */
  hasFinalize?: boolean;
  /**
   * Blended ("UNNEST-style") table-in-out: the function's positional args ARE
   * its per-row input columns (real typed args, no synthetic TABLE
   * placeholder), so ONE registration serves f(52,13) (literal -> 1 input
   * row), FROM t, f(t.x, t.y) (columns -> streaming), and LATERAL f(t.x,t.y).
   * Set by {@link defineRowTransformFunction}; surfaces as
   * FunctionInfo.input_from_args. The C++ extension reads it to enter the
   * in-out registration branch with real-typed args and drive the literal
   * single-row scan-mode. Mirrors vgi-python's RowTransformFunction /
   * ResolvedMetadata.input_from_args.
   */
  inputFromArgs?: boolean;
  /**
   * table_buffering: force ParallelSink=false in the C++ operator
   * (single-thread, source-ordered ingest).
   */
  sinkOrderDependent?: boolean;
  /**
   * table_buffering: force serial Source drain in finalize_queue order
   * (ParallelSource=false, SourceOrder=FIXED_ORDER).
   */
  sourceOrderDependent?: boolean;
  /**
   * table_buffering: thread DuckDB's per-chunk batch_index into every
   * process() call (RequiredPartitionInfo=BatchIndex).
   */
  requiresInputBatchIndex?: boolean;
  /**
   * table (generator): the function tags every emitted Arrow batch with a
   * per-partition `vgi_batch_index` so DuckDB's ordered sinks reassemble
   * parallel output in partition order. Surfaces as FunctionInfo
   * `supports_batch_index`.
   */
  supportsBatchIndex?: boolean;
  /**
   * table (generator): the scan divides into named, independently redeemable
   * splits (see the `plan` / `onSplit` hooks). Surfaces as FunctionInfo
   * `supports_splits`.
   *
   * The declaration is what a distributed engine reads to decide whether it can
   * retry a task: a split NAMES its work, so re-running one reads exactly the
   * same rows. Without it, the division lives behind an opaque worker-side queue
   * and a retry reads neither what it read before nor what it missed.
   */
  supportsSplits?: boolean;
  /**
   * table (generator): the worker applies every pushed filter EXACTLY, so the
   * engine may drop its own copy rather than re-filtering. Wrong answers if
   * declared falsely — leave unset unless certain.
   */
  filtersExactlyApplied?: boolean;
  /**
   * table (generator): addressable positions in the data, for incremental and
   * streaming reads. The engine owns the checkpoint; this declares only that
   * positions are meaningful to externalize.
   */
  supportsPositions?: boolean;
  /**
   * table (generator): how long a split token stays redeemable.
   *
   * UNSET MEANS UNBOUNDED, not "expires immediately" — a client must not assume
   * a TTL exists, or long-running streams are foreclosed. A client rejects a
   * plan whose TTL is below its own planning-to-task-start horizon, which on a
   * busy cluster is hours rather than seconds.
   */
  splitTokenTtlSeconds?: number;
  /**
   * table (generator): Hive-style partition-columns mode. Functions declare
   * a PartitionKind and annotate bind-schema fields; emitted batches carry
   * `vgi_partition_values#b64` metadata. Surfaces as FunctionInfo
   * `partition_kind`.
   */
  partitionKind?: "NOT_PARTITIONED" | "SINGLE_VALUE_PARTITIONS" | "OVERLAPPING_PARTITIONS" | "DISJOINT_PARTITIONS";
  /**
   * COPY ... FROM custom format reader: the SQL `FORMAT` identifier this
   * function backs (e.g. `example_lines`). Set by {@link defineCopyFromFunction}.
   * When present, the catalog advertises this function via
   * `copyFromFormats()` / the `catalog_copy_from_formats` RPC so the VGI
   * extension registers a DuckDB CopyFunction for it. Mirrors vgi-python's
   * `CopyFromFunction.COPY_FROM_FORMAT`.
   */
  copyFromFormat?: string;
  /** COPY direction; only `"from"` is supported today. Default `"from"`. */
  copyFromDirection?: string;
  /** Optional free-text comment surfaced by `vgi_copy_formats()`. */
  copyFromComment?: string | null;
  /**
   * COPY ... TO custom format writer: the SQL `FORMAT` identifier this function
   * backs (e.g. `example_lines_out`). Set by {@link defineCopyToFunction}. When
   * present, the catalog advertises this function via `copyFromFormats()` (the
   * `catalog_copy_from_formats` RPC returns all directions) so the VGI
   * extension registers a DuckDB CopyFunction for it. The writer is a
   * `table_buffering` function under the hood, reusing the
   * `table_buffering_process` / `table_buffering_combine` RPCs. Mirrors
   * vgi-python's `CopyToFunction.COPY_TO_FORMAT`.
   */
  copyToFormat?: string;
  /** COPY direction for a TO writer; always `"to"`. Default `"to"`. */
  copyToDirection?: string;
  /** Optional free-text comment surfaced by `vgi_copy_formats()`. */
  copyToComment?: string | null;
}

export interface StreamHandlers {
  // For producer streams (table functions, finalize phase)
  producerInit?: () => any;
  producerFn?: (state: any, out: OutputCollector) => void | Promise<void>;
  // For exchange streams (scalar, table-in-out input phase)
  exchangeInit?: () => any;
  exchangeFn?: (
    state: any,
    input: VgiBatch,
    out: OutputCollector
  ) => void | Promise<void>;
  // Output schema for the stream
  outputSchema: VgiSchema;
  // Input schema for exchange (empty for producer)
  inputSchema?: VgiSchema;
  /**
   * Fires once per tick batch on the producer path, before `producerFn`.
   * Receives the tick's Arrow custom metadata so the handler can pick up
   * per-tick signals like `vgi_pushdown_filters` (dynamic filter updates
   * from DuckDB's Top-N optimizer).
   */
  onTick?: (state: any, tickMetadata: Map<string, string> | undefined) => void | Promise<void>;
}

/**
 * A resolved, registered VGI function definition.
 */
export interface VgiFunction {
  kind: "scalar" | "table" | "table_in_out" | "table_buffering";
  meta: FunctionMeta;
  argumentSpecs: ArgumentSpec[];
  /** Default output schema (for catalog registration). May be overridden at bind time. */
  defaultOutputSchema?: VgiSchema;
  bind(request: BindRequest): BindResponse | Promise<BindResponse>;
  globalInit(request: InitRequest): GlobalInitResponse | Promise<GlobalInitResponse>;
  createStreamHandlers(
    request: InitRequest,
    response: GlobalInitResponse,
    accumulatedState?: any,
  ): StreamHandlers;
  cardinality?(request: TableFunctionCardinalityRequest): TableCardinality | Promise<TableCardinality>;
  /**
   * Divide a scan into named, independently redeemable splits. Absent means the
   * function is not split-capable: the whole scan is one unit of work, and the
   * framework answers with a single empty-payload split.
   */
  plan?(request: import("../protocol/serializers/splits.js").TableFunctionPlanRequest):
    | import("../protocol/serializers/splits.js").PlanResult
    | Promise<import("../protocol/serializers/splits.js").PlanResult>;
  /**
   * Per-column statistics for this table function's output given the user's
   * bind-time arguments. Returns null/[] when stats are unknown. Wired to the
   * `table_function_statistics` RPC; DuckDB uses the bounds for plan-time
   * filter elimination.
   */
  statistics?(request: TableFunctionCardinalityRequest): import("../util/statistics.js").ColumnStatistics[] | null;
  /**
   * Per-execution diagnostics for EXPLAIN ANALYZE. DuckDB calls this at
   * pipeline FinishSource via the `table_function_dynamic_to_string` RPC.
   * Returns ordered key→value strings; the C++ extension merges these with
   * the intrinsic keys (Function, Rows Read, Threads).
   */
  dynamicToString?(request: DynamicToStringRequest): Record<string, string> | Promise<Record<string, string>>;
}

/** Request bundle for the `table_function_dynamic_to_string` RPC. */
export interface DynamicToStringRequest {
  bindCall: BindRequest;
  bindOpaqueData: Uint8Array | null;
  globalExecutionId: Uint8Array;
}
