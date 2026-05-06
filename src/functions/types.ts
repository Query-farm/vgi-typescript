// Function config interfaces and VgiFunction base type.

import type { Schema, RecordBatch, DataType } from "@query-farm/apache-arrow";
import type { OutputCollector } from "vgi-rpc";
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
}

export interface StreamHandlers {
  // For producer streams (table functions, finalize phase)
  producerInit?: () => any;
  producerFn?: (state: any, out: OutputCollector) => void | Promise<void>;
  // For exchange streams (scalar, table-in-out input phase)
  exchangeInit?: () => any;
  exchangeFn?: (
    state: any,
    input: RecordBatch,
    out: OutputCollector
  ) => void | Promise<void>;
  // Output schema for the stream
  outputSchema: Schema;
  // Input schema for exchange (empty for producer)
  inputSchema?: Schema;
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
  kind: "scalar" | "table" | "table_in_out";
  meta: FunctionMeta;
  argumentSpecs: ArgumentSpec[];
  /** Default output schema (for catalog registration). May be overridden at bind time. */
  defaultOutputSchema?: Schema;
  bind(request: BindRequest): BindResponse;
  globalInit(request: InitRequest): GlobalInitResponse | Promise<GlobalInitResponse>;
  createStreamHandlers(
    request: InitRequest,
    response: GlobalInitResponse,
    accumulatedState?: any,
  ): StreamHandlers;
  cardinality?(request: TableFunctionCardinalityRequest): TableCardinality;
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
