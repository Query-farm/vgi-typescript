// Aggregate function framework — mirrors vgi-python's vgi/aggregate_function.py
// protocol at the wire level. Per-execution state lives in an in-memory store
// keyed by (execution_id, group_id); fine for subprocess workers and for
// single-instance HTTP. Multi-host HTTP deployments would need a pluggable
// state backend (same tradeoff Python's FunctionStorage wraps).
//
// DuckDB drives the lifecycle via five unary RPCs:
//
//   aggregate_bind      → resolve output schema + allocate execution_id
//   aggregate_update    → fold rows into per-group state
//   aggregate_combine   → merge parallel-build states
//   aggregate_finalize  → emit one result row per group_id
//   aggregate_destructor → free states (best-effort; see Python worker.py)

import { Schema, Field, RecordBatch, DataType } from "@query-farm/apache-arrow";
import type { Arguments } from "../arguments/arguments.js";
import type { ArgumentSpec } from "../arguments/argument-spec.js";
import type {
  FunctionMeta,
  VgiFunction,
} from "./types.js";
import { DEFAULT_MAX_WORKERS } from "../types.js";
import type { FunctionExample } from "./types.js";

// Column name DuckDB prepends to every aggregate_update batch. One entry per
// input row; the value identifies which group the row belongs to.
export const GROUP_COLUMN_NAME = "__vgi_group_id";

export interface AggregateBindParams<TArgs = Record<string, any>> {
  args: TArgs;
  arguments: Arguments;
  inputSchema: Schema | null;
  settings: Record<string, any>;
  secrets: Record<string, Record<string, any>>;
}

export interface AggregateUpdateParams<TArgs = Record<string, any>, TState = any> {
  states: Map<bigint, TState>;
  groupIds: bigint[];
  columns: any[];  // arrow Vectors per declared param
  args: TArgs;
  /**
   * Get-or-initialize accessor for per-group state. Call this inside the loop
   * only when the row actually contributes to the aggregate — skipping it
   * (e.g. on NULL input under NullHandling.DEFAULT) leaves `states` without
   * an entry for the group, so finalize() can return SQL NULL for that
   * group (matches vgi-python's "state absent = NULL" semantics for
   * SUM/AVG/MIN/MAX over zero non-null rows).
   */
  ensureState: (gid: bigint) => TState;
}

export interface AggregateFinalizeParams<TArgs = Record<string, any>, TState = any> {
  groupIds: bigint[];
  states: Map<bigint, TState | null>;
  outputSchema: Schema;
  args: TArgs;
}

export interface AggregateFunctionConfig<TArgs = Record<string, any>, TState = any> {
  name: string;
  description?: string;
  /**
   * Map of positional argument name → Arrow type. Passed through to DuckDB
   * function registration. Use `null` (untyped) for varargs placeholders.
   */
  args?: Record<string, DataType>;
  /**
   * Arrow type of the aggregate's output (one value per group). DuckDB uses
   * this to build the result column type at bind time.
   */
  outputType: DataType;
  /** Optional per-arg default values (positional only here). */
  argDefaults?: Record<string, any>;

  initialState: (params: AggregateBindParams<TArgs>) => TState;
  /**
   * Fold a batch of input rows into the provided per-group state map.
   * Implementations mutate `states` in place. `groupIds[i]` identifies which
   * group row i belongs to; `columns[k][i]` is the value for the kth declared
   * column argument at row i.
   */
  update: (params: AggregateUpdateParams<TArgs, TState>) => void;
  combine: (source: TState, target: TState, params: AggregateBindParams<TArgs>) => TState;
  /** Must return a single-column RecordBatch with `groupIds.length` rows. */
  finalize: (params: AggregateFinalizeParams<TArgs, TState>) => RecordBatch;

  examples?: FunctionExample[];
  categories?: string[];
  nullHandling?: "DEFAULT" | "SPECIAL";
}

// Per-execution state registry. Key = execution_id hex (binary → hex for Map
// keying). Value = (gid → state). Cleared on aggregate_destructor or worker
// shutdown.
type StateMap = Map<bigint, any>;
const stateStore = new Map<string, { states: StateMap; bindArgs: Arguments; bindParams: AggregateBindParams<any> }>();

function execKey(executionId: Uint8Array): string {
  let s = "";
  for (const b of executionId) s += b.toString(16).padStart(2, "0");
  return s;
}

export function getExecutionState(executionId: Uint8Array) {
  return stateStore.get(execKey(executionId));
}
export function setExecutionState(
  executionId: Uint8Array,
  entry: { states: StateMap; bindArgs: Arguments; bindParams: AggregateBindParams<any> },
) {
  stateStore.set(execKey(executionId), entry);
}
export function deleteExecutionState(executionId: Uint8Array) {
  stateStore.delete(execKey(executionId));
}

/**
 * Declare an aggregate function. The returned VgiFunction has `kind:
 * "aggregate"` plus a `config` handle the dispatch layer uses to drive the
 * aggregate_* RPCs; the scalar/table-function code paths ignore it.
 */
export function defineAggregate<TArgs = Record<string, any>, TState = any>(
  config: AggregateFunctionConfig<TArgs, TState>,
): VgiFunction & { aggregateConfig: AggregateFunctionConfig<TArgs, TState> } {
  const specs: ArgumentSpec[] = [];
  let posIdx = 0;
  if (config.args) {
    for (const [name, type] of Object.entries(config.args)) {
      const hasDefault = config.argDefaults?.[name] !== undefined;
      specs.push({
        name,
        position: hasDefault ? name : posIdx++,
        arrowType: type,
        isAnyType: false,
        isVarargs: false,
      });
    }
  }

  const meta: FunctionMeta = {
    name: config.name,
    description: config.description,
    examples: config.examples,
    categories: config.categories,
    nullHandling: config.nullHandling as any,
  };

  // The normal VgiFunction methods (bind/globalInit/createStreamHandlers) are
  // never invoked for aggregates — the dispatch layer routes aggregate_* RPCs
  // through the separate aggregate_* handler. Provide stubs that throw so
  // misrouted calls surface loudly rather than silently hanging.
  // Catalog output schema: single "result" column typed as the aggregate's
  // declared output type. DuckDB reads this at catalog introspection time to
  // register the function with the correct return type.
  const defaultOutputSchema = new Schema([new Field("result", config.outputType, true)]);

  return {
    kind: "aggregate" as any,
    meta,
    argumentSpecs: specs,
    defaultOutputSchema,
    bind: () => { throw new Error(`aggregate '${config.name}' received bind() — should be routed through aggregate_bind`); },
    globalInit: () => { throw new Error(`aggregate '${config.name}' received globalInit() — should be routed through aggregate_bind`); },
    createStreamHandlers: () => { throw new Error(`aggregate '${config.name}' is unary — no stream handlers`); },
    aggregateConfig: config,
  } as VgiFunction & { aggregateConfig: AggregateFunctionConfig<TArgs, TState> };
}
