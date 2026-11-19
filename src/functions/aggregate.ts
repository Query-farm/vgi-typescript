// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Aggregate function framework — mirrors vgi-python's vgi/aggregate_function.py
// protocol at the wire level. Per-execution state lives in FunctionStorage
// (sharded per attach, like every other function kind), so any worker process
// -- or host, or Cloudflare isolate -- can serve any aggregate RPC of an
// execution: group states under FrameworkNS.AGGREGATE_STATE keyed by
// packIntKey(group_id), and the bind context, sealed, at packIntKey(-2).
//
// DuckDB drives the lifecycle via five unary RPCs:
//
//   aggregate_bind      → resolve output schema + allocate execution_id
//   aggregate_update    → fold rows into per-group state
//   aggregate_combine   → merge parallel-build states
//   aggregate_finalize  → emit one result row per group_id
//   aggregate_destructor → free states (best-effort; see Python worker.py)

import { normalizeRequiredSecrets, type RequiredSecret } from "../secrets/requirements.js";
import { type VgiSchema, schema, type VgiField, field, type VgiBatch, type VgiDataType, nullType, isNull } from "../arrow/index.js";
import type { Arguments } from "../arguments/arguments.js";
import {
  constraintSpecFields,
  type ArgumentSpec,
  type ArgumentConstraints,
} from "../arguments/argument-spec.js";
import { assertArrowType, assertArrowTypes } from "../arguments/argument-spec.js";
import type {
  FunctionMeta,
  VgiFunction,
} from "./types.js";
import { DEFAULT_MAX_WORKERS } from "../types.js";
import type { FunctionExample } from "./types.js";
import { FrameworkNS, packIntKey, type BoundStorage } from "./storage.js";
import { ExecutionCache } from "./execution-cache.js";
import { StoragePayloadError } from "./storage-seal.js";

// Column name DuckDB prepends to every aggregate_update batch. One entry per
// input row; the value identifies which group the row belongs to.
export const GROUP_COLUMN_NAME = "__vgi_group_id";

export interface AggregateBindParams<TArgs = Record<string, any>> {
  args: TArgs;
  arguments: Arguments;
  inputSchema: VgiSchema | null;
  settings: Record<string, any>;
  secrets: Record<string, Record<string, any>>;
  /** Resolved names aligned with the complete logical argument order. */
  argumentNames: (string | null)[] | null;
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
  outputSchema: VgiSchema;
  args: TArgs;
}

export interface AggregateFunctionConfig<TArgs = Record<string, any>, TState = any> {
  name: string;
  description?: string;
  /**
   * Map of positional argument name → Arrow type. Passed through to DuckDB
   * function registration. Use `null` (untyped) for varargs placeholders.
   */
  args?: Record<string, VgiDataType>;
  /**
   * Names of args that accept a variable number of column arguments. DuckDB
   * treats these as varargs of the declared Arrow type; at call time the
   * function may receive 1..N columns of that type. update() sees all of
   * them as consecutive entries in `columns`.
   */
  varargs?: string[];
  /**
   * Names of args whose value is constant (known at bind time) and must be
   * folded away by DuckDB before reaching update(). The value arrives in
   * `bindParams.args[name]`; it is NOT passed as an input column in the
   * update batch. Use for aggregates parameterized by a literal (e.g.
   * percentile=0.5 in vgi_percentile).
   */
  constParams?: string[];
  /**
   * Arrow type of the aggregate's output (one value per group). DuckDB uses
   * this to build the result column type at bind time. May be overridden
   * at bind time by `onBind` (returning a different Arrow type) for
   * aggregates whose return type depends on the input column type
   * (e.g. vgi_generic_sum: BIGINT input → BIGINT output, DOUBLE → DOUBLE).
   */
  outputType: VgiDataType;
  /**
   * Optional bind-time hook for dynamic output types. Receives the bind
   * parameters (including input_schema) and returns the Arrow type to
   * advertise for this invocation. Defaults to returning `config.outputType`.
   */
  onBind?: (params: AggregateBindParams<TArgs>) => VgiDataType | Promise<VgiDataType>;
  /** Optional per-arg default values (positional only here). */
  argDefaults?: Record<string, any>;
  /** Authoritative typed defaults: one row, defaulted parameters only. */
  parameterDefaultValues?: VgiBatch | null;
  /**
   * Per-argument discovery constraints (choices / ge / le / gt / lt / pattern),
   * keyed by argument name. Surfaced via `vgi_function_arguments()` and enforced
   * at aggregate_bind for const params: a value violating a declared constraint
   * fails the bind with an ArgumentValidationError.
   */
  argConstraints?: Record<string, ArgumentConstraints>;

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
  finalize: (params: AggregateFinalizeParams<TArgs, TState>) => VgiBatch;

  examples?: FunctionExample[];
  categories?: string[];
  /** Arbitrary function-level tags surfaced into duckdb_functions().tags. */
  tags?: Record<string, string>;
  nullHandling?: "DEFAULT" | "SPECIAL";
  /**
   * DuckDB secret types this aggregate needs. Advertised on the catalog's
   * `required_secrets` so the C++ extension pre-resolves matching secrets and
   * delivers their VALUES on the aggregate_bind request (keyed by secret name).
   * The secret is read at bind time only — update/combine/finalize receive an
   * empty ResolvedSecrets. Mirrors vgi-python's `Secret()` annotation on an
   * aggregate `on_bind`.
   */
  requiredSecrets?: RequiredSecret[];
}

// ============================================================================
// Execution state in FunctionStorage
//
// Under DuckDB's parallel aggregate with worker pooling (and over HTTP, where
// every RPC may land on another host), the aggregate_* RPCs of one execution
// reach different processes, so state lives in FunctionStorage, the same
// shared store the table functions use:
//
//   ns = FrameworkNS.AGGREGATE_STATE ("_vgi/aggregate_state")
//   key = packIntKey(group_id)  → that group's state (JSON, bigint-tagged);
//                                 the function's own data, not sealed
//   key = packIntKey(-2)        → the bind context (constant arguments, input
//                                 schema, argument names), SEALED to its slot
//
// Each update/combine/finalize RPC is one batched read of the groups it
// touches plus (update/combine) one batched write: two storage round trips per
// batch, whatever the group count. The bind context is written once at bind
// and cached per (shard, execution) in-process, so it costs a read only on a
// process's first RPC of an execution. Secret VALUES are never stored: they are
// delivered on aggregate_bind only, and update/combine/finalize see an empty
// `secrets` -- the documented contract (see `requiredSecrets`).
// ============================================================================

/** Key of the sealed bind context within FrameworkNS.AGGREGATE_STATE. */
export const AGGREGATE_CONTEXT_KEY = packIntKey(-2);

/** Per-(shard, execution) cache of the decoded bind context. */
export const aggregateContextCache = new ExecutionCache<AggregateBindParams<any>>(64);

// Custom JSON codec: BigInt ↔ {"__bigint__":"…"}. Keep the encoder/decoder
// centralized so aggregate state classes with int64 fields (CountState,
// SumState, etc.) round-trip without extra wiring.
function jsonReplacer(_: string, v: any): any {
  if (typeof v === "bigint") return { __bigint__: v.toString() };
  return v;
}
function jsonReviver(_: string, v: any): any {
  if (v && typeof v === "object" && typeof (v as any).__bigint__ === "string") {
    return BigInt((v as any).__bigint__);
  }
  return v;
}

const UTF8_ENC = new TextEncoder();
const UTF8_DEC = new TextDecoder();

/** Encode one group's state for storage. */
export function encodeGroupState(state: unknown): Uint8Array {
  return UTF8_ENC.encode(JSON.stringify(state, jsonReplacer));
}

/** Decode one group's state from storage. */
export function decodeGroupState(bytes: Uint8Array): any {
  return JSON.parse(UTF8_DEC.decode(bytes), jsonReviver);
}

function b64(buf: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < buf.length; i++) binary += String.fromCharCode(buf[i]!);
  return btoa(binary);
}
function unb64(s: string): Uint8Array {
  const binary = atob(s);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}

/**
 * The stored bind context: the wire `arguments` and `input_schema` bytes and
 * the argument names, exactly as aggregate_bind received them, so a cold
 * process rebuilds the same AggregateBindParams (args are re-extracted from
 * the arguments with the function's declaration). No secrets.
 */
export interface StoredAggregateContext {
  argumentsIpc: Uint8Array;
  inputSchemaIpc: Uint8Array | null;
  argumentNames: (string | null)[] | null;
}

export function encodeAggregateContext(ctx: StoredAggregateContext): Uint8Array {
  return UTF8_ENC.encode(
    JSON.stringify({
      v: 1,
      arguments: b64(ctx.argumentsIpc),
      input_schema: ctx.inputSchemaIpc ? b64(ctx.inputSchemaIpc) : null,
      argument_names: ctx.argumentNames,
    }),
  );
}

export function decodeAggregateContext(bytes: Uint8Array): StoredAggregateContext {
  let d: any;
  try {
    d = JSON.parse(UTF8_DEC.decode(bytes));
  } catch {
    throw new StoragePayloadError("aggregate: unrecognized bind context payload");
  }
  if (!d || d.v !== 1 || typeof d.arguments !== "string") {
    throw new StoragePayloadError("aggregate: unrecognized bind context payload format");
  }
  return {
    argumentsIpc: unb64(d.arguments),
    inputSchemaIpc: typeof d.input_schema === "string" ? unb64(d.input_schema) : null,
    argumentNames: Array.isArray(d.argument_names) ? d.argument_names : null,
  };
}

/** Read the stored states of `gids` (absent groups are left out of the map). */
export async function loadGroupStates(storage: BoundStorage, gids: bigint[]): Promise<Map<bigint, any>> {
  const states = new Map<bigint, any>();
  if (gids.length === 0) return states;
  const values = await storage.stateGetMany(FrameworkNS.AGGREGATE_STATE, gids.map((g) => packIntKey(g)));
  for (let i = 0; i < gids.length; i++) {
    const v = values[i];
    if (v != null) states.set(gids[i]!, decodeGroupState(v));
  }
  return states;
}

/** Write the states of `gids` that are present in `states` (one batched put). */
export async function storeGroupStates(
  storage: BoundStorage,
  states: Map<bigint, any>,
  gids: Iterable<bigint>,
): Promise<void> {
  const items: Array<[Uint8Array, Uint8Array]> = [];
  for (const gid of gids) {
    if (states.has(gid)) items.push([packIntKey(gid), encodeGroupState(states.get(gid))]);
  }
  if (items.length > 0) await storage.statePutMany(FrameworkNS.AGGREGATE_STATE, items);
}

/**
 * Declare an aggregate function. The returned VgiFunction has `kind:
 * "aggregate"` plus a `config` handle the dispatch layer uses to drive the
 * aggregate_* RPCs; the scalar/table-function code paths ignore it.
 */
export function defineAggregate<TArgs = Record<string, any>, TState = any>(
  config: AggregateFunctionConfig<TArgs, TState>,
): VgiFunction & { aggregateConfig: AggregateFunctionConfig<TArgs, TState> } {
  assertArrowTypes((config as any).args, `defineAggregate("${config.name}"): args`);

  const specs: ArgumentSpec[] = [];
  let posIdx = 0;
  const varargsSet = new Set(config.varargs ?? []);
  const constSet = new Set(config.constParams ?? []);
  if (config.args) {
    for (const [name, type] of Object.entries(config.args)) {
      const hasDefault = config.argDefaults?.[name] !== undefined;
      const isConst = constSet.has(name);
      const isAnyType = isNull(type);
      // Const params are always positional at the SQL call-site — a default
      // value just means "the user may omit it and we'll use the default",
      // not "rename to a named kwarg". Non-const args with defaults become
      // named (matches table-function behavior).
      specs.push({
        name,
        position: isConst || !hasDefault ? posIdx++ : name,
        arrowType: isAnyType ? nullType() : type,
        isAnyType,
        isVarargs: varargsSet.has(name),
        isConst,
        ...constraintSpecFields(
          config.argConstraints?.[name],
          config.argDefaults?.[name],
        ),
      });
    }
  }

  const meta: FunctionMeta = {
    name: config.name,
    description: config.description,
    examples: config.examples,
    categories: config.categories,
    tags: config.tags,
    parameterDefaultValues: config.parameterDefaultValues,
    nullHandling: config.nullHandling as any,
    requiredSecrets: normalizeRequiredSecrets(config.requiredSecrets, config.name),
  };

  // The normal VgiFunction methods (bind/globalInit/createStreamHandlers) are
  // never invoked for aggregates — the dispatch layer routes aggregate_* RPCs
  // through the separate aggregate_* handler. Provide stubs that throw so
  // misrouted calls surface loudly rather than silently hanging.
  // Catalog output schema: single "result" column. Advertise config.outputType
  // when it's a real type (anything other than Null) — onBind may still override
  // at bind time, but for the catalog listing we prefer the static declaration
  // so duckdb_functions() shows a meaningful return type. Aggregates whose
  // return type genuinely depends on input set outputType: nullType() to
  // advertise ANY explicitly.
  const isAnyType = isNull(config.outputType);
  const defaultOutputSchema = isAnyType
    ? schema([field("result", nullType(), true, new Map([["vgi:any", "true"]]))])
    : schema([field("result", config.outputType, true)]);

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
