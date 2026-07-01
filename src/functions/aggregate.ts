// Copyright 2025, 2026 Query Farm LLC - https://query.farm
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

import { type VgiSchema, schema, type VgiField, field, type VgiBatch, type VgiDataType, nullType, isNull } from "../arrow/index.js";
import type { Arguments } from "../arguments/arguments.js";
import type { ArgumentSpec } from "../arguments/argument-spec.js";
import type {
  FunctionMeta,
  VgiFunction,
} from "./types.js";
import { DEFAULT_MAX_WORKERS } from "../types.js";
import type { FunctionExample } from "./types.js";
// Aggregate state persistence uses node:fs/path/os to spill per-group state to
// disk under VGI_AGG_STATE_DIR. Workers (Cloudflare workerd / browsers)
// don't define aggregate functions in the first place, so we resolve the
// node:* modules through indirect string variables that esbuild can't trace.
// The bundle stays clean even without nodejs_compat polyfills.
const _NODE_FS_MOD = "node:fs";
const _NODE_PATH_MOD = "node:path";
const _NODE_OS_MOD = "node:os";
function _aggReq(): any {
  const req: any = (import.meta as any).require ?? (globalThis as any).require ?? null;
  if (!req) {
    throw new Error(
      "Aggregate state persistence requires Node.js or Bun (node:fs/path/os).",
    );
  }
  return req;
}
const fs: any = new Proxy({} as any, {
  get: (_t, prop) => (_aggReq()(_NODE_FS_MOD) as any)[prop],
});
const path: any = new Proxy({} as any, {
  get: (_t, prop) => (_aggReq()(_NODE_PATH_MOD) as any)[prop],
});
const os: any = new Proxy({} as any, {
  get: (_t, prop) => (_aggReq()(_NODE_OS_MOD) as any)[prop],
});

// Column name DuckDB prepends to every aggregate_update batch. One entry per
// input row; the value identifies which group the row belongs to.
export const GROUP_COLUMN_NAME = "__vgi_group_id";

export interface AggregateBindParams<TArgs = Record<string, any>> {
  args: TArgs;
  arguments: Arguments;
  inputSchema: VgiSchema | null;
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
  requiredSecrets?: string[];
}

// Per-execution state registry. Value = (gid → state) + bind context.
//
// Under DuckDB's parallel aggregate with worker pooling, aggregate_* RPCs for
// the same execution_id can land on different worker processes — so state
// needs to live in a place all workers can reach. We use a filesystem-backed
// store (one directory per execution_id, one JSON file per group_id) with
// atomic rename-on-write. Fine for subprocess transport and for
// single-instance HTTP; multi-host HTTP would need a shared path or a
// swap-in network backend. Override with VGI_AGG_STATE_DIR.
//
// In-memory cache sits in front of the filesystem to keep single-process
// workloads fast — writes go to disk, reads prefer cache, and getExecutionState
// lazily reconstructs from disk when a different worker's exec is requested.

type StateMap = Map<bigint, any>;

interface ExecEntry {
  states: StateMap;
  bindArgs: Arguments;
  bindParams: AggregateBindParams<any>;
  loaded: Set<bigint>; // gids fetched from disk (so we don't refetch in same call)
}

const stateStore = new Map<string, ExecEntry>();

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

function stateDirRoot(): string {
  return process.env.VGI_AGG_STATE_DIR ?? path.join(os.tmpdir(), "vgi-agg");
}

function execDir(execHex: string): string {
  return path.join(stateDirRoot(), execHex);
}

function execKey(executionId: Uint8Array): string {
  let s = "";
  for (const b of executionId) s += b.toString(16).padStart(2, "0");
  return s;
}

// Atomic write: write to tmp then rename. Avoids partial reads from another
// worker mid-update. Node's fs.renameSync is atomic on POSIX for same FS.
function atomicWriteJson(filePath: string, value: any): void {
  const tmp = filePath + ".tmp." + process.pid + "." + Math.random().toString(36).slice(2);
  const parent = path.dirname(filePath);
  fs.mkdirSync(parent, { recursive: true });
  fs.writeFileSync(tmp, JSON.stringify(value, jsonReplacer));
  fs.renameSync(tmp, filePath);
}

function tryReadJson(filePath: string): any | undefined {
  try {
    const text = fs.readFileSync(filePath, "utf8");
    return JSON.parse(text, jsonReviver);
  } catch (e: any) {
    if (e?.code === "ENOENT") return undefined;
    throw e;
  }
}

/**
 * Fetch the execution context — bind args/params + states for requested gids.
 * On cache miss, tries to reconstruct from on-disk files written by a
 * sibling worker. Returns undefined if the exec never existed anywhere.
 * `requestedGids` (optional) tells us which group states to pre-load.
 */
export function getExecutionState(
  executionId: Uint8Array,
  requestedGids?: Iterable<bigint>,
): ExecEntry | undefined {
  const key = execKey(executionId);
  let entry = stateStore.get(key);
  if (!entry) {
    // Try to reconstruct bind context from disk.
    const ctx = tryReadJson(path.join(execDir(key), "context.json"));
    if (!ctx) return undefined;
    // Restore bindParams + args from the serialized context. args is kept
    // only as a plain dict; the Arguments instance isn't needed after bind.
    entry = {
      states: new Map(),
      bindArgs: ctx.bindArgs, // placeholder — Arguments instance is only needed during bind
      bindParams: ctx.bindParams,
      loaded: new Set(),
    };
    stateStore.set(key, entry);
  }
  if (requestedGids) {
    // Always re-read from disk each RPC. A sibling worker may have just
    // written a newer value for this gid; relying on our in-memory cache
    // would overwrite their update on our next persist (read-modify-write
    // race). File I/O per-RPC is cheap compared to the RPC overhead.
    for (const gid of requestedGids) {
      const stateFile = path.join(execDir(key), gid.toString() + ".json");
      const s = tryReadJson(stateFile);
      if (s !== undefined) {
        entry.states.set(gid, s);
      } else {
        entry.states.delete(gid);
      }
      entry.loaded.add(gid);
    }
  }
  return entry;
}

export function setExecutionState(
  executionId: Uint8Array,
  entry: Omit<ExecEntry, "loaded">,
): void {
  const key = execKey(executionId);
  stateStore.set(key, { ...entry, loaded: new Set() });
  // Persist bind context so sibling workers can reconstruct it on demand.
  atomicWriteJson(path.join(execDir(key), "context.json"), {
    bindParams: entry.bindParams,
    // bindArgs is a live Arguments instance; stash its args map separately
    // if needed for future state-dependent behavior. Workers reading the
    // context receive `bindParams.args` which is what update/finalize use.
  });
}

/**
 * Persist the given group states to disk so sibling workers can pick them up.
 * Deletes the on-disk entry when the in-memory state was removed (gid no
 * longer present in states map).
 */
export function persistGroupStates(
  executionId: Uint8Array,
  gids: Iterable<bigint>,
): void {
  const key = execKey(executionId);
  const entry = stateStore.get(key);
  if (!entry) return;
  for (const gid of gids) {
    const stateFile = path.join(execDir(key), gid.toString() + ".json");
    if (entry.states.has(gid)) {
      atomicWriteJson(stateFile, entry.states.get(gid));
    } else {
      try { fs.unlinkSync(stateFile); } catch { /* already gone */ }
    }
    entry.loaded.add(gid);
  }
}

export function deleteExecutionState(executionId: Uint8Array): void {
  const key = execKey(executionId);
  stateStore.delete(key);
  try { fs.rmSync(execDir(key), { recursive: true, force: true }); } catch { /* best-effort */ }
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
      });
    }
  }

  const meta: FunctionMeta = {
    name: config.name,
    description: config.description,
    examples: config.examples,
    categories: config.categories,
    tags: config.tags,
    nullHandling: config.nullHandling as any,
    requiredSecrets: config.requiredSecrets,
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
