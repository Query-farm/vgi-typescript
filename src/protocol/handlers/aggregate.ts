// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Handlers for the aggregate function lifecycle: bind allocates execution_id
// and output schema, update folds rows into per-group state, combine merges
// states across workers, finalize emits one result row per group, destructor
// drops finished group state.

import {
  type VgiSchema,
  type VgiBatch,
  type VgiDataType,
  schema as makeSchema,
  field,
  isDecimal,
  serializeBatch,
  deserializeBatch,
} from "../../arrow/index.js";
import type { VgiService } from "../../generated/vgi-service.js";
import type { FunctionRegistry } from "../../functions/registry.js";
import { deserializeArguments } from "../serialize.js";
import { deserializeSchema, serializeSchema, batchToSecretDict } from "../../util/arrow/index.js";
import { toUint8Array } from "../../util/bytes.js";
import {
  AggregateBindResultSchema,
  AggregateFinalizeResultSchema,
} from "../../generated/vgi-protocol-schemas.js";
import {
  GROUP_COLUMN_NAME,
  AGGREGATE_CONTEXT_KEY,
  aggregateContextCache,
  encodeAggregateContext,
  decodeAggregateContext,
  loadGroupStates,
  storeGroupStates,
  type AggregateFunctionConfig,
  type AggregateBindParams,
} from "../../functions/aggregate.js";
import { BoundStorage, FrameworkNS, storage as defaultStorage } from "../../functions/storage.js";
import { openAttachWithShard } from "./catalog/shared.js";
import { currentRequestAuth } from "../../request-auth.js";
import { Arguments } from "../../arguments/arguments.js";
import { validateConstConstraints } from "../../arguments/argument-spec.js";
import { unwrapRequest, wrapResult } from "./shared.js";

/** The aggregate lifecycle's `vgi.v3` handlers. */
export function aggregateHandlers(
  registry: FunctionRegistry,
  signingKey?: Uint8Array,
  attachSecretKey?: Uint8Array,
  storageSealKey: Uint8Array | undefined = signingKey,
): Partial<VgiService> {
  // `schemaPath` scopes resolution to the schema the caller named. Every
  // aggregate RPC carries it (protocol 1.2.0), so an aggregate name declared in
  // two schemas dispatches to the one the request names rather than colliding
  // on the flat by-name index. `null` (no schema) keeps the cross-schema lookup.
  function resolveAggregate(
    name: string,
    schemaPath?: string[] | null,
  ): AggregateFunctionConfig<any, any> {
    const func = registry.get(name, {
      arguments: new Arguments(),
      inputSchema: null,
      isScalar: false,
      schemaPath: schemaPath ?? null,
    }) as any;
    if (!func || func.kind !== "aggregate") {
      throw new Error(`Function '${name}' is not an aggregate function`);
    }
    return func.aggregateConfig as AggregateFunctionConfig<any, any>;
  }

  // The execution's storage: its attach's shard (derived from the request's
  // authenticated attach, as for every other function kind), scoped by the
  // execution id.
  async function boundFor(innerParams: Record<string, any>, ctx: any, executionId: Uint8Array): Promise<BoundStorage> {
    const raw = innerParams.attach_opaque_data ? toUint8Array(innerParams.attach_opaque_data) : null;
    const { shardKey } = await openAttachWithShard(raw, ctx?.auth ?? currentRequestAuth(), signingKey, attachSecretKey);
    return new BoundStorage(defaultStorage, executionId, null, shardKey);
  }

  // The bind context of an execution: cached in-process, else the sealed
  // storage copy (authoritative). Throws when the execution is unknown.
  async function loadContext(
    cfg: AggregateFunctionConfig<any, any>,
    storage: BoundStorage,
    rpc: string,
  ): Promise<AggregateBindParams<any>> {
    const cached = aggregateContextCache.get(storage.shardKey, storage.executionId);
    if (cached) return cached;
    const payload = await storage.sealedGet(FrameworkNS.AGGREGATE_STATE, AGGREGATE_CONTEXT_KEY, storageSealKey);
    if (payload == null) {
      throw new Error(`${rpc}: unknown execution_id for '${cfg.name}' (bind missing or state dropped)`);
    }
    const stored = decodeAggregateContext(payload);
    const args = deserializeArguments(stored.argumentsIpc);
    const params: AggregateBindParams<any> = {
      args: extractArgMap(cfg, args),
      arguments: args,
      inputSchema: stored.inputSchemaIpc ? deserializeSchema(stored.inputSchemaIpc) : null,
      settings: {},
      // Secret values are delivered on aggregate_bind only, and never stored.
      secrets: {},
      argumentNames: stored.argumentNames,
    };
    aggregateContextCache.set(storage.shardKey, storage.executionId, params);
    return params;
  }

  // ------------------------------------------------------------------------
  // aggregate_bind — allocate execution_id, return output schema
  // ------------------------------------------------------------------------
  const aggregateBind: VgiService["aggregateBind"] = async (params, ctx?: any) => {
    const innerParams = unwrapRequest(params.request);
    const functionName: string = innerParams.function_name;
    const cfg = resolveAggregate(functionName, innerParams.schema_path ?? null);
    const argBytes = toUint8Array(innerParams.arguments);
    const args = deserializeArguments(argBytes);
    const inputSchemaIpc = innerParams.input_schema ? toUint8Array(innerParams.input_schema) : null;
    const inputSchema = inputSchemaIpc && inputSchemaIpc.length > 0 ? deserializeSchema(inputSchemaIpc) : null;
    // Decode the resolved-secrets batch the extension pre-resolves for
    // aggregates that declared `requiredSecrets`. Mirrors the table-buffering
    // handler (batchToSecretDict). The secret VALUES are delivered only here,
    // at bind time — update/combine/finalize see no secrets by design, and
    // they are never written to storage.
    const secretsBytes = innerParams.secrets ? toUint8Array(innerParams.secrets) : null;
    const secretsBatch =
      secretsBytes && secretsBytes.length > 0 ? deserializeBatch(secretsBytes) : null;
    const secrets = secretsBatch ? batchToSecretDict(secretsBatch) : {};
    const argumentNames: (string | null)[] | null = innerParams.argument_names
      ? (Array.isArray(innerParams.argument_names)
        ? innerParams.argument_names
        : [...innerParams.argument_names])
        .map((value: unknown) => value == null ? null : String(value))
      : null;
    const bindParams: AggregateBindParams<any> = {
      args: extractArgMap(cfg, args),
      arguments: args,
      inputSchema,
      settings: {},
      secrets,
      argumentNames,
    };
    // Output schema is a single "result" column. Type comes from the
    // optional onBind hook (for aggregates whose return type depends on
    // input — e.g. vgi_generic_sum takes ANY and returns the same type);
    // otherwise the declared cfg.outputType.
    const outType = cfg.onBind ? await cfg.onBind(bindParams) : cfg.outputType;
    const outSchema = makeSchema([field("result", outType, true)]);

    const executionId = new Uint8Array(16);
    crypto.getRandomValues(executionId);
    // Persist the bind context (sealed; no secrets) so any worker can serve
    // this execution's update/combine/finalize RPCs.
    const storage = await boundFor(innerParams, ctx, executionId);
    await storage.sealedPut(
      FrameworkNS.AGGREGATE_STATE,
      AGGREGATE_CONTEXT_KEY,
      encodeAggregateContext({ argumentsIpc: argBytes, inputSchemaIpc, argumentNames }),
      storageSealKey,
    );
    aggregateContextCache.set(storage.shardKey, executionId, { ...bindParams, secrets: {} });
    return wrapResult(
      { output_schema: serializeSchema(outSchema), execution_id: executionId },
      AggregateBindResultSchema,
    );
  };

  // ------------------------------------------------------------------------
  // aggregate_update — fold the incoming batch into per-group state
  // ------------------------------------------------------------------------
  const aggregateUpdate: VgiService["aggregateUpdate"] = async (params, ctx?: any) => {
    const innerParams = unwrapRequest(params.request);
    const functionName: string = innerParams.function_name;
    const executionId = toUint8Array(innerParams.execution_id);
    const cfg = resolveAggregate(functionName, innerParams.schema_path ?? null);
    const batch = readSingleBatch(toUint8Array(innerParams.input_batch));
    if (!batch || batch.numRows === 0) return wrapResult({}, makeSchema([]));

    const gidIdx = batch.schema.fields.findIndex((f) => f.name === GROUP_COLUMN_NAME);
    if (gidIdx < 0) {
      throw new Error(`aggregate_update: input batch missing ${GROUP_COLUMN_NAME} column`);
    }
    const gidCol = batch.getChildAt(gidIdx)!;
    const groupIds: bigint[] = [];
    const uniqueGids = new Set<bigint>();
    for (let i = 0; i < batch.numRows; i++) {
      const v = gidCol.get(i);
      const g = typeof v === "bigint" ? v : BigInt(v as any);
      groupIds.push(g);
      uniqueGids.add(g);
    }
    const storage = await boundFor(innerParams, ctx, executionId);
    const bindParams = await loadContext(cfg, storage, "aggregate_update");
    // One batched read of every group this batch touches. Always from
    // storage: a sibling worker may have just written a newer value, and a
    // cached copy would overwrite it (read-modify-write race).
    const gids = [...uniqueGids];
    const states = await loadGroupStates(storage, gids);

    // Build column-arg arrays — every non-__vgi_group_id column, in
    // schema order — and pass to user's update(). We deliberately do NOT
    // pre-allocate state for every group_id seen — the user decides when
    // to call ensureState() so that groups with only-NULL input (skipped
    // rows) never get an entry, which lets finalize() return SQL NULL.
    const columns: any[] = [];
    for (let i = 0; i < batch.schema.fields.length; i++) {
      if (i === gidIdx) continue;
      columns.push(batch.getChildAt(i));
    }
    const ensureState = (gid: bigint) => {
      let s = states.get(gid);
      if (s === undefined) {
        s = cfg.initialState(bindParams);
        states.set(gid, s);
      }
      return s;
    };
    cfg.update({
      states,
      groupIds,
      columns,
      args: bindParams.args,
      ensureState,
    });
    // One batched write of every referenced group that has a state (groups
    // skipped by NULL handling stay absent, so finalize sees null).
    await storeGroupStates(storage, states, gids);
    return wrapResult({}, makeSchema([]));
  };

  // ------------------------------------------------------------------------
  // aggregate_combine — merge (source_group_id, target_group_id) pairs
  // ------------------------------------------------------------------------
  const aggregateCombine: VgiService["aggregateCombine"] = async (params, ctx?: any) => {
    const innerParams = unwrapRequest(params.request);
    const functionName: string = innerParams.function_name;
    const executionId = toUint8Array(innerParams.execution_id);
    const cfg = resolveAggregate(functionName, innerParams.schema_path ?? null);
    const batch = readSingleBatch(toUint8Array(innerParams.merge_batch));
    if (!batch || batch.numRows === 0) return wrapResult({}, makeSchema([]));

    const srcCol = batch.getChild("source_group_id")!;
    const tgtCol = batch.getChild("target_group_id")!;
    const referencedGids = new Set<bigint>();
    const srcIds: bigint[] = [];
    const tgtIds: bigint[] = [];
    for (let i = 0; i < batch.numRows; i++) {
      const src = BigInt(srcCol.get(i) as any);
      const tgt = BigInt(tgtCol.get(i) as any);
      srcIds.push(src); tgtIds.push(tgt);
      referencedGids.add(src); referencedGids.add(tgt);
    }
    const storage = await boundFor(innerParams, ctx, executionId);
    const bindParams = await loadContext(cfg, storage, "aggregate_combine");
    const states = await loadGroupStates(storage, [...referencedGids]);
    const touchedTargets = new Set<bigint>();
    for (let i = 0; i < batch.numRows; i++) {
      const src = srcIds[i]!;
      const tgt = tgtIds[i]!;
      const srcHas = states.has(src);
      const tgtHas = states.has(tgt);
      if (!srcHas && !tgtHas) continue;
      const srcState = srcHas ? states.get(src)! : cfg.initialState(bindParams);
      const tgtState = tgtHas ? states.get(tgt)! : cfg.initialState(bindParams);
      states.set(tgt, cfg.combine(srcState, tgtState, bindParams));
      touchedTargets.add(tgt);
    }
    await storeGroupStates(storage, states, touchedTargets);
    return wrapResult({}, makeSchema([]));
  };

  // ------------------------------------------------------------------------
  // aggregate_finalize — emit one result row per requested group_id
  // ------------------------------------------------------------------------
  const aggregateFinalize: VgiService["aggregateFinalize"] = async (params, ctx?: any) => {
    const innerParams = unwrapRequest(params.request);
    const functionName: string = innerParams.function_name;
    const executionId = toUint8Array(innerParams.execution_id);
    const cfg = resolveAggregate(functionName, innerParams.schema_path ?? null);
    const outputSchema = deserializeSchema(toUint8Array(innerParams.output_schema));
    const gidBatch = readSingleBatch(toUint8Array(innerParams.group_ids_batch));
    const groupIds: bigint[] = [];
    const uniqueGids = new Set<bigint>();
    if (gidBatch) {
      const col = gidBatch.getChild("group_id")!;
      for (let i = 0; i < gidBatch.numRows; i++) {
        const v = col.get(i);
        const g = typeof v === "bigint" ? v : BigInt(v as any);
        groupIds.push(g);
        uniqueGids.add(g);
      }
    }
    const storage = await boundFor(innerParams, ctx, executionId);
    const bindParams = await loadContext(cfg, storage, "aggregate_finalize");
    const stored = await loadGroupStates(storage, [...uniqueGids]);
    // Groups never updated pass state = null so finalize() can emit SQL NULL
    // (matches Python semantics for SUM/AVG/MIN/MAX over zero rows).
    const states = new Map<bigint, any | null>();
    for (const gid of groupIds) {
      states.set(gid, stored.has(gid) ? stored.get(gid) : null);
    }
    const resultBatch = cfg.finalize({
      groupIds, states, outputSchema, args: bindParams.args,
    });
    return wrapResult(
      { result_batch: serializeBatchWithSchema(resultBatch) },
      AggregateFinalizeResultSchema,
    );
  };

  // ------------------------------------------------------------------------
  // aggregate_destructor — drop the execution's state (best-effort)
  //
  // The extension calls this once, when every group state of the execution
  // has been destroyed (it counts destroys against allocated group ids), so
  // it clears the whole execution -- group states and the sealed bind
  // context -- and evicts the in-process cache, as vgi-python does.
  // ------------------------------------------------------------------------
  const aggregateDestructor: VgiService["aggregateDestructor"] = async (params, ctx?: any) => {
    try {
      const innerParams = unwrapRequest(params.request);
      const executionId = toUint8Array(innerParams.execution_id);
      const storage = await boundFor(innerParams, ctx, executionId);
      aggregateContextCache.delete(storage.shardKey, executionId);
      await storage.executionClear();
    } catch {
      // Teardown path — best-effort.
    }
    return wrapResult({}, makeSchema([]));
  };

  return { aggregateBind, aggregateUpdate, aggregateCombine, aggregateFinalize, aggregateDestructor };
}

function extractArgMap(
  cfg: AggregateFunctionConfig<any, any>,
  args: Arguments,
): Record<string, any> {
  const out: Record<string, any> = {};
  // The wire-side Arguments contains only const params — DuckDB erases const
  // args from the expression tree at bind time and serializes their values
  // into the aggregate_bind request's `arguments` field. So we index the
  // Arguments by the declaration order of const params only.
  const constSet = new Set(cfg.constParams ?? []);
  const constNames: string[] = [];
  if (cfg.args) {
    for (const name of Object.keys(cfg.args)) {
      if (constSet.has(name)) constNames.push(name);
    }
  }
  for (let i = 0; i < constNames.length; i++) {
    const name = constNames[i]!;
    let v: any;
    try {
      v = args.get(i);
      // Decode DECIMAL scalars (DuckDB serializes 0.5-style literals as
      // DECIMAL(2,1), which Arrow stores as the unscaled integer — a raw
      // BigInt here). Scale back to float using the declared scale.
      const field = args.argumentsSchema?.fields.find(f => f.name === `positional_${i}`);
      if (field && isDecimal(field.type)) {
        const scale = (field.type as any).scale;
        if (typeof v === "bigint") v = Number(v) / Math.pow(10, scale);
        else if (typeof v === "number") v = v / Math.pow(10, scale);
      } else if (typeof v === "bigint") {
        v = Number(v);
      }
    } catch {
      // args.get throws when the arg isn't set — fall back to the declared
      // default so aggregates called without an explicit const value still
      // see it in update/finalize.
      const def = cfg.argDefaults?.[name];
      if (def !== undefined) out[name] = def;
      continue;
    }
    // Enforce declared constraints on the supplied value. This sits OUTSIDE the
    // try above so a constraint violation propagates as a bind error rather than
    // being swallowed and silently replaced by the default.
    const constraints = cfg.argConstraints?.[name];
    if (constraints !== undefined) {
      validateConstConstraints(name, constraints, v);
    }
    out[name] = v;
  }
  // Non-const args aren't in the wire Arguments, but users may still want
  // defaults surfaced for `params.args` — apply them here.
  if (cfg.argDefaults) {
    for (const [name, def] of Object.entries(cfg.argDefaults)) {
      if (!(name in out) && !constSet.has(name)) out[name] = def;
    }
  }
  return out;
}

function readSingleBatch(bytes: Uint8Array): VgiBatch | null {
  if (!bytes || bytes.length === 0) return null;
  const batch = deserializeBatch(bytes);
  return batch.numRows === 0 && batch.schema.fields.length === 0 ? null : batch;
}

function serializeBatchWithSchema(batch: VgiBatch): Uint8Array {
  // serializeBatch emits a full IPC stream (schema + batch + EOS) — the C++
  // aggregate_finalize reader expects exactly that.
  return serializeBatch(batch);
}
