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
import { Protocol } from "@query-farm/vgi-rpc";
import type { FunctionRegistry } from "../../functions/registry.js";
import { deserializeArguments } from "../serialize.js";
import { deserializeSchema, serializeSchema } from "../../util/arrow/index.js";
import { toUint8Array } from "../../util/bytes.js";
import {
  AggregateBindResultSchema,
  AggregateFinalizeResultSchema,
} from "../../generated/vgi-protocol-schemas.js";
import {
  getExecutionState,
  setExecutionState,
  persistGroupStates,
  GROUP_COLUMN_NAME,
  type AggregateFunctionConfig,
  type AggregateBindParams,
} from "../../functions/aggregate.js";
import { Arguments } from "../../arguments/arguments.js";
import {
  REQUEST_PARAMS_SCHEMA,
  RESULT_BINARY_SCHEMA,
  unwrapRequest,
  wrapResult,
} from "./shared.js";

export function registerAggregateMethods(protocol: Protocol, registry: FunctionRegistry): void {
  function resolveAggregate(name: string): AggregateFunctionConfig<any, any> {
    const func = registry.get(name, { arguments: new Arguments(), inputSchema: null, isScalar: false }) as any;
    if (!func || func.kind !== "aggregate") {
      throw new Error(`Function '${name}' is not an aggregate function`);
    }
    return func.aggregateConfig as AggregateFunctionConfig<any, any>;
  }

  // ------------------------------------------------------------------------
  // aggregate_bind — allocate execution_id, return output schema
  // ------------------------------------------------------------------------
  protocol.unary("aggregate_bind", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_SCHEMA,
    handler: async (params) => {
      const innerParams = unwrapRequest(params.request);
      const functionName: string = innerParams.function_name;
      const cfg = resolveAggregate(functionName);
      const argBytes = toUint8Array(innerParams.arguments);
      const args = deserializeArguments(argBytes);
      const inputSchema = innerParams.input_schema
        ? deserializeSchema(toUint8Array(innerParams.input_schema))
        : null;
      const bindParams: AggregateBindParams<any> = {
        args: extractArgMap(cfg, args),
        arguments: args,
        inputSchema,
        settings: {},
        secrets: {},
      };
      const executionId = new Uint8Array(16);
      crypto.getRandomValues(executionId);
      setExecutionState(executionId, {
        states: new Map(),
        bindArgs: args,
        bindParams,
      });
      // Output schema is a single "result" column. Type comes from the
      // optional onBind hook (for aggregates whose return type depends on
      // input — e.g. vgi_generic_sum takes ANY and returns the same type);
      // otherwise the declared cfg.outputType.
      const outType = cfg.onBind ? await cfg.onBind(bindParams) : cfg.outputType;
      const outSchema = makeSchema([field("result", outType, true)]);
      return wrapResult(
        { output_schema: serializeSchema(outSchema), execution_id: executionId },
        AggregateBindResultSchema,
      );
    },
  });

  // ------------------------------------------------------------------------
  // aggregate_update — fold the incoming batch into per-group state
  // ------------------------------------------------------------------------
  protocol.unary("aggregate_update", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const innerParams = unwrapRequest(params.request);
      const functionName: string = innerParams.function_name;
      const executionId = toUint8Array(innerParams.execution_id);
      const cfg = resolveAggregate(functionName);
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
      const exec = getExecutionState(executionId, uniqueGids);
      if (!exec) {
        throw new Error(`aggregate_update: unknown execution_id for '${functionName}' (bind missing or state dropped)`);
      }

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
        let s = exec.states.get(gid);
        if (s === undefined) {
          s = cfg.initialState(exec.bindParams);
          exec.states.set(gid, s);
        }
        return s;
      };
      cfg.update({
        states: exec.states,
        groupIds,
        columns,
        args: exec.bindParams.args,
        ensureState,
      });
      // Flush mutated states to disk so sibling workers see them. We persist
      // every group that was referenced this call — some may be unchanged
      // (skipped by NULL handling), in which case the file simply reflects
      // the pre-existing initial state; the idempotent rewrite is cheap
      // relative to the RPC overhead.
      persistGroupStates(executionId, uniqueGids);
      return wrapResult({}, makeSchema([]));
    },
  });

  // ------------------------------------------------------------------------
  // aggregate_combine — merge (source_group_id, target_group_id) pairs
  // ------------------------------------------------------------------------
  protocol.unary("aggregate_combine", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const innerParams = unwrapRequest(params.request);
      const functionName: string = innerParams.function_name;
      const executionId = toUint8Array(innerParams.execution_id);
      const cfg = resolveAggregate(functionName);
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
      const exec = getExecutionState(executionId, referencedGids);
      if (!exec) {
        throw new Error(
          `aggregate_combine: unknown execution_id for '${functionName}' ` +
          `(context missing on disk — bind may have failed or been cleaned up)`,
        );
      }
      const touchedTargets = new Set<bigint>();
      for (let i = 0; i < batch.numRows; i++) {
        const src = srcIds[i];
        const tgt = tgtIds[i];
        const srcHas = exec.states.has(src);
        const tgtHas = exec.states.has(tgt);
        if (!srcHas && !tgtHas) continue;
        const srcState = srcHas ? exec.states.get(src)! : cfg.initialState(exec.bindParams);
        const tgtState = tgtHas ? exec.states.get(tgt)! : cfg.initialState(exec.bindParams);
        exec.states.set(tgt, cfg.combine(srcState, tgtState, exec.bindParams));
        touchedTargets.add(tgt);
      }
      persistGroupStates(executionId, touchedTargets);
      return wrapResult({}, makeSchema([]));
    },
  });

  // ------------------------------------------------------------------------
  // aggregate_finalize — emit one result row per requested group_id
  // ------------------------------------------------------------------------
  protocol.unary("aggregate_finalize", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const innerParams = unwrapRequest(params.request);
      const functionName: string = innerParams.function_name;
      const executionId = toUint8Array(innerParams.execution_id);
      const cfg = resolveAggregate(functionName);
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
      const exec = getExecutionState(executionId, uniqueGids);
      if (!exec) {
        throw new Error(`aggregate_finalize: unknown execution_id for '${functionName}'`);
      }
      // Groups never updated pass state = null so finalize() can emit SQL NULL
      // (matches Python semantics for SUM/AVG/MIN/MAX over zero rows).
      const states = new Map<bigint, any | null>();
      for (const gid of groupIds) {
        states.set(gid, exec.states.has(gid) ? exec.states.get(gid) : null);
      }
      const resultBatch = cfg.finalize({
        groupIds, states, outputSchema, args: exec.bindParams.args,
      });
      return wrapResult(
        { result_batch: serializeBatchWithSchema(resultBatch) },
        AggregateFinalizeResultSchema,
      );
    },
  });

  // ------------------------------------------------------------------------
  // aggregate_destructor — drop state for finished groups (best-effort)
  //
  // DuckDB sends a batch of group_ids to discard; we remove each from the
  // in-memory state map. We DO NOT tear down the entire execution on an
  // empty batch — DuckDB may still call finalize afterwards in some plans
  // (matches Python's conservative behavior; the execution entry is cleaned
  // up when the worker process exits).
  // ------------------------------------------------------------------------
  protocol.unary("aggregate_destructor", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_SCHEMA,
    handler: (params) => {
      const innerParams = unwrapRequest(params.request);
      const executionId = toUint8Array(innerParams.execution_id);
      const exec = getExecutionState(executionId);
      if (!exec) return wrapResult({}, makeSchema([]));
      const gidBatch = readSingleBatch(toUint8Array(innerParams.group_ids_batch));
      if (!gidBatch || gidBatch.numRows === 0) return wrapResult({}, makeSchema([]));
      const col = gidBatch.getChild("group_id")!;
      const toDrop = new Set<bigint>();
      for (let i = 0; i < gidBatch.numRows; i++) {
        const v = col.get(i);
        const g = typeof v === "bigint" ? v : BigInt(v as any);
        exec.states.delete(g);
        toDrop.add(g);
      }
      // Persist deletions (unlinks the per-group file so sibling workers
      // don't revive the state via getExecutionState's lazy disk load).
      persistGroupStates(executionId, toDrop);
      return wrapResult({}, makeSchema([]));
    },
  });
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
    try {
      let v = args.get(i);
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
      out[constNames[i]] = v;
    } catch {
      // args.get throws when the arg isn't set — fall back to the declared
      // default so aggregates called without an explicit const value still
      // see it in update/finalize.
      const def = cfg.argDefaults?.[constNames[i]];
      if (def !== undefined) out[constNames[i]] = def;
    }
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
