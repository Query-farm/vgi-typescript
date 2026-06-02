// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Unary handlers for the table_buffering (Sink+Source) lifecycle:
//   table_buffering_process    — sink one input batch, return state_id
//   table_buffering_combine    — group/merge state_ids -> finalize_state_ids
//   table_buffering_destructor — best-effort end-of-query storage cleanup
//
// These RPCs carry no init context, so each cold-loads the init metadata that
// the TABLE_BUFFERING init phase persisted to FunctionStorage (keyed by
// execution_id), then rebuilds the user-facing TableBufferingParams. Mirrors
// vgi-python's worker._load_table_buffering_params.

import { deserializeBatch } from "../../arrow/index.js";
import { Protocol } from "@query-farm/vgi-rpc";
import type { FunctionRegistry } from "../../functions/registry.js";
import { toUint8Array } from "../../util/bytes.js";
import { batchToScalarDict, batchToSecretDict } from "../../util/arrow/index.js";
import {
  TableBufferingProcessResultSchema,
  TableBufferingCombineResultSchema,
  TableBufferingDestructorResultSchema,
} from "../../generated/vgi-protocol-schemas.js";
import {
  REQUEST_PARAMS_SCHEMA,
  RESULT_BINARY_SCHEMA,
  unwrapRequest,
  wrapResult,
} from "./shared.js";
import {
  BoundStorage,
  storage as defaultStorage,
  FrameworkNS,
} from "../../functions/storage.js";
import {
  decodeBufferingInit,
  type TableBufferingVgiFunction,
  type TableBufferingParams,
} from "../../functions/table-buffering.js";
import { Arguments } from "../../arguments/arguments.js";

export function registerTableBufferingMethods(
  protocol: Protocol,
  registry: FunctionRegistry,
): void {
  function resolveBuffering(name: string): TableBufferingVgiFunction {
    const func = registry.get(name, {
      arguments: new Arguments(),
      inputSchema: null,
      isScalar: false,
    }) as any;
    if (!func || func.kind !== "table_buffering") {
      throw new Error(`Function '${name}' is not a table_buffering function`);
    }
    return func as TableBufferingVgiFunction;
  }

  // Cold-load init metadata + build TableBufferingParams from a unary request.
  async function loadParams(
    inner: Record<string, any>,
    clientLog: (level: string, message: string) => void,
  ): Promise<{ func: TableBufferingVgiFunction; params: TableBufferingParams<any> }> {
    const functionName: string = inner.function_name;
    const executionId = toUint8Array(inner.execution_id);
    const attach = inner.attach_opaque_data ? toUint8Array(inner.attach_opaque_data) : null;
    const transactionId = inner.transaction_id ? toUint8Array(inner.transaction_id) : null;

    const func = resolveBuffering(functionName);
    const bound = new BoundStorage(defaultStorage, executionId);
    const payload = await bound.stateGet(
      FrameworkNS.BUFFERING_INIT,
      BoundStorage.packIntKey(-1),
    );
    if (payload == null) {
      throw new Error(
        `table_buffering: unknown execution_id ${hex(executionId)} ` +
          `(init never ran or destructor already fired)`,
      );
    }
    const { request } = decodeBufferingInit(payload);
    const args = func.bufferingExtractArgs(request.bind_call);
    const settings = batchToScalarDict(request.bind_call.settings);
    const secrets = batchToSecretDict(request.bind_call.secrets);

    const params: TableBufferingParams<any> = {
      args,
      initCall: request,
      outputSchema: request.output_schema,
      settings,
      secrets,
      storage: bound,
      executionId,
      attachId: attach ?? new Uint8Array(0),
      transactionId,
      function_name: functionName,
      batchIndex: null,
      clientLog,
    };
    return { func, params };
  }

  // ------------------------------------------------------------------------
  // table_buffering_process — sink one batch, return state_id (unary)
  // ------------------------------------------------------------------------
  protocol.unary("table_buffering_process", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_SCHEMA,
    handler: async (rpcParams, ctx?: any) => {
      const inner = unwrapRequest(rpcParams.request);
      const clientLog = makeClientLog(ctx);
      const { func, params } = await loadParams(inner, clientLog);
      if (inner.batch_index != null) {
        params.batchIndex = Number(inner.batch_index);
      }
      const batch = deserializeBatch(toUint8Array(inner.input_batch));
      const stateId = await func.bufferingConfig.process(batch, params);
      if (!(stateId instanceof Uint8Array)) {
        throw new Error(
          `${func.meta.name}.process() must return Uint8Array (the opaque state_id)`,
        );
      }
      return wrapResult({ state_id: stateId }, TableBufferingProcessResultSchema);
    },
  });

  // ------------------------------------------------------------------------
  // table_buffering_combine — group/merge state_ids (unary)
  // ------------------------------------------------------------------------
  protocol.unary("table_buffering_combine", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_SCHEMA,
    handler: async (rpcParams, ctx?: any) => {
      const inner = unwrapRequest(rpcParams.request);
      const clientLog = makeClientLog(ctx);
      const { func, params } = await loadParams(inner, clientLog);
      const stateIds = decodeBytesList(inner.state_ids);
      const finalizeStateIds = await func.bufferingConfig.combine(stateIds, params);
      const out = finalizeStateIds.map((fid, i) => {
        if (!(fid instanceof Uint8Array)) {
          throw new Error(
            `${func.meta.name}.combine() returned non-Uint8Array finalize_state_id at index ${i}`,
          );
        }
        return fid;
      });
      return wrapResult(
        { finalize_state_ids: out },
        TableBufferingCombineResultSchema,
      );
    },
  });

  // ------------------------------------------------------------------------
  // table_buffering_destructor — best-effort end-of-query cleanup (unary)
  // ------------------------------------------------------------------------
  protocol.unary("table_buffering_destructor", {
    params: REQUEST_PARAMS_SCHEMA,
    result: RESULT_BINARY_SCHEMA,
    handler: async (rpcParams) => {
      try {
        const inner = unwrapRequest(rpcParams.request);
        const executionId = toUint8Array(inner.execution_id);
        const bound = new BoundStorage(defaultStorage, executionId);
        await bound.executionClear();
      } catch {
        // Teardown path — swallow; the entry is wiped when the worker exits.
      }
      return wrapResult({}, TableBufferingDestructorResultSchema);
    },
  });
}

function makeClientLog(ctx: any): (level: string, message: string) => void {
  if (ctx && typeof ctx.clientLog === "function") {
    return (level: string, message: string) => ctx.clientLog(level, message);
  }
  return () => {};
}

function decodeBytesList(raw: any): Uint8Array[] {
  if (raw == null) return [];
  const out: Uint8Array[] = [];
  const iter: Iterable<any> = Array.isArray(raw)
    ? raw
    : typeof raw[Symbol.iterator] === "function"
    ? raw
    : [];
  for (const entry of iter) {
    if (entry == null) continue;
    out.push(toUint8Array(entry));
  }
  return out;
}

function hex(buf: Uint8Array): string {
  return Array.from(buf, (b) => b.toString(16).padStart(2, "0")).join("");
}
