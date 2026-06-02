// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Shared schemas and helpers used by every protocol handler.

import { type VgiSchema, schema, type VgiField, field, type VgiDataType, binary } from "../../arrow/index.js";
import type { OverloadContext } from "../../functions/registry.js";
import { batchToScalarDict, deserializeBatch, serializeBatch, batchFromColumns } from "../../util/arrow/index.js";
import { toUint8Array } from "../../util/bytes.js";
import { TableInOutPhase } from "../../types.js";
import type { InitRequest } from "../types.js";

// The Python vgi-rpc framework wraps ALL non-void unary results in a single
// "result" column. For ArrowSerializableDataclass types, the result is serialized
// as Arrow IPC bytes in a Binary column. DuckDB's VGI extension expects this format.
export const RESULT_BINARY_SCHEMA = schema([
  field("result", binary(), false),
]);

// DuckDB wraps ArrowSerializableDataclass parameters in a single "request" Binary column.
export const REQUEST_PARAMS_SCHEMA = schema([
  field("request", binary(), false),
]);

export const RESULT_BINARY_NULLABLE_SCHEMA = schema([
  field("result", binary(), true),
]);

/**
 * Unwrap a "request" Binary column: deserialize the inner Arrow IPC batch
 * and return flat columns as a dict (row 0).
 */
export function unwrapRequest(requestBytes: any): Record<string, any> {
  const bytes = toUint8Array(requestBytes);
  const innerBatch = deserializeBatch(bytes);
  return batchToScalarDict(innerBatch);
}

/**
 * Wrap a dict of values into a single "result" Binary column.
 * Builds a 1-row batch from the values using the given schema,
 * serializes it to IPC bytes, and returns { result: bytes }.
 */
export function wrapResult(
  values: Record<string, any>,
  innerSchema: VgiSchema | import("../../arrow/index.js").VgiSchema,
): { result: Uint8Array } {
  const a = innerSchema as VgiSchema;
  const batch = batchFromColumns(
    Object.fromEntries(a.fields.map(f => [f.name, [values[f.name] ?? null]])),
    a,
  );
  return { result: serializeBatch(batch) };
}

export function overloadContext(req: { function_name: string; arguments: any; input_schema: any; function_type: any }): OverloadContext {
  return {
    arguments: req.arguments,
    inputSchema: req.input_schema,
    isScalar: String(req.function_type).toLowerCase() === "scalar",
  };
}

/**
 * Recover accumulated exchange state from a FINALIZE init request.
 * For HTTP transport, this unpacks the state token that the C++ extension
 * passes from the last INPUT exchange to the FINALIZE init request.
 */
export function recoverFinalizeState(
  request: InitRequest,
  recoverExchangeState: ((opaqueData: Uint8Array) => any) | undefined,
): any {
  if (request.phase === TableInOutPhase.FINALIZE && request.init_opaque_data && recoverExchangeState) {
    try {
      const recovered = recoverExchangeState(request.init_opaque_data);
      return recovered?.userState;
    } catch (e: any) {
      throw new Error(`Failed to recover FINALIZE state from init_opaque_data: ${e.message}`);
    }
  }
  return undefined;
}
