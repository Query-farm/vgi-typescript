// Wrap/unwrap helpers for the VGI double-wrapping protocol.
// Mirrors the server's dispatch.ts:64-84 (unwrapRequest/wrapResult).

import {
  serializeBatch,
  deserializeBatch,
  batchToScalarDict,
} from "../util/arrow/index.js";
import { toUint8Array } from "../util/bytes.js";
import type { VgiBatch } from "../arrow/index.js";
export { toUint8Array };

/**
 * Wrap a RecordBatch as a { request: Uint8Array } dict for RPC params.
 * DuckDB wraps ArrowSerializableDataclass params in a single "request" Binary column.
 */
export function wrapRequest(batch: VgiBatch): { request: Uint8Array } {
  return { request: serializeBatch(batch) };
}

/**
 * Unwrap a { result: Uint8Array } response dict.
 * The "result" Binary column contains a serialized inner batch;
 * returns its row 0 as a flat dict.
 */
export function unwrapResult(
  result: Record<string, any>,
): Record<string, any> {
  const resultBytes = toUint8Array(result.result);
  const innerBatch = deserializeBatch(resultBytes);
  return batchToScalarDict(innerBatch);
}
