// Shared helpers for protocol wire serialization.

import {
  type VgiSchema,
  type VgiBatch,
  isInt,
  batchFromColumns,
} from "../../arrow/index.js";
import { toUint8Array as toUint8ArrayBase } from "../../util/bytes.js";

/**
 * Coerce a wire value (string / Uint8Array / Buffer / etc.) to a Uint8Array.
 * Throws when the input is non-null but produces zero bytes — that signals a
 * truly unsupported shape, distinct from a legitimate empty Uint8Array.
 */
export function toUint8Array(val: any): Uint8Array {
  if (typeof val === "string") return new TextEncoder().encode(val);
  const result = toUint8ArrayBase(val);
  if (result.length === 0 && val != null) {
    throw new Error(`Cannot convert ${typeof val} to Uint8Array`);
  }
  return result;
}

/**
 * Build a single-row RecordBatch from a {fieldName: value} dict matching the
 * given schema. Auto-coerces JS numbers to BigInt for Int64 fields.
 */
export function buildSingleRowBatch(
  schema: VgiSchema,
  values: Record<string, any>,
): VgiBatch {
  // Build column dict, coercing Int64 numbers to BigInt where the schema
  // declares one — vectorFromArray/columnFromArray rejects plain Number for
  // 64-bit ints under both backends.
  const cols: Record<string, any[]> = {};
  for (const f of schema.fields) {
    let val = values[f.name];
    if (isInt(f.type) && (f.type as any).bitWidth === 64 && typeof val === "number") {
      val = BigInt(val);
    }
    cols[f.name] = [val];
  }
  return batchFromColumns(cols, schema);
}
