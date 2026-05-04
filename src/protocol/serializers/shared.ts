// Shared helpers for protocol wire serialization.

import {
  Schema,
  Field,
  RecordBatch,
  DataType,
  Struct,
  makeData,
  vectorFromArray,
} from "@query-farm/apache-arrow";
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
  schema: Schema,
  values: Record<string, any>
): RecordBatch {
  const children = schema.fields.map((f: Field) => {
    let val = values[f.name];
    // Coerce int64
    if (DataType.isInt(f.type) && (f.type as any).bitWidth === 64) {
      if (typeof val === "number") val = BigInt(val);
    }
    const arr = vectorFromArray([val], f.type);
    return arr.data[0];
  });

  const structType = new Struct(schema.fields);
  const data = makeData({
    type: structType,
    length: 1,
    children,
    nullCount: 0,
  });

  return new RecordBatch(schema, data);
}
