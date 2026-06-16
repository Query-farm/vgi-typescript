// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Arrow IPC state serializer for HTTP transport.
// Replaces JSON serialization with Arrow IPC for the exchange state token,
// avoiding base64/hex overhead and staying consistent with the rest of the protocol.

import {
  type VgiDataType,
  type VgiField,
  schema as makeSchema,
  field,
  struct as makeStruct,
  binary,
  int64,
  bool,
  float64,
  utf8,
  nullType,
  isStruct,
  isBinary,
  serializeBatch,
  deserializeBatch,
  batchFromColumns,
} from "../arrow/index.js";
import type { StateSerializer } from "@query-farm/vgi-rpc";
import { toUint8Array } from "../util/bytes.js";

/** Schema for the exchange state carried in HTTP state tokens. */
export const EXCHANGE_STATE_SCHEMA = makeSchema([
  field("function_name", binary(), false),
  field("init_request", binary(), false),
  field("execution_id", binary(), false),
  field("max_workers", int64(), false),
  field("opaque_data", binary(), true),
  field("is_producer", bool(), false),
  field("user_state", binary(), true),
]);

const TEXT_ENCODER = new TextEncoder();
const TEXT_DECODER = new TextDecoder();

/** Infer an Arrow DataType from a JS value for user state serialization. */
export function inferFieldType(value: any): VgiDataType {
  if (value === null || value === undefined) return nullType();
  switch (typeof value) {
    case "number": return float64();
    case "bigint": return int64();
    case "string": return utf8();
    case "boolean": return bool();
    case "object":
      if (value instanceof Uint8Array || value instanceof ArrayBuffer) return binary();
      if (ArrayBuffer.isView(value)) return binary();
      if (Array.isArray(value)) {
        throw new Error(`inferFieldType: arrays are not supported in userState (convert to a serializable form first)`);
      }
      if (value instanceof Map) {
        throw new Error(`inferFieldType: Map is not supported in userState (use a plain object instead)`);
      }
      if (value instanceof Set) {
        throw new Error(`inferFieldType: Set is not supported in userState (use an array or plain object instead)`);
      }
      if (value instanceof Date) {
        throw new Error(`inferFieldType: Date is not supported in userState (use a number (epoch ms) or ISO string instead)`);
      }
      if (value instanceof RegExp) {
        throw new Error(`inferFieldType: RegExp is not supported in userState (use a string pattern instead)`);
      }
      // Plain object → Struct
      const fields = Object.entries(value).map(
        ([k, v]) => field(k, inferFieldType(v), true),
      );
      return makeStruct(fields);
    default:
      throw new Error(`inferFieldType: unsupported type '${typeof value}'`);
  }
}

/**
 * Serialize userState to Arrow IPC bytes.
 * Infers schema from the JS object at runtime. Arrow IPC is self-describing,
 * so deserialization doesn't need the schema ahead of time.
 *
 * For an empty object `{}`, we emit a 0-row batch with empty schema —
 * `deserializeUserState` recognizes that shape and returns `{}` rather than
 * `null`. (`null` is reserved for "no userState declared at all".)
 */
export function serializeUserState(userState: any): Uint8Array | null {
  if (userState == null) return null;
  const entries = Object.entries(userState);
  if (entries.length === 0) {
    // 0-row, 0-field batch — round-trips back to {} via deserializeUserState.
    return serializeBatch(batchFromColumns({}, makeSchema([])));
  }
  const fields = entries.map(
    ([k, v]) => field(k, inferFieldType(v), true),
  );
  const sch = makeSchema(fields);
  const columns: Record<string, any[]> = {};
  for (const [key, val] of entries) {
    columns[key] = [val];
  }
  return serializeBatch(batchFromColumns(columns, sch));
}

/**
 * Extract a typed value from an Arrow column, preserving BigInt for Int64.
 * Recurses into Struct fields to produce plain JS objects.
 */
function extractTypedValue(col: any, index: number, type: VgiDataType): any {
  const val = col.get(index);
  if (val === null || val === undefined) return null;

  if (isStruct(type)) {
    // Read from the struct *value* (the object/StructRow returned by `get`)
    // rather than descending child vectors via `getChildAt`: that method is
    // arrow-js-only (undefined on the flechette backend), which silently
    // nulled every nested-struct field on flechette.
    return extractStructValue(val, type);
  }

  if (isBinary(type)) {
    return toUint8Array(val);
  }

  return val;
}

/**
 * Coerce a struct value (a plain object on flechette, a StructRow on arrow-js —
 * both support `obj[name]` access) into a plain JS object, applying the same
 * type-aware coercion as {@link extractTypedValue}: Uint8Array for Binary,
 * recursion for nested Structs, and the backend's decoded value (e.g. BigInt
 * for Int64) otherwise. Backend-agnostic — no `getChildAt`.
 */
function extractStructValue(value: any, type: VgiDataType): any {
  if (value === null || value === undefined) return null;
  const result: Record<string, any> = {};
  const children = (type as any).children as VgiField[];
  for (const childField of children) {
    const child = value[childField.name];
    if (child === null || child === undefined) {
      result[childField.name] = null;
    } else if (isStruct(childField.type)) {
      result[childField.name] = extractStructValue(child, childField.type);
    } else if (isBinary(childField.type)) {
      result[childField.name] = toUint8Array(child);
    } else {
      result[childField.name] = child;
    }
  }
  return result;
}

/**
 * Deserialize userState from Arrow IPC bytes.
 * Reconstructs a plain JS object, preserving BigInt for Int64 columns.
 */
export function deserializeUserState(bytes: Uint8Array | null): any {
  if (bytes == null) return null;
  const batch = deserializeBatch(bytes);
  if (batch.numRows === 0 && batch.schema.fields.length === 0) {
    return {};
  }
  if (batch.numRows === 0) return null;
  const result: Record<string, any> = {};
  for (const f of batch.schema.fields) {
    const col = batch.getChild(f.name);
    if (!col) { result[f.name] = null; continue; }
    result[f.name] = extractTypedValue(col, 0, f.type);
  }
  return result;
}

/** Arrow IPC state serializer — stores all data as native binary columns. */
export const arrowStateSerializer: StateSerializer = {
  serialize(state: any): Uint8Array {
    const columns: Record<string, any[]> = {
      function_name: [TEXT_ENCODER.encode(state.functionName)],
      init_request: [state.initRequestIpc],
      execution_id: [state.executionId],
      max_workers: [state.maxWorkers],
      opaque_data: [state.opaqueData ?? null],
      is_producer: [state.isProducer],
      user_state: [serializeUserState(state.userState)],
    };
    const batch = batchFromColumns(columns, EXCHANGE_STATE_SCHEMA);
    return serializeBatch(batch);
  },

  deserialize(bytes: Uint8Array): any {
    const batch = deserializeBatch(bytes);
    const get = (name: string) => {
      const col = batch.getChild(name);
      return col ? col.get(0) : null;
    };

    const isProducer = get("is_producer");
    const opaqueDataRaw = get("opaque_data");
    const fnNameRaw = get("function_name");

    return {
      functionName: fnNameRaw != null ? TEXT_DECODER.decode(toUint8Array(fnNameRaw)) : "",
      initRequestIpc: toUint8Array(get("init_request")),
      executionId: toUint8Array(get("execution_id")),
      maxWorkers: Number(get("max_workers")),
      opaqueData: opaqueDataRaw != null ? toUint8Array(opaqueDataRaw) : null,
      isProducer,
      // vgi-rpc dispatch reads __isProducer to choose producer vs exchange mode
      __isProducer: isProducer,
      userState: deserializeUserState(
        get("user_state") != null ? toUint8Array(get("user_state")) : null,
      ),
    };
  },
};
