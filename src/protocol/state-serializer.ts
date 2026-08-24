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
  list as makeList,
  nullType,
  typeSignature,
  isStruct,
  isBinary,
  serializeBatch,
  deserializeBatch,
  batchFromColumns,
  readCanonicalValue,
} from "../arrow/index.js";
import { codecFor } from "../arrow/codec/registry.js";
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

/**
 * Child field name for an inferred list. Arrow matches list children by
 * position, not name, so this is cosmetic — but keeping it at Arrow's
 * conventional `item` makes a dumped schema read the way people expect.
 */
const LIST_CHILD_NAME = "item";

/**
 * Element type for an array with nothing to infer from — `[]`, or an array
 * whose every element is null.
 *
 * Deliberately NOT `nullType()`, which is the semantically honest choice:
 * flechette's column builder rejects a Null-typed list child ("Unsupported
 * data type: Null"), so a `[]` in userState would round-trip on arrow-js and
 * throw on flechette. Utf8 builds on both backends and, with no non-null
 * element to carry, is indistinguishable on the way back out: `[]` returns
 * `[]` and `[null, null]` returns `[null, null]`.
 */
const emptyListElementType = (): VgiDataType => utf8();

/**
 * Infer the single Arrow element type an array's contents share.
 *
 * Arrow lists are homogeneous, so a genuinely mixed array (`[1, "a"]`,
 * or structs with differing field sets) has no faithful representation and is
 * rejected rather than silently coerced. Nulls are skipped — they are
 * representable at any element type — and an array with no non-null element
 * falls back to {@link emptyListElementType}.
 */
function inferElementType(values: any[]): VgiDataType {
  let elementType: VgiDataType | null = null;
  let elementSig = "";
  for (const v of values) {
    if (v === null || v === undefined) continue;
    const t = inferFieldType(v);
    const sig = typeSignature(t);
    if (elementType === null) {
      elementType = t;
      elementSig = sig;
      continue;
    }
    if (sig !== elementSig) {
      throw new Error(
        `inferFieldType: arrays in userState must be homogeneous; ` +
          `found both '${elementSig}' and '${sig}' in the same array ` +
          `(split the values into separate fields, or normalize them to one type)`,
      );
    }
  }
  return elementType ?? emptyListElementType();
}

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
        return makeList(field(LIST_CHILD_NAME, inferElementType(value), true));
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
 * Extract a typed value from an Arrow column in RICH form via the codec /
 * canonical path. Backend-agnostic and lossless (BigInt for Int64, Uint8Array
 * for Binary, plain objects for Struct). userState forbids Date/temporal types
 * (see {@link inferFieldType}), so rich == canonical for everything stored here.
 */
function extractTypedValue(col: any, index: number, type: VgiDataType): any {
  return codecFor(type).canonicalToRich(readCanonicalValue(type, col, index));
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
    const fieldByName = new Map(batch.schema.fields.map((f) => [f.name, f]));
    const get = (name: string) => {
      const col = batch.getChild(name);
      const f = fieldByName.get(name);
      if (!col || !f) return null;
      const type = f.type as unknown as VgiDataType;
      return codecFor(type).canonicalToRich(readCanonicalValue(type, col, 0));
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
