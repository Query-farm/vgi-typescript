// Arrow IPC state serializer for HTTP transport.
// Replaces JSON serialization with Arrow IPC for the exchange state token,
// avoiding base64/hex overhead and staying consistent with the rest of the protocol.

import {
  Schema, Field, DataType, Struct, Binary, Int64, Bool, Float64, Utf8, Null,
  makeData, RecordBatch,
} from "@query-farm/apache-arrow";
import type { StateSerializer } from "vgi-rpc";
import { serializeBatch, deserializeBatch, batchFromColumns } from "../util/arrow.js";
import { toUint8Array } from "../util/bytes.js";

/** Schema for the exchange state carried in HTTP state tokens. */
export const EXCHANGE_STATE_SCHEMA = new Schema([
  new Field("function_name", new Binary(), false),
  new Field("init_request", new Binary(), false),
  new Field("execution_id", new Binary(), false),
  new Field("max_workers", new Int64(), false),
  new Field("opaque_data", new Binary(), true),
  new Field("is_producer", new Bool(), false),
  new Field("user_state", new Binary(), true),
]);

const TEXT_ENCODER = new TextEncoder();
const TEXT_DECODER = new TextDecoder();

/** Infer an Arrow DataType from a JS value for user state serialization. */
export function inferFieldType(value: any): DataType {
  if (value === null || value === undefined) return new Null();
  switch (typeof value) {
    case "number": return new Float64();
    case "bigint": return new Int64();
    case "string": return new Utf8();
    case "boolean": return new Bool();
    case "object":
      if (value instanceof Uint8Array || value instanceof ArrayBuffer) return new Binary();
      if (ArrayBuffer.isView(value)) return new Binary();
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
        ([k, v]) => new Field(k, inferFieldType(v), true),
      );
      return new Struct(fields);
    default:
      throw new Error(`inferFieldType: unsupported type '${typeof value}'`);
  }
}

/**
 * Serialize userState to Arrow IPC bytes.
 * Infers schema from the JS object at runtime. Arrow IPC is self-describing,
 * so deserialization doesn't need the schema ahead of time.
 */
export function serializeUserState(userState: any): Uint8Array | null {
  if (userState == null) return null;
  const entries = Object.entries(userState);
  if (entries.length === 0) {
    // Empty object: create a 1-row batch with no fields.
    // batchFromColumns would produce 0 rows, so build manually.
    const schema = new Schema([]);
    const structType = new Struct([]);
    const data = makeData({ type: structType, length: 1, children: [], nullCount: 0 });
    return serializeBatch(new RecordBatch(schema, data));
  }
  const fields = entries.map(
    ([k, v]) => new Field(k, inferFieldType(v), true),
  );
  const schema = new Schema(fields);
  const columns: Record<string, any[]> = {};
  for (const [key, val] of entries) {
    columns[key] = [val];
  }
  return serializeBatch(batchFromColumns(columns, schema));
}

/**
 * Extract a typed value from an Arrow column, preserving BigInt for Int64.
 * Recurses into Struct fields to produce plain JS objects.
 */
function extractTypedValue(col: any, index: number, type: DataType): any {
  const val = col.get(index);
  if (val === null || val === undefined) return null;

  if (DataType.isStruct(type)) {
    const result: Record<string, any> = {};
    for (let i = 0; i < type.children.length; i++) {
      const childField = type.children[i];
      const childVec = col.getChildAt(i);
      result[childField.name] = childVec
        ? extractTypedValue(childVec, index, childField.type)
        : null;
    }
    return result;
  }

  if (DataType.isBinary(type)) {
    return toUint8Array(val);
  }

  // Arrow's .get() returns BigInt for Int64, number for Float64, etc.
  return val;
}

/**
 * Deserialize userState from Arrow IPC bytes.
 * Reconstructs a plain JS object, preserving BigInt for Int64 columns.
 */
export function deserializeUserState(bytes: Uint8Array | null): any {
  if (bytes == null) return null;
  const batch = deserializeBatch(bytes);
  if (batch.numRows === 0 && batch.schema.fields.length === 0) {
    // Empty schema with 0 rows → was an empty object {}
    return {};
  }
  if (batch.numRows === 0) return null;
  const result: Record<string, any> = {};
  for (const field of batch.schema.fields) {
    const col = batch.getChild(field.name);
    if (!col) { result[field.name] = null; continue; }
    result[field.name] = extractTypedValue(col, 0, field.type);
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
