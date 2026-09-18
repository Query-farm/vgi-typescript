// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// State serializer for the HTTP transport: the exchange state an HTTP cursor
// carries between turns, and the Arrow IPC encoding of user state inside it.

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
import { decodeFilterHistory, encodeFilterHistory } from "../filter-pushdown/history.js";
import { packInitRequest } from "./carried-init-request.js";

/**
 * The fields of the exchange state an HTTP cursor carries, by name and type.
 *
 * Descriptive: {@link arrowStateSerializer} writes them as a flat binary frame
 * (see there), not as an Arrow batch of this schema. Kept exported for API
 * stability.
 */
export const EXCHANGE_STATE_SCHEMA = makeSchema([
  field("function_name", binary(), false),
  // The init request, packed by carried-init-request.ts (a codec byte, then
  // the IPC bytes, zstd-compressed where the runtime can).
  field("init_request", binary(), false),
  field("execution_id", binary(), false),
  field("max_workers", int64(), false),
  field("opaque_data", binary(), true),
  field("is_producer", bool(), false),
  field("user_state", binary(), true),
  // The stream's compacted dynamic-filter history (filter-pushdown/history.ts),
  // null when no delta has been applied. Bounded by the number of predicate
  // ids, so the token stays flat-sized however many turns the scan takes.
  field("filter_history", binary(), true),
]);

const TEXT_ENCODER = new TextEncoder();
const TEXT_DECODER = new TextDecoder();

/** Version byte of the exchange state frame (never 0xFF, the first byte of an Arrow IPC stream). */
const STATE_FORMAT_VERSION = 1;
const FLAG_PRODUCER = 1;
const FLAG_OPAQUE_DATA = 2;
const FLAG_USER_STATE = 4;
const FLAG_FILTER_HISTORY = 8;

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

/**
 * The HTTP exchange state serializer: what a stream's cursor carries between
 * turns.
 *
 * A flat, versioned binary frame, not an Arrow batch. The cursor is built and
 * parsed on every turn of every HTTP stream, and wrapping eight scalars in a
 * one-row Arrow batch cost ~0.4 ms a turn (a generic column builder per field,
 * an IPC schema message each way) plus ~1 KB of framing that was then sealed
 * and base64-encoded twice per turn. The fields are the ones
 * {@link EXCHANGE_STATE_SCHEMA} names; `user_state` stays Arrow IPC inside the
 * frame, because it carries typed user values.
 *
 * Layout (little-endian): `u8 version | u8 flags | f64 max_workers |
 * bytes function_name | bytes init_request | bytes execution_id |
 * [bytes opaque_data] [bytes user_state] [bytes filter_history]`, where
 * `bytes` is `u32 length, payload` and a bracketed field is present only when
 * its flag bit is set. The frame never leaves this worker unsealed, so the
 * version byte exists to turn a token minted by another build into a clean
 * refusal rather than a misread.
 *
 * The name is kept for API stability; the Arrow in it is the user state.
 */
export const arrowStateSerializer: StateSerializer = {
  serialize(state: any): Uint8Array {
    const functionName = TEXT_ENCODER.encode(state.functionName ?? "");
    // Packed on the init turn (the only one holding the raw bytes); every
    // continuation carries the packed bytes through unchanged.
    const initRequest: Uint8Array = state.initRequestPacked ?? packInitRequest(state.initRequestIpc);
    const executionId = toUint8Array(state.executionId);
    const opaqueData = state.opaqueData != null ? toUint8Array(state.opaqueData) : null;
    const userState = serializeUserState(state.userState);
    const filterHistory = encodeFilterHistory(state.filterHistory);
    const flags =
      (state.isProducer ? FLAG_PRODUCER : 0) |
      (opaqueData ? FLAG_OPAQUE_DATA : 0) |
      (userState ? FLAG_USER_STATE : 0) |
      (filterHistory ? FLAG_FILTER_HISTORY : 0);
    const chunks = [functionName, initRequest, executionId];
    if (opaqueData) chunks.push(opaqueData);
    if (userState) chunks.push(userState);
    if (filterHistory) chunks.push(filterHistory);
    let size = 2 + 8;
    for (const chunk of chunks) size += 4 + chunk.byteLength;
    const out = new Uint8Array(size);
    const view = new DataView(out.buffer);
    out[0] = STATE_FORMAT_VERSION;
    out[1] = flags;
    view.setFloat64(2, Number(state.maxWorkers ?? 1), true);
    let offset = 10;
    for (const chunk of chunks) {
      view.setUint32(offset, chunk.byteLength, true);
      out.set(chunk, offset + 4);
      offset += 4 + chunk.byteLength;
    }
    return out;
  },

  deserialize(bytes: Uint8Array): any {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    if (bytes.byteLength < 10 || bytes[0] !== STATE_FORMAT_VERSION) {
      throw new Error("exchange state has an unknown format (minted by another build of this worker?)");
    }
    const flags = bytes[1];
    const maxWorkers = view.getFloat64(2, true);
    let offset = 10;
    // Each field is copied out, not viewed: Arrow readers want a buffer of
    // their own (byteOffset 0), and nothing should pin the whole frame.
    const take = (): Uint8Array => {
      if (offset + 4 > bytes.byteLength) throw new Error("exchange state is truncated");
      const length = view.getUint32(offset, true);
      offset += 4;
      if (offset + length > bytes.byteLength) throw new Error("exchange state is truncated");
      const chunk = bytes.slice(offset, offset + length);
      offset += length;
      return chunk;
    };
    const functionName = TEXT_DECODER.decode(take());
    const initRequestPacked = take();
    const executionId = take();
    const opaqueData = flags & FLAG_OPAQUE_DATA ? take() : null;
    const userStateBytes = flags & FLAG_USER_STATE ? take() : null;
    const filterHistoryBytes = flags & FLAG_FILTER_HISTORY ? take() : null;
    if (offset !== bytes.byteLength) throw new Error("exchange state has trailing bytes");
    const isProducer = (flags & FLAG_PRODUCER) !== 0;
    return {
      functionName,
      initRequestPacked,
      executionId,
      maxWorkers,
      opaqueData,
      isProducer,
      // vgi-rpc dispatch reads __isProducer to choose producer vs exchange mode
      __isProducer: isProducer,
      userState: deserializeUserState(userStateBytes),
      filterHistory: decodeFilterHistory(filterHistoryBytes),
    };
  },
};
