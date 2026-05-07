// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Generic ArrowSerializableDataclass (ASD) codec.
 *
 * Bridges between typed TS objects (snake_case, matching vgi-python dataclass
 * field names) and Arrow IPC bytes (single-row batch with the dataclass'
 * ARROW_SCHEMA). Used by the generated vgi-client.ts encode<X>/decode<X>
 * wrappers to make the typed client interfaces honest at the wire boundary.
 *
 * Encoding delegates to the facade's `batchFromRows` (which handles the
 * complex types — Decimal/BigInt/List/Map/Struct/Dictionary — uniformly
 * across both arrow-js and flechette backends). Decoding pulls each field's
 * value with `batchToScalarDict`, then runs `normalizeValue` to convert
 * Arrow representations (MapRow, BigInt, etc.) back into plain JS shapes.
 */

import {
  type VgiSchema,
  type VgiField,
  type VgiDataType,
  isBinary,
  isBool,
  isDictionary,
  isInt,
  isList,
  isMap,
  isStruct,
  batchFromRows,
  serializeBatch,
  deserializeBatch,
} from "../arrow/index.js";

/**
 * Encode a typed object as a single-row Arrow IPC stream using `schema`.
 * Matches Python's `ArrowSerializableDataclass.serialize_to_bytes()`.
 */
export function encodeASD(
  schema: VgiSchema,
  obj: Record<string, any>,
): Uint8Array {
  return serializeBatch(batchFromRows([obj], schema));
}

/**
 * Decode a single-row Arrow IPC stream into a typed object using `schema`.
 * Matches Python's `ArrowSerializableDataclass.deserialize_from_bytes()`.
 */
export function decodeASD<T>(
  schema: VgiSchema,
  bytes: Uint8Array,
): T {
  const batch = deserializeBatch(bytes);
  if (batch.numRows === 0) {
    const names = schema.fields.map((f) => f.name).join(",");
    throw new Error(`decodeASD: empty batch (expected 1 row for ${names})`);
  }
  const out: Record<string, any> = {};
  for (const field of schema.fields) {
    const col = batch.getChild(field.name);
    const raw = col ? col.get(0) : null;
    out[field.name] = normalizeValue(raw, field.type);
  }
  return out as T;
}

// --------------------------------------------------------------------------- //
// Decode side: Arrow value -> normalized JS value
// --------------------------------------------------------------------------- //

function normalizeValue(raw: any, type: VgiDataType): any {
  if (raw == null) return null;

  if (isList(type)) {
    const childType = (type as any).children[0].type as VgiDataType;
    const out: any[] = [];
    for (const item of raw) out.push(normalizeValue(item, childType));
    return out;
  }

  if (isMap(type)) {
    const out: Record<string, string> = {};
    // arrow-js MapRow is iterable of [k,v]; flechette returns [[k,v],...] arrays.
    if (raw[Symbol.iterator]) {
      for (const entry of raw) {
        if (Array.isArray(entry)) {
          out[String(entry[0])] = String(entry[1] ?? "");
        } else if (entry && typeof entry === "object") {
          const k = entry.key ?? entry[0];
          const v = entry.value ?? entry[1];
          out[String(k)] = v == null ? "" : String(v);
        }
      }
    } else if (typeof raw === "object") {
      // Plain-object map (flechette without useMap): Object.entries fallback
      for (const [k, v] of Object.entries(raw)) {
        out[String(k)] = v == null ? "" : String(v);
      }
    }
    return out;
  }

  if (isStruct(type)) {
    const childFields = (type as any).children as VgiField[];
    const out: Record<string, any> = {};
    for (const cf of childFields) {
      out[cf.name] = normalizeValue(raw[cf.name], cf.type);
    }
    return out;
  }

  if (isDictionary(type)) {
    return raw == null ? null : String(raw);
  }

  if (isBinary(type)) {
    return toUint8Array(raw);
  }

  if (isInt(type) && (type as any).bitWidth === 64) {
    // Prefer number when representable; callers that need BigInt can convert.
    if (typeof raw === "bigint") {
      if (raw >= BigInt(Number.MIN_SAFE_INTEGER) && raw <= BigInt(Number.MAX_SAFE_INTEGER)) {
        return Number(raw);
      }
      return raw;
    }
    return raw;
  }

  if (isBool(type)) {
    return Boolean(raw);
  }

  // Utf8, Int8/16/32, Float32/64 — pass through as-is.
  return raw;
}

function toUint8Array(val: any): Uint8Array {
  if (val instanceof Uint8Array) return val;
  if (val instanceof ArrayBuffer) return new Uint8Array(val);
  if (val && val.buffer instanceof ArrayBuffer) {
    return new Uint8Array(val.buffer, val.byteOffset, val.byteLength);
  }
  return new Uint8Array(0);
}
