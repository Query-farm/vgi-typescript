// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

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
  return serializeBatch(batchFromRows([sortMapsForSchema(schema, obj)], schema));
}

// --------------------------------------------------------------------------- //
// Encode side: deterministic map columns
// --------------------------------------------------------------------------- //
//
// A map value arrives as a JS object (or Map / pair list), whose iteration
// order is insertion order -- except that integer-like keys ("1", "2") always
// come first, ascending. Two builds of the same record could therefore encode
// differently (and a catalog_contents content-hash etag would change with no
// catalog change). Every map is written in sorted key order instead, matching
// the Go / Java / C# / Rust SDKs: keys compared by Unicode code point (the
// same order as comparing their UTF-8 bytes), not by UTF-16 code unit.

/** Compare two strings by Unicode code point (= UTF-8 byte order). */
export function compareCodePoints(a: string, b: string): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const x = a.charCodeAt(i);
    const y = b.charCodeAt(i);
    if (x === y) continue;
    // A surrogate (a code point above U+FFFF) sorts after every BMP code unit,
    // though as a code unit (0xD800-0xDFFF) it sorts below U+E000-U+FFFF.
    const xs = x >= 0xd800 && x <= 0xdfff;
    const ys = y >= 0xd800 && y <= 0xdfff;
    if (xs !== ys) return xs ? 1 : -1;
    return x - y;
  }
  return a.length - b.length;
}

function mapPairs(raw: any): Array<[unknown, unknown]> {
  if (Array.isArray(raw)) return raw.map((e: any) => (Array.isArray(e) ? [e[0], e[1]] : [e?.key, e?.value]));
  if (raw instanceof Map) return Array.from(raw.entries());
  if (typeof raw[Symbol.iterator] === "function") return Array.from(raw as Iterable<[unknown, unknown]>);
  return Object.entries(raw);
}

function compareKeys(a: unknown, b: unknown): number {
  if (typeof a === "string" && typeof b === "string") return compareCodePoints(a, b);
  if ((typeof a === "number" || typeof a === "bigint") && (typeof b === "number" || typeof b === "bigint")) {
    return a < b ? -1 : a > b ? 1 : 0;
  }
  return compareCodePoints(String(a), String(b));
}

/** Whether a type contains a map anywhere (memoized per type object). */
const HAS_MAP = new WeakMap<object, boolean>();
function typeHasMap(type: VgiDataType): boolean {
  const cached = HAS_MAP.get(type as object);
  if (cached !== undefined) return cached;
  let has = isMap(type);
  if (!has && (isList(type) || isStruct(type))) {
    has = ((type as any).children as VgiField[]).some((c) => typeHasMap(c.type));
  }
  HAS_MAP.set(type as object, has);
  return has;
}

function sortMapsForType(type: VgiDataType, value: any): any {
  if (value == null || !typeHasMap(type)) return value;
  if (isMap(type)) {
    const entries = (type as any).children[0].type as VgiDataType;
    const valueType = ((entries as any).children as VgiField[])[1].type;
    return mapPairs(value)
      .map(([k, v]): [unknown, unknown] => [k, sortMapsForType(valueType, v)])
      .sort((x, y) => compareKeys(x[0], y[0]));
  }
  if (isList(type)) {
    const child = (type as any).children[0].type as VgiDataType;
    return Array.from(value as Iterable<unknown>, (v) => sortMapsForType(child, v));
  }
  if (isStruct(type)) {
    const out: Record<string, any> = { ...value };
    for (const cf of (type as any).children as VgiField[]) out[cf.name] = sortMapsForType(cf.type, value[cf.name]);
    return out;
  }
  return value;
}

/**
 * `obj` with every map-typed field (at any depth) as a key-sorted pair list.
 * Returns `obj` itself when the schema has no map column. Never mutates `obj`
 * (catalog items may be frozen and shared).
 */
export function sortMapsForSchema(schema: VgiSchema, obj: Record<string, any>): Record<string, any> {
  let out: Record<string, any> | null = null;
  for (const f of schema.fields) {
    if (!typeHasMap(f.type)) continue;
    const v = obj[f.name];
    if (v == null) continue;
    out ??= { ...obj };
    out[f.name] = sortMapsForType(f.type, v);
  }
  return out ?? obj;
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
