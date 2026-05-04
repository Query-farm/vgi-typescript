// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

/**
 * Generic ArrowSerializableDataclass (ASD) codec.
 *
 * Bridges between typed TS objects (snake_case, matching vgi-python dataclass
 * field names) and Arrow IPC bytes (single-row RecordBatch with the dataclass'
 * ARROW_SCHEMA). Used by the generated vgi-client.ts encode<X>/decode<X>
 * wrappers to make the typed client interfaces honest at the wire boundary.
 *
 * Handles the Arrow type families used by the 7 INFO_TYPES schemas:
 *   Utf8, Binary, Bool, Int32, Int64, List, Map_<Utf8,Utf8>,
 *   Dictionary(Utf8, Int16), Struct, nullable wrappers.
 *
 * Extend as new types cross the wire; unhandled types throw loudly rather
 * than silently producing garbage.
 */

import {
  DataType,
  type Field,
  makeData,
  RecordBatch,
  RecordBatchStreamWriter,
  type Schema,
  Struct,
  vectorFromArray,
} from "@query-farm/apache-arrow";
import { deserializeBatch } from "../util/arrow/index.js";

/**
 * Encode a typed object as a single-row Arrow IPC stream using `schema`.
 * Matches Python's `ArrowSerializableDataclass.serialize_to_bytes()`.
 */
export function encodeASD(schema: Schema, obj: Record<string, any>): Uint8Array {
  const children = schema.fields.map((f) => buildFieldData(obj[f.name], f));

  const structType = new Struct(schema.fields);
  const data = makeData({
    type: structType,
    length: 1,
    children,
    nullCount: 0,
  });

  const batch = new RecordBatch(schema, data);
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  // arrow-js's public `write()` silently drops batches on nullability mismatch;
  // `_writeRecordBatch` bypasses that check. Tracks the workaround in
  // `catalog/interface.ts:serializeInfoBatch` — same reasoning applies.
  (writer as any)._writeRecordBatch(batch);
  writer.close();
  return writer.toUint8Array(true);
}

/**
 * Decode a single-row Arrow IPC stream into a typed object using `schema`.
 * Matches Python's `ArrowSerializableDataclass.deserialize_from_bytes()`.
 */
export function decodeASD<T>(schema: Schema, bytes: Uint8Array): T {
  const batch = deserializeBatch(bytes);
  if (batch.numRows === 0) {
    throw new Error(`decodeASD: empty batch (expected 1 row for ${batch.schema.fields.map(f => f.name).join(",")})`);
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
// Encode side: JS value -> Arrow column Data
// --------------------------------------------------------------------------- //

function buildFieldData(value: any, field: Field): any {
  const type = field.type;

  // Null
  if (value == null) {
    if (!field.nullable) {
      // Some fields (tags, items) default to empty container rather than null.
      // Fall through with an appropriate empty if the type is a container.
      if (DataType.isList(type)) return buildList([], field);
      if (DataType.isMap(type)) return buildMap({}, field);
      throw new Error(`encodeASD: non-nullable field '${field.name}' got null/undefined`);
    }
    return vectorFromArray([null], type).data[0];
  }

  if (DataType.isList(type)) return buildList(value, field);
  if (DataType.isMap(type)) return buildMap(value, field);
  if (DataType.isStruct(type)) return buildStruct(value, field);

  // Dictionary(Utf8, Int16) — serialize as the underlying string; arrow-js
  // builds the dictionary automatically.
  if (DataType.isDictionary(type)) {
    return vectorFromArray([String(value)], type).data[0];
  }

  // Int64 — arrow-js requires BigInt at this bit width.
  if (DataType.isInt(type) && (type as any).bitWidth === 64) {
    const coerced = typeof value === "number" ? BigInt(value) : value;
    return vectorFromArray([coerced], type).data[0];
  }

  // Binary — accept Uint8Array directly.
  // Bool, Utf8, Int*/Float* — primitive passthrough.
  return vectorFromArray([value], type).data[0];
}

function buildList(value: any[], field: Field): any {
  const type = field.type;
  const childField = type.children[0] as Field;
  const items = Array.isArray(value) ? value : [...value];

  if (items.length === 0) {
    const emptyChild = buildEmptyData(childField.type);
    return makeData({
      type,
      length: 1,
      child: emptyChild,
      valueOffsets: new Int32Array([0, 0]),
      nullCount: 0,
    } as any);
  }

  // Build the child by delegating: wrap each item through buildFieldData
  // so nested Lists / Dictionary / Struct elements get the same treatment.
  // We need a single child Data covering all items; the simplest approach
  // is to route recursively via a virtual field per item and splice.
  const childData = buildChildArrayData(items, childField);

  return makeData({
    type,
    length: 1,
    child: childData,
    valueOffsets: new Int32Array([0, items.length]),
    nullCount: 0,
  } as any);
}

function buildChildArrayData(items: any[], childField: Field): any {
  const ct = childField.type;

  // Nested list — recurse by building one parent at length N.
  if (DataType.isList(ct)) {
    const inner = ct.children[0] as Field;
    // Flatten all inner items while tracking offsets.
    const offsets = new Int32Array(items.length + 1);
    const flat: any[] = [];
    for (let i = 0; i < items.length; i++) {
      offsets[i] = flat.length;
      const sub = items[i];
      if (sub != null) {
        const subArr = Array.isArray(sub) ? sub : [...sub];
        for (const v of subArr) flat.push(v);
      }
    }
    offsets[items.length] = flat.length;
    const childData = flat.length === 0
      ? makeData({ type: inner.type, length: 0, nullCount: 0 })
      : buildChildArrayData(flat, inner);
    return makeData({
      type: ct,
      length: items.length,
      child: childData,
      valueOffsets: offsets,
      nullCount: 0,
    } as any);
  }

  if (DataType.isStruct(ct)) {
    return buildStructArrayData(items, childField);
  }

  if (DataType.isDictionary(ct)) {
    return vectorFromArray(items.map((v) => (v == null ? null : String(v))), ct).data[0];
  }

  if (DataType.isInt(ct) && (ct as any).bitWidth === 64) {
    return vectorFromArray(
      items.map((v) => (typeof v === "number" ? BigInt(v) : v)),
      ct,
    ).data[0];
  }

  // Binary / Utf8 / Bool / primitive numbers
  return vectorFromArray(items, ct).data[0];
}

/**
 * Build a zero-row Data node for any type — including containers that
 * require their own children (List/Map/Struct). arrow-js's IPC writer walks
 * `data.children[0]` on nested lists, so a plain makeData with length=0 and
 * no children breaks on List<List<T>> / Struct children.
 */
function buildEmptyData(type: DataType): any {
  if (DataType.isList(type)) {
    const inner = (type as any).children[0] as Field;
    const innerChild = buildEmptyData(inner.type);
    return makeData({
      type,
      length: 0,
      child: innerChild,
      valueOffsets: new Int32Array([0]),
      nullCount: 0,
    } as any);
  }
  if (DataType.isMap(type)) {
    const entriesField = (type as any).children[0] as Field;
    const entriesData = buildEmptyData(entriesField.type);
    return makeData({
      type,
      length: 0,
      child: entriesData,
      valueOffsets: new Int32Array([0]),
      nullCount: 0,
    } as any);
  }
  if (DataType.isStruct(type)) {
    const children = (type as any).children.map((cf: Field) => buildEmptyData(cf.type));
    return makeData({ type, length: 0, children, nullCount: 0 });
  }
  return makeData({ type, length: 0, nullCount: 0 });
}

function buildMap(value: Record<string, any>, field: Field): any {
  const type = field.type;
  const entriesField = type.children[0] as Field;
  const entries = Object.entries(value ?? {});
  const keys = entries.map(([k]) => k);
  const vals = entries.map(([, v]) => v);

  const keyField = (entriesField.type as any).children[0] as Field;
  const valueField = (entriesField.type as any).children[1] as Field;

  const keyData = keys.length === 0
    ? makeData({ type: keyField.type, length: 0, nullCount: 0 })
    : vectorFromArray(keys, keyField.type).data[0];
  const valueData = vals.length === 0
    ? makeData({ type: valueField.type, length: 0, nullCount: 0 })
    : vectorFromArray(vals.map((v) => v == null ? null : String(v)), valueField.type).data[0];

  const structData = makeData({
    type: entriesField.type,
    length: entries.length,
    children: [keyData, valueData],
    nullCount: 0,
  });

  return makeData({
    type,
    length: 1,
    child: structData,
    valueOffsets: new Int32Array([0, entries.length]),
    nullCount: 0,
  } as any);
}

function buildStruct(value: Record<string, any>, field: Field): any {
  const type = field.type;
  const children = type.children.map((cf: Field) => buildFieldData(value?.[cf.name], cf));
  return makeData({
    type,
    length: 1,
    children,
    nullCount: 0,
  });
}

function buildStructArrayData(items: any[], field: Field): any {
  const type = field.type;
  const children = type.children.map((cf: Field) => {
    const col = items.map((row) => row?.[cf.name] ?? null);
    if (DataType.isList(cf.type) || DataType.isStruct(cf.type)) {
      return buildChildArrayData(col, cf);
    }
    if (DataType.isDictionary(cf.type)) {
      return vectorFromArray(col.map((v) => v == null ? null : String(v)), cf.type).data[0];
    }
    if (DataType.isInt(cf.type) && (cf.type as any).bitWidth === 64) {
      return vectorFromArray(col.map((v) => typeof v === "number" ? BigInt(v) : v), cf.type).data[0];
    }
    return vectorFromArray(col, cf.type).data[0];
  });
  return makeData({
    type,
    length: items.length,
    children,
    nullCount: 0,
  });
}

// --------------------------------------------------------------------------- //
// Decode side: Arrow value -> normalized JS value
// --------------------------------------------------------------------------- //

function normalizeValue(raw: any, type: DataType): any {
  if (raw == null) return null;

  if (DataType.isList(type)) {
    const childType = (type as any).children[0].type;
    const out: any[] = [];
    for (const item of raw) out.push(normalizeValue(item, childType));
    return out;
  }

  if (DataType.isMap(type)) {
    const out: Record<string, string> = {};
    // arrow-js MapRow is iterable of [key, value] OR {key, value}
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
    }
    return out;
  }

  if (DataType.isStruct(type)) {
    const childFields = (type as any).children as Field[];
    const out: Record<string, any> = {};
    for (const cf of childFields) {
      const val = raw[cf.name];
      out[cf.name] = normalizeValue(val, cf.type);
    }
    return out;
  }

  if (DataType.isDictionary(type)) {
    return raw == null ? null : String(raw);
  }

  if (DataType.isBinary(type)) {
    return toUint8Array(raw);
  }

  if (DataType.isInt(type) && (type as any).bitWidth === 64) {
    // Prefer number when representable; callers that need BigInt can convert.
    if (typeof raw === "bigint") {
      if (raw >= BigInt(Number.MIN_SAFE_INTEGER) && raw <= BigInt(Number.MAX_SAFE_INTEGER)) {
        return Number(raw);
      }
      return raw;
    }
    return raw;
  }

  if (DataType.isBool(type)) {
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
