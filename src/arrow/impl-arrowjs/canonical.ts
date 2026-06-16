// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Canonical Arrow I/O for the arrow-js backend.
//
// This is the ONLY place in the arrow-js backend that knows arrow-js-specific
// build/read details. Everything else routes values through the codec registry
// (rich <-> canonical) and then through here (canonical <-> arrow-js column
// data). The canonical representation is defined in ../codec/registry.ts and is
// byte-for-byte identical across the arrow-js and flechette backends, which is
// what keeps the two backends in agreement.
//
// Canonical units consumed/produced here:
//   date32      -> number  (days since epoch)
//   date64      -> bigint  (ms since epoch)
//   time32      -> number  (raw s/ms)
//   time64      -> bigint  (raw us/ns)
//   timestamp   -> bigint  (raw unit)
//   duration    -> bigint  (raw unit)
//   decimal     -> bigint  (unscaled integer)
//   int64/u64   -> bigint
//   bool/int*/float* -> boolean/number
//   utf8        -> string
//   binary/fsb  -> Uint8Array
//   struct/list/map -> object / array / Array<[k,v]>
//   dictionary  -> decoded value's canonical

import {
  Struct,
  List,
  DataType,
  DateUnit,
  makeData,
  vectorFromArray,
  Map_,
  type Field,
} from "@query-farm/apache-arrow";
import type { VgiDataType, VgiColumnData } from "../types.js";

// ===========================================================================
// WRITE: canonical[] -> arrow-js column Data
// ===========================================================================

/**
 * Build an arrow-js column `Data` node from an array of CANONICAL values for
 * `type`. Folds together what used to be scattered across build.ts
 * (`buildColumnData`, `dateTimeBitWidth`, decimal/bigint/date special-casing).
 */
export function writeCanonicalColumn(
  type: VgiDataType,
  canonical: unknown[],
): VgiColumnData {
  return buildColumnData(canonical, type as DataType) as VgiColumnData;
}

function buildColumnData(values: unknown[], type: DataType): any {
  if (DataType.isList(type)) return buildListData(values, type as List);
  if (DataType.isMap(type)) return buildMapData(values, type as Map_);
  if (DataType.isStruct(type)) return buildStructData(values, type as Struct);
  if (DataType.isDecimal(type)) return buildDecimalData(values, type);

  // date32: canonical = day-number (number/bigint). date64: canonical = ms
  // (bigint). time32: number; time64/timestamp/duration: bigint. All of these
  // are "raw unit" integers that arrow-js's vectorFromArray mishandles
  // (silently zeros 32-bit Date/Time fed raw units; throws on nulls). Build
  // every Date/Time/Timestamp/Duration column manually so it round-trips.
  if (DataType.isDate(type) || DataType.isTime(type) ||
      DataType.isTimestamp(type) || DataType.isDuration(type)) {
    const bitWidth = dateTimeBitWidth(type);
    if (bitWidth === 64) return buildBigIntData(values, type);
    return buildInt32Data(values, type);
  }

  // Int64 / Uint64: ensure bigint storage.
  if (DataType.isInt(type) && (type as any).bitWidth === 64) {
    return buildBigIntData(values, type);
  }

  // Everything else (bool, int8..32, float, utf8, binary, fsb, dictionary):
  // vectorFromArray handles canonical values directly.
  return vectorFromArray(values as any[], type).data[0];
}

/** Storage bit width for a temporal type. */
function dateTimeBitWidth(type: any): number {
  if (DataType.isDate(type)) return type.unit === DateUnit.DAY ? 32 : 64;
  if (DataType.isTime(type)) return type.bitWidth ?? 32;
  return 64; // Timestamp / Duration
}

function buildInt32Data(values: unknown[], type: DataType): any {
  const length = values.length;
  const buf = new Int32Array(length);
  const nullBitmap = new Uint8Array(Math.ceil(length / 8));
  let nullCount = 0;
  for (let i = 0; i < length; i++) {
    const v = values[i];
    if (v === null || v === undefined) { nullCount++; continue; }
    nullBitmap[i >> 3] |= 1 << (i & 7);
    buf[i] = typeof v === "bigint" ? Number(v) : (v as number);
  }
  return makeData({
    type, length,
    nullBitmap: nullCount > 0 ? nullBitmap : undefined,
    data: buf, nullCount,
  } as any);
}

function buildBigIntData(values: unknown[], type: DataType): any {
  const length = values.length;
  const bigBuf = new BigInt64Array(length);
  const nullBitmap = new Uint8Array(Math.ceil(length / 8));
  let nullCount = 0;
  for (let i = 0; i < length; i++) {
    const v = values[i];
    if (v === null || v === undefined) { nullCount++; continue; }
    nullBitmap[i >> 3] |= 1 << (i & 7);
    bigBuf[i] = typeof v === "bigint" ? v : BigInt(v as number);
  }
  const data = new Int32Array(bigBuf.buffer);
  return makeData({
    type, length,
    nullBitmap: nullCount > 0 ? nullBitmap : undefined,
    data, nullCount,
  } as any);
}

function buildDecimalData(values: unknown[], type: DataType): any {
  const byteWidth: number = (type as any).byteWidth ?? ((type as any).bitWidth ? (type as any).bitWidth / 8 : 16);
  const length = values.length;
  const buf = new Uint8Array(length * byteWidth);
  const nullBitmap = new Uint8Array(Math.ceil(length / 8));
  let nullCount = 0;
  for (let i = 0; i < length; i++) {
    const val = values[i];
    if (val === null || val === undefined) { nullCount++; continue; }
    nullBitmap[i >> 3] |= 1 << (i & 7);
    let bytes: Uint8Array;
    if (val instanceof Uint8Array) {
      bytes = val;
    } else if (ArrayBuffer.isView(val)) {
      const view = val as ArrayBufferView;
      bytes = new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
    } else {
      bytes = bigIntToDecimalBytes(toBig(val), byteWidth);
    }
    buf.set(bytes.subarray(0, byteWidth), i * byteWidth);
  }
  return makeData({
    type, length,
    nullBitmap: nullCount > 0 ? nullBitmap : undefined,
    data: new Uint32Array(buf.buffer, buf.byteOffset, buf.byteLength / 4),
    nullCount,
  } as any);
}

function toBig(v: unknown): bigint {
  return typeof v === "bigint" ? v : BigInt(Math.trunc(Number(v)));
}

/** Little-endian two's-complement bytes of an unscaled decimal bigint. */
function bigIntToDecimalBytes(value: bigint, byteWidth: number): Uint8Array {
  const bytes = new Uint8Array(byteWidth);
  let v = value;
  if (v < 0n) v = (1n << BigInt(byteWidth * 8)) + v;
  for (let i = 0; i < byteWidth; i++) {
    bytes[i] = Number(v & 0xffn);
    v >>= 8n;
  }
  return bytes;
}

function buildListData(values: unknown[], listType: List): any {
  const childField = listType.children[0];
  const numRows = values.length;
  const offsets = new Int32Array(numRows + 1);
  const allItems: unknown[] = [];
  const nullBitmap = new Uint8Array(Math.ceil(numRows / 8));
  let nullCount = 0;
  for (let i = 0; i < numRows; i++) {
    offsets[i] = allItems.length;
    const row = values[i];
    if (row == null) {
      nullCount++;
    } else {
      nullBitmap[i >> 3] |= 1 << (i & 7);
      const items = Array.isArray(row) ? row
        : (typeof (row as any)[Symbol.iterator] === "function") ? Array.from(row as Iterable<unknown>)
        : null;
      if (items) for (const item of items) allItems.push(item);
    }
  }
  offsets[numRows] = allItems.length;
  const childData = allItems.length > 0
    ? buildColumnData(allItems, childField.type)
    : emptyDataForType(childField.type);
  return makeData({
    type: listType, length: numRows, nullCount,
    nullBitmap: nullCount > 0 ? nullBitmap : undefined,
    child: childData, offset: 0, valueOffsets: offsets,
  });
}

function buildMapData(values: unknown[], mapType: Map_): any {
  const numRows = values.length;
  const entriesField = mapType.children[0];
  const keyField = (entriesField.type as any).children[0] as Field;
  const valueField = (entriesField.type as any).children[1] as Field;
  const offsets = new Int32Array(numRows + 1);
  const allKeys: unknown[] = [];
  const allValues: unknown[] = [];
  const nullBitmap = new Uint8Array(Math.ceil(numRows / 8));
  let nullCount = 0;
  for (let i = 0; i < numRows; i++) {
    offsets[i] = allKeys.length;
    const row = values[i];
    if (row == null) {
      nullCount++;
    } else {
      nullBitmap[i >> 3] |= 1 << (i & 7);
      // Canonical map is Array<[k, v]>, but tolerate Map / iterable / object.
      const pairs: Array<[unknown, unknown]> = Array.isArray(row) ? row as any
        : row instanceof Map ? Array.from(row.entries())
        : (typeof (row as any)[Symbol.iterator] === "function") ? Array.from(row as any)
        : (typeof row === "object") ? Object.entries(row as any)
        : [];
      for (const [k, v] of pairs) { allKeys.push(k); allValues.push(v); }
    }
  }
  offsets[numRows] = allKeys.length;
  const keyData = allKeys.length > 0
    ? buildColumnData(allKeys, keyField.type)
    : emptyDataForType(keyField.type);
  const valueData = allValues.length > 0
    ? buildColumnData(allValues, valueField.type)
    : emptyDataForType(valueField.type);
  const entriesData = makeData({
    type: entriesField.type, length: allKeys.length,
    children: [keyData, valueData], nullCount: 0,
  });
  return makeData({
    type: mapType, length: numRows, nullCount,
    nullBitmap: nullCount > 0 ? nullBitmap : undefined,
    child: entriesData, offset: 0, valueOffsets: offsets,
  });
}

function buildStructData(values: unknown[], structType: Struct): any {
  const childFields = structType.children;
  const numRows = values.length;
  const nullBitmap = new Uint8Array(Math.ceil(numRows / 8));
  let nullCount = 0;
  const childArrays: unknown[][] = childFields.map(() => []);
  for (let i = 0; i < numRows; i++) {
    const row = values[i] as any;
    if (row == null) {
      nullCount++;
      for (let c = 0; c < childFields.length; c++) childArrays[c].push(null);
    } else {
      nullBitmap[i >> 3] |= 1 << (i & 7);
      for (let c = 0; c < childFields.length; c++) {
        const name = childFields[c].name;
        childArrays[c].push(row[name] ?? row[c] ?? null);
      }
    }
  }
  const children = childFields.map((cf, idx) => buildColumnData(childArrays[idx], cf.type));
  return makeData({
    type: structType, length: numRows, nullCount,
    nullBitmap: nullCount > 0 ? nullBitmap : undefined, children,
  });
}

function emptyDataForType(type: DataType): any {
  if (DataType.isList(type)) {
    return makeData({
      type, length: 0, nullCount: 0,
      child: emptyDataForType((type as List).children[0].type),
      valueOffsets: new Int32Array([0]),
    });
  }
  if (DataType.isMap(type)) {
    return makeData({
      type, length: 0, nullCount: 0,
      child: emptyDataForType((type as Map_).children[0].type),
      valueOffsets: new Int32Array([0]),
    });
  }
  if (DataType.isStruct(type)) {
    const children = (type as any).children.map((cf: Field) => emptyDataForType(cf.type));
    return makeData({ type, length: 0, nullCount: 0, children });
  }
  return makeData({ type, length: 0, nullCount: 0 });
}

// ===========================================================================
// READ: arrow-js column + index -> canonical value
// ===========================================================================

/**
 * Read a single CANONICAL value at `index` from an arrow-js column (Vector).
 * Reads the underlying typed-array storage for scalars (so it is lossless and
 * never depends on Vector.get()'s lossy/divergent coercions — e.g. arrow-js
 * Vector.get() returns a JS number for timestamp[us], losing precision), and
 * recurses for composites.
 */
export function readCanonicalValue(
  type: VgiDataType,
  column: unknown,
  index: number,
): unknown {
  const col = column as any;
  const data = col?.data?.[0];
  return readFromData(type as DataType, col, data, index);
}

/** Validity check against a column/Data null bitmap. */
function isValid(col: any, data: any, index: number): boolean {
  if (typeof col?.isValid === "function") return col.isValid(index);
  const bitmap = data?.nullBitmap;
  if (bitmap && bitmap.length > 0) {
    return ((bitmap[index >> 3] >> (index & 7)) & 1) === 1;
  }
  return true;
}

function readFromData(type: DataType, col: any, data: any, index: number): unknown {
  if (!isValid(col, data, index)) return null;
  const tid = type.typeId;

  switch (tid) {
    case 1: // Null
      return null;
    case 6: // Bool
      return col.get(index) as boolean;
    case 2: { // Int
      const bw = (type as any).bitWidth ?? 32;
      const v = data?.values?.[index];
      if (bw === 64) return typeof v === "bigint" ? v : BigInt(v);
      return Number(v);
    }
    case 3: // Float
      return Number(data?.values?.[index]);
    case 5: // Utf8
    case 20: // LargeUtf8
      return String(col.get(index));
    case 4: // Binary
    case 19: // LargeBinary
    case 15: // FixedSizeBinary
      return toUint8(col.get(index));
    case 7: // Decimal -> unscaled bigint
      return decimalToBigInt(type, data, index);
    case 8: { // Date
      const v = data?.values?.[index];
      // DAY=0 -> date32 storage = Int32 day-number (canonical number)
      // MILLISECOND=1 -> date64 storage = BigInt64 ms (canonical bigint)
      if ((type as any).unit === DateUnit.DAY) return Number(v);
      return typeof v === "bigint" ? v : BigInt(v);
    }
    case 9: { // Time
      const v = data?.values?.[index];
      const bw = (type as any).bitWidth ?? 32;
      if (bw === 64) return typeof v === "bigint" ? v : BigInt(v);
      return Number(v);
    }
    case 10: // Timestamp
    case 18: { // Duration
      const v = data?.values?.[index];
      return typeof v === "bigint" ? v : BigInt(v);
    }
    case 13: // Struct
      return readStruct(type, col, index);
    case 12: // List
    case 16: // FixedSizeList
      return readList(type, col, data, index);
    case 17: // Map
      return readMap(type, col, data, index);
    case -1: // Dictionary
      return readDictionary(type, col, data, index);
    default:
      return col.get(index);
  }
}

function toUint8(v: any): Uint8Array | null {
  if (v == null) return null;
  if (v instanceof Uint8Array) return v;
  if (ArrayBuffer.isView(v)) {
    const view = v as ArrayBufferView;
    return new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
  }
  return v;
}

function decimalToBigInt(type: DataType, data: any, index: number): bigint {
  const byteWidth: number = (type as any).byteWidth ?? ((type as any).bitWidth ? (type as any).bitWidth / 8 : 16);
  // arrow-js stores decimals as Uint32Array; slice the element's bytes.
  const u32: Uint32Array = data.values;
  const wordsPerElem = byteWidth / 4;
  const start = index * wordsPerElem;
  const u8 = new Uint8Array(byteWidth);
  for (let w = 0; w < wordsPerElem; w++) {
    const word = u32[start + w] >>> 0;
    u8[w * 4] = word & 0xff;
    u8[w * 4 + 1] = (word >>> 8) & 0xff;
    u8[w * 4 + 2] = (word >>> 16) & 0xff;
    u8[w * 4 + 3] = (word >>> 24) & 0xff;
  }
  return decimalBytesToBigInt(u8);
}

/** Little-endian two's-complement bytes -> signed bigint. */
function decimalBytesToBigInt(u8: Uint8Array): bigint {
  let bi = 0n;
  for (let i = u8.length - 1; i >= 0; i--) bi = (bi << 8n) | BigInt(u8[i]);
  if (u8[u8.length - 1] & 0x80) bi -= 1n << BigInt(u8.length * 8);
  return bi;
}

function readStruct(type: DataType, col: any, index: number): Record<string, unknown> | null {
  const children = (type as any).children as Field[];
  const out: Record<string, unknown> = {};
  for (let c = 0; c < children.length; c++) {
    const cf = children[c];
    const childVec = typeof col.getChildAt === "function"
      ? col.getChildAt(c)
      : (col.children?.[c]);
    if (!childVec) { out[cf.name] = null; continue; }
    out[cf.name] = readCanonicalValue(cf.type as VgiDataType, childVec, index);
  }
  return out;
}

function readList(type: DataType, col: any, data: any, index: number): unknown[] {
  const childField = (type as any).children[0] as Field;
  const childVec = typeof col.getChildAt === "function" ? col.getChildAt(0) : col.children?.[0];
  const offsets = data?.valueOffsets;
  let start: number, end: number;
  if (offsets) {
    start = offsets[index];
    end = offsets[index + 1];
  } else {
    // FixedSizeList
    const listSize = (type as any).listSize ?? 0;
    start = index * listSize;
    end = start + listSize;
  }
  const out: unknown[] = [];
  for (let i = start; i < end; i++) {
    out.push(readCanonicalValue(childField.type as VgiDataType, childVec, i));
  }
  return out;
}

function readMap(type: DataType, col: any, data: any, index: number): Array<[unknown, unknown]> {
  const entriesField = (type as any).children[0] as Field;
  const keyField = (entriesField.type as any).children[0] as Field;
  const valueField = (entriesField.type as any).children[1] as Field;
  const offsets = data?.valueOffsets;
  const entriesVec = typeof col.getChildAt === "function" ? col.getChildAt(0) : col.children?.[0];
  const keyVec = entriesVec && (typeof entriesVec.getChildAt === "function" ? entriesVec.getChildAt(0) : entriesVec.children?.[0]);
  const valueVec = entriesVec && (typeof entriesVec.getChildAt === "function" ? entriesVec.getChildAt(1) : entriesVec.children?.[1]);
  const start = offsets ? offsets[index] : 0;
  const end = offsets ? offsets[index + 1] : 0;
  const out: Array<[unknown, unknown]> = [];
  for (let i = start; i < end; i++) {
    out.push([
      readCanonicalValue(keyField.type as VgiDataType, keyVec, i),
      readCanonicalValue(valueField.type as VgiDataType, valueVec, i),
    ]);
  }
  return out;
}

function readDictionary(type: DataType, col: any, data: any, index: number): unknown {
  const valueType = (type as any).dictionary as VgiDataType;
  if (data && data.dictionary && data.values) {
    const idx = Number(data.values[index]);
    const dictVec = data.dictionary;
    // Recurse so the decoded value is itself canonicalized.
    return readCanonicalValue(valueType, dictVec, idx);
  }
  // Fallback: plain get.
  return col.get(index);
}
