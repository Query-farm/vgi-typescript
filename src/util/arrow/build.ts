// Build Arrow RecordBatches from row objects or column arrays. Handles
// complex types (List, Map, Struct, Decimal, BigInt-backed Timestamp/Duration)
// that vectorFromArray can't construct directly.

import {
  RecordBatch,
  Schema,
  Field,
  Struct,
  List,
  DataType,
  makeData,
  vectorFromArray,
  Map_,
} from "@query-farm/apache-arrow";
import { emptyBatch } from "./empty.js";

/**
 * Build a RecordBatch from row objects.
 */
export function batchFromRows(
  rows: Record<string, any>[],
  schema: Schema
): RecordBatch {
  if (rows.length === 0) {
    return emptyBatch(schema);
  }
  const columns: Record<string, any[]> = {};
  for (const field of schema.fields) {
    columns[field.name] = rows.map((r) => r[field.name] ?? null);
  }
  return batchFromColumns(columns, schema);
}

/**
 * Build a RecordBatch from column arrays.
 * Handles complex types (List, Map, Binary) that vectorFromArray cannot.
 */
export function batchFromColumns(
  columns: Record<string, any[]>,
  schema: Schema
): RecordBatch {
  const numRows =
    schema.fields.length > 0
      ? columns[schema.fields[0].name]?.length ?? 0
      : 0;

  const children = schema.fields.map((f: Field) => {
    const values = columns[f.name];
    if (!values) {
      return makeData({ type: f.type, length: numRows, nullCount: numRows });
    }
    return buildColumnData(values, f);
  });

  const structType = new Struct(schema.fields);
  const data = makeData({
    type: structType,
    length: numRows,
    children,
    nullCount: 0,
  });

  return new RecordBatch(schema, data);
}

/**
 * Build column data for a field, handling complex types that vectorFromArray
 * cannot handle (List(Binary), List(Struct), Map, nested lists).
 */
function buildColumnData(values: any[], field: Field): any {
  const type = field.type;

  // List types: build manually with offsets + child data
  if (DataType.isList(type)) {
    return buildListData(values, field);
  }

  // Map types: build as List of Struct{key, value}
  if (DataType.isMap(type)) {
    return buildMapData(values, field);
  }

  // Struct types: build manually to handle Arrow StructRow values
  if (DataType.isStruct(type)) {
    return buildStructData(values, field);
  }

  // Decimal types: convert JS numbers/BigInts/Uint8Arrays to raw byte buffers
  if (DataType.isDecimal(type)) {
    return buildDecimalData(values, type);
  }

  // Int64: coerce numbers to BigInt
  if (DataType.isInt(type) && (type as any).bitWidth === 64) {
    const coerced = values.map((v: any) =>
      typeof v === "number" ? BigInt(v) : v
    );
    return vectorFromArray(coerced, type).data[0];
  }

  // Timestamp/Duration with BigInt values: build manually to preserve precision
  if ((DataType.isTimestamp(type) || DataType.isDuration(type)) &&
      values.length > 0 && typeof values[0] === "bigint") {
    return buildBigIntData(values, type);
  }

  // Time/Date/Timestamp/Duration: arrow-js vectorFromArray chokes on nulls
  // (setTimeMicrosecond et al. do ToBigInt without checking validity).
  // Build manually when any value is null.
  if ((DataType.isTime(type) || DataType.isDate(type) ||
       DataType.isTimestamp(type) || DataType.isDuration(type)) &&
      values.some((v: any) => v === null || v === undefined)) {
    const bitWidth = (type as any).bitWidth ?? 64;
    if (bitWidth === 64) {
      return buildBigIntData(values, type);
    }
    // 32-bit Time/Date: build as Int32 with null bitmap
    const length = values.length;
    const buf = new Int32Array(length);
    const nullBitmap = new Uint8Array(Math.ceil(length / 8));
    let nullCount = 0;
    for (let i = 0; i < length; i++) {
      const v = values[i];
      if (v === null || v === undefined) { nullCount++; continue; }
      nullBitmap[i >> 3] |= 1 << (i & 7);
      buf[i] = typeof v === "bigint" ? Number(v) : Number(v);
    }
    return makeData({
      type, length,
      nullBitmap: nullCount > 0 ? nullBitmap : undefined,
      data: buf, nullCount,
    } as any);
  }

  // Default: use vectorFromArray
  return vectorFromArray(values, type).data[0];
}

/**
 * Build Decimal column data from JS values.
 * Handles number, BigInt, and raw Uint8Array values.
 */
function buildDecimalData(values: any[], type: DataType): any {
  const decType = type as any;
  const byteWidth: number = decType.byteWidth ?? 16;
  const scale: number = decType.scale ?? 0;
  const length = values.length;
  const buf = new Uint8Array(length * byteWidth);
  const nullBitmap = new Uint8Array(Math.ceil(length / 8));

  for (let i = 0; i < length; i++) {
    const val = values[i];
    if (val === null || val === undefined) {
      // Leave null bitmap bit as 0 (null)
      continue;
    }

    // Set validity bit
    nullBitmap[i >> 3] |= 1 << (i & 7);

    let bytes: Uint8Array;
    if (val instanceof Uint8Array) {
      bytes = val;
    } else if (ArrayBuffer.isView(val)) {
      // Handle Uint32Array and other TypedArray views (e.g., from Arrow Decimal .get())
      const view = val as ArrayBufferView;
      bytes = new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
    } else {
      bytes = numberToDecimalBytes(val, scale, byteWidth);
    }

    buf.set(bytes.subarray(0, byteWidth), i * byteWidth);
  }

  const nullCount = values.filter((v: any) => v === null || v === undefined).length;
  return makeData({
    type,
    length,
    nullBitmap: nullCount > 0 ? nullBitmap : undefined,
    data: new Uint32Array(buf.buffer, buf.byteOffset, buf.byteLength / 4),
    nullCount,
  } as any);
}

/**
 * Convert a JS number or BigInt to Decimal byte representation.
 * Result is little-endian two's complement of value * 10^scale.
 */
function numberToDecimalBytes(val: any, scale: number, byteWidth: number): Uint8Array {
  // Both BigInt and Number values are taken to be the unscaled int already.
  // This matches what Arrow's Decimal column .get() / .valueOf() exposes
  // (the unscaled integer), so a user who reads a value, does arithmetic
  // on it, and writes it back gets the right representation. The `scale`
  // parameter is unused but retained for callers that may want it later.
  void scale;
  let bigVal: bigint;
  if (typeof val === "bigint") {
    bigVal = val;
  } else {
    bigVal = BigInt(Math.trunc(Number(val)));
  }

  const bytes = new Uint8Array(byteWidth);
  let v = bigVal;
  if (v < 0n) {
    v = (1n << BigInt(byteWidth * 8)) + v;
  }
  for (let i = 0; i < byteWidth; i++) {
    bytes[i] = Number(v & 0xFFn);
    v >>= 8n;
  }
  return bytes;
}

/**
 * Build column data for types stored as Int64 (Timestamp, Duration) from BigInt values.
 * Uses BigInt64Array to preserve full 64-bit precision.
 */
function buildBigIntData(values: any[], type: DataType): any {
  const length = values.length;
  const bigBuf = new BigInt64Array(length);
  const nullBitmap = new Uint8Array(Math.ceil(length / 8));
  let nullCount = 0;

  for (let i = 0; i < length; i++) {
    const val = values[i];
    if (val === null || val === undefined) {
      nullCount++;
      continue;
    }
    nullBitmap[i >> 3] |= 1 << (i & 7);
    bigBuf[i] = typeof val === "bigint" ? val : BigInt(val);
  }

  const data = new Int32Array(bigBuf.buffer);
  return makeData({
    type,
    length,
    nullBitmap: nullCount > 0 ? nullBitmap : undefined,
    data,
    nullCount,
  } as any);
}

/**
 * Build List column data manually using makeData with offsets.
 */
function buildListData(values: any[], field: Field): any {
  const listType = field.type as List;
  const childField = listType.children[0];
  const numRows = values.length;

  // Build offsets and flatten child values
  const offsets = new Int32Array(numRows + 1);
  const allItems: any[] = [];
  const nullBitmap = new Uint8Array(Math.ceil(numRows / 8));
  let nullCount = 0;

  for (let i = 0; i < numRows; i++) {
    offsets[i] = allItems.length;
    const row = values[i];
    if (row == null) {
      nullCount++;
      // Leave bit as 0 (null)
    } else {
      // Set validity bit
      nullBitmap[i >> 3] |= 1 << (i & 7);
      const items = Array.isArray(row) ? row
        : (typeof row[Symbol.iterator] === "function") ? Array.from(row)
        : null;
      if (items) {
        for (const item of items) {
          allItems.push(item);
        }
      }
    }
  }
  offsets[numRows] = allItems.length;

  // Build child data recursively
  const childData = allItems.length > 0
    ? buildColumnData(allItems, childField)
    : makeData({ type: childField.type, length: 0, nullCount: 0 });

  return makeData({
    type: listType,
    length: numRows,
    nullCount,
    nullBitmap: nullCount > 0 ? nullBitmap : undefined,
    child: childData,
    offset: 0,
    valueOffsets: offsets,
  });
}

/**
 * Build Map column data. Arrow maps are stored as List(Struct{key, value}).
 */
function buildMapData(values: any[], field: Field): any {
  const mapType = field.type as Map_;
  const numRows = values.length;

  // Map entries field: Struct{key, value}
  const entriesField = mapType.children[0]; // The struct field
  const keyField = entriesField.type.children[0];
  const valueField = entriesField.type.children[1];

  // Build offsets and flatten all key-value pairs
  const offsets = new Int32Array(numRows + 1);
  const allKeys: any[] = [];
  const allValues: any[] = [];
  const nullBitmap = new Uint8Array(Math.ceil(numRows / 8));
  let nullCount = 0;

  for (let i = 0; i < numRows; i++) {
    offsets[i] = allKeys.length;
    const row = values[i];
    if (row == null) {
      nullCount++;
    } else {
      nullBitmap[i >> 3] |= 1 << (i & 7);
      // row can be: array of [k,v] pairs, Map, iterable, or plain object
      if (Array.isArray(row)) {
        for (const [k, v] of row) {
          allKeys.push(k);
          allValues.push(v);
        }
      } else if (typeof row[Symbol.iterator] === "function") {
        for (const [k, v] of row) {
          allKeys.push(k);
          allValues.push(v);
        }
      } else if (typeof row === "object") {
        for (const [k, v] of Object.entries(row)) {
          allKeys.push(k);
          allValues.push(v);
        }
      }
    }
  }
  offsets[numRows] = allKeys.length;

  // Build key and value child data
  const keyData = allKeys.length > 0
    ? buildColumnData(allKeys, keyField)
    : makeData({ type: keyField.type, length: 0, nullCount: 0 });
  const valueData = allValues.length > 0
    ? buildColumnData(allValues, valueField)
    : makeData({ type: valueField.type, length: 0, nullCount: 0 });

  // Build the entries struct
  const entriesData = makeData({
    type: entriesField.type,
    length: allKeys.length,
    children: [keyData, valueData],
    nullCount: 0,
  });

  return makeData({
    type: mapType,
    length: numRows,
    nullCount,
    nullBitmap: nullCount > 0 ? nullBitmap : undefined,
    child: entriesData,
    offset: 0,
    valueOffsets: offsets,
  });
}

/**
 * Build Struct column data manually.
 * Handles Arrow StructRow values by extracting child field values.
 */
function buildStructData(values: any[], field: Field): any {
  const structType = field.type as Struct;
  const childFields = structType.children;
  const numRows = values.length;
  const nullBitmap = new Uint8Array(Math.ceil(numRows / 8));
  let nullCount = 0;

  // Extract child values from each struct row
  const childArrays: any[][] = childFields.map(() => []);

  for (let i = 0; i < numRows; i++) {
    const row = values[i];
    if (row == null) {
      nullCount++;
      for (let c = 0; c < childFields.length; c++) {
        childArrays[c].push(null);
      }
    } else {
      nullBitmap[i >> 3] |= 1 << (i & 7);
      for (let c = 0; c < childFields.length; c++) {
        const childName = childFields[c].name;
        // Handle Arrow StructRow (property access) and plain objects
        const val = row[childName] ?? row[c] ?? null;
        childArrays[c].push(val);
      }
    }
  }

  // Build child data recursively
  const children = childFields.map((cf: Field, idx: number) =>
    buildColumnData(childArrays[idx], cf)
  );

  return makeData({
    type: structType,
    length: numRows,
    nullCount,
    nullBitmap: nullCount > 0 ? nullBitmap : undefined,
    children,
  });
}
