// Arrow utility helpers for working with Apache Arrow in TypeScript.

import {
  RecordBatch,
  Schema,
  Field,
  DataType,
  Struct,
  List,
  Binary,
  Utf8,
  makeData,
  vectorFromArray,
  RecordBatchStreamWriter,
  RecordBatchReader,
  Int64,
  Map_,
} from "apache-arrow";

/**
 * Iterate rows of a RecordBatch as plain objects.
 */
export function* iterRows(
  batch: RecordBatch
): Generator<Record<string, any>> {
  for (let i = 0; i < batch.numRows; i++) {
    const row: Record<string, any> = {};
    for (const field of batch.schema.fields) {
      const col = batch.getChild(field.name);
      row[field.name] = col ? col.get(i) : null;
    }
    yield row;
  }
}

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
  let bigVal: bigint;
  if (typeof val === "bigint") {
    bigVal = val * (10n ** BigInt(scale));
  } else {
    // Use string conversion to avoid floating-point errors
    const numStr = Number(val).toFixed(scale);
    const parts = numStr.split(".");
    const intPart = parts[0];
    const fracPart = parts[1] ?? "";
    const combined = intPart + fracPart.padEnd(scale, "0").slice(0, scale);
    bigVal = BigInt(combined);
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

/**
 * Filter a RecordBatch using a Uint8Array mask (0=exclude, nonzero=include).
 * Returns a new batch containing only the rows where mask[i] is nonzero.
 */
export function filterBatch(
  batch: RecordBatch,
  mask: Uint8Array,
): RecordBatch {
  const n = batch.numRows;

  // Count passing rows
  let passCount = 0;
  for (let i = 0; i < n; i++) {
    if (mask[i]) passCount++;
  }

  // Fast paths
  if (passCount === n) return batch;
  if (passCount === 0) return emptyBatch(batch.schema);

  // Collect passing indices
  const indices: number[] = [];
  for (let i = 0; i < n; i++) {
    if (mask[i]) indices.push(i);
  }

  // Rebuild batch with only passing rows
  const columns: Record<string, any[]> = {};
  for (const field of batch.schema.fields) {
    const col = batch.getChild(field.name)!;
    columns[field.name] = indices.map((i) => col.get(i));
  }
  return batchFromColumns(columns, batch.schema);
}

/**
 * Create empty (0-length) data for any Arrow type, handling complex types
 * that need child data (List, Map, Struct) to be serializable.
 */
function emptyData(field: Field): any {
  const type = field.type;

  if (DataType.isList(type)) {
    const childField = (type as List).children[0];
    return makeData({
      type,
      length: 0,
      nullCount: 0,
      child: emptyData(childField),
      valueOffsets: new Int32Array([0]),
    });
  }

  if (DataType.isMap(type)) {
    const entriesField = (type as Map_).children[0];
    return makeData({
      type,
      length: 0,
      nullCount: 0,
      child: emptyData(entriesField),
      valueOffsets: new Int32Array([0]),
    });
  }

  if (DataType.isStruct(type)) {
    const children = type.children.map((child: Field) => emptyData(child));
    return makeData({ type, length: 0, nullCount: 0, children });
  }

  return makeData({ type, length: 0, nullCount: 0 });
}

/**
 * Create an empty (0-row) batch with the given schema.
 */
export function emptyBatch(schema: Schema): RecordBatch {
  const children = schema.fields.map((f: Field) => emptyData(f));

  const structType = new Struct(schema.fields);
  const data = makeData({
    type: structType,
    length: 0,
    children,
    nullCount: 0,
  });

  return new RecordBatch(schema, data);
}

/**
 * Project a schema by column indices, preserving only selected fields.
 */
export function projectSchema(
  projectionIds: number[] | null,
  schema: Schema
): Schema {
  if (!projectionIds) return schema;
  // Filter to valid column indices (DuckDB may send -1 for row_id sentinel)
  const validIds = projectionIds.filter((i) => i >= 0 && i < schema.fields.length);
  // If no valid projections, return full schema so functions still produce
  // rows with data (DuckDB only needs the row count for COUNT(*) etc.)
  if (validIds.length === 0) return schema;
  return new Schema(validIds.map((i) => schema.fields[i]));
}

/**
 * Project a RecordBatch by column indices.
 */
export function projectBatch(
  projectionIds: number[] | null,
  batch: RecordBatch
): RecordBatch {
  if (!projectionIds) return batch;
  const projectedSchema = projectSchema(projectionIds, batch.schema);
  const children = projectionIds.map((i) => {
    const col = batch.getChildAt(i);
    return col!.data[0];
  });

  const structType = new Struct(projectedSchema.fields);
  const data = makeData({
    type: structType,
    length: batch.numRows,
    children,
    nullCount: 0,
  });

  return new RecordBatch(projectedSchema, data);
}

/**
 * Extract single-row batch to a scalar dict.
 */
export function batchToScalarDict(
  batch: RecordBatch | null
): Record<string, any> {
  if (!batch || batch.numRows === 0) return {};
  const result: Record<string, any> = {};
  for (const field of batch.schema.fields) {
    const col = batch.getChild(field.name);
    if (col) {
      result[field.name] = col.get(0);
    }
  }
  return result;
}

/**
 * Extract single-row batch to a secret dict (column per secret, each value is a struct).
 */
export function batchToSecretDict(
  batch: RecordBatch | null
): Record<string, Record<string, any>> {
  if (!batch || batch.numRows === 0) return {};
  const result: Record<string, Record<string, any>> = {};
  for (const field of batch.schema.fields) {
    const col = batch.getChild(field.name);
    if (col) {
      const val = col.get(0);
      if (val && typeof val === "object" && !ArrayBuffer.isView(val)) {
        // Struct scalar -> convert to plain object
        const dict: Record<string, any> = {};
        if (val.toJSON) {
          Object.assign(dict, val.toJSON());
        } else {
          Object.assign(dict, val);
        }
        result[field.name] = dict;
      } else {
        result[field.name] = {};
      }
    }
  }
  return result;
}

/**
 * Safe number coercion for BigInt values.
 */
export function safeNumber(value: any): number {
  if (typeof value === "bigint") {
    return Number(value);
  }
  return value as number;
}

/**
 * Serialize a Schema to Arrow IPC bytes.
 */
export function serializeSchema(schema: Schema): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.close();
  return writer.toUint8Array(true);
}

/**
 * Deserialize an Arrow Schema from IPC bytes.
 * Note: In Bun, reader.schema is always undefined, so we must read from the batch.
 */
export function deserializeSchema(bytes: Uint8Array): Schema {
  const reader = RecordBatchReader.from(bytes);
  const batches = [...reader];
  if (batches.length > 0) {
    return batches[0].schema;
  }
  // Schema-only stream (no batches): try reader.schema as fallback,
  // otherwise parse the IPC messages manually
  if (reader.schema) return reader.schema;
  // For schema-only streams, create a dummy batch from the writer
  // and extract the schema from it
  throw new Error("Cannot deserialize schema from empty IPC stream");
}

/**
 * Serialize a RecordBatch to Arrow IPC bytes.
 */
export function serializeBatch(batch: RecordBatch): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, batch.schema);
  // Use _writeRecordBatch to bypass schema comparison bug
  // (public write() silently drops batches on nullability mismatch)
  (writer as any)._writeRecordBatch(batch);
  writer.close();
  return writer.toUint8Array(true);
}

/**
 * Deserialize a RecordBatch from Arrow IPC bytes.
 */
export function deserializeBatch(bytes: Uint8Array): RecordBatch {
  const reader = RecordBatchReader.from(bytes);
  const batches = [...reader];
  if (batches.length === 0) {
    // In Bun, reader.schema is always undefined; fallback to empty schema
    const schema = reader.schema ?? new Schema([]);
    return emptyBatch(schema);
  }
  return batches[0];
}
