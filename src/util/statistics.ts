// Column statistics: types and sparse-union wire serialization.
// Mirrors vgi-python's serialize_column_statistics in catalog_interface.py.
//
// The wire format is a single RecordBatch with schema
//   column_name: utf8
//   min: sparse_union<T0, T1, ...>
//   max: sparse_union<T0, T1, ...>
//   has_null: bool
//   has_not_null: bool
//   distinct_count: int64
//   contains_unicode: bool
//   max_string_length: uint64
// where Ti are the distinct Arrow types present among the stats' min/max values.
// A row's type_id picks which union child holds its real min/max; the other
// children carry null at that slot (sparse-union invariant).

import {
  Schema,
  Field,
  RecordBatch,
  Struct,
  SparseUnion,
  Binary,
  Int8,
  DataType,
  Utf8,
  Bool,
  Int64,
  Float64,
  Uint64,
  Null,
  makeData,
  vectorFromArray,
  RecordBatchStreamWriter,
} from "@query-farm/apache-arrow";

/**
 * Statistics for a single output column of a table function (or catalog
 * table). The `arrowType` field must match the Arrow type of the column it
 * describes — it is what the serializer uses to build the sparse-union child.
 */
export interface ColumnStatistics {
  columnName: string;
  arrowType: DataType;
  /** Must be a JS value assignable to `arrowType` via vectorFromArray, or null. */
  min: any;
  max: any;
  hasNull: boolean;
  hasNotNull: boolean;
  distinctCount: bigint | number | null;
  /** String/binary columns only; null for other types. */
  containsUnicode: boolean | null;
  /** String/binary columns only; null for other types. */
  maxStringLength: bigint | number | null;
}

// Stable key for a DataType so we can group stats by type. Uses toString()
// which renders with precision/scale/timezone/etc., so semantically-equal
// types collide in the map as expected.
function typeKey(t: DataType): string {
  return t.toString();
}

/**
 * Serialize column statistics to IPC bytes per vgi-python's wire format.
 * Returns an empty-stats batch when `stats` is empty (matching Python).
 */
export function serializeColumnStatistics(
  stats: ColumnStatistics[],
  cacheMaxAgeSeconds?: number | null,
): Uint8Array {
  const batch = buildStatsBatch(stats);
  const writer = RecordBatchStreamWriter.writeAll([batch]);
  return writer.toUint8Array(true);
}

function buildStatsBatch(stats: ColumnStatistics[]): RecordBatch {
  const n = stats.length;

  if (n === 0) {
    // Minimal empty batch: sparse_union with a single null child. Matches
    // Python's empty serialization — DuckDB tolerates this and reads zero
    // rows of stats.
    const nullField = new Field("0", new Null(), true);
    const unionType = new SparseUnion([0], [nullField]);
    const minData = makeData({
      type: unionType,
      length: 0,
      typeIds: new Int8Array(0),
      children: [makeData({ type: new Null(), length: 0, nullCount: 0 })],
    });
    const maxData = makeData({
      type: unionType,
      length: 0,
      typeIds: new Int8Array(0),
      children: [makeData({ type: new Null(), length: 0, nullCount: 0 })],
    });
    return assembleBatch(
      [],
      minData,
      maxData,
      [],
      [],
      [],
      [],
      [],
      unionType,
    );
  }

  // Group distinct Arrow types in stable insertion order to assign type codes.
  const typeOrder: DataType[] = [];
  const typeIdMap = new Map<string, number>();
  const rowTypeCodes: number[] = [];
  for (const s of stats) {
    const key = typeKey(s.arrowType);
    let code = typeIdMap.get(key);
    if (code === undefined) {
      code = typeOrder.length;
      typeIdMap.set(key, code);
      typeOrder.push(s.arrowType);
    }
    rowTypeCodes.push(code);
  }

  // Build sparse-union children: one per distinct type, each of length N.
  // Slot i is the real min/max when rowTypeCodes[i] matches this child's code,
  // null otherwise (sparse-union invariant — all children share the same
  // length, and only the child at this row's type_id holds a meaningful value).
  const unionFields: Field[] = [];
  const minChildren: any[] = [];
  const maxChildren: any[] = [];
  const typeIds: number[] = [];

  for (const [code, arrowType] of typeOrder.entries()) {
    unionFields.push(new Field(String(code), arrowType, true));
    typeIds.push(code);

    const minVals = stats.map((s, i) => (rowTypeCodes[i] === code ? s.min : null));
    const maxVals = stats.map((s, i) => (rowTypeCodes[i] === code ? s.max : null));
    minChildren.push(buildTypedColumnData(minVals, arrowType));
    maxChildren.push(buildTypedColumnData(maxVals, arrowType));
  }

  const unionType = new SparseUnion(typeIds, unionFields);
  const typeIdsBuf = Int8Array.from(rowTypeCodes);

  const minData = makeData({
    type: unionType,
    length: n,
    typeIds: typeIdsBuf,
    children: minChildren,
  });
  const maxData = makeData({
    type: unionType,
    length: n,
    typeIds: typeIdsBuf,
    children: maxChildren,
  });

  return assembleBatch(
    stats.map((s) => s.columnName),
    minData,
    maxData,
    stats.map((s) => s.hasNull),
    stats.map((s) => s.hasNotNull),
    stats.map((s) => (s.distinctCount == null ? null : BigInt(s.distinctCount))),
    stats.map((s) => s.containsUnicode),
    stats.map((s) => (s.maxStringLength == null ? null : BigInt(s.maxStringLength))),
    unionType,
  );
}

// Assemble the final RecordBatch with the fixed outer schema.
function assembleBatch(
  columnNames: string[],
  minData: any,
  maxData: any,
  hasNull: boolean[],
  hasNotNull: boolean[],
  distinctCount: (bigint | null)[],
  containsUnicode: (boolean | null)[],
  maxStringLength: (bigint | null)[],
  unionType: SparseUnion,
): RecordBatch {
  const schema = new Schema([
    new Field("column_name", new Utf8(), false),
    new Field("min", unionType, true),
    new Field("max", unionType, true),
    new Field("has_null", new Bool(), false),
    new Field("has_not_null", new Bool(), false),
    new Field("distinct_count", new Int64(), true),
    new Field("contains_unicode", new Bool(), true),
    new Field("max_string_length", new Uint64(), true),
  ]);

  const n = columnNames.length;
  const children = [
    buildTypedColumnData(columnNames, new Utf8()),
    minData,
    maxData,
    buildTypedColumnData(hasNull, new Bool()),
    buildTypedColumnData(hasNotNull, new Bool()),
    buildTypedColumnData(distinctCount, new Int64()),
    buildTypedColumnData(containsUnicode, new Bool()),
    buildTypedColumnData(maxStringLength, new Uint64()),
  ];

  const structType = new Struct(schema.fields);
  const data = makeData({ type: structType, length: n, children, nullCount: 0 });
  return new RecordBatch(schema, data);
}

// vectorFromArray handles most types. Null arrays need a dedicated path since
// vectorFromArray can't build them from a plain JS array.
function buildTypedColumnData(values: any[], type: DataType): any {
  if (DataType.isNull(type)) {
    return makeData({ type, length: values.length, nullCount: values.length });
  }
  // Int64/Uint64 with null slots: coerce numbers to BigInt, keep null.
  if (DataType.isInt(type) && (type as any).bitWidth === 64) {
    const coerced = values.map((v) =>
      v == null ? null : typeof v === "bigint" ? v : BigInt(v),
    );
    return vectorFromArray(coerced, type).data[0];
  }
  return vectorFromArray(values, type).data[0];
}
