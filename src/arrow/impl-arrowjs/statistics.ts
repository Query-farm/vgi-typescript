// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// arrow-js impl of column-statistics serialization. Builds a sparse-union
// column manually with `makeData` + `typeIds` buffer + per-type children.
// This is the only spot in vgi-typescript that touches makeData directly,
// because flechette's union-column API takes a fundamentally different
// shape (typeIdForValue closure).

import {
  Field,
  Null,
  makeData,
  vectorFromArray,
  RecordBatch,
  Struct,
  SparseUnion,
  Schema,
  Utf8,
  Bool,
  Int64,
  Uint64,
  type DataType,
} from "@query-farm/apache-arrow";
import type {
  VgiBatch,
  VgiDataType,
} from "../types.js";

export interface ColumnStatistics {
  columnName: string;
  arrowType: VgiDataType;
  min: any;
  max: any;
  hasNull: boolean;
  hasNotNull: boolean;
  distinctCount: bigint | number | null;
  containsUnicode: boolean | null;
  maxStringLength: bigint | number | null;
}

function typeKey(t: VgiDataType): string {
  return (t as any).toString?.() ?? String(t.typeId);
}

function buildTypedColumnData(values: any[], type: VgiDataType): any {
  const a = type as DataType;
  if (a.typeId === 1 /* Null */) {
    return makeData({ type: a, length: values.length, nullCount: values.length });
  }
  if (a.typeId === 2 /* Int */ && (a as any).bitWidth === 64) {
    const coerced = values.map((v) =>
      v == null ? null : typeof v === "bigint" ? v : BigInt(v),
    );
    return vectorFromArray(coerced, a).data[0];
  }
  return vectorFromArray(values, a).data[0];
}

export function buildStatisticsBatch(stats: ColumnStatistics[]): VgiBatch {
  if (stats.length === 0) {
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
    } as any);
    const maxData = makeData({
      type: unionType,
      length: 0,
      typeIds: new Int8Array(0),
      children: [makeData({ type: new Null(), length: 0, nullCount: 0 })],
    } as any);
    return assemble([], minData, maxData, [], [], [], [], [], unionType) as unknown as VgiBatch;
  }

  // Group distinct Arrow types in stable insertion order to assign type codes.
  const typeOrder: VgiDataType[] = [];
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

  const unionFields: Field[] = [];
  const minChildren: any[] = [];
  const maxChildren: any[] = [];
  const typeIds: number[] = [];

  for (const [code, arrowType] of typeOrder.entries()) {
    unionFields.push(new Field(String(code), arrowType as DataType, true));
    typeIds.push(code);
    const minVals = stats.map((s, i) => (rowTypeCodes[i] === code ? s.min : null));
    const maxVals = stats.map((s, i) => (rowTypeCodes[i] === code ? s.max : null));
    minChildren.push(buildTypedColumnData(minVals, arrowType));
    maxChildren.push(buildTypedColumnData(maxVals, arrowType));
  }

  const unionType = new SparseUnion(typeIds, unionFields);
  const typeIdsBuf = Int8Array.from(rowTypeCodes);

  const minData = makeData({
    type: unionType, length: stats.length, typeIds: typeIdsBuf, children: minChildren,
  } as any);
  const maxData = makeData({
    type: unionType, length: stats.length, typeIds: typeIdsBuf, children: maxChildren,
  } as any);

  return assemble(
    stats.map((s) => s.columnName),
    minData,
    maxData,
    stats.map((s) => s.hasNull),
    stats.map((s) => s.hasNotNull),
    stats.map((s) => (s.distinctCount == null ? null : BigInt(s.distinctCount))),
    stats.map((s) => s.containsUnicode),
    stats.map((s) => (s.maxStringLength == null ? null : BigInt(s.maxStringLength))),
    unionType,
  ) as unknown as VgiBatch;
}

function assemble(
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
    buildTypedColumnData(columnNames, new Utf8() as unknown as VgiDataType),
    minData,
    maxData,
    buildTypedColumnData(hasNull, new Bool() as unknown as VgiDataType),
    buildTypedColumnData(hasNotNull, new Bool() as unknown as VgiDataType),
    buildTypedColumnData(distinctCount, new Int64() as unknown as VgiDataType),
    buildTypedColumnData(containsUnicode, new Bool() as unknown as VgiDataType),
    buildTypedColumnData(maxStringLength, new Uint64() as unknown as VgiDataType),
  ];

  const structType = new Struct(schema.fields);
  const data = makeData({ type: structType, length: n, children, nullCount: 0 });
  return new RecordBatch(schema, data);
}
