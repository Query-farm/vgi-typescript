// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Factory-style constructors for arrow-js Schema/Field/DataType values.
// Mirrors flechette's API so internal code can call the same factory
// regardless of the active backend.

import {
  Schema as A_Schema,
  Field as A_Field,
  Null as A_Null,
  Bool as A_Bool,
  Int8 as A_Int8,
  Int16 as A_Int16,
  Int32 as A_Int32,
  Int64 as A_Int64,
  Uint8 as A_Uint8,
  Uint16 as A_Uint16,
  Uint32 as A_Uint32,
  Uint64 as A_Uint64,
  Float16 as A_Float16,
  Float32 as A_Float32,
  Float64 as A_Float64,
  Utf8 as A_Utf8,
  Binary as A_Binary,
  Decimal as A_Decimal,
  Date_ as A_Date,
  Time as A_Time,
  Timestamp as A_Timestamp,
  Duration as A_Duration,
  Interval as A_Interval,
  List as A_List,
  Struct as A_Struct,
  Map_ as A_Map,
  Dictionary as A_Dictionary,
  SparseUnion as A_SparseUnion,
  DenseUnion as A_DenseUnion,
  FixedSizeBinary as A_FixedSizeBinary,
  FixedSizeList as A_FixedSizeList,
  DateUnit as A_DateUnit,
  TimeUnit as A_TimeUnit,
  IntervalUnit as A_IntervalUnit,
  UnionMode as A_UnionMode,
  type DataType,
} from "@query-farm/apache-arrow";

import type { VgiField, VgiSchema, VgiDataType } from "../types.js";

// ----- TimeUnit / DateUnit / UnionMode constants ---------------------------
// Numeric values match the Arrow IPC spec; both arrow-js and flechette
// use the same integers, so internal code can freely cross backends.

export const TimeUnit = {
  SECOND: A_TimeUnit.SECOND,
  MILLISECOND: A_TimeUnit.MILLISECOND,
  MICROSECOND: A_TimeUnit.MICROSECOND,
  NANOSECOND: A_TimeUnit.NANOSECOND,
} as const;

export const DateUnit = {
  DAY: A_DateUnit.DAY,
  MILLISECOND: A_DateUnit.MILLISECOND,
} as const;

export const IntervalUnit = {
  YEAR_MONTH: A_IntervalUnit.YEAR_MONTH,
  DAY_TIME: A_IntervalUnit.DAY_TIME,
  MONTH_DAY_NANO: A_IntervalUnit.MONTH_DAY_NANO,
} as const;

export const UnionMode = {
  Sparse: A_UnionMode.Sparse,
  Dense: A_UnionMode.Dense,
} as const;

// ----- Schema / Field ------------------------------------------------------

export function schema(
  fields: readonly VgiField[],
  metadata?: Map<string, string>,
): VgiSchema {
  return new A_Schema(
    fields as A_Field[],
    metadata ?? new Map(),
  ) as unknown as VgiSchema;
}

export function field(
  name: string,
  type: VgiDataType,
  nullable = true,
  metadata?: Map<string, string>,
): VgiField {
  return new A_Field(
    name,
    type as DataType,
    nullable,
    metadata ?? new Map(),
  ) as unknown as VgiField;
}

// ----- Primitive types -----------------------------------------------------

export const nullType = (): VgiDataType => new A_Null() as unknown as VgiDataType;
export const bool = (): VgiDataType => new A_Bool() as unknown as VgiDataType;

export const int8 = (): VgiDataType => new A_Int8() as unknown as VgiDataType;
export const int16 = (): VgiDataType => new A_Int16() as unknown as VgiDataType;
export const int32 = (): VgiDataType => new A_Int32() as unknown as VgiDataType;
export const int64 = (): VgiDataType => new A_Int64() as unknown as VgiDataType;
export const uint8 = (): VgiDataType => new A_Uint8() as unknown as VgiDataType;
export const uint16 = (): VgiDataType => new A_Uint16() as unknown as VgiDataType;
export const uint32 = (): VgiDataType => new A_Uint32() as unknown as VgiDataType;
export const uint64 = (): VgiDataType => new A_Uint64() as unknown as VgiDataType;
export function int(bitWidth: 8 | 16 | 32 | 64 = 32, signed = true): VgiDataType {
  if (signed) {
    switch (bitWidth) {
      case 8: return int8();
      case 16: return int16();
      case 32: return int32();
      case 64: return int64();
    }
  } else {
    switch (bitWidth) {
      case 8: return uint8();
      case 16: return uint16();
      case 32: return uint32();
      case 64: return uint64();
    }
  }
  throw new Error(`int: unsupported bitWidth ${bitWidth}`);
}

export const float16 = (): VgiDataType => new A_Float16() as unknown as VgiDataType;
export const float32 = (): VgiDataType => new A_Float32() as unknown as VgiDataType;
export const float64 = (): VgiDataType => new A_Float64() as unknown as VgiDataType;

export const utf8 = (): VgiDataType => new A_Utf8() as unknown as VgiDataType;
export const binary = (): VgiDataType => new A_Binary() as unknown as VgiDataType;
export const fixedSizeBinary = (byteWidth: number): VgiDataType =>
  new A_FixedSizeBinary(byteWidth) as unknown as VgiDataType;

// ----- Decimal -------------------------------------------------------------

export function decimal(
  precision: number,
  scale: number,
  bitWidth: 32 | 64 | 128 | 256 = 128,
): VgiDataType {
  return new A_Decimal(precision, scale, bitWidth) as unknown as VgiDataType;
}
export const decimal128 = (precision: number, scale: number): VgiDataType =>
  decimal(precision, scale, 128);
export const decimal256 = (precision: number, scale: number): VgiDataType =>
  decimal(precision, scale, 256);

// ----- Date / Time / Timestamp / Duration / Interval -----------------------

export const date = (unit: number = A_DateUnit.MILLISECOND): VgiDataType =>
  new A_Date(unit) as unknown as VgiDataType;
export const dateDay = (): VgiDataType => date(A_DateUnit.DAY);
export const dateMillisecond = (): VgiDataType => date(A_DateUnit.MILLISECOND);

export function time(unit: number = A_TimeUnit.MILLISECOND, bitWidth?: 32 | 64): VgiDataType {
  const bw = bitWidth ?? (unit <= A_TimeUnit.MILLISECOND ? 32 : 64);
  return new A_Time(unit, bw) as unknown as VgiDataType;
}
export const timeSecond = (): VgiDataType => time(A_TimeUnit.SECOND, 32);
export const timeMillisecond = (): VgiDataType => time(A_TimeUnit.MILLISECOND, 32);
export const timeMicrosecond = (): VgiDataType => time(A_TimeUnit.MICROSECOND, 64);
export const timeNanosecond = (): VgiDataType => time(A_TimeUnit.NANOSECOND, 64);

export const timestamp = (
  unit: number = A_TimeUnit.MILLISECOND,
  timezone: string | null = null,
): VgiDataType => new A_Timestamp(unit, timezone) as unknown as VgiDataType;

export const duration = (unit: number = A_TimeUnit.MILLISECOND): VgiDataType =>
  new A_Duration(unit) as unknown as VgiDataType;

export const interval = (unit: number = A_IntervalUnit.MONTH_DAY_NANO): VgiDataType =>
  new A_Interval(unit) as unknown as VgiDataType;

// ----- Nested types --------------------------------------------------------

export function list(child: VgiField | VgiDataType): VgiDataType {
  const childField = isField(child) ? child : field("item", child as VgiDataType, true);
  return new A_List(childField as A_Field) as unknown as VgiDataType;
}

export function fixedSizeList(child: VgiField | VgiDataType, listSize: number): VgiDataType {
  const childField = isField(child) ? child : field("item", child as VgiDataType, true);
  return new A_FixedSizeList(listSize, childField as A_Field) as unknown as VgiDataType;
}

export function struct(children: readonly VgiField[]): VgiDataType {
  return new A_Struct(children as A_Field[]) as unknown as VgiDataType;
}

export function map(
  keyField: VgiField | VgiDataType,
  valueField: VgiField | VgiDataType,
  keysSorted = false,
): VgiDataType {
  const k = isField(keyField) ? keyField : field("key", keyField as VgiDataType, false);
  const v = isField(valueField) ? valueField : field("value", valueField as VgiDataType, true);
  // arrow-js Map_ takes a Field whose type is Struct{key,value}
  const entries = field("entries", struct([k, v]), false);
  return new A_Map(entries as A_Field, keysSorted) as unknown as VgiDataType;
}

export function dictionary(
  valueType: VgiDataType,
  indexType: VgiDataType = int16(),
  ordered = false,
  id?: number,
): VgiDataType {
  return new A_Dictionary(
    valueType as DataType,
    indexType as any,
    id,
    ordered,
  ) as unknown as VgiDataType;
}

export function union(
  mode: number,
  children: readonly (VgiField | VgiDataType)[],
  typeIds?: number[],
): VgiDataType {
  const childFields = children.map((c, i) =>
    isField(c) ? c : field(String(i), c as VgiDataType, true),
  );
  const ids = typeIds ?? childFields.map((_, i) => i);
  const Ctor = mode === A_UnionMode.Dense ? A_DenseUnion : A_SparseUnion;
  return new Ctor(ids, childFields as A_Field[]) as unknown as VgiDataType;
}

export const sparseUnion = (
  children: readonly (VgiField | VgiDataType)[],
  typeIds?: number[],
): VgiDataType => union(A_UnionMode.Sparse, children, typeIds);

export const denseUnion = (
  children: readonly (VgiField | VgiDataType)[],
  typeIds?: number[],
): VgiDataType => union(A_UnionMode.Dense, children, typeIds);

// Utility: distinguish Field from DataType. arrow-js Fields have `name` and
// `type`; bare DataTypes only have `typeId` (and class-specific props).
function isField(x: VgiField | VgiDataType): x is VgiField {
  return typeof (x as VgiField).name === "string" && (x as VgiField).type != null;
}
