// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Factory-style constructors for flechette Schema/Field/DataType values.
// flechette already exposes these as factory functions, so most of this
// file is direct re-exports. The few wrapper functions handle ergonomic
// gaps (e.g. allowing a bare DataType for `list(child)` rather than
// requiring a wrapped Field).

import {
  field as f_field,
  nullType as f_nullType,
  bool as f_bool,
  int as f_int,
  int8 as f_int8,
  int16 as f_int16,
  int32 as f_int32,
  int64 as f_int64,
  uint8 as f_uint8,
  uint16 as f_uint16,
  uint32 as f_uint32,
  uint64 as f_uint64,
  float as f_float,
  float16 as f_float16,
  float32 as f_float32,
  float64 as f_float64,
  utf8 as f_utf8,
  binary as f_binary,
  fixedSizeBinary as f_fixedSizeBinary,
  decimal as f_decimal,
  decimal128 as f_decimal128,
  decimal256 as f_decimal256,
  date as f_date,
  dateDay as f_dateDay,
  dateMillisecond as f_dateMillisecond,
  time as f_time,
  timeSecond as f_timeSecond,
  timeMillisecond as f_timeMillisecond,
  timeMicrosecond as f_timeMicrosecond,
  timeNanosecond as f_timeNanosecond,
  timestamp as f_timestamp,
  duration as f_duration,
  interval as f_interval,
  list as f_list,
  fixedSizeList as f_fixedSizeList,
  struct as f_struct,
  map as f_map,
  dictionary as f_dictionary,
  union as f_union,
  TimeUnit as F_TimeUnit,
  DateUnit as F_DateUnit,
  IntervalUnit as F_IntervalUnit,
  UnionMode as F_UnionMode,
} from "@query-farm/flechette";

import type { VgiField, VgiSchema, VgiDataType } from "../types.js";
import { aliasIntSigned } from "./arrowjs-shape.js";

// ----- Unit / mode constants -----------------------------------------------

export const TimeUnit = F_TimeUnit;
export const DateUnit = F_DateUnit;
export const IntervalUnit = F_IntervalUnit;
export const UnionMode = F_UnionMode;

// ----- Schema / Field ------------------------------------------------------

export function schema(
  fields: readonly VgiField[],
  metadata?: Map<string, string>,
): VgiSchema {
  return {
    fields: fields as readonly any[] as VgiField[],
    metadata: metadata ?? new Map(),
  } as VgiSchema;
}

export function field(
  name: string,
  type: VgiDataType,
  nullable = true,
  metadata?: Map<string, string>,
): VgiField {
  return f_field(
    name,
    type as any,
    nullable,
    metadata ?? new Map(),
  ) as unknown as VgiField;
}

// ----- Primitive types -----------------------------------------------------

export const nullType = (): VgiDataType => f_nullType() as unknown as VgiDataType;
export const bool = (): VgiDataType => f_bool() as unknown as VgiDataType;

// Int types carry the `isSigned` alias (arrow-js's spelling of flechette's
// `signed`) so worker code that branches on signedness reads the same property
// on both backends — see ./compat.ts.
export const int8 = (): VgiDataType => aliasIntSigned(f_int8()) as unknown as VgiDataType;
export const int16 = (): VgiDataType => aliasIntSigned(f_int16()) as unknown as VgiDataType;
export const int32 = (): VgiDataType => aliasIntSigned(f_int32()) as unknown as VgiDataType;
export const int64 = (): VgiDataType => aliasIntSigned(f_int64()) as unknown as VgiDataType;
export const uint8 = (): VgiDataType => aliasIntSigned(f_uint8()) as unknown as VgiDataType;
export const uint16 = (): VgiDataType => aliasIntSigned(f_uint16()) as unknown as VgiDataType;
export const uint32 = (): VgiDataType => aliasIntSigned(f_uint32()) as unknown as VgiDataType;
export const uint64 = (): VgiDataType => aliasIntSigned(f_uint64()) as unknown as VgiDataType;
export const int = (bitWidth: 8 | 16 | 32 | 64 = 32, signed = true): VgiDataType =>
  aliasIntSigned(f_int(bitWidth, signed)) as unknown as VgiDataType;

export const float16 = (): VgiDataType => f_float16() as unknown as VgiDataType;
export const float32 = (): VgiDataType => f_float32() as unknown as VgiDataType;
export const float64 = (): VgiDataType => f_float64() as unknown as VgiDataType;

export const utf8 = (): VgiDataType => f_utf8() as unknown as VgiDataType;
export const binary = (): VgiDataType => f_binary() as unknown as VgiDataType;
export const fixedSizeBinary = (stride: number): VgiDataType =>
  f_fixedSizeBinary(stride) as unknown as VgiDataType;

// ----- Decimal -------------------------------------------------------------

export function decimal(
  precision: number,
  scale: number,
  bitWidth: 32 | 64 | 128 | 256 = 128,
): VgiDataType {
  return f_decimal(precision, scale, bitWidth) as unknown as VgiDataType;
}
export const decimal128 = (precision: number, scale: number): VgiDataType =>
  f_decimal128(precision, scale) as unknown as VgiDataType;
export const decimal256 = (precision: number, scale: number): VgiDataType =>
  f_decimal256(precision, scale) as unknown as VgiDataType;

// ----- Date / Time / Timestamp / Duration / Interval -----------------------

export const date = (unit: number = F_DateUnit.MILLISECOND): VgiDataType =>
  f_date(unit as any) as unknown as VgiDataType;
export const dateDay = (): VgiDataType => f_dateDay() as unknown as VgiDataType;
export const dateMillisecond = (): VgiDataType => f_dateMillisecond() as unknown as VgiDataType;

export function time(unit: number = F_TimeUnit.MILLISECOND, _bitWidth?: 32 | 64): VgiDataType {
  // bitWidth is implicit in flechette (derived from unit), so we ignore _bitWidth.
  return f_time(unit as any) as unknown as VgiDataType;
}
export const timeSecond = (): VgiDataType => f_timeSecond() as unknown as VgiDataType;
export const timeMillisecond = (): VgiDataType => f_timeMillisecond() as unknown as VgiDataType;
export const timeMicrosecond = (): VgiDataType => f_timeMicrosecond() as unknown as VgiDataType;
export const timeNanosecond = (): VgiDataType => f_timeNanosecond() as unknown as VgiDataType;

export const timestamp = (
  unit: number = F_TimeUnit.MILLISECOND,
  timezone: string | null = null,
): VgiDataType => f_timestamp(unit as any, timezone) as unknown as VgiDataType;

export const duration = (unit: number = F_TimeUnit.MILLISECOND): VgiDataType =>
  f_duration(unit as any) as unknown as VgiDataType;

export const interval = (unit: number = F_IntervalUnit.MONTH_DAY_NANO): VgiDataType =>
  f_interval(unit as any) as unknown as VgiDataType;

// ----- Nested types --------------------------------------------------------

export function list(child: VgiField | VgiDataType): VgiDataType {
  return f_list(child as any) as unknown as VgiDataType;
}

export function fixedSizeList(child: VgiField | VgiDataType, listSize: number): VgiDataType {
  return f_fixedSizeList(child as any, listSize) as unknown as VgiDataType;
}

export function struct(children: readonly VgiField[]): VgiDataType {
  return f_struct(children as any) as unknown as VgiDataType;
}

export function map(
  keyField: VgiField | VgiDataType,
  valueField: VgiField | VgiDataType,
  keysSorted = false,
): VgiDataType {
  return f_map(keyField as any, valueField as any, keysSorted) as unknown as VgiDataType;
}

export function dictionary(
  valueType: VgiDataType,
  indexType: VgiDataType = int16(),
  ordered = false,
  id?: number,
): VgiDataType {
  return f_dictionary(valueType as any, indexType as any, ordered, id) as unknown as VgiDataType;
}

export function union(
  mode: number,
  children: readonly (VgiField | VgiDataType)[],
  typeIds?: number[],
): VgiDataType {
  return f_union(mode as any, children as any, typeIds) as unknown as VgiDataType;
}

export const sparseUnion = (
  children: readonly (VgiField | VgiDataType)[],
  typeIds?: number[],
): VgiDataType => union(F_UnionMode.Sparse, children, typeIds);

export const denseUnion = (
  children: readonly (VgiField | VgiDataType)[],
  typeIds?: number[],
): VgiDataType => union(F_UnionMode.Dense, children, typeIds);
