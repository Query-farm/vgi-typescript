// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Typed facade type-factories — phase 3 (the KEY ENABLER for static typing).
//
// The per-backend `schema.ts` factories build a runtime `VgiDataType` but only
// declare their return as `VgiDataType` (`{ readonly typeId }`), which is too
// coarse for the type-level value mapping in ./codec/repr.ts. This module wraps
// each factory and casts the (unchanged) runtime value to the PRECISE nominal
// descriptor from ./codec/type-descriptors.ts — e.g. `dateDay(): Date32Type`,
// `timestamp(u): TimestampType<...>`, `struct(children)` preserving its field
// tuple. Both backends route through the same `#arrow-impl` runtime, so they
// report IDENTICAL static types from the facade.
//
// Runtime behavior is identical to the backend factories (these are pure casts
// plus, for the unit-overloaded factories, a literal-unit overload set that
// maps the integer TimeUnit to the descriptor's string-literal unit).

import * as impl from "#arrow-impl";
import type { VgiField, VgiDataType } from "./types.js";
import type {
  NullDescriptor,
  BoolType,
  Int8Type, Int16Type, Int32Type, Int64Type,
  Uint8Type, Uint16Type, Uint32Type, Uint64Type,
  IntType,
  FloatType,
  Utf8Type,
  BinaryType,
  FixedSizeBinaryType,
  DecimalType,
  Date32Type, Date64Type,
  Time32Type, Time64Type,
  TimestampType,
  DurationType,
  IntervalType,
  ListType, FixedSizeListType,
  StructType,
  MapType,
  DictionaryType,
  UnionType,
  TypedField,
  TUnit,
} from "./codec/type-descriptors.js";

// Re-export the runtime unit/mode enums and schema/field helpers untouched.
export const { TimeUnit, DateUnit, IntervalUnit, UnionMode, schema } = impl;

/** field(): preserve the precise name + type so struct() can carry a typed
 *  child tuple. Runtime is the backend `field`. */
export function field<Name extends string, T extends VgiDataType>(
  name: Name,
  type: T,
  nullable?: boolean,
  metadata?: Map<string, string>,
): TypedField<Name, T> {
  return impl.field(name, type as any, nullable, metadata) as unknown as TypedField<Name, T>;
}

// ----- Primitives ----------------------------------------------------------

export const nullType = (): NullDescriptor => impl.nullType() as unknown as NullDescriptor;
export const bool = (): BoolType => impl.bool() as unknown as BoolType;

export const int8 = (): Int8Type => impl.int8() as unknown as Int8Type;
export const int16 = (): Int16Type => impl.int16() as unknown as Int16Type;
export const int32 = (): Int32Type => impl.int32() as unknown as Int32Type;
export const int64 = (): Int64Type => impl.int64() as unknown as Int64Type;
export const uint8 = (): Uint8Type => impl.uint8() as unknown as Uint8Type;
export const uint16 = (): Uint16Type => impl.uint16() as unknown as Uint16Type;
export const uint32 = (): Uint32Type => impl.uint32() as unknown as Uint32Type;
export const uint64 = (): Uint64Type => impl.uint64() as unknown as Uint64Type;

// int(bitWidth, signed): bitWidth/signedness aren't statically known from a
// runtime `number`, so this degrades to the generic IntType (rich/raw fall back
// to number/bigint per the width literal when callers pass a literal). Prefer
// the named factories (int32(), int64(), …) for precise typing.
export function int(bitWidth?: 8 | 16 | 32 | 64, signed?: boolean): IntType<8 | 16 | 32 | 64, boolean> {
  return impl.int(bitWidth, signed) as unknown as IntType<8 | 16 | 32 | 64, boolean>;
}

export const float16 = (): FloatType => impl.float16() as unknown as FloatType;
export const float32 = (): FloatType => impl.float32() as unknown as FloatType;
export const float64 = (): FloatType => impl.float64() as unknown as FloatType;

export const utf8 = (): Utf8Type => impl.utf8() as unknown as Utf8Type;
export const binary = (): BinaryType => impl.binary() as unknown as BinaryType;
export const fixedSizeBinary = (byteWidth: number): FixedSizeBinaryType =>
  impl.fixedSizeBinary(byteWidth) as unknown as FixedSizeBinaryType;

// ----- Decimal -------------------------------------------------------------

export const decimal = (precision: number, scale: number, bitWidth?: 32 | 64 | 128 | 256): DecimalType =>
  impl.decimal(precision, scale, bitWidth) as unknown as DecimalType;
export const decimal128 = (precision: number, scale: number): DecimalType =>
  impl.decimal128(precision, scale) as unknown as DecimalType;
export const decimal256 = (precision: number, scale: number): DecimalType =>
  impl.decimal256(precision, scale) as unknown as DecimalType;

// ----- Date / Time / Timestamp / Duration / Interval -----------------------

// `date(unit)` with a runtime unit isn't statically resolvable; it widens to
// Date64Type (ms) — the default. Use dateDay()/dateMillisecond() for precision.
export const date = (unit?: number): Date64Type => impl.date(unit) as unknown as Date64Type;
export const dateDay = (): Date32Type => impl.dateDay() as unknown as Date32Type;
export const dateMillisecond = (): Date64Type => impl.dateMillisecond() as unknown as Date64Type;

export const timeSecond = (): Time32Type<"s"> => impl.timeSecond() as unknown as Time32Type<"s">;
export const timeMillisecond = (): Time32Type<"ms"> => impl.timeMillisecond() as unknown as Time32Type<"ms">;
export const timeMicrosecond = (): Time64Type<"us"> => impl.timeMicrosecond() as unknown as Time64Type<"us">;
export const timeNanosecond = (): Time64Type<"ns"> => impl.timeNanosecond() as unknown as Time64Type<"ns">;

// Generic time(unit, bitWidth): degrades to a union of the precise time types.
export function time(unit?: number, bitWidth?: 32 | 64): Time32Type<"s" | "ms"> | Time64Type<"us" | "ns"> {
  return impl.time(unit, bitWidth) as unknown as Time32Type<"s" | "ms"> | Time64Type<"us" | "ns">;
}

const UNIT_TO_TAG: Record<number, TUnit> = {
  [impl.TimeUnit.SECOND]: "s",
  [impl.TimeUnit.MILLISECOND]: "ms",
  [impl.TimeUnit.MICROSECOND]: "us",
  [impl.TimeUnit.NANOSECOND]: "ns",
};

/** timestamp(unit): when called with a literal unit constant the result type
 *  carries that unit; with a runtime number it widens to TimestampType<TUnit>. */
export function timestamp<U extends TUnit = TUnit>(
  unit?: number,
  timezone?: string | null,
): TimestampType<U> {
  return impl.timestamp(unit, timezone ?? null) as unknown as TimestampType<U>;
}

export function duration<U extends TUnit = TUnit>(unit?: number): DurationType<U> {
  return impl.duration(unit) as unknown as DurationType<U>;
}

// Unit-precise convenience factories — these carry the wire unit in the static
// type without the caller needing an explicit type argument, so raw-mode
// branded typing is automatic. (The generic timestamp()/duration() above
// can't infer the unit from a runtime `number`, so prefer these.)
export const timestampSeconds = (timezone?: string | null): TimestampType<"s"> =>
  impl.timestamp(impl.TimeUnit.SECOND, timezone ?? null) as unknown as TimestampType<"s">;
export const timestampMillis = (timezone?: string | null): TimestampType<"ms"> =>
  impl.timestamp(impl.TimeUnit.MILLISECOND, timezone ?? null) as unknown as TimestampType<"ms">;
export const timestampMicros = (timezone?: string | null): TimestampType<"us"> =>
  impl.timestamp(impl.TimeUnit.MICROSECOND, timezone ?? null) as unknown as TimestampType<"us">;
export const timestampNanos = (timezone?: string | null): TimestampType<"ns"> =>
  impl.timestamp(impl.TimeUnit.NANOSECOND, timezone ?? null) as unknown as TimestampType<"ns">;

export const durationSeconds = (): DurationType<"s"> =>
  impl.duration(impl.TimeUnit.SECOND) as unknown as DurationType<"s">;
export const durationMillis = (): DurationType<"ms"> =>
  impl.duration(impl.TimeUnit.MILLISECOND) as unknown as DurationType<"ms">;
export const durationMicros = (): DurationType<"us"> =>
  impl.duration(impl.TimeUnit.MICROSECOND) as unknown as DurationType<"us">;
export const durationNanos = (): DurationType<"ns"> =>
  impl.duration(impl.TimeUnit.NANOSECOND) as unknown as DurationType<"ns">;

export const interval = (unit?: number): IntervalType => impl.interval(unit) as unknown as IntervalType;

// ----- Nested --------------------------------------------------------------

export function list<T extends VgiDataType>(child: TypedField<string, T> | T): ListType<T> {
  return impl.list(child as any) as unknown as ListType<T>;
}

export function fixedSizeList<T extends VgiDataType>(
  child: TypedField<string, T> | T,
  listSize: number,
): FixedSizeListType<T> {
  return impl.fixedSizeList(child as any, listSize) as unknown as FixedSizeListType<T>;
}

export function struct<const C extends readonly TypedField<string, VgiDataType>[]>(
  children: C,
): StructType<C> {
  return impl.struct(children as unknown as readonly VgiField[]) as unknown as StructType<C>;
}

export function map<K extends VgiDataType, V extends VgiDataType>(
  keyField: TypedField<string, K> | K,
  valueField: TypedField<string, V> | V,
  keysSorted?: boolean,
): MapType<K, V> {
  return impl.map(keyField as any, valueField as any, keysSorted) as unknown as MapType<K, V>;
}

export function dictionary<V extends VgiDataType>(
  valueType: V,
  indexType?: VgiDataType,
  ordered?: boolean,
  id?: number,
): DictionaryType<V> {
  return impl.dictionary(valueType as any, indexType as any, ordered, id) as unknown as DictionaryType<V>;
}

export function union(
  mode: number,
  children: readonly (VgiField | VgiDataType)[],
  typeIds?: number[],
): UnionType {
  return impl.union(mode, children as any, typeIds) as unknown as UnionType;
}

export const sparseUnion = (
  children: readonly (VgiField | VgiDataType)[],
  typeIds?: number[],
): UnionType => impl.sparseUnion(children as any, typeIds) as unknown as UnionType;

export const denseUnion = (
  children: readonly (VgiField | VgiDataType)[],
  typeIds?: number[],
): UnionType => impl.denseUnion(children as any, typeIds) as unknown as UnionType;
