// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Precise NOMINAL static type descriptors for Arrow types — phase 3.
//
// These interfaces are the compile-time face of the runtime `VgiDataType`
// values built by the facade factories (see ../schema-types.ts). Each carries
// a LITERAL `typeId` (and `unit` / `bitWidth` / child shape where relevant) so
// the type-level mapping in ./repr.ts can recover the exact JS value a type
// represents in `rich` vs `raw` mode.
//
// They are purely structural over `VgiDataType` (`{ readonly typeId }`): the
// runtime value is unchanged — the facade factories just cast their return to
// the matching descriptor (`new A_Int64() as unknown as Int64Type`), so both
// the arrow-js and flechette backends report IDENTICAL static types from the
// facade while keeping their native runtime values.

import type { VgiDataType, VgiField } from "../types.js";
import { TypeId } from "../predicates.js";

/** Time/timestamp/duration unit tag (matches the facade `TimeUnit` integers,
 *  but expressed as a string literal so it can flow through the type level). */
export type TUnit = "s" | "ms" | "us" | "ns";

// ---------------------------------------------------------------------------
// Scalar descriptors
// ---------------------------------------------------------------------------

export interface NullDescriptor extends VgiDataType { readonly typeId: typeof TypeId.Null; }
export interface BoolType extends VgiDataType { readonly typeId: typeof TypeId.Bool; }

// The width/sign discriminants are REQUIRED phantom properties (they don't
// exist at runtime — factories cast through `as unknown as` — but being
// required lets the conditional types in ./repr.ts discriminate int widths
// cleanly; optional phantoms collapse `IntType<64>` into the number arm).
export interface IntType<BW extends 8 | 16 | 32 | 64, Signed extends boolean>
  extends VgiDataType {
  readonly typeId: typeof TypeId.Int;
  readonly __bitWidth: BW;
  readonly __signed: Signed;
}
export type Int8Type = IntType<8, true>;
export type Int16Type = IntType<16, true>;
export type Int32Type = IntType<32, true>;
export type Int64Type = IntType<64, true>;
export type Uint8Type = IntType<8, false>;
export type Uint16Type = IntType<16, false>;
export type Uint32Type = IntType<32, false>;
export type Uint64Type = IntType<64, false>;

export interface FloatType extends VgiDataType { readonly typeId: typeof TypeId.Float; }
export interface Utf8Type extends VgiDataType { readonly typeId: typeof TypeId.Utf8; }
export interface LargeUtf8Type extends VgiDataType { readonly typeId: typeof TypeId.LargeUtf8; }
export interface BinaryType extends VgiDataType { readonly typeId: typeof TypeId.Binary; }
export interface LargeBinaryType extends VgiDataType { readonly typeId: typeof TypeId.LargeBinary; }
export interface FixedSizeBinaryType extends VgiDataType { readonly typeId: typeof TypeId.FixedSizeBinary; }

export interface DecimalType extends VgiDataType { readonly typeId: typeof TypeId.Decimal; }

// As with IntType, the unit/width discriminants below are REQUIRED phantom
// properties so the conditional value mappings in ./repr.ts can tell apart
// date32/date64, time32/time64, and the timestamp/duration units. They are
// type-only (factories cast through `as unknown as`).

/** date32 (unit DAY) — days. */
export interface Date32Type extends VgiDataType {
  readonly typeId: typeof TypeId.Date;
  readonly __dateUnit: "day";
}
/** date64 (unit MILLISECOND) — ms. */
export interface Date64Type extends VgiDataType {
  readonly typeId: typeof TypeId.Date;
  readonly __dateUnit: "ms";
}

/** time32 (bitWidth 32; unit s or ms). */
export interface Time32Type<U extends "s" | "ms"> extends VgiDataType {
  readonly typeId: typeof TypeId.Time;
  readonly __timeBits: 32;
  readonly __unit: U;
}
/** time64 (bitWidth 64; unit us or ns). */
export interface Time64Type<U extends "us" | "ns"> extends VgiDataType {
  readonly typeId: typeof TypeId.Time;
  readonly __timeBits: 64;
  readonly __unit: U;
}

export interface TimestampType<U extends TUnit> extends VgiDataType {
  readonly typeId: typeof TypeId.Timestamp;
  readonly __tsUnit: U;
}
export interface DurationType<U extends TUnit> extends VgiDataType {
  readonly typeId: typeof TypeId.Duration;
  readonly __durUnit: U;
}

export interface IntervalType extends VgiDataType { readonly typeId: typeof TypeId.Interval; }

// ---------------------------------------------------------------------------
// Composite descriptors
// ---------------------------------------------------------------------------

/** A typed field carrying its child's precise descriptor in the static type. */
export interface TypedField<Name extends string, T extends VgiDataType> extends VgiField {
  readonly name: Name;
  readonly type: T;
}

export interface ListType<T extends VgiDataType> extends VgiDataType {
  readonly typeId: typeof TypeId.List;
  readonly __child?: T;
}
export interface FixedSizeListType<T extends VgiDataType> extends VgiDataType {
  readonly typeId: typeof TypeId.FixedSizeList;
  readonly __child?: T;
}

/** Struct descriptor carrying its children as a tuple of TypedFields so the
 *  value mapping can build a precise `{ name: value }` shape. */
export interface StructType<Children extends readonly TypedField<string, VgiDataType>[]>
  extends VgiDataType {
  readonly typeId: typeof TypeId.Struct;
  readonly __children?: Children;
}

export interface MapType<K extends VgiDataType, V extends VgiDataType> extends VgiDataType {
  readonly typeId: typeof TypeId.Map;
  readonly __key?: K;
  readonly __value?: V;
}

export interface DictionaryType<V extends VgiDataType> extends VgiDataType {
  readonly typeId: typeof TypeId.Dictionary;
  readonly __dictValue?: V;
}

export interface UnionType extends VgiDataType { readonly typeId: typeof TypeId.Union; }
