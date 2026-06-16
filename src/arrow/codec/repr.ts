// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Type-level value mapping — phase 3.
//
// Given the precise static descriptor for an Arrow type (see
// ./type-descriptors.ts), `RichValue<T>` / `RawValue<T>` / `ValueFor<T, M>`
// compute the exact JS value that type represents in the `rich` (default) and
// `raw` (branded) representations. These mirror the runtime codec rules:
//
//   rich  = canonical EXCEPT date32/date64 -> JS Date.
//   raw   = canonical, but with branded aliases that carry the wire unit.
//
// struct/list/map recurse. Corners that are genuinely ambiguous at the type
// level (union; an Int/Decimal/Timestamp built through a non-literal factory
// such as `int(bitWidth)` or `timestamp(unit)` with a runtime `number` unit)
// degrade to the canonical/`unknown` value and are noted inline.

import type { VgiDataType } from "../types.js";
import type {
  TypedField,
  BoolType,
  IntType,
  FloatType,
  Utf8Type,
  LargeUtf8Type,
  BinaryType,
  LargeBinaryType,
  FixedSizeBinaryType,
  DecimalType,
  Date32Type,
  Date64Type,
  Time32Type,
  Time64Type,
  TimestampType,
  DurationType,
  ListType,
  FixedSizeListType,
  StructType,
  MapType,
  DictionaryType,
  NullDescriptor,
  TUnit,
} from "./type-descriptors.js";
import type {
  Date32,
  Date64Ms,
  Time32S,
  Time32Ms,
  Time64Us,
  Time64Ns,
  TimestampSeconds,
  TimestampMillis,
  TimestampMicros,
  TimestampNanos,
  DurationSeconds,
  DurationMillis,
  DurationMicros,
  DurationNanos,
  UnscaledDecimal,
  Int64,
  Uint64,
} from "./branded.js";

// Re-export TypedField for callers building struct value shapes manually.
export type { TypedField };

/** Representation mode selector. */
export type Repr = "rich" | "raw";

// ---------------------------------------------------------------------------
// Per-unit branded selectors (raw mode)
// ---------------------------------------------------------------------------

type RawTimestamp<U extends TUnit> = U extends "s"
  ? TimestampSeconds
  : U extends "ms"
    ? TimestampMillis
    : U extends "us"
      ? TimestampMicros
      : U extends "ns"
        ? TimestampNanos
        : bigint;

type RawDuration<U extends TUnit> = U extends "s"
  ? DurationSeconds
  : U extends "ms"
    ? DurationMillis
    : U extends "us"
      ? DurationMicros
      : U extends "ns"
        ? DurationNanos
        : bigint;

type RawTime32<U extends "s" | "ms"> = U extends "s" ? Time32S : Time32Ms;
type RawTime64<U extends "us" | "ns"> = U extends "us" ? Time64Us : Time64Ns;

// 64-bit int brands. The descriptor's `__signed` phantom picks signed vs
// unsigned; anything else (e.g. a non-literal `int(64)`) falls back to bigint.
type RawInt64<Signed extends boolean | undefined> = Signed extends true
  ? Int64
  : Signed extends false
    ? Uint64
    : bigint;

// ---------------------------------------------------------------------------
// RichValue<T>
// ---------------------------------------------------------------------------

/** The default author-facing JS value for the Arrow type `T`. */
export type RichValue<T extends VgiDataType> =
  T extends NullDescriptor ? null :
  T extends BoolType ? boolean :
  T extends IntType<64, infer _S> ? bigint :
  T extends IntType<8 | 16 | 32, infer _S> ? number :
  T extends FloatType ? number :
  T extends Utf8Type ? string :
  T extends LargeUtf8Type ? string :
  T extends BinaryType ? Uint8Array :
  T extends LargeBinaryType ? Uint8Array :
  T extends FixedSizeBinaryType ? Uint8Array :
  T extends DecimalType ? bigint :
  // date32 / date64 -> JS Date (the ONLY rich != canonical case)
  T extends Date32Type ? Date :
  T extends Date64Type ? Date :
  T extends Time32Type<"s" | "ms"> ? number :
  T extends Time64Type<"us" | "ns"> ? bigint :
  T extends TimestampType<TUnit> ? bigint :
  T extends DurationType<TUnit> ? bigint :
  T extends StructType<infer C> ? RichStruct<C> :
  T extends ListType<infer E> ? Array<RichValue<E> | null> :
  T extends FixedSizeListType<infer E> ? Array<RichValue<E> | null> :
  T extends MapType<infer K, infer V> ? Array<[RichValue<K>, RichValue<V> | null]> :
  T extends DictionaryType<infer V> ? RichValue<V> :
  unknown;

type RichStruct<C extends readonly TypedField<string, VgiDataType>[]> = {
  [F in C[number] as F["name"]]: RichValue<F["type"]> | null;
};

// ---------------------------------------------------------------------------
// RawValue<T>
// ---------------------------------------------------------------------------

/** The branded `raw` JS value for the Arrow type `T`. */
export type RawValue<T extends VgiDataType> =
  T extends NullDescriptor ? null :
  T extends BoolType ? boolean :
  T extends IntType<64, infer S> ? RawInt64<S> :
  T extends IntType<8 | 16 | 32, infer _S> ? number :
  T extends FloatType ? number :
  T extends Utf8Type ? string :
  T extends LargeUtf8Type ? string :
  T extends BinaryType ? Uint8Array :
  T extends LargeBinaryType ? Uint8Array :
  T extends FixedSizeBinaryType ? Uint8Array :
  T extends DecimalType ? UnscaledDecimal :
  T extends Date32Type ? Date32 :
  T extends Date64Type ? Date64Ms :
  T extends Time32Type<infer U> ? RawTime32<U> :
  T extends Time64Type<infer U> ? RawTime64<U> :
  T extends TimestampType<infer U> ? RawTimestamp<U> :
  T extends DurationType<infer U> ? RawDuration<U> :
  T extends StructType<infer C> ? RawStruct<C> :
  T extends ListType<infer E> ? Array<RawValue<E> | null> :
  T extends FixedSizeListType<infer E> ? Array<RawValue<E> | null> :
  T extends MapType<infer K, infer V> ? Array<[RawValue<K>, RawValue<V> | null]> :
  T extends DictionaryType<infer V> ? RawValue<V> :
  unknown;

type RawStruct<C extends readonly TypedField<string, VgiDataType>[]> = {
  [F in C[number] as F["name"]]: RawValue<F["type"]> | null;
};

// ---------------------------------------------------------------------------
// ValueFor<T, M>
// ---------------------------------------------------------------------------

/** The JS value for the Arrow type `T` under representation mode `M`. */
export type ValueFor<T extends VgiDataType, M extends Repr> = M extends "raw"
  ? RawValue<T>
  : RichValue<T>;
