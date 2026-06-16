// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Branded "raw" scalar types — phase 3.
//
// The `raw` representation is a branded view layered directly on top of the
// CANONICAL value (see ./registry.ts). A branded raw value is, at runtime,
// exactly the underlying canonical primitive (a `number` or a `bigint`); the
// brand exists only at the type level so the compiler can distinguish, say, a
// `Date32` (days since epoch) from a plain `number` or a `TimestampMicros`
// (raw us) from a plain `bigint`. This carries the WIRE UNIT in the type and
// makes a wrong-unit mix-up a compile error in `raw` mode.
//
// Constructors (`asDate32`, `asTimestampMicros`, …) validate the underlying
// value by reusing the codec's validation (so a raw constructor enforces the
// same range/integrality rules as building a column) and then brand it.
// Unwrappers (`fromBranded`) strip the brand back to the plain primitive; they
// are identity at runtime.
//
// Rich mode (the phase-1 default) uses JS `Date` for date32/date64 and plain
// number/bigint everywhere else. Raw mode replaces those with the branded
// aliases below so every temporal/decimal/64-bit-int slot is unit-tagged.

import {
  int32 as int32Type,
  int64 as int64Type,
  uint64 as uint64Type,
  decimal128 as decimal128Type,
  TimeUnit,
} from "../schema-types.js";
import { codecFor } from "./registry.js";

// ---------------------------------------------------------------------------
// Brand machinery
// ---------------------------------------------------------------------------

declare const __brand: unique symbol;

/** A nominal brand over a base primitive. `Branded<number, 'date32'>` is a
 *  `number` at runtime but a distinct type at compile time. */
export type Branded<Base, Tag extends string> = Base & {
  readonly [__brand]: Tag;
};

// ---------------------------------------------------------------------------
// Branded scalar aliases (over the CANONICAL value, carrying unit info)
// ---------------------------------------------------------------------------

/** date32 raw: days since the Unix epoch. */
export type Date32 = Branded<number, "date32">;
/** date64 raw: milliseconds since the Unix epoch. */
export type Date64Ms = Branded<bigint, "date64ms">;

/** time32[s] raw: seconds since midnight. */
export type Time32S = Branded<number, "time32s">;
/** time32[ms] raw: milliseconds since midnight. */
export type Time32Ms = Branded<number, "time32ms">;
/** time64[us] raw: microseconds since midnight. */
export type Time64Us = Branded<bigint, "time64us">;
/** time64[ns] raw: nanoseconds since midnight. */
export type Time64Ns = Branded<bigint, "time64ns">;

/** timestamp[s] raw. */
export type TimestampSeconds = Branded<bigint, "ts_s">;
/** timestamp[ms] raw. */
export type TimestampMillis = Branded<bigint, "ts_ms">;
/** timestamp[us] raw. */
export type TimestampMicros = Branded<bigint, "ts_us">;
/** timestamp[ns] raw. */
export type TimestampNanos = Branded<bigint, "ts_ns">;

/** duration[s] raw. */
export type DurationSeconds = Branded<bigint, "dur_s">;
/** duration[ms] raw. */
export type DurationMillis = Branded<bigint, "dur_ms">;
/** duration[us] raw. */
export type DurationMicros = Branded<bigint, "dur_us">;
/** duration[ns] raw. */
export type DurationNanos = Branded<bigint, "dur_ns">;

/** A decimal's UNSCALED integer (the on-wire representation). */
export type UnscaledDecimal = Branded<bigint, "decimal_unscaled">;

/** int64 raw. (Rich int64 is already a plain bigint; the brand makes the
 *  declared width explicit in raw mode.) */
export type Int64 = Branded<bigint, "int64">;
/** uint64 raw. */
export type Uint64 = Branded<bigint, "uint64">;

// ---------------------------------------------------------------------------
// Validation-reusing constructors
// ---------------------------------------------------------------------------

// One codec instance per branded type, reused for validation. `richToCanonical`
// is the validating canonical-producer; for these scalar types rich===canonical
// (except date, which the raw constructor sidesteps by validating the numeric
// directly), so this is exactly the right range/integrality check.

const dec128Codec = codecFor(decimal128Type(38, 0));

/** Validate `n` against int32 integrality/range, returning it unbranded. */
function validateInt32(n: number): number {
  codecFor(int32Type()).richToCanonical(n);
  return n;
}

function validateBigInt(
  codecType: ReturnType<typeof int64Type> | ReturnType<typeof uint64Type>,
  b: bigint,
): bigint {
  codecFor(codecType).richToCanonical(b);
  return b;
}

/** date32 days: validated as a 32-bit integer day-number, then branded. */
export function asDate32(days: number): Date32 {
  if (!Number.isInteger(days)) {
    throw new TypeError(`asDate32: expected an integer day-number, got ${days}`);
  }
  return validateInt32(days) as Date32;
}

/** date64 ms: validated as a 64-bit integer, then branded. */
export function asDate64Ms(ms: bigint): Date64Ms {
  return validateBigInt(int64Type(), ms) as unknown as Date64Ms;
}

function asTime32(label: string, n: number): number {
  if (!Number.isInteger(n)) {
    throw new TypeError(`${label}: expected an integer, got ${n}`);
  }
  return validateInt32(n);
}

/** time32[s]. */
export const asTime32S = (n: number): Time32S => asTime32("asTime32S", n) as Time32S;
/** time32[ms]. */
export const asTime32Ms = (n: number): Time32Ms => asTime32("asTime32Ms", n) as Time32Ms;

/** time64[us]. */
export const asTime64Us = (b: bigint): Time64Us =>
  validateBigInt(int64Type(), b) as unknown as Time64Us;
/** time64[ns]. */
export const asTime64Ns = (b: bigint): Time64Ns =>
  validateBigInt(int64Type(), b) as unknown as Time64Ns;

/** timestamp[s]. */
export const asTimestampSeconds = (b: bigint): TimestampSeconds =>
  validateBigInt(int64Type(), b) as unknown as TimestampSeconds;
/** timestamp[ms]. */
export const asTimestampMillis = (b: bigint): TimestampMillis =>
  validateBigInt(int64Type(), b) as unknown as TimestampMillis;
/** timestamp[us]. */
export const asTimestampMicros = (b: bigint): TimestampMicros =>
  validateBigInt(int64Type(), b) as unknown as TimestampMicros;
/** timestamp[ns]. */
export const asTimestampNanos = (b: bigint): TimestampNanos =>
  validateBigInt(int64Type(), b) as unknown as TimestampNanos;

/** duration[s]. */
export const asDurationSeconds = (b: bigint): DurationSeconds =>
  validateBigInt(int64Type(), b) as unknown as DurationSeconds;
/** duration[ms]. */
export const asDurationMillis = (b: bigint): DurationMillis =>
  validateBigInt(int64Type(), b) as unknown as DurationMillis;
/** duration[us]. */
export const asDurationMicros = (b: bigint): DurationMicros =>
  validateBigInt(int64Type(), b) as unknown as DurationMicros;
/** duration[ns]. */
export const asDurationNanos = (b: bigint): DurationNanos =>
  validateBigInt(int64Type(), b) as unknown as DurationNanos;

/** A decimal's unscaled integer (validated as a bigint, then branded). */
export function asUnscaledDecimal(unscaled: bigint): UnscaledDecimal {
  dec128Codec.richToCanonical(unscaled);
  return unscaled as unknown as UnscaledDecimal;
}

/** int64 (validated for signed 64-bit range). */
export const asInt64 = (b: bigint): Int64 =>
  validateBigInt(int64Type(), b) as unknown as Int64;
/** uint64 (validated for unsigned 64-bit range). */
export const asUint64 = (b: bigint): Uint64 =>
  validateBigInt(uint64Type(), b) as unknown as Uint64;

// ---------------------------------------------------------------------------
// Unwrappers — strip the brand back to the plain primitive (identity at runtime)
// ---------------------------------------------------------------------------

/** Strip a numeric brand, returning the plain `number`. */
export function fromBrandedNumber(v: Branded<number, string>): number {
  return v as number;
}

/** Strip a bigint brand, returning the plain `bigint`. */
export function fromBrandedBigInt(v: Branded<bigint, string>): bigint {
  return v as bigint;
}

/** Generic unbrand for either base type. */
export function fromBranded<Base, Tag extends string>(v: Branded<Base, Tag>): Base {
  return v as Base;
}

// Re-export TimeUnit for callers that want to pick a timestamp/duration unit
// when constructing facade types alongside branded values.
export { TimeUnit };
