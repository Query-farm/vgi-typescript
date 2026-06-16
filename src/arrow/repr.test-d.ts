// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// COMPILE-TIME (type-only) tests for the phase-3 typed author API.
//
// This file is type-checked by `bun run build:types` / `tsc -p
// tsconfig.build.json` (it matches `src/**/*.ts` and is NOT excluded — only
// `*.test.ts` / `*.spec.ts` are). It contains no runtime assertions; the
// `@ts-expect-error` markers ARE the assertions — each must sit on a line that
// genuinely fails to type-check, or the build fails. It proves:
//   - RichValue / RawValue / ValueFor resolve to the correct JS value per type.
//   - defineScalarFunction infers compute()'s return type from `returns`+`repr`.
//   - A wrong representation in compute() is a COMPILE error.

import {
  defineScalarFunction,
} from "../functions/scalar.js";
import {
  dateDay, dateMillisecond, int64, int32, utf8, decimal128,
  timestampMicros, struct, list, field, bool, float64,
  asDate32, asDate64Ms, asTimestampMicros, asUnscaledDecimal, asInt64,
} from "./index.js";
import type {
  RichValue, RawValue, ValueFor,
  Date32Type, Date64Type, Int64Type, Int32Type, Utf8Type, BoolType,
  TimestampType, DecimalType, FloatType,
  StructType, ListType, TypedField,
  Date32, TimestampMicros, UnscaledDecimal,
} from "./index.js";

// ---------------------------------------------------------------------------
// Static equality helpers
// ---------------------------------------------------------------------------

type Expect<T extends true> = T;
type Equal<A, B> =
  (<G>() => G extends A ? 1 : 2) extends (<G>() => G extends B ? 1 : 2) ? true : false;

// ---------------------------------------------------------------------------
// RichValue / RawValue / ValueFor mapping
// ---------------------------------------------------------------------------

type _r1 = Expect<Equal<RichValue<Date32Type>, Date>>;
type _r2 = Expect<Equal<RawValue<Date32Type>, Date32>>;
type _r3 = Expect<Equal<RichValue<Date64Type>, Date>>;
type _r4 = Expect<Equal<RichValue<Int64Type>, bigint>>;
type _r5 = Expect<Equal<RichValue<Int32Type>, number>>;
type _r6 = Expect<Equal<RichValue<Utf8Type>, string>>;
type _r7 = Expect<Equal<RichValue<BoolType>, boolean>>;
type _r8 = Expect<Equal<ValueFor<TimestampType<"us">, "rich">, bigint>>;
type _r9 = Expect<Equal<ValueFor<TimestampType<"us">, "raw">, TimestampMicros>>;
type _r10 = Expect<Equal<RawValue<DecimalType>, UnscaledDecimal>>;
type _r11 = Expect<Equal<RichValue<DecimalType>, bigint>>;
type _r12 = Expect<Equal<RichValue<FloatType>, number>>;

// struct recursion: { a: number | null; d: Date | null }
type _StructT = StructType<[TypedField<"a", Int32Type>, TypedField<"d", Date32Type>]>;
type _r13 = Expect<Equal<RichValue<_StructT>, { a: number | null; d: Date | null }>>;
type _r14 = Expect<Equal<RawValue<_StructT>, { a: number | null; d: Date32 | null }>>;

// list recursion
type _ListT = ListType<Int64Type>;
type _r15 = Expect<Equal<RichValue<_ListT>, Array<bigint | null>>>;

// ---------------------------------------------------------------------------
// defineScalarFunction inference: RICH mode (default)
// ---------------------------------------------------------------------------

defineScalarFunction({
  name: "rich_date",
  params: { d: dateDay() },
  returns: dateDay(),
  // rich date32 -> Date. Correct: return Date.
  compute: () => [new Date(), null],
});

defineScalarFunction({
  name: "rich_date_wrong",
  params: { d: dateDay() },
  returns: dateDay(),
  // @ts-expect-error rich mode expects Date, not a branded raw Date32 day-number.
  compute: () => [asDate32(20000)],
});

defineScalarFunction({
  name: "rich_int64",
  params: { v: int64() },
  returns: int64(),
  compute: () => [1n, null],
});

defineScalarFunction({
  name: "rich_int64_wrong",
  params: { v: int64() },
  returns: int64(),
  // @ts-expect-error int64 output is bigint, not number.
  compute: () => [1, 2, 3],
});

defineScalarFunction({
  name: "rich_utf8",
  returns: utf8(),
  compute: () => ["a", "b", null],
});

defineScalarFunction({
  name: "rich_utf8_wrong",
  returns: utf8(),
  // @ts-expect-error utf8 output is string, not number.
  compute: () => [42],
});

// ---------------------------------------------------------------------------
// defineScalarFunction inference: RAW mode
// ---------------------------------------------------------------------------

defineScalarFunction({
  name: "raw_date",
  repr: "raw",
  params: { d: dateDay() },
  returns: dateDay(),
  // raw date32 -> branded Date32 day-number. Correct.
  compute: () => [asDate32(20000), null],
});

defineScalarFunction({
  name: "raw_date_wrong_date",
  repr: "raw",
  returns: dateDay(),
  // @ts-expect-error raw mode expects a branded Date32, not a JS Date.
  compute: () => [new Date()],
});

defineScalarFunction({
  name: "raw_date_wrong_plain_number",
  repr: "raw",
  returns: dateDay(),
  // @ts-expect-error raw date32 is a BRANDED Date32, a plain number is rejected.
  compute: () => [20000],
});

defineScalarFunction({
  name: "raw_timestamp",
  repr: "raw",
  returns: timestampMicros(),
  compute: () => [asTimestampMicros(1745107200000000n), null],
});

defineScalarFunction({
  name: "raw_timestamp_wrong",
  repr: "raw",
  returns: timestampMicros(),
  // @ts-expect-error raw timestamp[us] is a branded TimestampMicros, not a plain bigint.
  compute: () => [1745107200000000n],
});

defineScalarFunction({
  name: "raw_decimal",
  repr: "raw",
  returns: decimal128(38, 4),
  compute: () => [asUnscaledDecimal(12345n), null],
});

// raw int64 accepts a branded Int64
defineScalarFunction({
  name: "raw_int64",
  repr: "raw",
  returns: int64(),
  compute: () => [asInt64(7n), null],
});

// rich + struct return shape is checked structurally
defineScalarFunction({
  name: "rich_struct",
  returns: struct([field("a", int32()), field("d", dateDay())]),
  compute: () => [{ a: 1, d: new Date() }, null],
});

defineScalarFunction({
  name: "rich_struct_wrong",
  returns: struct([field("a", int32()), field("d", dateDay())]),
  // @ts-expect-error struct child `d` is rich Date, not a number.
  compute: () => [{ a: 1, d: 20000 }],
});

// Reference everything so unused-locals (if ever enabled) and lint stay quiet.
export type _AllChecks = [
  _r1, _r2, _r3, _r4, _r5, _r6, _r7, _r8, _r9, _r10, _r11, _r12, _r13, _r14, _r15,
];
export const _unusedFactories = { dateMillisecond, bool, float64, list, asDate64Ms };
