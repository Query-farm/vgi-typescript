// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Phase-3 runtime tests: branded "raw" mode round-trips and the decimal
// precision/scale parity fix. Run on BOTH backends:
//
//   bun test src/arrow/__tests__/raw-mode.test.ts
//   bun --conditions=worker test src/arrow/__tests__/raw-mode.test.ts

import { describe, test, expect } from "bun:test";
import {
  schema, field,
  int32, int64, utf8,
  decimal128,
  dateDay, dateMillisecond,
  timeSecond, timeMicrosecond,
  timestampMicros, durationMillis,
  struct, list,
  batchFromColumns, serializeBatch, deserializeBatch, iterRows,
  codecFor, backend,
} from "../index.js";
import {
  asDate32, asDate64Ms,
  asTime32S, asTime64Us,
  asTimestampMicros, asDurationMillis,
  asUnscaledDecimal, asInt64,
} from "../index.js";
import type { VgiDataType } from "../index.js";

/** Round-trip a single-column batch in RAW representation. */
function rawRoundTrip(type: VgiDataType, values: unknown[]): unknown[] {
  const sch = schema([field("c", type, true)]);
  const batch = batchFromColumns({ c: values }, sch, "raw");
  const round = deserializeBatch(serializeBatch(batch));
  return [...iterRows(round, "raw")].map((r) => r.c);
}

describe(`raw-mode round-trip (backend=${backend.name})`, () => {
  test("int32 / int64 / utf8 (raw === rich for these)", () => {
    expect(rawRoundTrip(int32(), [1, -2, 0, null])).toEqual([1, -2, 0, null]);
    expect(rawRoundTrip(int64(), [asInt64(42n), asInt64(-7n), null])).toEqual([42n, -7n, null]);
    expect(rawRoundTrip(utf8(), ["a", "", null])).toEqual(["a", "", null]);
  });

  test("date32 raw is a branded day-number (NOT a Date)", () => {
    const out = rawRoundTrip(dateDay(), [asDate32(20000), asDate32(0), null]);
    expect(out).toEqual([20000, 0, null]);
    expect(out[0]).not.toBeInstanceOf(Date);
  });

  test("date64 raw is a branded ms-bigint (NOT a Date)", () => {
    const out = rawRoundTrip(dateMillisecond(), [asDate64Ms(1745107200000n), null]);
    expect(out).toEqual([1745107200000n, null]);
    expect(out[0]).not.toBeInstanceOf(Date);
  });

  test("time32 / time64 raw", () => {
    expect(rawRoundTrip(timeSecond(), [asTime32S(3600), null])).toEqual([3600, null]);
    expect(rawRoundTrip(timeMicrosecond(), [asTime64Us(3600000000n), null])).toEqual([3600000000n, null]);
  });

  test("timestamp / duration raw", () => {
    expect(rawRoundTrip(timestampMicros(), [asTimestampMicros(1745107200000000n), null]))
      .toEqual([1745107200000000n, null]);
    expect(rawRoundTrip(durationMillis(), [asDurationMillis(5000n), null]))
      .toEqual([5000n, null]);
  });

  test("decimal raw is the branded unscaled bigint", () => {
    expect(rawRoundTrip(decimal128(38, 4), [asUnscaledDecimal(12345n), asUnscaledDecimal(-99999n), null]))
      .toEqual([12345n, -99999n, null]);
  });

  test("struct with a raw date child round-trips numeric", () => {
    const t = struct([field("a", int32(), true), field("d", dateDay(), true)]);
    const out = rawRoundTrip(t, [{ a: 1, d: asDate32(20000) }, null]);
    expect(out[0]).toEqual({ a: 1, d: 20000 });
    expect((out[0] as any).d).not.toBeInstanceOf(Date);
    expect(out[1]).toBeNull();
  });

  test("list<date32> raw round-trips numeric", () => {
    const t = list(field("item", dateDay(), true));
    const out = rawRoundTrip(t, [[asDate32(1), asDate32(2)], null]);
    expect(out[0]).toEqual([1, 2]);
    expect(out[1]).toBeNull();
  });

  test("raw constructors validate (out-of-range / non-integer throws)", () => {
    expect(() => asDate32(1.5)).toThrow();
    expect(() => asInt64(1n << 100n)).toThrow();
  });

  test("codec rawToCanonical for date32 rejects a JS Date", () => {
    // raw date32 is a day-number; a Date is a rich-only value.
    expect(() => codecFor(dateDay()).rawToCanonical(new Date())).toThrow();
  });
});

describe(`decimal precision/scale parity (backend=${backend.name})`, () => {
  test("decimal128(38, 4) reports precision 38, scale 4 on BOTH backends", () => {
    const t = decimal128(38, 4) as any;
    expect(Number(t.precision)).toBe(38);
    expect(Number(t.scale)).toBe(4);
  });

  test("decimal128(10, 2) reports precision 10, scale 2", () => {
    const t = decimal128(10, 2) as any;
    expect(Number(t.precision)).toBe(10);
    expect(Number(t.scale)).toBe(2);
  });
});
