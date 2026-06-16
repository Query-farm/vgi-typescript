// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Canonical-codec round-trip tests for the Arrow facade.
//
// Asserts the rich contract is SYMMETRIC and IDENTICAL across both backends:
// for every Arrow type, building a batch from rich values, IPC round-tripping
// it, then reading it back via iterRows must reproduce the original rich values
// (build(read(x)) === x). Run both:
//
//   bun test src/arrow/__tests__/codec.test.ts
//   bun --conditions=worker test src/arrow/__tests__/codec.test.ts
//
// Also asserts the codec THROWS on invalid / lossy inputs.

import { describe, test, expect } from "bun:test";
import {
  schema, field,
  bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64,
  float32, float64, utf8, binary, fixedSizeBinary,
  decimal128, decimal256,
  dateDay, dateMillisecond,
  timeSecond, timeMillisecond, timeMicrosecond, timeNanosecond,
  timestamp, duration, TimeUnit,
  list, struct, map, dictionary,
  batchFromColumns, serializeBatch, deserializeBatch, iterRows,
  backend,
} from "../index.js";
import { codecFor } from "../codec/registry.js";
import type { VgiDataType } from "../index.js";

/** Build a single-column batch from rich values, IPC round-trip, read back. */
function roundTrip(type: VgiDataType, values: unknown[]): unknown[] {
  const sch = schema([field("c", type, true)]);
  const batch = batchFromColumns({ c: values }, sch);
  const round = deserializeBatch(serializeBatch(batch));
  return [...iterRows(round)].map((r) => r.c);
}

/** Deep-equality that treats bigint/Date/Uint8Array correctly. */
function eq(a: any, b: any): boolean {
  if (a === b) return true;
  if (a == null || b == null) return a === b;
  if (a instanceof Date && b instanceof Date) return a.getTime() === b.getTime();
  if (a instanceof Uint8Array && b instanceof Uint8Array) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
    return true;
  }
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    return a.every((v, i) => eq(v, b[i]));
  }
  if (typeof a === "object" && typeof b === "object") {
    const ak = Object.keys(a), bk = Object.keys(b);
    if (ak.length !== bk.length) return false;
    return ak.every((k) => eq(a[k], b[k]));
  }
  return false;
}

function expectRoundTrip(name: string, type: VgiDataType, values: unknown[]) {
  const out = roundTrip(type, values);
  for (let i = 0; i < values.length; i++) {
    if (!eq(values[i], out[i])) {
      throw new Error(
        `${name}[${i}] mismatch: in=${fmt(values[i])} out=${fmt(out[i])}`,
      );
    }
  }
}

function fmt(v: any): string {
  if (typeof v === "bigint") return v + "n";
  if (v instanceof Date) return v.toISOString();
  if (v instanceof Uint8Array) return `[${Array.from(v).join(",")}]`;
  return JSON.stringify(v);
}

describe(`codec round-trip (backend=${backend.name})`, () => {
  test("bool", () => expectRoundTrip("bool", bool(), [true, false, null]));

  test("integers (number-backed)", () => {
    expectRoundTrip("int8", int8(), [-128, 0, 127, null]);
    expectRoundTrip("int16", int16(), [-32768, 0, 32767, null]);
    expectRoundTrip("int32", int32(), [-2147483648, 0, 2147483647, null]);
    expectRoundTrip("uint8", uint8(), [0, 255, null]);
    expectRoundTrip("uint16", uint16(), [0, 65535, null]);
    expectRoundTrip("uint32", uint32(), [0, 4294967295, null]);
  });

  test("integers (bigint-backed)", () => {
    expectRoundTrip("int64", int64(), [-9223372036854775808n, 0n, 9223372036854775807n, null]);
    expectRoundTrip("uint64", uint64(), [0n, 18446744073709551615n, null]);
  });

  test("floats", () => {
    expectRoundTrip("float32", float32(), [1.5, -2.5, 0, null]);
    expectRoundTrip("float64", float64(), [3.14159, -2.71828, 0, null]);
  });

  test("utf8 / binary / fixedSizeBinary", () => {
    expectRoundTrip("utf8", utf8(), ["hello", "", "héllo", null]);
    expectRoundTrip("binary", binary(), [new Uint8Array([1, 2, 3]), new Uint8Array([]), null]);
    expectRoundTrip("fsb", fixedSizeBinary(3), [new Uint8Array([9, 8, 7]), null]);
  });

  test("date32 -> Date (incl. far-past / far-future)", () => {
    expectRoundTrip("date32", dateDay(), [
      new Date(Date.UTC(2025, 3, 20)),
      new Date(Date.UTC(1970, 0, 1)),
      new Date(Date.UTC(1900, 0, 1)),
      new Date(Date.UTC(2200, 11, 31)),
      null,
    ]);
  });

  test("date64 -> Date (incl. far-past / far-future)", () => {
    expectRoundTrip("date64", dateMillisecond(), [
      new Date(Date.UTC(2025, 3, 20, 12, 34, 56)),
      new Date(Date.UTC(1970, 0, 1)),
      new Date(Date.UTC(1850, 5, 15)),
      new Date(Date.UTC(2250, 0, 1)),
      null,
    ]);
  });

  test("time32 -> number (s, ms)", () => {
    expectRoundTrip("time32(s)", timeSecond(), [0, 3600, 86399, null]);
    expectRoundTrip("time32(ms)", timeMillisecond(), [0, 3600000, 86399999, null]);
  });

  test("time64 -> bigint (us, ns)", () => {
    expectRoundTrip("time64(us)", timeMicrosecond(), [0n, 3600000000n, 86399999999n, null]);
    expectRoundTrip("time64(ns)", timeNanosecond(), [0n, 3600000000000n, 86399999999999n, null]);
  });

  test("timestamp -> bigint (all units, incl. large)", () => {
    expectRoundTrip("ts(s)", timestamp(TimeUnit.SECOND), [0n, 1745107200n, null]);
    expectRoundTrip("ts(ms)", timestamp(TimeUnit.MILLISECOND), [0n, 1745107200000n, null]);
    expectRoundTrip("ts(us)", timestamp(TimeUnit.MICROSECOND), [0n, 1745107200000000n, -62135596800000000n, null]);
    expectRoundTrip("ts(ns)", timestamp(TimeUnit.NANOSECOND), [0n, 1745107200000000000n, null]);
  });

  test("duration -> bigint", () => {
    expectRoundTrip("dur(ms)", duration(TimeUnit.MILLISECOND), [0n, 5000n, -1234n, null]);
    expectRoundTrip("dur(ns)", duration(TimeUnit.NANOSECOND), [0n, 9876543210n, null]);
  });

  test("decimal -> unscaled bigint (with scale on type)", () => {
    expectRoundTrip("decimal128", decimal128(38, 4), [12345n, 0n, -99999n, null]);
    expectRoundTrip("decimal256", decimal256(50, 6), [123456789012345678901234567890n, -1n, null]);
  });

  test("struct -> plain object", () => {
    const t = struct([field("a", int32(), true), field("b", utf8(), true), field("d", dateDay(), true)]);
    expectRoundTrip("struct", t, [
      { a: 1, b: "x", d: new Date(Date.UTC(2025, 0, 1)) },
      { a: 2, b: "y", d: new Date(Date.UTC(1999, 11, 31)) },
      null,
    ]);
  });

  test("list -> array (incl. empty, null, dates)", () => {
    expectRoundTrip("list<int32>", list(field("item", int32(), true)), [[1, 2, 3], [], null, [42]]);
    expectRoundTrip("list<date32>", list(field("item", dateDay(), true)), [
      [new Date(Date.UTC(2025, 0, 1)), new Date(Date.UTC(2025, 0, 2))],
      null,
    ]);
  });

  test("map -> Array<[k,v]>", () => {
    const t = map(field("key", utf8(), false), field("value", int32(), true));
    expectRoundTrip("map", t, [[["k1", 1], ["k2", 2]], [], null]);
  });

  test("dictionary<utf8> -> string", () => {
    expectRoundTrip("dict", dictionary(utf8(), int32()), ["red", "green", "red", null, "blue"]);
  });

  test("nested: list<struct{date,ts}>", () => {
    const t = list(field("item", struct([
      field("d", dateDay(), true),
      field("ts", timestamp(TimeUnit.MICROSECOND), true),
    ]), true));
    expectRoundTrip("list<struct>", t, [
      [{ d: new Date(Date.UTC(2025, 3, 20)), ts: 1745107200000000n }],
      null,
      [],
    ]);
  });
});

describe(`codec validation throws (backend=${backend.name})`, () => {
  test("int32 rejects non-integer number", () => {
    expect(() => codecFor(int32()).richToCanonical(1.5)).toThrow();
  });
  test("int8 rejects out-of-range", () => {
    expect(() => codecFor(int8()).richToCanonical(999)).toThrow();
  });
  test("int64 rejects out-of-range bigint", () => {
    expect(() => codecFor(int64()).richToCanonical(1n << 100n)).toThrow();
  });
  test("date64 canonicalToRich rejects bigint that overflows safe-number", () => {
    expect(() => codecFor(dateMillisecond()).canonicalToRich(1n << 80n)).toThrow();
  });
  test("date32 rejects an invalid Date", () => {
    expect(() => codecFor(dateDay()).richToCanonical(new Date(NaN))).toThrow();
  });
  test("utf8 read-side rejects a non-string canonical", () => {
    // Write side stringifies foreign scalars (legacy behavior); the read side
    // (canonical -> rich) validates that storage produced a real string.
    expect(() => codecFor(utf8()).canonicalToRich(42 as any)).toThrow();
    expect(codecFor(utf8()).richToCanonical(42)).toBe("42");
  });
  test("bool rejects a string", () => {
    expect(() => codecFor(bool()).richToCanonical("true")).toThrow();
  });
  test("timestamp rejects a non-integer number", () => {
    expect(() => codecFor(timestamp(TimeUnit.MICROSECOND)).richToCanonical(1.25)).toThrow();
  });
  test("nulls pass through (no throw)", () => {
    expect(codecFor(int32()).richToCanonical(null)).toBe(null);
    expect(codecFor(dateDay()).canonicalToRich(null)).toBe(null);
    expect(codecFor(struct([field("a", int32(), true)])).richToCanonical(null)).toBe(null);
  });
});
