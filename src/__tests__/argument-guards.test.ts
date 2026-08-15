// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Guards on the bind-time argument path: type-factory rejection and the
// bigint→number narrowing that used to be silently lossy.

import { test, expect, describe } from "bun:test";
import {
  assertArrowType,
  assertArrowTypes,
  narrowArgValue,
} from "../arguments/argument-spec.js";
import { safeNumber } from "../util/arrow/index.js";
import {
  defineScalarFunction,
  defineTableFunction,
  int64,
  utf8,
  toSchema,
} from "../index.js";

describe("assertArrowType", () => {
  test("rejects an uncalled type factory and names the fix", () => {
    expect(() => assertArrowType(int64, "params.n")).toThrow(/params\.n is a type factory/);
    expect(() => assertArrowType(int64, "params.n")).toThrow(/call it: int64\(\)/);
  });

  test("explains the per-entry-point difference", () => {
    // The message has to teach this: the same identifier is a ready-made type
    // on @query-farm/vgi and a factory on @query-farm/vgi/worker-cf, so a
    // correct declaration becomes a wrong one on a changed import line.
    expect(() => assertArrowType(int64, "params.n")).toThrow(/worker-cf/);
  });

  test("rejects null and undefined", () => {
    expect(() => assertArrowType(null, "returns")).toThrow(/returns is null/);
    expect(() => assertArrowType(undefined, "returns")).toThrow(/returns is undefined/);
  });

  test("accepts a real Arrow type", () => {
    expect(() => assertArrowType(int64(), "params.n")).not.toThrow();
    expect(() => assertArrowTypes({ a: int64(), b: utf8() }, "params")).not.toThrow();
  });

  test("assertArrowTypes tolerates an absent record", () => {
    expect(() => assertArrowTypes(undefined, "params")).not.toThrow();
  });
});

describe("define* reject uncalled factories", () => {
  test("defineScalarFunction params", () => {
    expect(() =>
      defineScalarFunction({
        name: "double",
        params: { n: int64 as any },
        returns: int64(),
        compute: () => [],
      }),
    ).toThrow(/defineScalarFunction\("double"\): params\.n/);
  });

  test("defineScalarFunction returns", () => {
    expect(() =>
      defineScalarFunction({
        name: "double",
        params: { n: int64() },
        returns: int64 as any,
        compute: () => [],
      }),
    ).toThrow(/defineScalarFunction\("double"\): returns/);
  });

  test("defineTableFunction args", () => {
    expect(() =>
      defineTableFunction({
        name: "series",
        args: { count: int64 as any },
        onBind: () => ({ outputSchema: toSchema({ n: int64() }) }),
        process: () => {},
      }),
    ).toThrow(/defineTableFunction\("series"\): args\.count/);
  });

  test("the correct form still builds", () => {
    const f = defineScalarFunction({
      name: "double",
      params: { n: int64() },
      returns: int64(),
      compute: () => [],
    });
    expect(f.argumentSpecs?.length).toBe(1);
  });
});

describe("narrowArgValue", () => {
  test("narrows a representable bigint to number", () => {
    expect(narrowArgValue(42n)).toBe(42);
    expect(narrowArgValue(0n)).toBe(0);
    expect(narrowArgValue(-7n)).toBe(-7);
    expect(narrowArgValue(BigInt(Number.MAX_SAFE_INTEGER))).toBe(Number.MAX_SAFE_INTEGER);
  });

  test("keeps the bigint when narrowing would lose precision", () => {
    // The regression this exists for: a bare Number() here rounded INT64_MAX to
    // 9223372036854775808, silently, and nothing downstream could tell.
    const intMax = 9223372036854775807n;
    expect(narrowArgValue(intMax)).toBe(intMax);
    expect(narrowArgValue(-9223372036854775808n)).toBe(-9223372036854775808n);
    expect(narrowArgValue(BigInt(Number.MAX_SAFE_INTEGER) + 2n)).toBe(
      BigInt(Number.MAX_SAFE_INTEGER) + 2n,
    );
  });

  test("does NOT throw — extractArgs resolves varargs specs the function reads raw", () => {
    // constant_columns(2, 9223372036854775807) is exactly this shape: the spec
    // is resolved eagerly, the value is never read off `args`, and a throw here
    // would fail a query that works.
    expect(() => narrowArgValue(9223372036854775807n)).not.toThrow();
  });

  test("passes non-bigint values through untouched", () => {
    expect(narrowArgValue("x")).toBe("x");
    expect(narrowArgValue(1.5)).toBe(1.5);
    expect(narrowArgValue(null)).toBe(null);
    expect(narrowArgValue(undefined)).toBe(undefined);
  });
});

describe("safeNumber", () => {
  test("narrows what it can", () => {
    expect(safeNumber(42n)).toBe(42);
    expect(safeNumber(BigInt(Number.MAX_SAFE_INTEGER))).toBe(Number.MAX_SAFE_INTEGER);
    expect(safeNumber(3.5)).toBe(3.5);
  });

  test("refuses a lossy narrowing rather than rounding", () => {
    expect(() => safeNumber(BigInt(Number.MAX_SAFE_INTEGER) + 2n)).toThrow(RangeError);
    expect(() => safeNumber(-(BigInt(Number.MAX_SAFE_INTEGER) + 2n))).toThrow(RangeError);
  });
});
