// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import { describe, expect, test } from "bun:test";
import { serializeTableCardinality } from "../cardinality.js";
import { wrapResult } from "../../handlers/shared.js";
import { TableFunctionCardinalityResultSchema } from "../../../generated/vgi-protocol-schemas.js";
import { deserializeBatch, iterRows } from "../../../arrow/index.js";
import type { TableCardinality } from "../../../types.js";

function roundTrip(card: TableCardinality) {
  const { result } = wrapResult(serializeTableCardinality(card), TableFunctionCardinalityResultSchema);
  return [...iterRows(deserializeBatch(result))][0];
}

describe("TableCardinality serialization", () => {
  test("accepts bigint (int64 args arrive as bigint)", () => {
    const count = 42n;
    expect(serializeTableCardinality({ estimate: count, max: count })).toEqual({ estimate: 42n, max: 42n });
    expect(roundTrip({ estimate: count, max: count })).toEqual({ estimate: 42n, max: 42n });
  });

  test("normalises integer numbers to bigint", () => {
    expect(serializeTableCardinality({ estimate: 7, max: null })).toEqual({ estimate: 7n, max: null });
    expect(roundTrip({ estimate: 7, max: 2 ** 60 })).toEqual({ estimate: 7n, max: 2n ** 60n });
  });

  test("mixed number/bigint and null", () => {
    expect(roundTrip({ estimate: 3, max: 9_007_199_254_740_993n })).toEqual({
      estimate: 3n,
      max: 9_007_199_254_740_993n,
    });
    expect(roundTrip({ estimate: null, max: null })).toEqual({ estimate: null, max: null });
  });

  test("rejects non-integer numbers with a field-specific error", () => {
    expect(() => serializeTableCardinality({ estimate: 2.5, max: null })).toThrow(/TableCardinality\.estimate/);
    expect(() => serializeTableCardinality({ estimate: null, max: Number.NaN })).toThrow(/TableCardinality\.max/);
  });
});
