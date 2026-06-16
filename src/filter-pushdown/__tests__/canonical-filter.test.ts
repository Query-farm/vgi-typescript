// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Filter-pushdown comparison over temporal / decimal columns, on BOTH Arrow
// backends. Phase 2 routes both the column cells AND the filter literals through
// the canonical reader, so they compare like-for-like (timestamp -> bigint,
// decimal -> unscaled bigint) regardless of backend. Run both:
//
//   bun test src/filter-pushdown/__tests__/canonical-filter.test.ts
//   bun --conditions=worker test src/filter-pushdown/__tests__/canonical-filter.test.ts

import { describe, test, expect } from "bun:test";
import {
  schema,
  field,
  utf8,
  int32,
  timestamp,
  decimal128,
  dateDay,
  TimeUnit,
  batchFromColumns,
  iterRows,
  backend,
  type VgiDataType,
} from "../../arrow/index.js";
import { deserializeFilters } from "../deserialize.js";

/**
 * Build a filter wire batch: column 0 carries the JSON spec array (with the
 * required vgi_filter_version metadata), columns 1+ carry value-ref literals,
 * each in its own typed column so the literal is read back through the codec.
 */
function filterWireBatch(
  specs: any[],
  literals: Array<{ type: VgiDataType; value: any }>,
): ReturnType<typeof batchFromColumns> {
  const fields = [
    field("filters", utf8(), false, new Map([["vgi_filter_version", "1"]])),
    ...literals.map((l, i) => field(`v${i}`, l.type, true)),
  ];
  const columns: Record<string, any[]> = { filters: [JSON.stringify(specs)] };
  literals.forEach((l, i) => {
    columns[`v${i}`] = [l.value];
  });
  return batchFromColumns(columns, schema(fields));
}

describe(`canonical filter-pushdown (backend=${backend.name})`, () => {
  test("timestamp >= literal keeps the right rows", () => {
    const tsType = timestamp(TimeUnit.MICROSECOND);
    const dataSchema = schema([
      field("id", int32(), false),
      field("ts", tsType, true),
    ]);
    const data = batchFromColumns(
      {
        id: [1, 2, 3, 4],
        // raw micros; the boundary is exactly the literal.
        ts: [
          1_700_000_000_000_000n,
          1_700_000_000_000_001n,
          1_699_999_999_999_999n,
          null,
        ],
      },
      dataSchema,
    );

    const specs = [
      { type: "constant", column_name: "ts", column_index: 1, op: "ge", value_ref: 0 },
    ];
    const fb = filterWireBatch(specs, [{ type: tsType, value: 1_700_000_000_000_000n }]);

    const filters = deserializeFilters(fb);
    const out = filters.apply(data);
    const ids = [...iterRows(out)].map((r) => r.id);
    // rows 1 (==) and 2 (>) pass; row 3 (<) and row 4 (null) drop.
    expect(ids).toEqual([1, 2]);
  });

  test("decimal == literal compares unscaled bigints", () => {
    const decType = decimal128(38, 4);
    const dataSchema = schema([
      field("id", int32(), false),
      field("amount", decType, true),
    ]);
    const data = batchFromColumns(
      { id: [10, 20, 30], amount: [12_345n, 99_999n, 12_345n] },
      dataSchema,
    );

    const specs = [
      { type: "constant", column_name: "amount", column_index: 1, op: "eq", value_ref: 0 },
    ];
    const fb = filterWireBatch(specs, [{ type: decType, value: 12_345n }]);

    const out = deserializeFilters(fb).apply(data);
    const ids = [...iterRows(out)].map((r) => r.id);
    expect(ids).toEqual([10, 30]);
  });

  test("date32 IN (literals) over a Date column", () => {
    const dType = dateDay();
    const dataSchema = schema([
      field("id", int32(), false),
      field("d", dType, true),
    ]);
    const d0 = new Date("2020-01-01T00:00:00Z");
    const d1 = new Date("2020-06-15T00:00:00Z");
    const d2 = new Date("2021-12-31T00:00:00Z");
    const data = batchFromColumns(
      { id: [1, 2, 3], d: [d0, d1, d2] },
      dataSchema,
    );

    // IN literal is a single-element list column; the deserializer extracts it.
    const { list } = require("../../arrow/index.js");
    const inListType = list(field("item", dType, true)) as VgiDataType;
    const specs = [
      { type: "in", column_name: "d", column_index: 1, value_ref: 0 },
    ];
    // The list literal: rows d0 and d2.
    const fb = filterWireBatch(specs, [{ type: inListType, value: [d0, d2] }]);

    const out = deserializeFilters(fb).apply(data);
    const ids = [...iterRows(out)].map((r) => r.id);
    expect(ids).toEqual([1, 3]);
  });

  test("apply rebuild preserves temporal cell representation", () => {
    // No filter that drops rows -> exercises the all-pass fast path AND the
    // canonical->rich rebuild on a temporal column when a row IS dropped.
    const tsType = timestamp(TimeUnit.NANOSECOND);
    const dataSchema = schema([
      field("id", int32(), false),
      field("ts", tsType, true),
    ]);
    const data = batchFromColumns(
      { id: [1, 2], ts: [1_234_567_890_123_456_789n, 8_876_543_210_987_654_321n] },
      dataSchema,
    );
    const specs = [
      { type: "constant", column_name: "id", column_index: 0, op: "eq", value_ref: 0 },
    ];
    const fb = filterWireBatch(specs, [{ type: int32(), value: 2 }]);
    const out = deserializeFilters(fb).apply(data);
    const rows = [...iterRows(out)];
    expect(rows.length).toBe(1);
    // ns precision must survive the rebuild (no Number coercion).
    expect(rows[0].ts).toBe(8_876_543_210_987_654_321n);
  });
});
