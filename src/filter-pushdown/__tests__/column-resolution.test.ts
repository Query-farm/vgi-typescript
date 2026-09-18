// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Filter evaluation resolves each referenced column once per batch, not once
// per row. Resolving per row -- a field search plus getChildAt, which builds a
// fresh Vector on arrow-js -- cost more than the comparisons themselves
// (~0.3 ms per 1000-row batch per column, a third of positional_args.test's
// filtered scan). Run on both backends:
//
//   bun test src/filter-pushdown/__tests__/column-resolution.test.ts
//   bun --conditions=worker test src/filter-pushdown/__tests__/column-resolution.test.ts

import { describe, expect, spyOn, test } from "bun:test";
import { backend, batchFromColumns, field, int64, iterRows, schema, utf8 } from "../../arrow/index.js";
import { deserializeFilters } from "../deserialize.js";

const METADATA = new Map([
  ["vgi_filter_encoding", "vgi.filters.v2"],
  ["vgi_filter_version", "2"],
  ["vgi_evaluation_context", "vgi.none.v1"],
]);

const OUTPUT = schema([field("n", int64(), true), field("label", utf8(), true)]);

function snapshot(): ReturnType<typeof batchFromColumns> {
  const document = {
    encoding: "vgi.filters.v2",
    semantics: "vgi.duckdb.standard.v1",
    kind: "snapshot",
    predicates: [
      {
        id: "q0",
        revision: 0,
        mode: "required",
        source: "query",
        expression: {
          node: "and",
          children: [
            { node: "comparison", op: "ge", left: { node: "column_ref", column_index: 0, column_name: "n" }, right: { node: "literal", value_ref: 0 } },
            { node: "comparison", op: "lt", left: { node: "column_ref", column_index: 0, column_name: "n" }, right: { node: "literal", value_ref: 1 } },
          ],
        },
      },
    ],
  };
  return batchFromColumns(
    { filter_spec: [JSON.stringify(document)], value_0: [10n], value_1: [20n] },
    schema([field("filter_spec", utf8(), false), field("value_0", int64(), true), field("value_1", int64(), true)], METADATA),
  );
}

describe(`filter evaluation resolves columns once per batch (backend=${backend.name})`, () => {
  test("a 1000-row batch reads its referenced column once, and filters exactly", () => {
    const filters = deserializeFilters(snapshot(), { outputSchema: OUTPUT });
    const n = Array.from({ length: 1000 }, (_, i) => BigInt(i));
    const batch = batchFromColumns({ n, label: n.map((v) => `row ${v}`) }, OUTPUT);
    const spy = spyOn(batch as any, "getChildAt");
    const mask = filters.evaluate(batch);
    // One column_ref column (`n`), referenced twice, resolved once. Before: once per row per reference.
    expect(spy.mock.calls.length).toBe(1);
    spy.mockRestore();
    expect([...mask].reduce((a, b) => a + b, 0)).toBe(10);
    const kept = [...iterRows(filters.apply(batch))].map((row) => Number(row.n));
    expect(kept).toEqual([10, 11, 12, 13, 14, 15, 16, 17, 18, 19]);
  });
});
