// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// A BOOLEAN column is a predicate on its own.
//
// `WHERE flag` / `WHERE NOT flag` is idiomatic SQL, and DuckDB pushes it down
// as a bare `column_ref` rather than rewriting it to `flag = true`. The schema
// admits that — `coreExpression` lists `columnRef` first — but the decoder's
// boolean gate enumerated node *kinds* instead of asking for the resolved
// *type*, so it refused every one of them ("predicate root must resolve to
// BOOLEAN") on a node whose type it had already resolved to BOOLEAN and
// stored. See vgi-python 0.36.2, where the same gap was found.
//
//   bun test src/filter-pushdown/__tests__/boolean-column-predicate.test.ts

import { describe, test, expect } from "bun:test";
import { schema, field, bool, int64, utf8, batchFromColumns } from "../../arrow/index.js";
import { deserializeFilters } from "../deserialize.js";

const OUTPUT_SCHEMA = schema([
  field("flag", bool(), true),
  field("n", int64(), true),
]);

/** A single-predicate v2 snapshot, with optional int64 value-ref literals. */
function predicateBatch(expression: unknown, literals: number[] = []) {
  const document = {
    encoding: "vgi.filters.v2",
    semantics: "vgi.duckdb.standard.v1",
    kind: "snapshot",
    predicates: [{ id: "p", revision: 0, mode: "required", source: "query", expression }],
  };
  const fields = [
    field("filter_spec", utf8(), false),
    ...literals.map((_, index) => field(`value_${index}`, int64(), true)),
  ];
  const columns: Record<string, unknown[]> = { filter_spec: [JSON.stringify(document)] };
  literals.forEach((value, index) => { columns[`value_${index}`] = [value]; });
  return batchFromColumns(columns, schema(fields, new Map([
    ["vgi_filter_encoding", "vgi.filters.v2"],
    ["vgi_filter_version", "2"],
    ["vgi_evaluation_context", "vgi.none.v1"],
  ])));
}

const FLAG = { node: "column_ref", column_index: 0, column_name: "flag" };
const NOT_FLAG = { node: "not", expression: FLAG };

/** flag = TRUE, FALSE, NULL, TRUE with n = 1..4. */
function dataBatch() {
  return batchFromColumns(
    { flag: [true, false, null, true], n: [1n, 2n, 3n, 4n] },
    OUTPUT_SCHEMA,
  );
}

function survivingN(expression: unknown): bigint[] {
  const filters = deserializeFilters(predicateBatch(expression), { outputSchema: OUTPUT_SCHEMA });
  const applied = filters.apply(dataBatch());
  const out: bigint[] = [];
  for (let row = 0; row < applied.numRows; row++) {
    out.push(applied.getChild("n")!.get(row) as bigint);
  }
  return out;
}

describe("a boolean column is a predicate on its own", () => {
  test("`WHERE flag` — the column itself is the predicate", () => {
    // A NULL predicate is not satisfied, so the NULL row drops — exactly the
    // rows `flag = true` keeps, which is what makes the SQL projection below
    // a projection rather than a change of meaning.
    expect(survivingN(FLAG)).toEqual([1n, 4n]);
  });

  test("`WHERE NOT flag` — `not` over a bare column, which the gate also refused", () => {
    // `NOT NULL` is NULL, so the NULL row drops here too — exactly `flag = false`.
    expect(survivingN(NOT_FLAG)).toEqual([2n]);
  });

  test("they reach SQL, not just the evaluator", () => {
    // The half a row count cannot see. A bare `flag` is legal SQL but is the
    // spelling no other VGI SDK produces, and it is invisible to
    // getColumnValues — so a worker pruning on the constant sees nothing.
    const positive = deserializeFilters(predicateBatch(FLAG), { outputSchema: OUTPUT_SCHEMA });
    expect(positive.toSql()).toBe("flag = true");
    expect(positive.getColumnValues("flag")).toEqual([true]);

    const negative = deserializeFilters(predicateBatch(NOT_FLAG), { outputSchema: OUTPUT_SCHEMA });
    expect(negative.toSql()).toBe("flag = false");
    expect(negative.getColumnValues("flag")).toEqual([false]);
  });

  test("a boolean column inside a conjunction still pushes down", () => {
    // The shape that actually turns up: one child the worker cannot render
    // used to cost the whole conjunction its pushdown.
    const conjunction = {
      node: "and",
      children: [
        {
          node: "comparison",
          op: "gt",
          left: { node: "column_ref", column_index: 1, column_name: "n" },
          right: { node: "literal", value_ref: 0 },
        },
        NOT_FLAG,
      ],
    };
    const filters = deserializeFilters(predicateBatch(conjunction, [2]), { outputSchema: OUTPUT_SCHEMA });
    expect(filters.toSql()).toBe("(n > 2 AND flag = false)");
  });

  test("a non-boolean column is still refused as a predicate root", () => {
    // `WHERE n` where n is BIGINT is not a predicate, and the projection must
    // not make it look like one.
    expect(() => deserializeFilters(
      predicateBatch({ node: "column_ref", column_index: 1, column_name: "n" }),
      { outputSchema: OUTPUT_SCHEMA },
    )).toThrow(/predicate root/);
  });
});
