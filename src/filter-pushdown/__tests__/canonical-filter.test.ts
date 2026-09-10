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
  list,
  struct,
  TimeUnit,
  batchFromColumns,
  iterRows,
  backend,
  type VgiDataType,
} from "../../arrow/index.js";
import { deserializeFilters } from "../deserialize.js";

/**
 * Build a v2 filter batch: column 0 carries the snapshot document and
 * columns 1+ carry value-ref literals,
 * each in its own typed column so the literal is read back through the codec.
 */
function filterWireBatch(
  expressions: any[],
  literals: Array<{ type: VgiDataType; value: any }>,
): ReturnType<typeof batchFromColumns> {
  const fields = [
    field("filter_spec", utf8(), false),
    ...literals.map((l, i) => field(`value_${i}`, l.type, true)),
  ];
  const document = {
    encoding: "vgi.filters.v2",
    semantics: "vgi.duckdb.standard.v1",
    kind: "snapshot",
    predicates: expressions.map((expression, index) => ({
      id: `p${index}`,
      revision: 0,
      mode: "required",
      source: "query",
      expression,
    })),
  };
  const columns: Record<string, any[]> = { filter_spec: [JSON.stringify(document)] };
  literals.forEach((l, i) => {
    columns[`value_${i}`] = [l.value];
  });
  return batchFromColumns(columns, schema(fields, new Map([
    ["vgi_filter_encoding", "vgi.filters.v2"],
    ["vgi_filter_version", "2"],
    ["vgi_evaluation_context", "vgi.none.v1"],
  ])));
}

function documentWireBatch(
  document: Record<string, unknown>,
  literals: Array<{ type: VgiDataType; value: any }> = [],
): ReturnType<typeof batchFromColumns> {
  const fields = [field("filter_spec", utf8(), false), ...literals.map((item, index) =>
    field(`value_${index}`, item.type, true))];
  const columns: Record<string, any[]> = { filter_spec: [JSON.stringify(document)] };
  literals.forEach((item, index) => { columns[`value_${index}`] = [item.value]; });
  return batchFromColumns(columns, schema(fields, new Map([
    ["vgi_filter_encoding", "vgi.filters.v2"],
    ["vgi_filter_version", "2"],
    ["vgi_evaluation_context", "vgi.none.v1"],
  ])));
}

function rawDocumentWireBatch(document: string): ReturnType<typeof batchFromColumns> {
  return batchFromColumns({ filter_spec: [document] }, schema([
    field("filter_spec", utf8(), false),
  ], new Map([
    ["vgi_filter_encoding", "vgi.filters.v2"],
    ["vgi_filter_version", "2"],
    ["vgi_evaluation_context", "vgi.none.v1"],
  ])));
}

const column = (columnIndex: number, columnName: string) => ({ node: "column_ref", column_index: columnIndex, column_name: columnName });
const literal = (valueRef: number) => ({ node: "literal", value_ref: valueRef });

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
      { node: "comparison", op: "ge", left: column(1, "ts"), right: literal(0) },
    ];
    const fb = filterWireBatch(specs, [{ type: tsType, value: 1_700_000_000_000_000n }]);

    const filters = deserializeFilters(fb, { outputSchema: dataSchema });
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
      { node: "comparison", op: "eq", left: column(1, "amount"), right: literal(0) },
    ];
    const fb = filterWireBatch(specs, [{ type: decType, value: 12_345n }]);

    const out = deserializeFilters(fb, { outputSchema: dataSchema }).apply(data);
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
    const inListType = list(field("item", dType, true)) as VgiDataType;
    const specs = [
      { node: "in", expression: column(1, "d"), set: { kind: "literal", value_ref: 0 }, negated: false },
    ];
    // The list literal: rows d0 and d2.
    const fb = filterWireBatch(specs, [{ type: inListType, value: [d0, d2] }]);

    const out = deserializeFilters(fb, { outputSchema: dataSchema }).apply(data);
    const ids = [...iterRows(out)].map((r) => r.id);
    expect(ids).toEqual([1, 3]);
  });

  test("arbitrarily nested field_ref propagates NULL structs", () => {
    const address = struct([field("zip", int32(), true)]);
    const profile = struct([field("address", address, true)]);
    const dataSchema = schema([field("id", int32(), false), field("profile", profile, true)]);
    const data = batchFromColumns({
      id: [1, 2, 3, 4],
      profile: [
        { address: { zip: 10001 } },
        { address: { zip: 94107 } },
        { address: null },
        null,
      ],
    }, dataSchema);
    const nested = {
      node: "field_ref",
      expression: {
        node: "field_ref", expression: column(1, "profile"), field_index: 0, field_name: "address",
      },
      field_index: 0,
      field_name: "zip",
    };
    const batch = filterWireBatch([
      { node: "comparison", op: "eq", left: nested, right: literal(0) },
    ], [{ type: int32(), value: 94107 }]);
    const rows = [...iterRows(deserializeFilters(batch, { outputSchema: dataSchema }).apply(data))];
    expect(rows.map((row) => row.id)).toEqual([2]);
  });

  test("IN follows DuckDB three-valued truth table", () => {
    const dataSchema = schema([field("n", int32(), true)]);
    const data = batchFromColumns({ n: [1, 2, 3, null] }, dataSchema);
    const valueType = list(field("item", int32(), true));
    const batch = filterWireBatch([
      { node: "in", expression: column(0, "n"), set: { kind: "literal", value_ref: 0 }, negated: false },
    ], [{ type: valueType, value: [2, null] }]);
    expect([...iterRows(deserializeFilters(batch, { outputSchema: dataSchema }).apply(data))].map((row) => row.n)).toEqual([2]);
  });

  test("external IN honors arbitrary batch and column coordinates", () => {
    const dataSchema = schema([field("n", int32(), false)]);
    const data = batchFromColumns({ n: [1, 2, 3, 4] }, dataSchema);
    const unused = batchFromColumns({ ignored: [99] }, schema([field("ignored", int32(), false)]));
    const keys = batchFromColumns(
      { ignored: [20, 30], selected: [2, 4] },
      schema([field("ignored", int32(), false), field("selected", int32(), false)]),
    );
    const batch = filterWireBatch([{
      node: "in",
      expression: column(0, "n"),
      set: { kind: "external", batch_index: 1, column_index: 1, column_name: "selected" },
      negated: false,
    }], []);
    const filters = deserializeFilters(batch, { outputSchema: dataSchema, joinKeyBatches: [unused, keys] });
    expect([...iterRows(filters.apply(data))].map((row) => row.n)).toEqual([2, 4]);
  });

  test("evaluation remaps by name and never falls back to a projected index", () => {
    const dataSchema = schema([field("id", int32(), false), field("filtered", int32(), false)]);
    const batch = filterWireBatch([
      { node: "comparison", op: "eq", left: column(1, "filtered"), right: literal(0) },
    ], [{ type: int32(), value: 1 }]);
    const filters = deserializeFilters(batch, { outputSchema: dataSchema });
    const projected = batchFromColumns({ id: [1] }, schema([field("id", int32(), false)]));
    expect(() => filters.apply(projected)).toThrow("filter column filtered is unavailable");
  });

  test("delta application is atomic and protects required predicates", () => {
    const dataSchema = schema([field("n", int32(), false)]);
    const snapshot = deserializeFilters(documentWireBatch({
      encoding: "vgi.filters.v2",
      semantics: "vgi.duckdb.standard.v1",
      kind: "snapshot",
      predicates: [
        { id: "required", revision: 0, mode: "required", source: "query",
          expression: { node: "comparison", op: "ge", left: column(0, "n"), right: literal(0) } },
        { id: "dynamic", revision: 0, mode: "advisory", source: "top_n",
          expression: { node: "comparison", op: "lt", left: column(0, "n"), right: literal(1) } },
      ],
    }, [{ type: int32(), value: 0 }, { type: int32(), value: 10 }]), { outputSchema: dataSchema });

    const invalidDelta = documentWireBatch({
      encoding: "vgi.filters.v2",
      semantics: "vgi.duckdb.standard.v1",
      kind: "delta",
      updates: [
        { operation: "upsert", id: "dynamic", revision: 1, mode: "advisory", source: "top_n",
          expression: { node: "comparison", op: "lt", left: column(0, "n"), right: literal(0) } },
        { operation: "remove", id: "required", revision: 1 },
      ],
    }, [{ type: int32(), value: 5 }]);
    expect(() => snapshot.applyDelta(invalidDelta)).toThrow("required predicate");
    expect(snapshot.revisions.get("dynamic")).toBe(0);

    const malformedStaleDelta = documentWireBatch({
      encoding: "vgi.filters.v2",
      semantics: "vgi.duckdb.standard.v1",
      kind: "delta",
      updates: [{ operation: "upsert", id: "dynamic", revision: 0, mode: "required", source: "top_n",
        expression: {} }],
    });
    expect(() => snapshot.applyDelta(malformedStaleDelta)).toThrow("delta upserts must be advisory");
    expect(snapshot.revisions.get("dynamic")).toBe(0);

    const validDelta = documentWireBatch({
      encoding: "vgi.filters.v2",
      semantics: "vgi.duckdb.standard.v1",
      kind: "delta",
      updates: [{ operation: "upsert", id: "dynamic", revision: 1, mode: "advisory", source: "top_n",
        expression: { node: "comparison", op: "lt", left: column(0, "n"), right: literal(0) } }],
    }, [{ type: int32(), value: 5 }]);
    const updated = snapshot.applyDelta(validDelta);
    const data = batchFromColumns({ n: [1, 4, 5, 9] }, dataSchema);
    expect([...iterRows(updated.apply(data))].map((row) => row.n)).toEqual([1, 4]);
  });

  test("strict decoder rejects duplicate JSON members and name/index disagreement", () => {
    expect(() => deserializeFilters(rawDocumentWireBatch(
      '{"encoding":"vgi.filters.v2","encoding":"vgi.filters.v2","semantics":"vgi.duckdb.standard.v1","kind":"snapshot","predicates":[]}',
    ), { outputSchema: schema([]) })).toThrow("duplicate object key");

    const dataSchema = schema([field("actual", int32(), false)]);
    const batch = filterWireBatch([
      { node: "comparison", op: "eq", left: column(0, "wrong"), right: literal(0) },
    ], [{ type: int32(), value: 1 }]);
    expect(() => deserializeFilters(batch, { outputSchema: dataSchema })).toThrow("does not match index");
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
      { node: "comparison", op: "eq", left: column(0, "id"), right: literal(0) },
    ];
    const fb = filterWireBatch(specs, [{ type: int32(), value: 2 }]);
    const out = deserializeFilters(fb, { outputSchema: dataSchema }).apply(data);
    const rows = [...iterRows(out)];
    expect(rows.length).toBe(1);
    // ns precision must survive the rebuild (no Number coercion).
    expect(rows[0].ts).toBe(8_876_543_210_987_654_321n);
  });
});
