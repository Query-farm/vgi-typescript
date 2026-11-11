// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// `duckdb.spatial/intersects_extent@1` (`a && b`) conformance against DuckDB
// core. The fixture (data/intersects_extent_cases.json) was generated from
// DuckDB's own `&&` and covers empties, NaN/inf coordinates, Z/M, nested
// collections, and inclusive / exact-double boundaries. DuckDB pushes this
// filter as ADVISORY when it derives it from ST_Intersects & co. and as
// EXACT (removed from its own plan) when the user wrote `&&`, so any case
// where the SDK says false and DuckDB says true silently drops rows.
//
// Every case runs through the same path auto-applied filters take at runtime:
// deserializeFilters() -> PushdownFilters.apply(), on both Arrow backends:
//
//   bun test src/filter-pushdown/__tests__/intersects-extent-conformance.test.ts
//   bun --conditions=worker test src/filter-pushdown/__tests__/intersects-extent-conformance.test.ts

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { batchFromColumns, binary, field, int32, iterRows, schema, utf8 } from "../../arrow/index.js";
import { deserializeFilters } from "../deserialize.js";

interface Geometry { name: string; wkt: string; wkb_le: string; wkb_be: string }
interface Case { left: string; right: string; expected: boolean }

const FIXTURE = JSON.parse(readFileSync(join(import.meta.dir, "data", "intersects_extent_cases.json"), "utf8")) as {
  geometries: Geometry[];
  cases: Case[];
};
const INTERSECTS_EXTENT = { namespace: "duckdb.spatial", name: "intersects_extent", version: 1 };
const DATA_SCHEMA = schema([field("id", int32(), false), field("geom", binary(), false)]);

const hex = (text: string) => Uint8Array.from(text.match(/../g) ?? [], (pair) => Number.parseInt(pair, 16));

/** The wire batch for `geom && constant`, as the extension pushes it. */
function extentFilterWire(constant: Uint8Array, mode: "required" | "advisory") {
  const document = {
    encoding: "vgi.filters.v2",
    semantics: "vgi.duckdb.standard.v1",
    kind: "snapshot",
    predicates: [{
      id: "p0", revision: 0, mode, source: "query",
      expression: {
        node: "call",
        function: INTERSECTS_EXTENT,
        arguments: [
          { node: "column_ref", column_index: 1, column_name: "geom" },
          { node: "literal", value_ref: 0 },
        ],
      },
    }],
  };
  return batchFromColumns(
    { filter_spec: [JSON.stringify(document)], value_0: [constant] },
    schema([field("filter_spec", utf8(), false), field("value_0", binary(), true)], new Map([
      ["vgi_filter_encoding", "vgi.filters.v2"],
      ["vgi_filter_version", "2"],
      ["vgi_evaluation_context", "vgi.none.v1"],
    ])),
  );
}

function extentFilter(constant: Uint8Array, mode: "required" | "advisory") {
  return deserializeFilters(extentFilterWire(constant, mode), {
    outputSchema: DATA_SCHEMA, extensionFunctions: [INTERSECTS_EXTENT],
  });
}

const ENCODINGS = [["wkb_le", "wkb_le"], ["wkb_le", "wkb_be"], ["wkb_be", "wkb_le"], ["wkb_be", "wkb_be"]] as const;

describe("duckdb.spatial/intersects_extent@1 matches DuckDB core", () => {
  test("fixture is complete", () => {
    expect(FIXTURE.geometries.length).toBe(46);
    expect(FIXTURE.cases.length).toBe(FIXTURE.geometries.length ** 2);
  });

  for (const [leftEncoding, rightEncoding] of ENCODINGS) {
    for (const mode of ["required", "advisory"] as const) {
      test(`${mode}: column ${leftEncoding} && constant ${rightEncoding}`, () => {
        const rows = new Map(FIXTURE.geometries.map((geometry) => [geometry.name, batchFromColumns(
          { id: [0], geom: [hex(geometry[leftEncoding])] }, DATA_SCHEMA)]));
        const filters = new Map(FIXTURE.geometries.map((geometry) => [geometry.name,
          extentFilter(hex(geometry[rightEncoding]), mode)]));
        // One single-row batch per case, so each case's outcome is its own:
        // a batch's evaluation fails or succeeds as a whole.
        const mismatches: string[] = [];
        for (const { left, right, expected } of FIXTURE.cases) {
          let actual: boolean | string;
          try {
            actual = filters.get(right)!.apply(rows.get(left)!).numRows === 1;
          } catch (error) {
            actual = `threw ${(error as Error).message}`;
          }
          if (actual !== expected) mismatches.push(`${left} && ${right}: expected ${expected}, got ${actual}`);
        }
        expect(mismatches).toEqual([]);
      });
    }
  }
});

describe("intersects_extent on values it cannot evaluate", () => {
  const point = (x: number, y: number, type = 1, srid?: number) => {
    const bytes = new Uint8Array(srid === undefined ? 21 : 25);
    const view = new DataView(bytes.buffer);
    view.setUint8(0, 1);
    view.setUint32(1, srid === undefined ? type : (type | 0x20000000) >>> 0, true);
    let offset = 5;
    if (srid !== undefined) { view.setUint32(offset, srid, true); offset += 4; }
    view.setFloat64(offset, x, true);
    view.setFloat64(offset + 8, y, true);
    return bytes;
  };
  const constant = point(0.5, 0.5);
  const unusable = {
    truncated: point(0.5, 0.5).subarray(0, 12),
    "bad byte order": Uint8Array.from([7, ...point(0.5, 0.5).subarray(1)]),
    "unsupported type": point(0.5, 0.5, 17),
    "trailing bytes": Uint8Array.from([...point(0.5, 0.5), 0]),
  };
  // Row 0 matches, row 1 does not, row 2 cannot be evaluated.
  const ids = (geom: Uint8Array, mode: "required" | "advisory") => [...iterRows(extentFilter(constant, mode).apply(
    batchFromColumns({ id: [0, 1, 2], geom: [point(0.5, 0.5), point(9, 9), geom] }, DATA_SCHEMA)))]
    .map((row) => row.id);

  for (const [name, geom] of Object.entries(unusable)) {
    test(`advisory keeps a ${name} row and still filters the others`, () => {
      expect(ids(geom, "advisory")).toEqual([0, 2]);
    });
    test(`required fails the scan on a ${name} row`, () => {
      expect(() => ids(geom, "required")).toThrow();
    });
  }

  test("advisory keeps a row whose value is not binary", () => {
    const textSchema = schema([field("id", int32(), false), field("geom", utf8(), false)]);
    const data = batchFromColumns({ id: [0, 1], geom: ["POINT(0.5 0.5)", "POINT(9 9)"] }, textSchema);
    const filter = deserializeFilters(extentFilterWire(constant, "advisory"), {
      outputSchema: textSchema, extensionFunctions: [INTERSECTS_EXTENT],
    });
    expect([...iterRows(filter.apply(data))].map((row) => row.id)).toEqual([0, 1]);
    const exact = deserializeFilters(extentFilterWire(constant, "required"), {
      outputSchema: textSchema, extensionFunctions: [INTERSECTS_EXTENT],
    });
    expect(() => exact.apply(data)).toThrow("WKB binary");
  });

  test("EWKB with an SRID is read through the SRID", () => {
    for (const mode of ["required", "advisory"] as const) {
      expect(ids(point(0.5, 0.5, 1, 4326), mode)).toEqual([0, 2]);
      expect(ids(point(9, 9, 1, 4326), mode)).toEqual([0]);
    }
  });
});
