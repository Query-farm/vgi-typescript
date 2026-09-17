// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// A blended row-transform function's positional args ARE its per-row input
// columns, and one may be declared ANY (a Null arrow type — the same
// convention defineTableFunction / defineScalarFunction use). ANY names no
// concrete Arrow type, so it must reach the wire as `vgi_type=any` (DuckDB
// then advertises the arg as ANY and the client builds the input schema from
// the type it resolved for the call) rather than as a literal NULL-typed arg.

import { describe, test, expect } from "bun:test";
import { Int64, Null, Schema, Field } from "@query-farm/apache-arrow";
import { defineRowTransformFunction } from "../table-in-out.js";
import { argumentSpecsToSchema } from "../../arguments/argument-spec.js";
import { VGI_TYPE_KEY, VGI_TYPE_ANY } from "../../types.js";

const OUT = new Schema([new Field("n", new Int64(), true)]);

describe("defineRowTransformFunction ANY-typed input columns", () => {
  test("a Null positional arg is advertised as vgi_type=any", () => {
    const fn = defineRowTransformFunction({
      name: "any_one",
      args: { value: new Null(), n: new Int64() },
      onBind: () => ({ outputSchema: OUT }),
      process: () => {},
    });
    const byName = new Map(fn.argumentSpecs.map((s) => [s.name, s]));
    expect(byName.get("value")?.isAnyType).toBe(true);
    expect(byName.get("n")?.isAnyType).toBe(false);

    const fields = argumentSpecsToSchema(fn.argumentSpecs).fields;
    expect(fields.map((f) => f.name)).toEqual(["value", "n"]);
    expect(fields[0].metadata.get(VGI_TYPE_KEY)).toBe(VGI_TYPE_ANY);
    expect(fields[1].metadata.has(VGI_TYPE_KEY)).toBe(false);
  });

  test("Null varargs are advertised as ANY varargs", () => {
    const fn = defineRowTransformFunction({
      name: "any_many",
      varargs: { name: "values", type: new Null() },
      onBind: () => ({ outputSchema: OUT }),
      process: () => {},
    });
    expect(fn.argumentSpecs).toHaveLength(1);
    expect(fn.argumentSpecs[0]).toMatchObject({
      name: "values",
      isVarargs: true,
      isAnyType: true,
    });
    const [f] = argumentSpecsToSchema(fn.argumentSpecs).fields;
    expect(f.metadata.get(VGI_TYPE_KEY)).toBe(VGI_TYPE_ANY);
  });
});
