// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Table and aggregate functions can now declare per-argument constraints
// (choices / ge / le / gt / lt / pattern) via `argConstraints`, keyed by name.
// They surface as ArgumentSpec constraint fields (→ vgi_function_arguments())
// and are enforced at bind through the shared validateConstConstraints (which
// has its own unit tests in ../../arguments/argument-spec.test.ts).

import { describe, test, expect } from "bun:test";
import { Int64, Utf8, Schema, Field } from "@query-farm/apache-arrow";
import { defineTableFunction } from "../table.js";
import { defineAggregate } from "../aggregate.js";

const OUT = new Schema([new Field("n", new Int64(), true)]);

describe("table/aggregate argConstraints surfacing", () => {
  test("table function surfaces declared arg constraints", () => {
    const fn = defineTableFunction({
      name: "t",
      args: { unit: new Utf8(), n: new Int64() },
      argConstraints: { unit: { choices: ["mm", "cm"] }, n: { ge: 0, le: 10 } },
      onBind: () => ({ outputSchema: OUT }),
      process: () => {},
    } as any);
    const byName = new Map(fn.argumentSpecs.map((s) => [s.name, s]));
    expect(byName.get("unit")?.choicesJson).toBe('["mm","cm"]');
    expect(byName.get("n")?.rangeNotation).toBe("[0, 10]");
    // An arg without constraints stays clean.
    expect(byName.get("n")?.pattern).toBeUndefined();
  });

  test("aggregate function surfaces declared arg constraints", () => {
    const fn = defineAggregate({
      name: "a",
      args: { mode: new Utf8() },
      constParams: ["mode"],
      argConstraints: { mode: { choices: ["min", "max"] } },
      outputType: new Int64(),
      initialState: () => ({}),
      update: () => {},
      finalize: () => OUT,
    } as any);
    const byName = new Map(fn.argumentSpecs.map((s) => [s.name, s]));
    expect(byName.get("mode")?.choicesJson).toBe('["min","max"]');
  });
});
