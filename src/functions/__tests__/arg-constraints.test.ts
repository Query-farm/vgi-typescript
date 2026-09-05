// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Functional APIs can declare per-argument constraints
// (choices / ge / le / gt / lt / pattern) via `argConstraints`, keyed by name.
// They surface as ArgumentSpec constraint fields (→ vgi_function_arguments())
// and are enforced at bind through the shared validateConstConstraints (which
// has its own unit tests in ../../arguments/argument-spec.test.ts).

import { describe, test, expect } from "bun:test";
import { Int64, Utf8, Schema, Field } from "@query-farm/apache-arrow";
import { defineTableFunction } from "../table.js";
import { defineAggregate } from "../aggregate.js";
import {
  defineRowTransformFunction,
  defineTableInOutFunction,
} from "../table-in-out.js";
import { defineTableBufferingFunction } from "../table-buffering.js";

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

  test("argDefaults automatically surface as discovery defaults", () => {
    const table = defineTableFunction({
      name: "t",
      args: { unit: new Utf8() },
      argDefaults: { unit: "mm" },
      onBind: () => ({ outputSchema: OUT }),
      process: () => {},
    });
    expect(table.argumentSpecs[0]?.defaultJson).toBe('"mm"');

    const aggregate = defineAggregate({
      name: "a",
      args: { percentile: new Int64() },
      constParams: ["percentile"],
      argDefaults: { percentile: 50 },
      outputType: new Int64(),
      initialState: () => ({}),
      update: () => {},
      combine: (_source, target) => target,
      finalize: () => OUT as any,
    });
    expect(aggregate.argumentSpecs[0]?.defaultJson).toBe("50");
  });

  test("table-in-out APIs surface defaults, docs, and constraints", () => {
    const classic = defineTableInOutFunction({
      name: "classic",
      namedArgs: { mode: new Utf8() },
      argDefaults: { mode: "fast" },
      argDocs: { mode: "Execution mode" },
      argConstraints: { mode: { choices: ["fast", "safe"] } },
    });
    expect(classic.argumentSpecs.find((s) => s.name === "mode")).toMatchObject({
      defaultJson: '"fast"',
      choicesJson: '["fast","safe"]',
      doc: "Execution mode",
    });

    const rowTransform = defineRowTransformFunction({
      name: "row_transform",
      args: { latitude: new Int64() },
      namedArgs: { units: new Utf8() },
      argDefaults: { units: "metric" },
      argDocs: { units: "Unit system" },
      argConstraints: { units: { choices: ["metric", "imperial"] } },
      onBind: () => ({ outputSchema: OUT }),
      process: () => {},
    });
    expect(rowTransform.argumentSpecs.find((s) => s.name === "units")).toMatchObject({
      defaultJson: '"metric"',
      choicesJson: '["metric","imperial"]',
      doc: "Unit system",
    });

    const buffering = defineTableBufferingFunction({
      name: "buffering",
      namedArgs: { limit: new Int64() },
      argDefaults: { limit: 100 },
      argConstraints: { limit: { ge: 1 } },
      process: () => new Uint8Array(),
      combine: (stateIds) => stateIds,
      finalize: () => {},
    });
    expect(buffering.argumentSpecs.find((s) => s.name === "limit")).toMatchObject({
      defaultJson: "100",
      rangeNotation: "[1, +inf)",
    });
  });
});
