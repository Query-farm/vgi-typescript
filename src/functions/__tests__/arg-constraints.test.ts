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
import { Arguments } from "../../arguments/arguments.js";
import { ArgumentValidationError } from "../../errors.js";
import { FunctionType } from "../../types.js";

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

// Bind-time enforcement on NAMED args (an arg with a default is named, i.e. it
// arrives in Arguments' named map rather than positionally), and the
// cross-SDK `pattern` contract: an UNANCHORED search (JSON Schema / ECMA-262
// semantics, as in the Rust and Go SDKs) — authors anchor with ^...$ when they
// mean a full match.
describe("argConstraints enforced at bind on named args", () => {
  function bindRequest(named: Record<string, unknown>) {
    return {
      function_name: "t",
      arguments: new Arguments([], new Map(Object.entries(named))),
      function_type: FunctionType.TABLE,
      input_schema: null,
      settings: null,
      secrets: null,
      attach_opaque_data: null,
      transaction_opaque_data: null,
      resolved_secrets_provided: false,
    } as any;
  }

  const fn = defineTableFunction({
    name: "t",
    args: { code: new Utf8(), anchored: new Utf8(), n: new Int64(), unit: new Utf8() },
    argDefaults: { code: "x1", anchored: "AB", n: 5, unit: "mm" },
    argConstraints: {
      code: { pattern: "\\d" },
      anchored: { pattern: "^[A-Z]{2}$" },
      n: { ge: 1, le: 10 },
      unit: { choices: ["mm", "cm"] },
    },
    onBind: () => ({ outputSchema: OUT }),
    process: () => {},
  } as any);

  test("all specs are named (sanity)", () => {
    for (const s of fn.argumentSpecs) expect(typeof s.position).toBe("string");
  });

  test("defaults satisfy constraints", async () => {
    await expect(fn.bind(bindRequest({}))).resolves.toBeDefined();
  });

  test("pattern is an unanchored search", async () => {
    await expect(fn.bind(bindRequest({ code: "abc7xyz" }))).resolves.toBeDefined();
    await expect(fn.bind(bindRequest({ code: "abcxyz" }))).rejects.toThrow(ArgumentValidationError);
  });

  test("an explicitly anchored pattern is a full match", async () => {
    await expect(fn.bind(bindRequest({ anchored: "CD" }))).resolves.toBeDefined();
    await expect(fn.bind(bindRequest({ anchored: "xCDx" }))).rejects.toThrow(/must match pattern/);
  });

  test("ge/le on a named int64 arg (arrives as bigint)", async () => {
    await expect(fn.bind(bindRequest({ n: 10n }))).resolves.toBeDefined();
    await expect(fn.bind(bindRequest({ n: 0n }))).rejects.toThrow(/must be >= 1/);
    await expect(fn.bind(bindRequest({ n: 11n }))).rejects.toThrow(/must be <= 10/);
  });

  test("choices on a named arg", async () => {
    await expect(fn.bind(bindRequest({ unit: "cm" }))).resolves.toBeDefined();
    await expect(fn.bind(bindRequest({ unit: "km" }))).rejects.toThrow(/must be one of/);
  });

  test("table-in-out namedArgs are enforced too", async () => {
    const tio = defineTableInOutFunction({
      name: "tio",
      namedArgs: { tag: new Utf8() },
      argConstraints: { tag: { pattern: "ok" } },
      onBind: () => ({ outputSchema: OUT }),
      process: () => {},
    } as any);
    const req = (tag: string) => ({
      ...bindRequest({ tag }),
      function_name: "tio",
      input_schema: new Schema([new Field("n", new Int64(), true)]),
    });
    await expect(tio.bind(req("is-ok-here"))).resolves.toBeDefined();
    await expect(tio.bind(req("nope"))).rejects.toThrow(ArgumentValidationError);
  });
});
