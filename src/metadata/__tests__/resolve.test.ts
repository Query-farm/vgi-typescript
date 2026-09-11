// Copyright 2025, 2026 Query Farm LLC - https://query.farm

import { describe, expect, test } from "bun:test";
import type { VgiFunction } from "../../functions/types.js";
import { resolveMetadata } from "../resolve.js";
import { ArgumentMonotonicity } from "../../types.js";

function functionWithExtensionFilter(namespace: string, name: string, version: number): VgiFunction {
  return {
    kind: "table",
    meta: {
      name: "extension_filter",
      filterPushdown: true,
      additionalFilterFunctions: [{ namespace, name, version }],
    },
    argumentSpecs: [],
  } as unknown as VgiFunction;
}

describe("resolveMetadata Filter v2 capabilities", () => {
  test("advertises the registered spatial extension-filter evaluator", () => {
    const identity = { namespace: "duckdb.spatial", name: "intersects_extent", version: 1 };
    const metadata = resolveMetadata(functionWithExtensionFilter(identity.namespace, identity.name, identity.version));

    expect(metadata.additionalFilterFunctions).toEqual([identity]);
  });

  test("rejects an extension-filter function without an evaluator", () => {
    expect(() => resolveMetadata(functionWithExtensionFilter("example", "unknown", 1))).toThrow(
      "cannot advertise an extension-filter function without an evaluator",
    );
  });
});

describe("resolveMetadata argument monotonicity", () => {
  test("rejects claims on a non-scalar function", () => {
    const func = {
      kind: "table",
      meta: {
        name: "bad",
        argumentMonotonicity: [ArgumentMonotonicity.UNKNOWN],
      },
      argumentSpecs: [{ name: "value" }],
    } as unknown as VgiFunction;
    expect(() => resolveMetadata(func)).toThrow("only valid for scalar functions");
  });
});
