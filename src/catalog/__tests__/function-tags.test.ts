// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Worker-provided function-level tags must flow into FunctionInfo.tags so the
// metadata linter (vgi-lint-check) can read them from duckdb_functions().tags.
// In particular VGI307 requires a dynamic-schema table function to carry a
// `vgi.columns_md` tag. Tags are settable on every function kind (scalar,
// table, table_in_out, table_buffering, aggregate) and round-trip through the
// wire codec intact.

import { describe, test, expect } from "bun:test";
import { int64, batchFromColumns } from "../../arrow/index.js";
import { defineScalarFunction } from "../../functions/scalar.js";
import { defineTableFunction } from "../../functions/table.js";
import { defineAggregate } from "../../functions/aggregate.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { ReadOnlyCatalogInterface } from "../read-only.js";
import { encodeFunctionInfo, decodeFunctionInfo } from "../../generated/vgi-client.js";
import type { CatalogDescriptor } from "../descriptors.js";

function infoFor(descriptor: CatalogDescriptor, type: string, name: string) {
  const catalog = new ReadOnlyCatalogInterface(descriptor, new FunctionRegistry());
  const infos = catalog.schemaContentsFunctions(new Uint8Array([1]), "main", type);
  const info = infos.find((i) => i.name === name);
  expect(info, `function ${name} not found`).toBeDefined();
  return info!;
}

describe("ReadOnlyCatalogInterface function tags", () => {
  test("scalar function tags surface into FunctionInfo.tags and round-trip", () => {
    const tags = { "vgi.description_md": "doubles its input", owner: "rusty" };
    const fn = defineScalarFunction({
      name: "dbl",
      params: { x: int64() },
      outputType: () => int64(),
      compute: () => [],
      tags,
    });
    const info = infoFor(
      { name: "test", schemas: [{ name: "main", functions: [fn] }] },
      "scalar_function",
      "dbl",
    );
    expect(info.tags).toEqual(tags);
    expect(decodeFunctionInfo(encodeFunctionInfo(info)).tags).toEqual(tags);
  });

  test("table function carries vgi.columns_md (VGI307) into FunctionInfo.tags", () => {
    const tags = { "vgi.columns_md": "id: BIGINT — the row id" };
    const fn = defineTableFunction({
      name: "dyn_scan",
      args: { count: int64() },
      onBind: () => ({ outputSchema: { fields: [] } as any }),
      initialState: () => ({}),
      process: (_p: any, _s: any, out: any) => out.finish(),
      tags,
    });
    const info = infoFor(
      { name: "test", schemas: [{ name: "main", functions: [fn] }] },
      "table_function",
      "dyn_scan",
    );
    expect(info.tags["vgi.columns_md"]).toBe(tags["vgi.columns_md"]);
    expect(decodeFunctionInfo(encodeFunctionInfo(info)).tags).toEqual(tags);
  });

  test("aggregate function tags surface into FunctionInfo.tags", () => {
    const tags = { "vgi.description_md": "sums things" };
    const fn = defineAggregate({
      name: "mysum",
      args: { x: int64() },
      outputType: int64(),
      initialState: () => 0n,
      update: () => {},
      combine: (_s: any, t: any) => t,
      finalize: (p: any) => batchFromColumns({ result: p.groupIds.map(() => 0n) }, {
        fields: [{ name: "result", type: int64(), nullable: true }],
      } as any),
      tags,
    });
    const info = infoFor(
      { name: "test", schemas: [{ name: "main", functions: [fn] }] },
      "aggregate_function",
      "mysum",
    );
    expect(info.tags).toEqual(tags);
    expect(decodeFunctionInfo(encodeFunctionInfo(info)).tags).toEqual(tags);
  });

  test("no tags configured yields an empty tag map (not undefined)", () => {
    const fn = defineScalarFunction({
      name: "notags",
      params: { x: int64() },
      outputType: () => int64(),
      compute: () => [],
    });
    const info = infoFor(
      { name: "test", schemas: [{ name: "main", functions: [fn] }] },
      "scalar_function",
      "notags",
    );
    expect(info.tags).toEqual({});
  });
});
