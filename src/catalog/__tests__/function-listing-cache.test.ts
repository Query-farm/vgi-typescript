// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// A descriptor catalog builds each function listing once, and each item is
// encoded once.
//
// Every catalog_schema_contents_functions request used to rebuild every
// FunctionInfo in the schema and Arrow-encode each again -- ~236 ms for the
// example worker's main schema, on every function-set load. Now
// ReadOnlyCatalogInterface keeps one frozen FunctionInfo per (schema, function)
// and one listing per (schema, listing type), and the listing handler memoizes
// the encoding of a frozen item. Mirrors vgi-python 6cc522a.
//
// Driven against the real example catalog (the one the example worker
// serves), plus an end-to-end listing over HTTP.

import { describe, expect, test } from "bun:test";
import { httpConnect } from "@query-farm/vgi-rpc";
import { allFunctions, catalog, createExampleCatalog } from "../../../examples/common.js";
import { VgiClient } from "../../client/client.js";
import { decodeFunctionInfo, encodeFunctionInfo, type FunctionInfo } from "../../generated/vgi-client.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { SIGNING_KEY_BYTES, serveVgiWorker } from "../../serve-entry.js";
import { encodeFunctionInfoOnce, freezeCatalogItem } from "../item-encoding.js";
import { ReadOnlyCatalogInterface } from "../read-only.js";

function exampleCatalog(): ReadOnlyCatalogInterface {
  const registry = new FunctionRegistry();
  for (const fn of allFunctions) registry.register(fn);
  return createExampleCatalog(new ReadOnlyCatalogInterface(catalog, registry));
}

const ATTACH = new Uint8Array(16);

describe("function listings are built once", () => {
  test("repeat listings return the same frozen FunctionInfo instances", () => {
    const cat = exampleCatalog();
    for (const type of ["table_function", "scalar_function", "aggregate_function", "anything_else"]) {
      const first = cat.schemaContentsFunctions(ATTACH, ["main"], type) as FunctionInfo[];
      const second = cat.schemaContentsFunctions(ATTACH, ["main"], type) as FunctionInfo[];
      expect(first.length).toBeGreaterThan(0);
      expect(second.length).toBe(first.length);
      for (let i = 0; i < first.length; i++) {
        expect(second[i]).toBe(first[i]);
        expect(Object.isFrozen(first[i])).toBe(true);
        expect(Object.isFrozen(first[i].tags)).toBe(true);
      }
      // The array itself is the caller's: changing it leaves the cache alone.
      first.length = 0;
      expect((cat.schemaContentsFunctions(ATTACH, ["main"], type) as FunctionInfo[]).length).toBe(second.length);
    }
  });

  test("each listing type filters the same shared per-function items", () => {
    const cat = exampleCatalog();
    const tables = cat.schemaContentsFunctions(ATTACH, ["main"], "table_function") as FunctionInfo[];
    const scalars = cat.schemaContentsFunctions(ATTACH, ["main"], "scalar_function") as FunctionInfo[];
    const all = cat.schemaContentsFunctions(ATTACH, ["main"], "") as FunctionInfo[];
    expect(tables.every((f) => f.function_type === "TABLE" || f.function_type === "TABLE_BUFFERING")).toBe(true);
    expect(scalars.every((f) => f.function_type === "SCALAR")).toBe(true);
    // The unfiltered listing is built from the same per-function items (by
    // identity: names repeat across overloads).
    const shared = new Set(all);
    for (const f of [...tables, ...scalars]) expect(shared.has(f)).toBe(true);
  });

  test("a differently-spelled schema path keeps the caller's spelling, as before", () => {
    const cat = exampleCatalog();
    const canonical = cat.schemaContentsFunctions(ATTACH, ["main"], "table_function") as FunctionInfo[];
    const shouted = cat.schemaContentsFunctions(ATTACH, ["MAIN"], "table_function") as FunctionInfo[];
    expect(shouted.map((f) => f.name)).toEqual(canonical.map((f) => f.name));
    expect(shouted[0].schema_path).toEqual(["MAIN"]);
    expect(canonical[0].schema_path).toEqual(["main"]);
  });

  test("the function's own metadata is not frozen along with its listing item", () => {
    const cat = exampleCatalog();
    cat.schemaContentsFunctions(ATTACH, ["main"], "table_function");
    const fn = allFunctions.find((f) => (f.meta.categories ?? []).length > 0)!;
    expect(Object.isFrozen(fn.meta.categories)).toBe(false);
  });
});

describe("catalog items are encoded once", () => {
  test("a frozen item's encoding is computed once and reused", () => {
    const cat = exampleCatalog();
    const [info] = cat.schemaContentsFunctions(ATTACH, ["main"], "table_function") as FunctionInfo[];
    const first = encodeFunctionInfoOnce(info);
    expect(encodeFunctionInfoOnce(info)).toBe(first);
    expect(first).toEqual(encodeFunctionInfo(info));
  });

  test("an item that is not frozen is encoded on every call, so a mutation is never served stale", () => {
    const cat = exampleCatalog();
    const [shared] = cat.schemaContentsFunctions(ATTACH, ["main"], "table_function") as FunctionInfo[];
    const mine: FunctionInfo = { ...shared, tags: { ...shared.tags } };
    const before = encodeFunctionInfoOnce(mine);
    mine.comment = "changed after the first encode";
    const after = encodeFunctionInfoOnce(mine);
    expect(after).not.toBe(before);
    expect(decodeFunctionInfo(after).comment).toBe("changed after the first encode");
  });

  test("freezeCatalogItem leaves binary fields usable", () => {
    const item = freezeCatalogItem({ name: "x", bytes: new Uint8Array([1, 2, 3]), nested: { list: [1, 2] } });
    expect(Object.isFrozen(item.nested.list)).toBe(true);
    expect(item.bytes[2]).toBe(3);
  });
});

describe("the listing over HTTP", () => {
  test("repeat main-schema listings are identical, item for item", async () => {
    const registry = new FunctionRegistry();
    for (const fn of allFunctions) registry.register(fn);
    const server = serveVgiWorker({
      name: "demo",
      doc: "Function listing cache test worker.",
      version: "0.0.1",
      registry,
      catalogInterface: createExampleCatalog(new ReadOnlyCatalogInterface(catalog, registry)),
      prefix: "",
      port: 0,
      signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(3),
      quiet: true,
      env: {},
    });
    try {
      const client = new VgiClient(httpConnect(`http://localhost:${server.port}`, { prefix: "" }));
      const { attach_opaque_data: attach } = await client.catalogAttach(catalog.name);
      const first = await client.schemaContentsFunctions(attach, "main", "TABLE_FUNCTION");
      const second = await client.schemaContentsFunctions(attach, "main", "TABLE_FUNCTION");
      expect(first.length).toBeGreaterThan(100);
      expect(second).toEqual(first);
      expect(first[0].schema_path).toEqual(["main"]);
    } finally {
      server.stop(true);
    }
  });
});
