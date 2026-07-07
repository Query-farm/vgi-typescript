// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Unit tests for the describe.json producer (the VGI HTTP landing contract).
// Builds a small declarative catalog with ReadOnlyCatalogInterface and asserts
// the emitted document + lazy column payloads match the contract shape.

import { describe, expect, test } from "bun:test";
import { field, int64, schema as makeSchema, utf8 } from "../../arrow/index.js";
import type { CatalogDescriptor } from "../../catalog/descriptors.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { defineScalarFunction } from "../../functions/scalar.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { buildColumnsJson, buildDescribeJson } from "../describe-json.js";

const upper = defineScalarFunction({
  name: "upper",
  description: "Uppercase a string.",
  params: { s: utf8() },
  argDocs: { s: "Input string." },
  returns: utf8(),
  compute: (_batch, _consts) => [],
});

function makeCatalog(): ReadOnlyCatalogInterface {
  const registry = new FunctionRegistry();
  registry.register(upper);

  const descriptor: CatalogDescriptor = {
    name: "example",
    defaultSchema: "main",
    tags: {
      "vgi.title": "Example Catalog",
      "vgi.author": "Query.Farm",
      "vgi.keywords": JSON.stringify(["example", "landing"]),
    },
    schemas: [
      {
        name: "main",
        tables: [
          {
            name: "widgets",
            comment: "A table of widgets.",
            columns: makeSchema([
              field("id", int64(), false, new Map([["comment", "Primary key."]])),
              field("name", utf8(), true),
            ]),
          },
        ],
        views: [
          {
            name: "recent_widgets",
            definition: "SELECT * FROM widgets",
            comment: "Recent widgets.",
            columnComments: { id: "Primary key.", name: "" },
          },
        ],
        functions: [upper],
      },
    ],
  };

  return new ReadOnlyCatalogInterface(descriptor, registry);
}

describe("buildDescribeJson", () => {
  test("emits the contract envelope with worker + runtime fields", async () => {
    const doc = await buildDescribeJson(makeCatalog(), { name: "TestWorker", doc: "d", version: "1.2.3" }, {
      oauth: true,
      serverId: "srv-1",
    });
    expect(doc.landing_schema_version).toBe(1);
    expect(doc.worker).toEqual({ name: "TestWorker", doc: "d", version: "1.2.3", lang: "typescript" });
    expect(doc.server_id).toBe("srv-1");
    expect(doc.oauth).toBe(true);
    expect(doc.cupola_base).toBe("https://cupola.query-farm.services");
  });

  test("maps catalog tags, counts, tables, views, and functions", async () => {
    const doc = (await buildDescribeJson(makeCatalog(), { name: "W" })) as any;
    expect(doc.catalogs).toHaveLength(1);
    const cat = doc.catalogs[0];
    expect(cat.name).toBe("example");
    expect(cat.implementation_version).toBeNull();
    expect(cat.data_version_spec).toBeNull();
    expect(cat.data_versions).toEqual([]);
    expect(cat.attach_options).toEqual([]);
    expect(cat.tags).toEqual({
      title: "Example Catalog",
      author: "Query.Farm",
      keywords: ["example", "landing"],
    });
    expect(cat.counts).toEqual({ schemas: 1, tables: 1, views: 1, functions: 1 });

    const main = cat.schemas.find((s: any) => s.name === "main");
    expect(main.tables).toEqual([{ name: "widgets", cols: 2, comment: "A table of widgets." }]);
    expect(main.views).toEqual([
      { name: "recent_widgets", cols: 2, comment: "Recent widgets.", def: "SELECT * FROM widgets" },
    ]);

    expect(main.functions).toHaveLength(1);
    const fn = main.functions[0];
    expect(fn.name).toBe("upper");
    expect(fn.type).toBe("scalar");
    expect(fn.doc).toBe("Uppercase a string.");
    expect(fn.args).toEqual([{ name: "s", type: "VARCHAR", desc: "Input string." }]);
    expect(fn.returns).toBe("VARCHAR");
  });
});

describe("buildColumnsJson", () => {
  test("returns table columns with types and comments", async () => {
    const cols = await buildColumnsJson(makeCatalog(), "example", "main", "widgets");
    expect(cols).toEqual({
      columns: [
        { name: "id", type: "BIGINT", comment: "Primary key." },
        { name: "name", type: "VARCHAR" },
      ],
    });
  });

  test("returns view column comments with empty types", async () => {
    const cols = await buildColumnsJson(makeCatalog(), "example", "main", "recent_widgets");
    expect(cols).toEqual({
      columns: [
        { name: "id", type: "", comment: "Primary key." },
        { name: "name", type: "", comment: "" },
      ],
    });
  });

  test("returns null for an unknown object", async () => {
    expect(await buildColumnsJson(makeCatalog(), "example", "main", "nope")).toBeNull();
    expect(await buildColumnsJson(makeCatalog(), "nope", "main", "widgets")).toBeNull();
  });
});
