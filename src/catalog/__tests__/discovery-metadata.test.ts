// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// The declarative catalog must surface two pieces of discovery metadata the
// vgi-lint-check metadata-quality linter inspects:
//   - catalog `source_url`  (CatalogInfo / duckdb_databases) — VGI004
//   - per-schema `tags`      (SchemaInfo / duckdb_schemas.tags) — VGI116 / VGI118
// Both must be settable on the descriptor and flow into the encoded records.

import { describe, test, expect } from "bun:test";
import { FunctionRegistry } from "../../functions/registry.js";
import { ReadOnlyCatalogInterface } from "../read-only.js";
import { encodeCatalogInfo, decodeCatalogInfo, encodeSchemaInfo, decodeSchemaInfo } from "../../generated/vgi-client.js";

describe("ReadOnlyCatalogInterface discovery metadata", () => {
  test("catalogsInfo() emits the descriptor's source_url", async () => {
    const catalog = new ReadOnlyCatalogInterface(
      {
        name: "example",
        sourceUrl: "https://github.com/Query-farm/vgi-typescript",
        schemas: [{ name: "main" }],
      },
      new FunctionRegistry(),
    );
    const infos = await catalog.catalogsInfo!();
    expect(infos).toHaveLength(1);
    expect(infos[0].source_url).toBe("https://github.com/Query-farm/vgi-typescript");
    // Round-trips through the wire codec intact.
    const back = decodeCatalogInfo(encodeCatalogInfo(infos[0]));
    expect(back.source_url).toBe("https://github.com/Query-farm/vgi-typescript");
  });

  test("catalogsInfo() advertises null source_url when unset", async () => {
    const catalog = new ReadOnlyCatalogInterface(
      { name: "example", schemas: [{ name: "main" }] },
      new FunctionRegistry(),
    );
    const infos = await catalog.catalogsInfo!();
    expect(infos[0].source_url ?? null).toBeNull();
  });

  test("schemas() carries the descriptor's tags into SchemaInfo", () => {
    const tags = {
      "vgi.description_llm": "An example schema.",
      "vgi.description_md": "An example schema.",
    };
    const catalog = new ReadOnlyCatalogInterface(
      { name: "example", schemas: [{ name: "main", tags }] },
      new FunctionRegistry(),
    );
    const schemas = catalog.schemas(new Uint8Array([1]));
    expect(schemas).toHaveLength(1);
    expect(schemas[0].tags).toEqual(tags);
    const back = decodeSchemaInfo(encodeSchemaInfo(schemas[0]));
    expect(back.tags).toEqual(tags);
  });
});
