// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Descriptor-catalog behaviour that vgi-python's ReadOnlyCatalogInterface /
// Worker already have:
//
// 1. A table's scan (and write) functions need not also be listed in the
//    schema's `functions`. vgi-python's Worker._build_registry auto-registers
//    them; here the descriptor indexed them per schema only, so a bind that
//    resolved by name (a scan whose ScanFunctionResult carries no schema_path,
//    because the function is in no schema's `functions`) failed with
//    "Unknown function".
// 2. `CatalogDescriptor.attachOptions` declares ATTACH options the way
//    vgi-python's `ReadOnlyCatalogInterface.attach_option_specs` does: they
//    are advertised in catalogsInfo() and `required` ones are enforced at
//    attach, with no subclass.
// 3. The TABLE argument of a table in-out function takes its description from
//    `argDocs.data`, like any other argument (vgi-python: the TableInput
//    field's own `Arg(doc=...)`).

import { describe, expect, test } from "bun:test";
import { batchFromColumns, field, int64, schema, utf8 } from "../../arrow/index.js";
import { deserializeAttachOptionSpecs } from "../attach-option.js";
import { ReadOnlyCatalogInterface } from "../read-only.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { defineTableFunction } from "../../functions/table.js";
import { defineTableInOutFunction } from "../../functions/table-in-out.js";

const ROWS = schema([field("n", int64(), false)]);

const scan = defineTableFunction<Record<string, never>, { done: boolean }>({
  name: "numbers_scan",
  description: "Backing scan for the numbers table",
  onBind: () => ({ outputSchema: ROWS }),
  initialState: () => ({ done: false }),
  process: (params, state, out) => {
    if (state.done) return out.finish();
    state.done = true;
    out.emit(batchFromColumns({ n: [1n] }, params.outputSchema));
  },
});

describe("table functions not listed in `functions`", () => {
  test("resolve by name once the catalog is built", () => {
    const registry = new FunctionRegistry();
    new ReadOnlyCatalogInterface(
      { name: "demo", schemas: [{ name: "main", functions: [], tables: [{ name: "numbers", columns: ROWS, function: scan }] }] },
      registry,
    );
    expect(registry.get("numbers_scan")).toBe(scan);
    expect(registry.get("numbers_scan", { schemaPath: ["main"] })).toBe(scan);
  });

  test("a function both listed and backing a table is registered once", () => {
    const registry = new FunctionRegistry();
    registry.register(scan);
    new ReadOnlyCatalogInterface(
      { name: "demo", schemas: [{ name: "main", functions: [scan], tables: [{ name: "numbers", columns: ROWS, function: scan }] }] },
      registry,
    );
    registry.register(scan); // idempotent, as a Worker re-registering listed functions would be
    expect(registry.get("numbers_scan")).toBe(scan);
  });
});

describe("CatalogDescriptor.attachOptions", () => {
  const descriptor = {
    name: "opts",
    attachOptions: [
      { name: "region", description: "Region to read", type: utf8(), default: "us-east-1" },
      { name: "api_key", description: "API key", type: utf8(), required: true, secret: true },
    ],
    schemas: [{ name: "main", functions: [scan] }],
  };

  test("are advertised in catalogsInfo()", () => {
    const catalog = new ReadOnlyCatalogInterface(descriptor, new FunctionRegistry());
    const specs = deserializeAttachOptionSpecs(catalog.catalogsInfo()[0]!.attach_option_specs);
    expect(specs.map((s) => [s.name, s.required ?? false, s.secret ?? false])).toEqual([
      ["region", false, false],
      ["api_key", true, true],
    ]);
    expect(catalog.attachOptionSpecs("opts").map((s) => s.name)).toEqual(["region", "api_key"]);
  });

  test("required ones are enforced at attach", async () => {
    const catalog = new ReadOnlyCatalogInterface(descriptor, new FunctionRegistry());
    expect(() => catalog.attach("opts", {})).toThrow(/api_key/);
    const result = await catalog.attach("opts", { api_key: "k" });
    expect(result.attach_opaque_data.length).toBeGreaterThan(0);
  });

  test("a descriptor without attachOptions advertises none", () => {
    const catalog = new ReadOnlyCatalogInterface({ name: "plain", schemas: [] }, new FunctionRegistry());
    expect(catalog.catalogsInfo()[0]!.attach_option_specs).toEqual([]);
  });
});

describe("table in-out TABLE argument", () => {
  const make = (argDocs?: Record<string, string>) =>
    defineTableInOutFunction({
      name: "passthrough",
      description: "Pass rows through",
      ...(argDocs ? { argDocs } : {}),
    });

  test("takes its description from argDocs.data", () => {
    const spec = make({ data: "Rows to pass through." }).argumentSpecs.find((s) => s.isTableInput);
    expect(spec?.doc).toBe("Rows to pass through.");
  });

  test("stays undocumented without one", () => {
    expect(make().argumentSpecs.find((s) => s.isTableInput)?.doc).toBeUndefined();
  });
});
