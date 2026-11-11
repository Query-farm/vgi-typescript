// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Protocol 3.3 FunctionInfo.visibility. The DuckDB extension takes a table's
// scan/write capabilities (projection pushdown, has_finalize, ...) only from
// the function's listing entry, so every function a table names must be
// listed. Scans default to table_only (hidden, not callable), `exposeScan`
// or a schema `functions` declaration makes them public, and write functions
// are always table_only.

import { describe, expect, test } from "bun:test";
import { field, int64, schema, utf8 } from "../../arrow/index.js";
import { defineScalarFunction } from "../../functions/scalar.js";
import { defineTableFunction } from "../../functions/table.js";
import { defineTableInOutFunction } from "../../functions/table-in-out.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { decodeFunctionInfo, encodeFunctionInfo, type FunctionInfo } from "../../generated/vgi-client.js";
import { FunctionInfoSchema } from "../../generated/vgi-protocol-schemas.js";
import { VGI_PROTOCOL_VERSION } from "../../generated/vgi-service.js";
import { rowIdField } from "../../write-results.js";
import { deserializeBatch } from "../../util/arrow/index.js";
import { FUNCTION_VISIBILITY_PUBLIC, FUNCTION_VISIBILITY_TABLE_ONLY } from "../../index.core.js";
import type { CatalogDescriptor, TableDescriptor } from "../descriptors.js";
import { functionInfoFor, ReadOnlyCatalogInterface } from "../read-only.js";

const COLUMNS = schema([field("id", int64(), true), field("name", utf8(), true), rowIdField()]);
const ATTACH = new Uint8Array([1]);

const scanFn = (name: string) =>
  defineTableFunction<Record<string, never>, null>({
    name,
    description: name,
    projectionPushdown: true,
    onBind: () => ({ outputSchema: COLUMNS }),
    process: (_p: unknown, _s: unknown, out: { finish(): void }) => out.finish(),
  } as any);
const writer = (name: string, hasFinalize = false) =>
  defineTableInOutFunction({ name, description: name, ...(hasFinalize ? { hasFinalize: true, finalize: () => null } : {}) } as any);
const scalar = defineScalarFunction({ name: "dbl", params: { x: int64() }, outputType: () => int64(), compute: () => [] });

/** The schema_path an inlined ScanFunctionResult redirect carries. */
function redirectSchemaPath(bytes: Uint8Array): string[] | null {
  const value = deserializeBatch(bytes).getChild("schema_path")?.get(0);
  return value == null ? null : [...(value as Iterable<string>)];
}

function catalog(descriptor: CatalogDescriptor) {
  return new ReadOnlyCatalogInterface(descriptor, new FunctionRegistry());
}

function listing(cat: ReadOnlyCatalogInterface, path: string[] = ["main"], type = "table_function"): Map<string, FunctionInfo> {
  return new Map(cat.schemaContentsFunctions(ATTACH, path, type).map((i) => [i.name, i]));
}

describe("FunctionInfo.visibility wire field", () => {
  test("is a nullable utf8 field, not a dictionary", () => {
    const f = FunctionInfoSchema.fields.find((x) => x.name === "visibility");
    expect(f).toBeDefined();
    expect(f!.nullable).toBe(true);
    expect(String(f!.type.typeId ?? f!.type)).not.toContain("Dictionary");
  });

  test("protocol is 3.3.0", () => {
    expect(VGI_PROTOCOL_VERSION).toBe("3.3.0");
  });

  test("round-trips table_only, public and absent", () => {
    const info = listing(catalog({ name: "c", schemas: [{ name: "main", tables: [{ name: "t", function: scanFn("t_scan") }] }] })).get("t_scan")!;
    expect(decodeFunctionInfo(encodeFunctionInfo(info)).visibility).toBe("table_only");
    expect(decodeFunctionInfo(encodeFunctionInfo({ ...info, visibility: "public" })).visibility).toBe("public");
    const { visibility: _omit, ...withoutVisibility } = info;
    expect(decodeFunctionInfo(encodeFunctionInfo(withoutVisibility as FunctionInfo)).visibility ?? null).toBeNull();
  });
});

describe("descriptor listings", () => {
  test("ordinary functions are public", () => {
    const cat = catalog({ name: "c", schemas: [{ name: "main", functions: [scalar] }] });
    expect(listing(cat, ["main"], "scalar_function").get("dbl")!.visibility).toBe(FUNCTION_VISIBILITY_PUBLIC);
  });

  test("an unlisted scan is auto-included as table_only in its table's schema", async () => {
    const scan = scanFn("items_scan");
    const cat = catalog({ name: "c", schemas: [{ name: "main", tables: [{ name: "items", function: scan }] }] });
    const info = listing(cat).get("items_scan");
    expect(info).toBeDefined();
    expect(info!.visibility).toBe(FUNCTION_VISIBILITY_TABLE_ONLY);
    expect(info!.projection_pushdown).toBe(true);
    expect(info!.schema_path).toEqual(["main"]);
    // The unfiltered listing carries it too.
    expect(cat.schemaContentsFunctions(ATTACH, ["main"], "").map((i) => i.name)).toContain("items_scan");
    // The inlined scan redirect names the schema whose listing carries it.
    const table = ((await cat.schemaContentsTables(ATTACH, ["main"])) as any[]).find((t) => t.name === "items");
    expect(redirectSchemaPath(table.scan_function)).toEqual(["main"]);
  });

  test("exposeScan makes an unlisted scan public", () => {
    const cat = catalog({
      name: "c",
      schemas: [{ name: "main", tables: [{ name: "labels", function: scanFn("labels_scan"), exposeScan: true }] }],
    });
    expect(listing(cat).get("labels_scan")!.visibility).toBe(FUNCTION_VISIBILITY_PUBLIC);
  });

  test("a scan the schema also declares in functions is public", () => {
    const scan = scanFn("items_scan");
    const cat = catalog({ name: "c", schemas: [{ name: "main", functions: [scan], tables: [{ name: "items", function: scan }] }] });
    const infos = cat.schemaContentsFunctions(ATTACH, ["main"], "table_function").filter((i) => i.name === "items_scan");
    expect(infos.length).toBe(1);
    expect(infos[0]!.visibility).toBe(FUNCTION_VISIBILITY_PUBLIC);
  });

  test("a scan declared in another schema stays listed only there, public", async () => {
    const scan = scanFn("shared_scan");
    const cat = catalog({
      name: "c",
      schemas: [
        { name: "main", functions: [scan] },
        { name: "data", tables: [{ name: "t", function: scan }] },
      ],
    });
    expect(listing(cat, ["main"]).get("shared_scan")!.visibility).toBe(FUNCTION_VISIBILITY_PUBLIC);
    expect(listing(cat, ["data"]).has("shared_scan")).toBe(false);
    const table = ((await cat.schemaContentsTables(ATTACH, ["data"])) as any[])[0];
    expect(redirectSchemaPath(table.scan_function)).toEqual(["main"]);
  });

  test("a scan shared by two tables is listed once, public if either exposes it", () => {
    const scan = scanFn("s");
    const tables: TableDescriptor[] = [
      { name: "a", function: scan },
      { name: "b", function: scan, exposeScan: true },
    ];
    const infos = catalog({ name: "c", schemas: [{ name: "main", tables }] })
      .schemaContentsFunctions(ATTACH, ["main"], "table_function")
      .filter((i) => i.name === "s");
    expect(infos.length).toBe(1);
    expect(infos[0]!.visibility).toBe(FUNCTION_VISIBILITY_PUBLIC);
  });

  test("write functions are always table_only, even when exposeScan or declared", async () => {
    const scan = scanFn("items_scan");
    const insert = writer("items_insert");
    const update = writer("items_update", true);
    const del = writer("items_delete");
    const cat = catalog({
      name: "c",
      schemas: [{
        name: "main",
        functions: [del],
        tables: [{ name: "items", columns: COLUMNS, function: scan, exposeScan: true, insertFunction: insert, updateFunction: update, deleteFunction: del }],
      }],
    });
    const l = listing(cat);
    expect(l.get("items_scan")!.visibility).toBe(FUNCTION_VISIBILITY_PUBLIC);
    for (const name of ["items_insert", "items_update", "items_delete"]) {
      expect(l.get(name), name).toBeDefined();
      expect(l.get(name)!.visibility, name).toBe(FUNCTION_VISIBILITY_TABLE_ONLY);
    }
    // has_finalize survives: the extension reads it from this entry.
    expect(l.get("items_update")!.has_finalize).toBe(true);
    // The write redirects name the schema that lists them.
    const table = ((await cat.schemaContentsTables(ATTACH, ["main"])) as any[])[0];
    expect(redirectSchemaPath(table.update_function)).toEqual(["main"]);
  });

  test("catalog_contents carries the hidden scan with its visibility", async () => {
    const cat = catalog({ name: "c", schemas: [{ name: "main", tables: [{ name: "items", function: scanFn("items_scan") }] }] });
    const contents = await cat.catalogContents(ATTACH);
    const fns = (contents.schemas ?? []).flatMap((s: any) => s.table_functions as FunctionInfo[]);
    expect(fns.find((f) => f.name === "items_scan")?.visibility).toBe(FUNCTION_VISIBILITY_TABLE_ONLY);
  });

  test("a differently-spelled schema path gets the same visibilities", () => {
    const cat = catalog({ name: "c", schemas: [{ name: "main", tables: [{ name: "items", function: scanFn("items_scan") }] }] });
    expect(listing(cat, ["MAIN"]).get("items_scan")!.visibility).toBe(FUNCTION_VISIBILITY_TABLE_ONLY);
  });

  test("exposeScan without a scan function is rejected", () => {
    expect(() => catalog({ name: "c", schemas: [{ name: "main", tables: [{ name: "t", columns: COLUMNS, exposeScan: true }] }] }))
      .toThrow(/exposeScan requires a scan function/);
  });
});

describe("functionInfoFor (hand-written CatalogInterface listings)", () => {
  test("defaults to public and takes table_only", () => {
    const scan = scanFn("my_scan");
    expect(functionInfoFor(scan, ["main"]).visibility).toBe(FUNCTION_VISIBILITY_PUBLIC);
    const hidden = functionInfoFor(scan, ["main"], FUNCTION_VISIBILITY_TABLE_ONLY);
    expect(hidden.visibility).toBe(FUNCTION_VISIBILITY_TABLE_ONLY);
    expect(hidden.projection_pushdown).toBe(true);
    expect(hidden.schema_path).toEqual(["main"]);
  });

  test("the versioned-tables fixture lists every scan table_only", async () => {
    const { VersionedTablesCatalog } = await import("../../../examples/versioned-tables-common.js");
    const cat = new VersionedTablesCatalog();
    const fns = await cat.schemaContentsFunctions(ATTACH, ["main"], "table_function");
    expect(fns.map((f) => f.name).sort()).toEqual([
      "versioned_tables_animals_color_scan",
      "versioned_tables_animals_scan",
      "versioned_tables_plants_scan",
    ]);
    expect(fns.every((f) => f.visibility === FUNCTION_VISIBILITY_TABLE_ONLY)).toBe(true);
    expect(await cat.schemaContentsFunctions(ATTACH, ["main"], "scalar_function")).toEqual([]);
  });
});
