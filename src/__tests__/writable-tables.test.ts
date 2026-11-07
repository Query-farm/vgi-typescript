// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Writable descriptor tables: TableDescriptor.insertFunction / updateFunction /
// deleteFunction / writeResultModes, the catalog_table_*_function_get lookups,
// and the write-result helpers. Ports vgi-python's
// tests/catalog/test_writable_table.py and write_results coverage; the wire
// path itself runs in the C++ extension's simple_writable SQL suite against
// examples/simple-writable-worker.ts.

import { describe, expect, test } from "bun:test";
import { batchFromColumns, deserializeBatch, field, int64, iterRows, schema, serializeBatch, utf8 } from "../arrow/index.js";
import { Arguments } from "../arguments/arguments.js";
import { decodeScanFunctionResult } from "../catalog/interface.js";
import { ReadOnlyCatalogInterface } from "../catalog/read-only.js";
import type { TableDescriptor } from "../catalog/descriptors.js";
import { CatalogReadOnlyError } from "../errors.js";
import { FunctionRegistry } from "../functions/registry.js";
import { defineTableFunction } from "../functions/table.js";
import { defineTableInOutFunction } from "../functions/table-in-out.js";
import {
  rowIdField,
  supportsWriteResultMode,
  userColumns,
  validateWriteResultModes,
  writeChangesBatch,
  writeOptions,
  writeResultBatch,
  writeResultSchema,
} from "../write-results.js";

const USER = schema([field("id", int64(), true), field("name", utf8(), true)]);
const COLUMNS = schema([...USER.fields, rowIdField()]);

const scan = defineTableFunction<Record<string, never>, null>({
  name: "items_scan",
  description: "scan",
  onBind: () => ({ outputSchema: COLUMNS }),
  process: (_p, _s, out) => out.finish(),
});
const writer = (name: string) => defineTableInOutFunction({ name, description: name });
const insert = writer("items_insert");
const update = writer("items_update");
const del = writer("items_delete");

function catalogWith(tables: TableDescriptor[]) {
  return new ReadOnlyCatalogInterface({ name: "w", schemas: [{ name: "main", functions: [scan], tables }] }, new FunctionRegistry());
}

const writable = (extra: Partial<TableDescriptor> = {}): TableDescriptor => ({
  name: "items",
  columns: COLUMNS,
  function: scan,
  insertFunction: insert,
  updateFunction: update,
  deleteFunction: del,
  ...extra,
});

describe("descriptor validation (vgi-python Table.__post_init__)", () => {
  test("UPDATE and DELETE need a scan function for row ids", () => {
    expect(() => catalogWith([{ name: "t", columns: COLUMNS, updateFunction: update }])).toThrow(/require a scan function/);
    expect(() => catalogWith([{ name: "t", columns: COLUMNS, deleteFunction: del }])).toThrow(/require a scan function/);
    expect(() => catalogWith([{ name: "t", columns: COLUMNS, insertFunction: insert }])).not.toThrow();
  });

  test("a mode needs its function, and modes/operations must be known", () => {
    expect(() => catalogWith([{ name: "t", columns: COLUMNS, function: scan, writeResultModes: { update: "rows" } }])).toThrow(
      /without a corresponding updateFunction/,
    );
    expect(() => catalogWith([writable({ writeResultModes: { insert: "everything" as never } })])).toThrow(/Unknown write result mode/);
    expect(() => validateWriteResultModes({ upsert: "count" })).toThrow(/Unknown write operation/);
  });
});

describe("TableInfo advertises write support", () => {
  const info = async (t: TableDescriptor) => (await catalogWith([t]).tableGet(new Uint8Array(), ["main"], t.name))!;

  test("configured operations default to count; explicit maxima are kept", async () => {
    expect((await info(writable())).write_result_modes).toEqual({ insert: "count", update: "count", delete: "count" });
    expect((await info(writable({ writeResultModes: { update: "changes" } }))).write_result_modes).toEqual({
      insert: "count",
      update: "changes",
      delete: "count",
    });
    expect((await info({ name: "ro", columns: COLUMNS, function: scan })).write_result_modes).toEqual({});
  });

  test("write functions are inlined; an absent one stays null", async () => {
    const { deleteFunction: _omitted, ...noDelete } = writable();
    const t = await info(noDelete);
    const inlined = [...iterRows(deserializeBatch(t.update_function!))][0] as Record<string, unknown>;
    expect(decodeScanFunctionResult(inlined).functionName).toBe("items_update");
    expect(t.delete_function).toBeNull();
  });
});

describe("catalog_table_*_function_get", () => {
  const cat = catalogWith([writable(), { name: "readonly", columns: COLUMNS, function: scan }]);
  const attach = new Uint8Array();

  test("resolves each operation's function (table name case-insensitively)", () => {
    expect(cat.tableInsertFunctionGet(attach, ["main"], "items").function_name).toBe("items_insert");
    expect(cat.tableUpdateFunctionGet(attach, ["main"], "ITEMS").function_name).toBe("items_update");
    expect(cat.tableDeleteFunctionGet(attach, ["main"], "items").function_name).toBe("items_delete");
  });

  test("a read-only table refuses, and an unknown table is not found", () => {
    expect(() => cat.tableUpdateFunctionGet(attach, ["main"], "readonly")).toThrow(CatalogReadOnlyError);
    expect(() => cat.tableInsertFunctionGet(attach, ["main"], "missing")).toThrow(/not found/);
  });

  test("write functions are registered for dispatch without being listed", () => {
    const registry = new FunctionRegistry();
    new ReadOnlyCatalogInterface({ name: "w", schemas: [{ name: "main", functions: [scan], tables: [writable()] }] }, registry);
    expect(registry.get("items_update")).toBe(update);
  });
});

describe("write results", () => {
  test("modes are ordered count < rows < changes", () => {
    expect(supportsWriteResultMode("changes", "rows")).toBe(true);
    expect(supportsWriteResultMode("rows", "changes")).toBe(false);
    expect(supportsWriteResultMode("count", "nope")).toBe(false);
  });

  test("result schemas per mode; user columns drop the row id", () => {
    expect(userColumns(COLUMNS).fields.map((f) => f.name)).toEqual(["id", "name"]);
    expect(writeResultSchema("count", USER).fields.map((f) => [f.name, f.nullable])).toEqual([["count", false]]);
    expect(writeResultSchema("rows", USER)).toBe(USER);
    expect(writeResultSchema("changes", USER).fields.map((f) => f.name)).toEqual(["old", "new"]);
  });

  test("changes pair old and new images; rows shows the written image", () => {
    const old = [{ id: 1n, name: "a" }, null];
    const neu = [{ id: 1n, name: "b" }, { id: 2n, name: "c" }];
    const changes = [...iterRows(writeChangesBatch(USER, old, neu))];
    expect(changes[0]?.old?.name).toBe("a");
    expect(changes[1]?.old).toBeNull();
    expect(changes[1]?.new?.id).toBe(2n);
    expect(() => writeChangesBatch(USER, [null], [])).toThrow(/equal length/);
    // A delete returns the deleted row.
    expect([...iterRows(writeResultBatch("rows", USER, [{ id: 9n, name: "z" }], [null]))]).toEqual([{ id: 9n, name: "z" }]);
    expect([...iterRows(writeResultBatch("count", USER, [null, null], [{}, {}]))]).toEqual([{ count: 2n }]);
  });

  test("write_options decodes the mode DuckDB asked for", () => {
    const opts = batchFromColumns(
      { result_mode: ["changes"], on_conflict: ["throw"] },
      schema([field("result_mode", utf8(), true), field("on_conflict", utf8(), true)]),
    );
    const bindCall = { arguments: new Arguments([], new Map([["write_options", serializeBatch(opts)]])) };
    expect(writeOptions(bindCall)).toEqual({ resultMode: "changes", onConflict: "throw", onConflictColumns: [] });
    expect(writeOptions({ arguments: new Arguments() }).resultMode).toBe("count");
  });
});
