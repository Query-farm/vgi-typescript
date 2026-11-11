// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Protocol 3.3 write-path wire changes, and the two fixtures that exercise
// them in the extension's SQL suite:
//
//   1. catalog_table_scan_function_get is gone; catalog_table_scan_branches_get
//      is the only scan-discovery RPC (a catalog's tableScanFunctionGet keeps
//      working behind it). The client lost its tableScanFunctionGet too.
//   2. catalog_table_create answers `created: bool` (false when IF NOT EXISTS
//      found the table), so CTAS does not append into an existing table.
//   3. write_options carries only result_mode (see writable-tables.test.ts).
//
// The txfix fixture (examples/writable-tx-worker.ts) grew items_insert /
// items_delete with a finalize stage, staging, `qty DEFAULT 0` and writes();
// simple_writable (examples/simple-writable-worker.ts) grew CREATE TABLE /
// CTAS / DROP TABLE.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { deserializeSchema, field, int32, schema, serializeSchema, utf8 } from "../arrow/index.js";
import { decodeScanBranchesResult, singleBranchResult, encodeScanFunctionResult } from "../catalog/interface.js";
import { DEFAULT_EXPRESSION_KEY, validateEncoded } from "../catalog/column-expressions.js";
import { encodeASD } from "../codec/asd.js";
import { CatalogTableCreateResultSchema, TableCreateRequestSchema } from "../generated/vgi-protocol-schemas.js";
import { FunctionRegistry } from "../functions/registry.js";
import { buildVgiProtocol } from "../protocol/dispatch.js";
import { catalogTableHandlers } from "../protocol/handlers/catalog/table.js";
import { unwrapResult } from "../client/protocol.js";
import { VgiClient } from "../client/client.js";
import { CatalogInterface, type CatalogAttachResult } from "../catalog/interface.js";
import { serializeBatch, batchFromColumns } from "../util/arrow/index.js";
import {
  applyDelete,
  applyInsert,
  applyUpdate,
  commitStaged,
  descriptor as txfixDescriptor,
  logWriteCall,
  seed,
  TxfixCatalog,
  visibleItems,
  withState,
} from "../../examples/writable-tx-worker.js";
import { descriptor as simpleDescriptor, SimpleWritableCatalog } from "../../examples/simple-writable-worker.js";

const ATTACH = new Uint8Array([3, 3, 3]);

// ------------------------------------------------------------ wire change 1

describe("catalog_table_scan_function_get is removed (protocol 3.3)", () => {
  test("the worker no longer hosts it; scan_branches_get it does", () => {
    const protocol = buildVgiProtocol({ registry: new FunctionRegistry() }) as any;
    const methods: Map<string, unknown> = protocol._methods ?? protocol.methods;
    expect(methods.has("catalog_table_scan_function_get")).toBe(false);
    expect(methods.has("catalog_table_scan_branches_get")).toBe(true);
    expect("catalogTableScanFunctionGet" in catalogTableHandlers(() => null as any)).toBe(false);
  });

  test("a catalog's tableScanFunctionGet still answers scan_branches_get as one branch", async () => {
    class OneScan extends CatalogInterface {
      catalogs() { return ["c"]; }
      attach(): CatalogAttachResult { return { attach_opaque_data: ATTACH } as any; }
      detach() {}
      version() { return 1; }
      schemas() { return []; }
      override tableScanFunctionGet() {
        return {
          function_name: "the_scan",
          arguments: serializeBatch(batchFromColumns({ arg_0: ["x"] }, schema([field("arg_0", utf8(), true)]))),
          required_extensions: ["ext"],
          schema_path: ["main"],
        };
      }
    }
    const handlers = catalogTableHandlers(() => new OneScan()) as any;
    const reply = await handlers.catalogTableScanBranchesGet(
      { attach_opaque_data: ATTACH, schema_path: ["main"], name: "t", at_unit: null, at_value: null, transaction_opaque_data: null, pushdown_filters: null },
      {},
    );
    const decoded = decodeScanBranchesResult(unwrapResult(reply));
    expect(decoded.requiredExtensions).toEqual(["ext"]);
    expect(decoded.branches.length).toBe(1);
    expect(decoded.branches[0]!.functionName).toBe("the_scan");
    expect(decoded.branches[0]!.positionalArguments).toEqual(["x"]);
    expect(decoded.branches[0]!.schemaPath).toEqual(["main"]);
    expect(decoded.branches[0]!.formatName).toBeNull();
  });

  test("decodeScanBranchesResult reads the shim's wire dict", () => {
    const wire = singleBranchResult({
      function_name: "f",
      arguments: serializeBatch(batchFromColumns({ table: ["t1"] }, schema([field("table", utf8(), true)]))),
      required_extensions: [],
      schema_path: null,
    });
    const decoded = decodeScanBranchesResult(wire as any);
    expect(decoded.branches[0]!.functionName).toBe("f");
    expect(decoded.branches[0]!.namedArguments).toEqual({ table: "t1" });
    expect(decoded.branches[0]!.writable).toBe(false);
  });

  test("the client has tableScanBranchesGet and no tableScanFunctionGet", () => {
    expect(typeof (VgiClient.prototype as any).tableScanBranchesGet).toBe("function");
    expect((VgiClient.prototype as any).tableScanFunctionGet).toBeUndefined();
  });
});

// ------------------------------------------------------------ wire change 2

describe("catalog_table_create answers created (protocol 3.3)", () => {
  class Creating extends CatalogInterface {
    constructor(private readonly answer: unknown) { super(); }
    catalogs() { return ["c"]; }
    attach(): CatalogAttachResult { return { attach_opaque_data: ATTACH } as any; }
    detach() {}
    version() { return 1; }
    schemas() { return []; }
    override tableCreate(): any { return this.answer; }
  }
  const request = (onConflict: string) => ({
    request: encodeASD(TableCreateRequestSchema, {
      attach_opaque_data: ATTACH, schema_path: ["main"], name: "n",
      columns: serializeSchema(schema([field("a", int32(), true)])), on_conflict: onConflict,
      constraint_definitions: [], transaction_opaque_data: null, comment: null, tags: null, options: null, layout: null,
    }),
  });
  const created = async (answer: unknown, onConflict = "ERROR") =>
    unwrapResult(await (catalogTableHandlers(() => new Creating(answer)) as any).catalogTableCreate(request(onConflict), {})).created;

  test("nothing or true means created; false means IF NOT EXISTS found it", async () => {
    expect(await created(undefined)).toBe(true);
    expect(await created(true)).toBe(true);
    expect(await created(false, "IGNORE")).toBe(false);
  });

  test("anything but a boolean is refused", async () => {
    await expect(created("yes")).rejects.toThrow(/must return a boolean/);
  });

  test("the reply schema is the generated one", () => {
    expect(CatalogTableCreateResultSchema.fields.map((f) => [f.name, f.nullable])).toEqual([["created", false]]);
  });
});

// ------------------------------------------------------------- txfix fixture

describe("txfix fixture: items_insert / items_delete, staging, writes()", () => {
  let dir: string;
  const prev = process.env.VGI_TXFIX_STATE_DIR;
  beforeAll(() => {
    dir = mkdtempSync(join(tmpdir(), "txfix-unit-"));
    process.env.VGI_TXFIX_STATE_DIR = dir;
  });
  afterAll(() => {
    if (prev === undefined) delete process.env.VGI_TXFIX_STATE_DIR;
    else process.env.VGI_TXFIX_STATE_DIR = prev;
    rmSync(dir, { recursive: true, force: true });
  });
  const ids = (part: string, tx: string | null) => withState((db) => [...visibleItems(db, part, tx).keys()]);
  const row = (id: number, name: string | null, qty: bigint | null = 0n) => ({ id, name, qty });

  test("the descriptor: three write functions with a finalize stage, qty DEFAULT 0, writes() listed", async () => {
    const items = txfixDescriptor.schemas[0]!.tables!.find((t) => t.name === "items")!;
    for (const fn of [items.insertFunction, items.updateFunction, items.deleteFunction]) {
      expect(fn?.meta.hasFinalize).toBe(true);
    }
    expect(items.writeResultModes).toEqual({ insert: "changes", update: "changes", delete: "changes" });
    const registry = new FunctionRegistry();
    const cat = new TxfixCatalog(txfixDescriptor, registry);
    const info = await cat.tableGet(ATTACH, ["main"], "items");
    const qty = deserializeSchema(info!.columns).fields.find((f) => f.name === "qty")!;
    // Canonical vgi.column.default.v1: base64 of one SqlExpression record whose SQL is "0".
    const encoded = qty.metadata?.get(DEFAULT_EXPRESSION_KEY);
    expect(encoded).toBeDefined();
    expect(validateEncoded(encoded!, ["main"])).toBe("0");
    expect(info!.write_result_modes).toEqual({ insert: "changes", update: "changes", delete: "changes" });
    const listed = new Map(cat.schemaContentsFunctions(ATTACH, ["main"], "TABLE_FUNCTION").map((f) => [f.name, f]));
    for (const name of ["items_insert", "items_update", "items_delete"]) {
      expect(listed.get(name)?.visibility).toBe("table_only");
      expect(listed.get(name)?.has_finalize).toBe(true);
    }
    expect(listed.get("writes")?.visibility ?? "public").toBe("public");
    // labels stays read-only.
    const labels = txfixDescriptor.schemas[0]!.tables!.find((t) => t.name === "labels")!;
    expect(labels.insertFunction ?? labels.updateFunction ?? labels.deleteFunction).toBeUndefined();
  });

  test("plain attach: insert validates the whole batch before applying any", () => {
    const part = "unit:plain-insert";
    withState((db) => seed(db, part, true));
    withState((db) => applyInsert(db, part, null, [row(4, "d"), row(5, "e", 7n)]));
    expect(ids(part, null)).toEqual([1, 2, 3, 4, 5]);
    for (const [rows, message] of [
      [[row(6, "f"), row(1, "dup")], /txfix: duplicate id 1/],
      [[row(6, "f"), row(6, "f2")], /txfix: duplicate id 6/],
      [[row(6, "f"), row(7, null)], /txfix: name required/],
      [[row(6, "f"), row(7, "g", -1n)], /txfix: qty must be >= 0/],
    ] as const) {
      expect(() => withState((db) => applyInsert(db, part, null, rows as any))).toThrow(message);
      expect(ids(part, null)).toEqual([1, 2, 3, 4, 5]);
    }
  });

  test("delete: rowid = id, a missing id is a no-op, a repeated one counts once", () => {
    const part = "unit:plain-delete";
    withState((db) => seed(db, part, true));
    const deleted = withState((db) => applyDelete(db, part, null, [2, 2, 99]));
    expect(deleted.map((r) => r.id)).toEqual([2]);
    expect(ids(part, null)).toEqual([1, 3]);
  });

  test("transactional attach: staged inserts/deletes, read-your-writes, commit applies them", () => {
    const part = "unit:tx";
    const tx = "aa";
    withState((db) => seed(db, part, true));
    withState((db) => applyInsert(db, part, tx, [row(4, "d")]));
    withState((db) => applyDelete(db, part, tx, [1]));
    // Update and delete of a staged insert edit the staged state.
    withState((db) => applyUpdate(db, part, tx, [4], [{ qty: 40n }]));
    withState((db) => applyInsert(db, part, tx, [row(5, "e")]));
    withState((db) => applyDelete(db, part, tx, [5]));
    expect(ids(part, tx)).toEqual([2, 3, 4]);
    expect(ids(part, null)).toEqual([1, 2, 3]);
    // A staged id is a duplicate within the transaction; a staged-deleted one is free again.
    expect(() => withState((db) => applyInsert(db, part, tx, [row(4, "x")]))).toThrow(/duplicate id 4/);
    expect(withState((db) => commitStaged(db, part, tx))).toBeNull();
    expect(ids(part, null)).toEqual([2, 3, 4]);
    expect(withState((db) => visibleItems(db, part, null).get(4)?.qty)).toBe(40n);
  });

  test("transactional attach: a negative qty is accepted when staged and refused at commit, discarding everything", () => {
    const part = "unit:tx-refused";
    const tx = "bb";
    withState((db) => seed(db, part, true));
    withState((db) => applyInsert(db, part, tx, [row(4, "d", -5n)]));
    withState((db) => applyDelete(db, part, tx, [1]));
    expect(withState((db) => commitStaged(db, part, tx))).toBe("txfix: commit refused: qty -5 for id 4");
    expect(ids(part, null)).toEqual([1, 2, 3]);
    expect(ids(part, tx)).toEqual([1, 2, 3]);
  });

  test("writes(): one row per finished call, in finalize order; reset clears it", () => {
    const part = "unit:writes";
    withState((db) => seed(db, part, true));
    withState((db) => {
      logWriteCall(db, part, "insert", null, 3000);
      logWriteCall(db, part, "update", "ab", 1);
      logWriteCall(db, part, "delete", "ab", 0);
    });
    const log = () =>
      withState((db) => db.query("SELECT op, tx, rows, finalized FROM write_calls WHERE part = ? ORDER BY seq").all(part) as any[]).map(
        (r) => [r.op, r.tx, Number(r.rows), Number(r.finalized)],
      );
    expect(log()).toEqual([
      ["insert", null, 3000, 1],
      ["update", "ab", 1, 1],
      ["delete", "ab", 0, 1],
    ]);
    withState((db) => seed(db, part, true));
    expect(log()).toEqual([]);
  });
});

// ------------------------------------------------------ simple_writable CTAS

describe("simple_writable fixture: CREATE TABLE reports created", () => {
  const cols = serializeSchema(schema([field("val", int32(), true), field("label", utf8(), true)]));

  test("create, IF NOT EXISTS, OR REPLACE, listing, insert function, drop", async () => {
    const registry = new FunctionRegistry();
    const cat = new SimpleWritableCatalog(simpleDescriptor, registry);
    const attach = (await cat.attach("simple_writable")).attach_opaque_data as Uint8Array;
    const v0 = cat.version(attach);
    expect(cat.tableCreate(attach, ["main"], "made", cols, "ERROR")).toBe(true);
    expect(cat.version(attach)).toBeGreaterThan(v0);
    expect(cat.tableCreate(attach, ["main"], "made", cols, "IGNORE")).toBe(false);
    expect(() => cat.tableCreate(attach, ["main"], "made", cols, "ERROR")).toThrow(/already exists/i);
    expect(cat.tableCreate(attach, ["main"], "made", cols, "REPLACE")).toBe(true);
    // Built-in tables: IF NOT EXISTS leaves them alone; anything else is refused.
    expect(cat.tableCreate(attach, ["main"], "items", cols, "IGNORE")).toBe(false);
    expect(() => cat.tableCreate(attach, ["main"], "items", cols, "REPLACE")).toThrow(/already exists/i);

    const info = await cat.tableGet(attach, ["main"], "made");
    expect(deserializeSchema(info!.columns).fields.map((f) => f.name)).toEqual(["val", "label", "rowid"]);
    expect(info!.write_result_modes).toEqual({ insert: "changes" });
    expect(info!.insert_function).toBeNull();
    expect(cat.tableInsertFunctionGet(attach, ["main"], "made").function_name).toBe("simple_writable_created_insert");
    expect(cat.tableInsertFunctionGet(attach, ["main"], "items").function_name).toBe("simple_writable_items_insert");
    const fn = cat.schemaContentsFunctions(attach, ["main"], "TABLE_FUNCTION").find((f) => f.name === "simple_writable_created_insert");
    expect(fn?.visibility).toBe("table_only");

    cat.tableDrop(attach, ["main"], "made");
    expect(await cat.tableGet(attach, ["main"], "made")).toBeNull();
    expect(() => cat.tableDrop(attach, ["main"], "made")).toThrow(/not found|does not exist/i);
    cat.tableDrop(attach, ["main"], "made", true);
    expect(() => cat.tableDrop(attach, ["main"], "items")).toThrow(/cannot be dropped/);
  });

  test("unsupported column types are refused at CREATE", async () => {
    const cat = new SimpleWritableCatalog(simpleDescriptor, new FunctionRegistry());
    const attach = (await cat.attach("simple_writable")).attach_opaque_data as Uint8Array;
    const { list } = await import("../arrow/index.js");
    const bad = serializeSchema(schema([field("xs", list(field("item", utf8(), true)), true)]));
    expect(() => cat.tableCreate(attach, ["main"], "bad", bad, "ERROR")).toThrow(/not supported/);
  });

  test("the scan of a created table is inlined with its table argument", async () => {
    const cat = new SimpleWritableCatalog(simpleDescriptor, new FunctionRegistry());
    const attach = (await cat.attach("simple_writable")).attach_opaque_data as Uint8Array;
    cat.tableCreate(attach, ["main"], "scanned", cols, "ERROR");
    const info = await cat.tableGet(attach, ["main"], "scanned");
    const expected = encodeScanFunctionResult(
      "simple_writable_scan",
      serializeBatch(batchFromColumns({ table: ["scanned"] }, schema([field("table", utf8(), true)]))),
      [],
      ["main"],
    );
    expect(info!.scan_function).toEqual(expected);
  });
});
