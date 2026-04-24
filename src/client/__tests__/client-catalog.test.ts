// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// End-to-end VgiClient tests against vgi-python's vgi-example-http worker.
//
// Scope: read-only catalog methods (listing, discovery, single-item getters).
// Writable ops (schemaCreate, tableCreate, transactionBegin, etc.) are
// deferred — the ReadOnlyCatalogInterface the example worker uses doesn't
// support them, and the wider writable test gaps are tracked separately.
//
// Runs one worker for the whole file (beforeAll/afterAll). Auto-skips when
// vgi-python isn't available alongside vgi-typescript.

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import {
  pythonHttpWorkerAvailable,
  startPythonHttpWorker,
  type PythonHttpWorkerHandle,
} from "./helpers/python-http-worker.js";
import { VgiClient } from "../../index.js";

const skip = !pythonHttpWorkerAvailable();

let handle: PythonHttpWorkerHandle;
let client: VgiClient;
let attachId: Uint8Array;

// Long timeout: `uv run` + Python import + waitress startup can easily take
// 10+ seconds on a cold cache.
beforeAll(async () => {
  if (skip) return;
  handle = await startPythonHttpWorker();
  client = handle.client;
  const result = await client.catalogAttach("example");
  attachId = result.attach_id;
}, 30_000);

afterAll(async () => {
  if (skip) return;
  try {
    if (attachId) await client.catalogDetach(attachId);
  } catch { /* already detached / server gone */ }
  await handle?.stop();
});

// ============================================================================
// catalog_catalogs / catalog_attach / catalog_detach / catalog_version
// ============================================================================

describe.skipIf(skip)("VgiClient — catalog discovery and attach", () => {
  test("catalogsInfo() returns the example catalog", async () => {
    const infos = await client.catalogsInfo();
    expect(infos.map((i) => i.name).sort()).toContain("example");
    const ex = infos.find((i) => i.name === "example")!;
    // vgi-python's ReadOnlyCatalogInterface advertises both fields as null
    // for non-versioned catalogs.
    expect(ex.implementation_version ?? null).toBeNull();
    expect(ex.data_version_spec ?? null).toBeNull();
  });

  test("catalogs() returns just names", async () => {
    const names = await client.catalogs();
    expect(names).toContain("example");
  });

  test("catalogVersion returns a non-negative int", async () => {
    const v = await client.catalogVersion(attachId);
    expect(typeof v).toBe("number");
    expect(v).toBeGreaterThanOrEqual(0);
  });

  test("catalogAttach on a second name is rejected", async () => {
    await expect(client.catalogAttach("does-not-exist")).rejects.toThrow();
  });

  test("attach_id bytes length is sensible", () => {
    expect(attachId.byteLength).toBeGreaterThan(0);
  });
});

// ============================================================================
// schemas / schemaGet
// ============================================================================

describe.skipIf(skip)("VgiClient — schemas", () => {
  test("schemas() lists at least 'main' and 'data'", async () => {
    const schemas = await client.schemas(attachId);
    const names = schemas.map((s) => s.name).sort();
    expect(names).toContain("main");
    expect(names).toContain("data");
  });

  test("schemaGet('main') returns a schema with the right name", async () => {
    const main = await client.schemaGet(attachId, "main");
    expect(main).not.toBeNull();
    expect(main!.name).toBe("main");
  });

  test("schemaGet('not-real') returns null", async () => {
    const nope = await client.schemaGet(attachId, "not-a-real-schema");
    expect(nope).toBeNull();
  });

  test("each SchemaInfo carries a byte attach_id", async () => {
    const schemas = await client.schemas(attachId);
    for (const s of schemas) {
      expect(s.attach_id).toBeInstanceOf(Uint8Array);
    }
  });
});

// ============================================================================
// schemaContentsTables / tableGet
// ============================================================================

describe.skipIf(skip)("VgiClient — tables", () => {
  test("schemaContentsTables('data') returns the expected tables", async () => {
    const tables = await client.schemaContentsTables(attachId, "data");
    const names = tables.map((t) => t.name);
    // A handful we know exist — don't hard-pin the full list (too brittle).
    for (const expected of ["large_sequence", "versioned_data", "numbers", "departments"]) {
      expect(names).toContain(expected);
    }
    // Schema name round-trip
    for (const t of tables) expect(t.schema_name).toBe("data");
  });

  test("tableGet('data', 'numbers') returns a populated TableInfo", async () => {
    const t = await client.tableGet(attachId, "data", "numbers");
    expect(t).not.toBeNull();
    expect(t!.name).toBe("numbers");
    expect(t!.schema_name).toBe("data");
    expect(t!.columns).toBeInstanceOf(Uint8Array);
    expect(t!.columns.byteLength).toBeGreaterThan(0);
  });

  test("tableGet on unknown table returns null", async () => {
    const t = await client.tableGet(attachId, "data", "not-a-real-table");
    expect(t).toBeNull();
  });

  test("schemaContentsTables on unknown schema returns [] ", async () => {
    const tables = await client.schemaContentsTables(attachId, "no-such-schema");
    expect(tables).toEqual([]);
  });
});

// ============================================================================
// schemaContentsViews / viewGet  (example worker has no views → empty)
// ============================================================================

describe.skipIf(skip)("VgiClient — views", () => {
  test("schemaContentsViews('data') returns an array (may be empty)", async () => {
    const views = await client.schemaContentsViews(attachId, "data");
    expect(Array.isArray(views)).toBe(true);
  });

  test("viewGet on an unknown view returns null", async () => {
    const v = await client.viewGet(attachId, "data", "definitely-not-a-view");
    expect(v).toBeNull();
  });
});

// ============================================================================
// schemaContentsFunctions — SCALAR_FUNCTION / TABLE_FUNCTION filters
// ============================================================================

describe.skipIf(skip)("VgiClient — functions", () => {
  test("schemaContentsFunctions('main', 'SCALAR_FUNCTION') returns scalars only", async () => {
    const scalars = await client.schemaContentsFunctions(attachId, "main", "SCALAR_FUNCTION");
    expect(scalars.length).toBeGreaterThan(0);
    const names = scalars.map((f) => f.name);
    // Known scalar from the example worker.
    expect(names).toContain("double");
    // Must not contain a table function
    expect(names).not.toContain("sequence");
    for (const f of scalars) expect(f.function_type).toBe("SCALAR");
  });

  test("schemaContentsFunctions('main', 'TABLE_FUNCTION') returns tables only", async () => {
    const tables = await client.schemaContentsFunctions(attachId, "main", "TABLE_FUNCTION");
    expect(tables.length).toBeGreaterThan(0);
    const names = tables.map((f) => f.name);
    expect(names).toContain("sequence");
    // Must not contain a scalar
    expect(names).not.toContain("double");
    for (const f of tables) expect(f.function_type).toBe("TABLE");
  });

  test("each FunctionInfo carries typed metadata fields", async () => {
    const fns = await client.schemaContentsFunctions(attachId, "main", "SCALAR_FUNCTION");
    const d = fns.find((f) => f.name === "double")!;
    expect(d).toBeDefined();
    expect(d.schema_name).toBe("main");
    expect(Array.isArray(d.examples ?? [])).toBe(true);
    expect(Array.isArray(d.categories ?? [])).toBe(true);
    expect(d.arguments).toBeInstanceOf(Uint8Array);
    expect(d.output_schema).toBeInstanceOf(Uint8Array);
  });
});

// ============================================================================
// schemaContentsMacros / macroGet — SCALAR_MACRO / TABLE_MACRO filters
// ============================================================================

describe.skipIf(skip)("VgiClient — macros", () => {
  test("schemaContentsMacros('main', 'SCALAR_MACRO') contains vgi_multiply + vgi_clamp", async () => {
    const macros = await client.schemaContentsMacros(attachId, "main", "SCALAR_MACRO");
    const names = macros.map((m) => m.name).sort();
    expect(names).toContain("vgi_multiply");
    expect(names).toContain("vgi_clamp");
    for (const m of macros) expect(m.macro_type).toBe("SCALAR");
  });

  test("schemaContentsMacros('main', 'TABLE_MACRO') contains vgi_range_table", async () => {
    const macros = await client.schemaContentsMacros(attachId, "main", "TABLE_MACRO");
    const names = macros.map((m) => m.name);
    expect(names).toContain("vgi_range_table");
    for (const m of macros) expect(m.macro_type).toBe("TABLE");
  });

  test("macroGet returns the right macro", async () => {
    const m = await client.macroGet(attachId, "main", "vgi_multiply");
    expect(m).not.toBeNull();
    expect(m!.name).toBe("vgi_multiply");
    expect(m!.parameters).toEqual(["x", "y"]);
    expect(m!.definition).toContain("*");
  });

  test("macroGet on unknown macro returns null", async () => {
    const m = await client.macroGet(attachId, "main", "not-a-real-macro");
    expect(m).toBeNull();
  });
});

// ============================================================================
// tableScanFunctionGet — the "how does DuckDB read this table" indirection
// ============================================================================

describe.skipIf(skip)("VgiClient — tableScanFunctionGet", () => {
  test("returns a record describing the scan function for a known table", async () => {
    const result = await client.tableScanFunctionGet(attachId, "data", "numbers");
    // Shape comes back as { result: <bytes> } unwrapped by VgiClient — the
    // outer call returns the inner dict. We only assert it's a non-null
    // object; its exact schema (function_name, arguments, required_extensions)
    // is an internal wire detail covered by DuckDB's integration tests.
    expect(result).toBeDefined();
    expect(typeof result).toBe("object");
  });
});
