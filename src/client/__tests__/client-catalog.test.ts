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

// ============================================================================
// Content fidelity: comments + tags should round-trip byte-exact from the
// Python catalog definitions. These tests explicitly target the Map<Utf8,Utf8>
// and nullable-Utf8 wire paths that a naive implementation is most likely to
// mangle.
// ============================================================================

describe.skipIf(skip)("VgiClient — content fidelity: comments and tags", () => {
  test("CatalogAttachResult tags round-trip all entries with exact values", async () => {
    // Attach fresh so we see the full tag map (our top-level fixture attached
    // early and discarded tags via the happy-path variant).
    const r = await client.catalogAttach("example");
    try {
      // Python side sets tags={"source": "vgi-example-worker", "version": "1"}.
      // Exact-match on both keys + values. Multi-entry Map was the place a
      // prior hand-written Map encoder silently produced {"0": "source,vgi-..."} —
      // this is the regression guard.
      expect(r.tags).toEqual({
        source: "vgi-example-worker",
        version: "1",
      });
      // Catalog-level comment must round-trip byte-exact.
      expect(r.comment).toBe("Example VGI catalog for testing");
    } finally {
      await client.catalogDetach(r.attach_id);
    }
  });

  test("schemaGet('main').comment round-trips exactly", async () => {
    const main = await client.schemaGet(attachId, "main");
    expect(main).not.toBeNull();
    expect(main!.comment).toBe("Example functions for testing VGI");
  });

  test("schemaGet('data').comment round-trips exactly", async () => {
    const data = await client.schemaGet(attachId, "data");
    expect(data).not.toBeNull();
    expect(data!.comment).toBe("Example tables backed by functions");
  });

  test("tableGet.comment round-trips exactly for a known table", async () => {
    const t = await client.tableGet(attachId, "data", "numbers");
    expect(t).not.toBeNull();
    expect(t!.comment).toBe("First 100 integers (demonstrates explicit columns)");
  });

  test("tableGet.tags is an empty plain object for tables without tags", async () => {
    // Most example tables don't set tags. The generic ASD Map decoder must
    // produce {} — not null, not Map, not some iterator shell.
    const t = await client.tableGet(attachId, "data", "numbers");
    expect(t).not.toBeNull();
    expect(t!.tags).toEqual({});
    expect(Object.prototype.toString.call(t!.tags)).toBe("[object Object]");
  });

  test("viewGet retrieves a view with its comment round-tripped", async () => {
    const v = await client.viewGet(attachId, "main", "first_ten");
    expect(v).not.toBeNull();
    expect(v!.name).toBe("first_ten");
    expect(v!.definition).toBe("SELECT * FROM sequence(10)");
    expect(v!.comment).toBe("First 10 integers");
  });

  test("schemaContentsViews('main') returns all views with exact comments", async () => {
    const views = await client.schemaContentsViews(attachId, "main");
    const byName = Object.fromEntries(views.map((v) => [v.name, v]));
    expect(byName["first_ten"]?.comment).toBe("First 10 integers");
    expect(byName["even_numbers"]?.comment).toBe("Even numbers from 0 to 98");
    expect(byName["first_ten"]?.definition).toBe("SELECT * FROM sequence(10)");
  });

  test("FunctionInfo.description round-trips for a known scalar", async () => {
    const scalars = await client.schemaContentsFunctions(attachId, "main", "SCALAR_FUNCTION");
    const d = scalars.find((f) => f.name === "double");
    expect(d).toBeDefined();
    expect(d!.description).toBe("Doubles numeric values");
  });

  test("FunctionInfo.description round-trips for a known table function", async () => {
    const fns = await client.schemaContentsFunctions(attachId, "main", "TABLE_FUNCTION");
    const seq = fns.find((f) => f.name === "sequence");
    expect(seq).toBeDefined();
    // Exact description depends on SequenceFunction; assert non-empty at
    // minimum (too-specific strings break under upstream copy tweaks).
    expect(typeof seq!.description).toBe("string");
    expect(seq!.description!.length).toBeGreaterThan(0);
  });

  test("MacroInfo.comment round-trips exactly", async () => {
    const m = await client.macroGet(attachId, "main", "vgi_multiply");
    expect(m).not.toBeNull();
    expect(m!.comment).toBe("Multiply two values");
  });

  test("MacroInfo.comment round-trips for the table-macro variant", async () => {
    const m = await client.macroGet(attachId, "main", "vgi_range_table");
    expect(m).not.toBeNull();
    expect(m!.comment).toBe("Table macro returning range of values");
  });

  test("SchemaInfo.tags is {} for schemas that don't set tags", async () => {
    const schemas = await client.schemas(attachId);
    for (const s of schemas) {
      // Python side defaults to empty dict.
      expect(s.tags).toEqual({});
    }
  });
});

// ============================================================================
// attach options: send a non-null options RecordBatch on the wire and
// verify (a) the worker accepts it, (b) malformed bytes don't crash the
// server. These exercise the `options?: Uint8Array` leg of catalogAttach
// that no prior test has touched.
// ============================================================================

describe.skipIf(skip)("VgiClient — catalogAttach options", () => {
  test("attach with zero-byte options is coerced to null (no server crash)", async () => {
    // REGRESSION: passing `new Uint8Array(0)` used to reach the Python
    // server as 0 bytes on a non-null binary field, which pyarrow's IPC
    // reader rejects with "Tried reading schema message, was null or
    // length 0". The client now coerces zero-byte options to null so a
    // natural "no options" gesture doesn't produce an opaque server
    // error. This test pins that behavior.
    const r = await client.catalogAttach("example", { optionsBytes: new Uint8Array(0) });
    try {
      expect(r.attach_id).toBeInstanceOf(Uint8Array);
      expect(r.attach_id.byteLength).toBeGreaterThan(0);
    } finally {
      await client.catalogDetach(r.attach_id);
    }
  });

  test("attach with undefined options is equivalent to zero-byte", async () => {
    // Same result path as above — explicit vs implicit "no options".
    const r = await client.catalogAttach("example");
    try {
      expect(r.attach_id.byteLength).toBeGreaterThan(0);
    } finally {
      await client.catalogDetach(r.attach_id);
    }
  });

  test("attach with a rich plain-object options bag succeeds over the wire", async () => {
    // The ExampleCatalog ignores options, but if the encoding is malformed
    // (wrong column types, zero-byte, etc.) the attach would fail before
    // reaching the handler. So a successful attach proves the
    // JS-value → RecordBatch → wire pipeline is correct end-to-end.
    const r = await client.catalogAttach("example", {
      options: {
        region: "us-east-1",
        maxRows: 10_000n,
        readOnly: true,
        ratio: 0.25,
        token: new Uint8Array([1, 2, 3, 4]),
        maybe: null,
      },
    });
    try {
      expect(r.attach_id.byteLength).toBeGreaterThan(0);
    } finally {
      await client.catalogDetach(r.attach_id);
    }
  });

  test("attach with empty options object is equivalent to no options", async () => {
    // Empty object → null on the wire (zero-column IPC would fail downstream).
    const r = await client.catalogAttach("example", { options: {} });
    try {
      expect(r.attach_id.byteLength).toBeGreaterThan(0);
    } finally {
      await client.catalogDetach(r.attach_id);
    }
  });

  test("passing both `options` and `optionsBytes` throws a client-side error", async () => {
    await expect(
      client.catalogAttach("example", {
        options: { x: "1" },
        optionsBytes: new Uint8Array([1, 2, 3]),
      }),
    ).rejects.toThrow(/cannot specify both/);
  });

  test("attach with malformed options bytes fails cleanly (does not hang)", async () => {
    // Random bytes that aren't valid Arrow IPC. The server should either
    // (a) error in deserialization or (b) the catalog handler ignores it.
    // Either is acceptable — the important invariant is we don't hang and
    // we get a typed result, not a torn stream.
    const junk = new Uint8Array([0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02, 0x03]);
    try {
      const r = await client.catalogAttach("example", { optionsBytes: junk });
      // If the server accepts it silently (common when options are ignored),
      // that's fine — clean up and move on.
      await client.catalogDetach(r.attach_id);
    } catch (err) {
      // If it fails, it must fail with a structured error, not a hang or
      // unhandled rejection. Just checking the promise rejected is enough.
      expect(err).toBeDefined();
    }
  });
});
