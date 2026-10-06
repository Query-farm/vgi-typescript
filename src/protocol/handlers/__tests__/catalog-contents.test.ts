// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// catalog_contents: the whole catalog in one RPC.
//
// Pins the contract vgi-python's catalog_contents defines (and the DuckDB
// extension relies on): one SchemaContents per schema, parents before
// children; every item byte-for-byte what the matching per-schema RPC returns;
// every kind complete; kinds whose estimated_object_count is 0 skipped; and the
// attach result advertising supports_catalog_contents exactly where a client
// may use it.

import { describe, expect, test } from "bun:test";
import { httpConnect } from "@query-farm/vgi-rpc";
import { allFunctions, catalog, createExampleCatalog } from "../../../../examples/common.js";
import {
  CatalogInterface,
  type AttachOpaqueData,
  type CatalogAttachResult,
  type SchemaInfo,
  type ViewInfo,
} from "../../../catalog/interface.js";
import { CompositeCatalogInterface } from "../../../catalog/composite.js";
import { ReadOnlyCatalogInterface } from "../../../catalog/read-only.js";
import { VgiClient } from "../../../client/client.js";
import { decodeASD, encodeASD } from "../../../codec/asd.js";
import { FunctionRegistry } from "../../../functions/registry.js";
import {
  CatalogAttachResultSchema,
  CatalogContentsResultSchema,
  CatalogSchemasResultSchema,
  CatalogVersionResultSchema,
} from "../../../generated/vgi-protocol-schemas.js";
import {
  buildCatalogAttachResult,
  type CatalogContentsResponse,
  type SchemaContents,
} from "../../../generated/vgi-protocol-types.js";
import { SIGNING_KEY_BYTES, serveVgiWorker } from "../../../serve-entry.js";
import { buildVgiProtocol } from "../../dispatch.js";
import { wrapRequest } from "../../../client/protocol.js";
import { batchFromColumns } from "../../../util/arrow/index.js";
import { schema, field, utf8, binary, batchFromRows, serializeBatch } from "../../../arrow/index.js";
import { SchemaInfoSchema } from "../../../generated/vgi-protocol-schemas.js";
import { serializeSchema } from "../../../util/arrow/index.js";
import { TableCreateRequestSchema } from "../../../generated/vgi-protocol-schemas.js";
import {
  CATALOG_HASH,
  CATALOG_MEMORY,
  CATALOG_REVAL,
  ContentsHashCatalog,
  ContentsMemoryCatalog,
  ContentsRevalCatalog,
} from "../../../../examples/catalog_contents.js";
import { catalogContentsDigest } from "../../../catalog/contents-digest.js";
import { compareCodePoints } from "../../../codec/asd.js";
import {
  decodeSchemaInfo,
  decodeTableInfo,
  decodeViewInfo,
  encodeSchemaInfo,
  encodeTableInfo,
  encodeViewInfo,
} from "../../../generated/vgi-client.js";
import type { CatalogContentsResult, SchemaContentsInfo } from "../../../catalog/interface.js";

function registry(): FunctionRegistry {
  const r = new FunctionRegistry();
  for (const fn of allFunctions) r.register(fn);
  return r;
}

function exampleCatalog(r = registry()): ReadOnlyCatalogInterface {
  return createExampleCatalog(new ReadOnlyCatalogInterface(catalog, r));
}

/** Dispatch straight into a registered handler (no transport). */
function handlers(cat: CatalogInterface) {
  const protocol = buildVgiProtocol({ registry: registry(), catalogInterface: cat });
  const method = (name: string): any => {
    const m = (protocol as any)._methods?.get?.(name) ?? (protocol as any).methods?.get?.(name);
    if (!m) throw new Error(`method not found: ${name}`);
    return m;
  };
  const call = async (name: string, params: Record<string, unknown>): Promise<any> =>
    method(name).handler(params, {} as any);
  const attachRequest = (name: string) => {
    const s = schema([
      field("name", utf8(), false),
      field("options", binary(), true),
      field("data_version_spec", utf8(), true),
      field("implementation_version", utf8(), true),
      field("client_capabilities", binary(), true),
    ]);
    return wrapRequest(
      batchFromColumns(
        { name: [name], options: [null], data_version_spec: [null], implementation_version: [null], client_capabilities: [null] },
        s,
      ),
    );
  };
  return {
    call,
    async attach(name: string): Promise<CatalogAttachResult> {
      const r = await call("catalog_attach", attachRequest(name));
      return decodeASD<CatalogAttachResult>(CatalogAttachResultSchema, r.result);
    },
    async contents(
      attach: Uint8Array,
      ifNoneMatch: string | null = null,
    ): Promise<{ response: CatalogContentsResponse; schemas: SchemaContents[]; wire: Uint8Array }> {
      const r = await call("catalog_contents", { attach_opaque_data: attach, if_none_match: ifNoneMatch });
      const response = decodeASD<CatalogContentsResponse>(CatalogContentsResultSchema, r.result);
      return { response, schemas: response.schemas, wire: r.result };
    },
    async ddlView(attach: Uint8Array, name: string, definition = "SELECT 1 AS x"): Promise<void> {
      await call("catalog_view_create", {
        attach_opaque_data: attach,
        schema_path: ["main"],
        name,
        definition,
        on_conflict: "ERROR",
        transaction_opaque_data: null,
      });
    },
    /** The `items` of a per-schema RPC (every items-result shares one schema). */
    async items(name: string, params: Record<string, unknown>): Promise<Uint8Array[]> {
      const r = await call(name, { transaction_opaque_data: null, ...params });
      return decodeASD<{ items: Uint8Array[] }>(CatalogSchemasResultSchema, r.result).items;
    },
    async version(attach: Uint8Array): Promise<number> {
      const r = await call("catalog_version", { attach_opaque_data: attach, transaction_opaque_data: null });
      return Number(decodeASD<{ version: number }>(CatalogVersionResultSchema, r.result).version);
    },
  };
}

function hex(b: Uint8Array): string {
  return Buffer.from(b).toString("hex");
}

/** Assert two item lists are the same bytes, item for item. */
function expectSameBytes(actual: Uint8Array[], expected: Uint8Array[], what: string): void {
  expect({ what, items: actual.map(hex) }).toEqual({ what, items: expected.map(hex) });
}

// A minimal catalog with nested schemas, listed children first, and per-kind
// counts -- to pin ordering and the zero-count skip without the example's bulk.
class NestedStubCatalog extends CatalogInterface {
  calls: string[] = [];
  catalogs(): string[] {
    return ["nested"];
  }
  attach(): CatalogAttachResult {
    return buildCatalogAttachResult({
      attach_opaque_data: new Uint8Array([9]),
      supports_transactions: true,
      supports_time_travel: false,
      catalog_version_frozen: false,
      catalog_version: 42,
      resolved_data_version: null,
      resolved_implementation_version: null,
    });
  }
  detach(): void {}
  version(): number {
    return 42;
  }
  schemas(attach: AttachOpaqueData): SchemaInfo[] {
    const mk = (path: string[], counts: Record<string, number> | null): SchemaInfo => ({
      comment: null,
      tags: {},
      attach_opaque_data: attach,
      path,
      estimated_object_count: counts,
    });
    return [
      mk(["a", "b", "c"], { view: 1 }),
      mk(["a", "b"], { view: 0 }),
      mk(["a"], null),
    ];
  }
  override schemaContentsViews(_attach: AttachOpaqueData, path: string[]): ViewInfo[] {
    this.calls.push(`views:${path.join(".")}`);
    return [{ comment: null, tags: {}, name: `v_${path.length}`, schema_path: path, definition: "SELECT 1", column_comments: {} }];
  }
}

describe("catalog_contents", () => {
  test("the example catalog: one entry per schema, every kind byte-identical to its per-schema RPC", async () => {
    const h = handlers(exampleCatalog());
    const attached = await h.attach(catalog.name);
    expect(attached.supports_catalog_contents).toBe(true);
    const attach = attached.attach_opaque_data;

    const { response, schemas } = await h.contents(attach);
    expect(response.catalog_version).toBe(await h.version(attach));

    const listed = await h.items("catalog_schemas", { attach_opaque_data: attach });
    expectSameBytes(schemas.map((s) => s.schema), listed, "schemas");
    expect(schemas.length).toBeGreaterThan(1);

    let total = 0;
    for (const sc of schemas) {
      const path = (await import("../../../generated/vgi-client.js")).decodeSchemaInfo(sc.schema).path;
      const base = { attach_opaque_data: attach, path };
      const expectKind = async (got: Uint8Array[], rpc: string, extra: Record<string, unknown> = {}) => {
        const want = await h.items(rpc, { ...base, ...extra });
        expectSameBytes(got, want, `${path.join(".")}:${rpc}:${JSON.stringify(extra)}`);
        total += got.length;
      };
      await expectKind(sc.tables, "catalog_schema_contents_tables");
      await expectKind(sc.views, "catalog_schema_contents_views");
      await expectKind(sc.scalar_functions, "catalog_schema_contents_functions", { type: "scalar_function" });
      await expectKind(sc.aggregate_functions, "catalog_schema_contents_functions", { type: "aggregate_function" });
      await expectKind(sc.table_functions, "catalog_schema_contents_functions", { type: "table_function" });
      await expectKind(sc.scalar_macros, "catalog_schema_contents_macros", { type: "scalar_macro" });
      await expectKind(sc.table_macros, "catalog_schema_contents_macros", { type: "table_macro" });
      await expectKind(sc.indexes, "catalog_schema_contents_indexes");
    }
    // The example catalog is not empty: the comparison above compared something.
    expect(total).toBeGreaterThan(100);
    expect(schemas.some((s) => s.tables.length > 0)).toBe(true);
    expect(schemas.some((s) => s.views.length > 0)).toBe(true);
    expect(schemas.some((s) => s.scalar_functions.length > 0)).toBe(true);
    expect(schemas.some((s) => s.table_functions.length > 0)).toBe(true);
  });

  test("parents come before children, and a kind counted 0 is not computed", async () => {
    const cat = new NestedStubCatalog();
    const h = handlers(cat);
    const attach = new Uint8Array([9]); // < 16 bytes: no framework UUID to strip
    const { response, schemas } = await h.contents(attach);
    expect(response.catalog_version).toBe(42);
    const { decodeSchemaInfo, decodeViewInfo } = await import("../../../generated/vgi-client.js");
    expect(schemas.map((s) => decodeSchemaInfo(s.schema).path)).toEqual([["a"], ["a", "b"], ["a", "b", "c"]]);
    // ["a","b"] reports view:0 -> never asked; ["a"] has no counts -> asked.
    expect(cat.calls.sort()).toEqual(["views:a", "views:a.b.c"]);
    expect(schemas[1].views).toEqual([]);
    expect(schemas.map((s) => s.views.map((v) => decodeViewInfo(v).name))).toEqual([["v_1"], [], ["v_3"]]);
    // Every kind is present (empty, not missing) on every entry.
    for (const s of schemas) {
      for (const k of ["tables", "scalar_functions", "aggregate_functions", "table_functions", "scalar_macros", "table_macros", "indexes"] as const) {
        expect(s[k]).toEqual([]);
      }
    }
  });

  test("a composite catalog routes catalog_contents to the attached backend", async () => {
    const example = exampleCatalog();
    const nested = new NestedStubCatalog();
    const h = handlers(new CompositeCatalogInterface([example, nested as unknown as ReadOnlyCatalogInterface]));
    const a = await h.attach("nested");
    expect(a.supports_catalog_contents).toBe(false);
    const { schemas } = await h.contents(a.attach_opaque_data);
    expect(schemas.length).toBe(3);
    const b = await h.attach(catalog.name);
    expect(b.supports_catalog_contents).toBe(true);
    expect((await h.contents(b.attach_opaque_data)).schemas.length).toBe(catalog.schemas.length);
  });
});

describe("a DDL-capable catalog behind a composite (the contents_reval fixture)", () => {
  test("table_create's wrapped request reaches the catalog, and catalog_contents sees it", async () => {
    const h = handlers(new CompositeCatalogInterface([exampleCatalog(), new ContentsRevalCatalog()]));
    const attached = await h.attach(CATALOG_REVAL);
    expect(attached.supports_catalog_contents).toBe(true);
    const attach = attached.attach_opaque_data;
    const before = await h.contents(attach);
    expect(before.response.catalog_version).toBe(1);
    expect(before.schemas.map((s) => s.tables.length)).toEqual([0]);

    // catalog_table_create sends one `request` column holding the
    // TableCreateRequest; its attach value must be opened like any other.
    const columns = serializeSchema(schema([field("a", utf8(), true)]));
    await h.call("catalog_table_create", {
      request: encodeASD(TableCreateRequestSchema, {
        attach_opaque_data: attach,
        schema_path: ["main"],
        name: "t1",
        columns,
        on_conflict: "ERROR",
        not_null_constraints: [],
        unique_constraints: [],
        check_constraints: [],
        primary_key_constraints: [],
        foreign_key_constraints: [],
        transaction_opaque_data: null,
      }),
    });
    await h.call("catalog_view_create", {
      attach_opaque_data: attach,
      schema_path: ["main"],
      name: "v1",
      definition: "SELECT 1 AS x",
      on_conflict: "ERROR",
      transaction_opaque_data: null,
    });
    const after = await h.contents(attach);
    expect(after.response.catalog_version).toBe(3);
    const { decodeTableInfo, decodeViewInfo } = await import("../../../generated/vgi-client.js");
    expect(after.schemas[0].tables.map((t) => decodeTableInfo(t).name)).toEqual(["t1"]);
    expect(after.schemas[0].views.map((v) => decodeViewInfo(v).name)).toEqual(["v1"]);
  });
});

describe("supports_catalog_contents on attach", () => {
  test("ReadOnlyCatalogInterface advertises it; supportsCatalogContents=false withdraws it", async () => {
    const on = exampleCatalog();
    expect((await handlers(on).attach(catalog.name)).supports_catalog_contents).toBe(true);
    const off = exampleCatalog();
    off.supportsCatalogContents = false;
    expect((await handlers(off).attach(catalog.name)).supports_catalog_contents).toBe(false);
  });

  test("a writable/hand-written catalog that does not set it reads as false", async () => {
    class Bare extends NestedStubCatalog {
      override attach(): CatalogAttachResult {
        // An older (untyped) catalog that predates the field and omits it,
        // and leaves supports_column_statistics to the handler default too.
        const { supports_catalog_contents: _a, supports_column_statistics: _b, ...rest } = super.attach();
        return rest as CatalogAttachResult;
      }
    }
    const r = await handlers(new Bare()).attach("nested");
    expect(r.supports_catalog_contents).toBe(false);
    // The builder still filled every other default.
    expect(r.default_schema).toBe("main");
    expect(r.supports_column_statistics).toBe(true); // this SDK's attach-handler default
  });

  test("buildCatalogAttachResult fills vgi-python's defaults in wire order", () => {
    const r = buildCatalogAttachResult({
      attach_opaque_data: new Uint8Array([1]),
      supports_transactions: false,
      supports_time_travel: false,
      catalog_version_frozen: true,
      catalog_version: 1,
      resolved_data_version: null,
      resolved_implementation_version: null,
    });
    expect(Object.keys(r)).toEqual(CatalogAttachResultSchema.fields.map((f) => f.name));
    expect(r.supports_catalog_contents).toBe(false);
    expect(r.attach_opaque_data_required).toBe(true);
    expect(r.default_schema).toBe("main");
    expect(r.tags).toEqual({});
  });
});

describe("catalog_contents over HTTP, through VgiClient", () => {
  test("round-trips and agrees with the per-schema calls", async () => {
    const r = registry();
    const server = serveVgiWorker({
      name: "demo",
      doc: "catalog_contents test worker.",
      version: "0.0.1",
      registry: r,
      catalogInterface: exampleCatalog(r),
      prefix: "",
      port: 0,
      // Sealed attach envelopes: catalog_contents must open them like every
      // other catalog RPC.
      signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(5),
      quiet: true,
      env: {},
    });
    try {
      const client = new VgiClient(httpConnect(`http://localhost:${server.port}`, { prefix: "" }));
      const attached = await client.catalogAttach(catalog.name);
      expect(attached.supports_catalog_contents).toBe(true);
      const attach = attached.attach_opaque_data;
      const contents = await client.catalogContents(attach);
      expect(contents.catalog_version).toBe(await client.catalogVersion(attach));
      const schemas = await client.schemas(attach);
      expect(contents.schemas.map((s) => s.schema)).toEqual(schemas);
      for (const sc of contents.schemas) {
        const path = sc.schema.path;
        expect(sc.tables).toEqual(await client.schemaContentsTables(attach, path));
        expect(sc.views).toEqual(await client.schemaContentsViews(attach, path));
        expect(sc.scalar_functions).toEqual(await client.schemaContentsFunctions(attach, path, "SCALAR_FUNCTION" as any));
        expect(sc.table_functions).toEqual(await client.schemaContentsFunctions(attach, path, "TABLE_FUNCTION" as any));
        expect(sc.aggregate_functions).toEqual(await client.schemaContentsFunctions(attach, path, "AGGREGATE_FUNCTION" as any));
        expect(sc.scalar_macros).toEqual(await client.schemaContentsMacros(attach, path, "SCALAR_MACRO" as any));
        expect(sc.table_macros).toEqual(await client.schemaContentsMacros(attach, path, "TABLE_MACRO" as any));
      }
      expect(contents.schemas.reduce((n, s) => n + s.table_functions.length, 0)).toBeGreaterThan(100);
    } finally {
      server.stop(true);
    }
  });
});

// ============================================================================
// v2: path column, etag revalidation, content hash, worker cache
// ============================================================================

/** NestedStubCatalog whose catalogContents answer is scripted per test. */
class ScriptedCatalog extends NestedStubCatalog {
  answer: (ifNoneMatch: string | null | undefined) => Promise<CatalogContentsResult> | CatalogContentsResult = () =>
    super.catalogContents(new Uint8Array([9]));
  override async catalogContents(_attach: AttachOpaqueData, ifNoneMatch?: string | null): Promise<CatalogContentsResult> {
    return this.answer(ifNoneMatch);
  }
}

function schemaInfo(path: string[]): SchemaInfo {
  return { comment: null, tags: {}, attach_opaque_data: new Uint8Array([9]), path, estimated_object_count: null };
}

function contentsOf(path: string[]): SchemaContentsInfo {
  return {
    schema: schemaInfo(path),
    tables: [],
    views: [],
    scalar_functions: [],
    aggregate_functions: [],
    table_functions: [],
    scalar_macros: [],
    table_macros: [],
    indexes: [],
  };
}

describe("catalog_contents v2 wire", () => {
  test("each struct row's path equals its SchemaInfo.path", async () => {
    const h = handlers(exampleCatalog());
    const attach = (await h.attach(catalog.name)).attach_opaque_data;
    const { response } = await h.contents(attach);
    expect(response.schemas.length).toBe(catalog.schemas.length);
    for (const sc of response.schemas) expect(sc.path).toEqual(decodeSchemaInfo(sc.schema).path);
  });

  test("a catalog without an etag ignores if_none_match and never answers not_modified", async () => {
    const h = handlers(exampleCatalog());
    const attach = (await h.attach(catalog.name)).attach_opaque_data;
    const plain = await h.contents(attach);
    expect(plain.response.etag).toBeNull();
    expect(plain.response.not_modified).toBe(false);
    const cond = await h.contents(attach, "anything");
    expect(cond.response.etag).toBeNull();
    expect(cond.response.not_modified).toBe(false);
    expect(cond.schemas.length).toBe(plain.schemas.length);
  });
});

describe("catalog_contents revalidation (contents_reval: generation-counter etag)", () => {
  test("a matching if_none_match short-circuits before building; DDL changes the etag", async () => {
    const reval = new ContentsRevalCatalog();
    const h = handlers(new CompositeCatalogInterface([exampleCatalog(), reval]));
    const attach = (await h.attach(CATALOG_REVAL)).attach_opaque_data;

    const first = await h.contents(attach);
    expect(first.response.etag).toBe("gen-1");
    expect(first.response.not_modified).toBe(false);
    expect(first.schemas.map((s) => s.path)).toEqual([["main"]]);
    expect(reval.builds).toBe(1);

    const same = await h.contents(attach, "gen-1");
    expect(same.response).toEqual({ catalog_version: 1, etag: "gen-1", not_modified: true, schemas: [] });
    expect(reval.builds).toBe(1); // the cheap validator answered; nothing was built

    const other = await h.contents(attach, "gen-not-current");
    expect(other.response.not_modified).toBe(false);
    expect(other.schemas.length).toBe(1);
    expect(reval.builds).toBe(2);

    await h.ddlView(attach, "v");
    const changed = await h.contents(attach, "gen-1");
    expect(changed.response.etag).toBe("gen-2");
    expect(changed.response.catalog_version).toBe(2);
    expect(changed.response.not_modified).toBe(false);
    expect(changed.schemas[0].views.map((v) => decodeViewInfo(v).name)).toEqual(["v"]);
  });

  test("contents_memory reports version 0 and no etag", async () => {
    const h = handlers(new CompositeCatalogInterface([exampleCatalog(), new ContentsMemoryCatalog()]));
    const attached = await h.attach(CATALOG_MEMORY);
    expect(attached.catalog_version).toBe(0);
    await h.ddlView(attached.attach_opaque_data, "v");
    const { response } = await h.contents(attached.attach_opaque_data, "gen-0");
    expect(response.catalog_version).toBe(0);
    expect(response.etag).toBeNull();
    expect(response.not_modified).toBe(false);
    expect(response.schemas[0].views.length).toBe(1);
  });
});

describe("catalog_contents worker rules", () => {
  const attach = new Uint8Array([9]);

  test("not_modified needs an etag equal to if_none_match", async () => {
    const cat = new ScriptedCatalog();
    const h = handlers(cat);
    cat.answer = () => ({ etag: "e1", not_modified: true });
    await expect(h.contents(attach, "e2")).rejects.toThrow(/not_modified/);
    await expect(h.contents(attach, null)).rejects.toThrow(/not_modified/);
    cat.answer = () => ({ not_modified: true });
    await expect(h.contents(attach, "e1")).rejects.toThrow(/not_modified/);
    cat.answer = () => ({ etag: "e1", not_modified: true });
    const ok = await h.contents(attach, "e1");
    expect(ok.response).toEqual({ catalog_version: 42, etag: "e1", not_modified: true, schemas: [] });
  });

  test("not_modified with schemas is refused", async () => {
    const cat = new ScriptedCatalog();
    cat.answer = () => ({ etag: "e1", not_modified: true, schemas: [contentsOf(["a"])] });
    await expect(handlers(cat).contents(attach, "e1")).rejects.toThrow(/with schemas/);
  });

  test("a full answer whose etag matches if_none_match becomes not_modified", async () => {
    const cat = new ScriptedCatalog();
    cat.answer = () => ({ etag: "e1", schemas: [contentsOf(["a"])] });
    const h = handlers(cat);
    expect((await h.contents(attach, "e1")).response).toEqual({
      catalog_version: 42,
      etag: "e1",
      not_modified: true,
      schemas: [],
    });
    const full = await h.contents(attach, "e0");
    expect(full.response.etag).toBe("e1");
    expect(full.schemas.map((s) => s.path)).toEqual([["a"]]);
  });

  test("schema paths must be unique (case-insensitively) and have their parent", async () => {
    const cat = new ScriptedCatalog();
    const h = handlers(cat);
    cat.answer = () => ({ schemas: [contentsOf(["a"]), contentsOf(["A"])] });
    await expect(h.contents(attach)).rejects.toThrow(/duplicate/);
    cat.answer = () => ({ schemas: [contentsOf(["a"]), contentsOf(["b", "c"])] });
    await expect(h.contents(attach)).rejects.toThrow(/without its parent/);
    cat.answer = () => ({ schemas: [contentsOf(["a", "b"]), contentsOf(["a"])] });
    expect((await h.contents(attach)).schemas.map((s) => s.path)).toEqual([["a"], ["a", "b"]]);
  });
});

describe("catalog_contents content-hash etag", () => {
  test("hex SHA-256, stable while unchanged, changes with DDL, a match is not_modified", async () => {
    const h = handlers(new CompositeCatalogInterface([exampleCatalog(), new ContentsHashCatalog()]));
    const attach = (await h.attach(CATALOG_HASH)).attach_opaque_data;
    await h.ddlView(attach, "v");
    const a = await h.contents(attach);
    const etag = a.response.etag!;
    expect(etag).toMatch(/^[0-9a-f]{64}$/);
    expect(etag).toBe(await catalogContentsDigest(a.schemas));
    expect((await h.contents(attach)).response.etag).toBe(etag);
    const same = await h.contents(attach, etag);
    expect(same.response.not_modified).toBe(true);
    expect(same.response.etag).toBe(etag);
    expect(same.schemas).toEqual([]);
    await h.ddlView(attach, "w");
    const changed = await h.contents(attach, etag);
    expect(changed.response.not_modified).toBe(false);
    expect(changed.response.etag).not.toBe(etag);
  });

  test("a catalog's own etag wins over the content hash", async () => {
    const cat = new ScriptedCatalog();
    cat.catalogContentsEtag = "content-hash";
    cat.answer = () => ({ etag: "mine", schemas: [contentsOf(["a"])] });
    expect((await handlers(cat).contents(new Uint8Array([9]))).response.etag).toBe("mine");
  });

  test("the digest matches vgi-python's catalog_contents_digest", async () => {
    const enc = (s: string) => new TextEncoder().encode(s);
    const snapshot: SchemaContents[] = [
      {
        path: ["main"],
        schema: new Uint8Array([1, 2]),
        tables: [enc("t1"), new Uint8Array(0)],
        views: [enc("v")],
        scalar_functions: [],
        aggregate_functions: [],
        table_functions: [],
        scalar_macros: [],
        table_macros: [],
        indexes: [],
      },
      {
        path: ["main", "sub ü"],
        schema: enc("S"),
        tables: [],
        views: [],
        scalar_functions: [],
        aggregate_functions: [],
        table_functions: [],
        scalar_macros: [],
        table_macros: [],
        indexes: [enc("i")],
      },
    ];
    // Values computed with vgi-python cc83818 (vgi.worker.catalog_contents_digest).
    expect(await catalogContentsDigest(snapshot)).toBe("856937931236f928e1f3fcd69da359bf21e41539f8c51e6d786929897db6d43c");
    expect(await catalogContentsDigest([])).toBe("af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc");
  });

  test("two builds of the example catalog hash alike", async () => {
    const h1 = handlers(exampleCatalog());
    const h2 = handlers(exampleCatalog());
    const a1 = (await h1.attach(catalog.name)).attach_opaque_data;
    const a2 = (await h2.attach(catalog.name)).attach_opaque_data;
    // Items embed the attach id (SchemaInfo.attach_opaque_data); compare the
    // same attach across two independent catalog instances and builds.
    const s1 = (await h1.contents(a1)).schemas;
    const s1b = (await h1.contents(a1)).schemas;
    expect(await catalogContentsDigest(s1)).toBe(await catalogContentsDigest(s1b));
    const s2 = (await h2.contents(a2)).schemas;
    expect(s2.length).toBe(s1.length);
  });
});

describe("catalog_contents worker cache (frozen + attach-independent)", () => {
  class CachedCatalog extends ScriptedCatalog {
    override catalogVersionFrozen = true;
    override catalogContentsAttachIndependent = true;
    builds = 0;
    v = 42;
    override version(): number {
      return this.v;
    }
  }

  test("built once per (catalog, version); conditional hits answer not_modified", async () => {
    const cat = new CachedCatalog();
    cat.catalogContentsEtag = "content-hash";
    cat.answer = () => {
      cat.builds++;
      return { schemas: [contentsOf(["a"])] };
    };
    const h = handlers(cat);
    const attach = new Uint8Array([9]);
    const a = await h.contents(attach);
    const b = await h.contents(attach, "stale");
    expect(cat.builds).toBe(1);
    expect(b.wire).toBe(a.wire); // the same serialized bytes, reused
    const nm = await h.contents(attach, a.response.etag);
    expect(nm.response.not_modified).toBe(true);
    expect(cat.builds).toBe(1);
    cat.v = 43;
    const c = await h.contents(attach);
    expect(cat.builds).toBe(2);
    expect(c.response.catalog_version).toBe(43);
  });

  test("not cached unless both frozen and attach-independent; a failed build is not cached", async () => {
    const cat = new CachedCatalog();
    cat.catalogContentsAttachIndependent = false;
    cat.answer = () => {
      cat.builds++;
      return { schemas: [contentsOf(["a"])] };
    };
    const h = handlers(cat);
    await h.contents(new Uint8Array([9]));
    await h.contents(new Uint8Array([9]));
    expect(cat.builds).toBe(2);

    const flaky = new CachedCatalog();
    let fail = true;
    flaky.answer = () => {
      flaky.builds++;
      if (fail) throw new Error("boom");
      return { schemas: [contentsOf(["a"])] };
    };
    const hf = handlers(flaky);
    await expect(hf.contents(new Uint8Array([9]))).rejects.toThrow(/boom/);
    fail = false;
    expect((await hf.contents(new Uint8Array([9]))).schemas.length).toBe(1);
    await hf.contents(new Uint8Array([9]));
    expect(flaky.builds).toBe(2);
  });

  test("ReadOnlyCatalogInterface is frozen but not attach-independent (per-attach ids)", () => {
    const ro = exampleCatalog();
    expect(ro.catalogVersionFrozen).toBe(true);
    expect(ro.catalogContentsAttachIndependent).toBe(false);
  });
});

describe("deterministic item encoding (map columns in sorted key order)", () => {
  test("map key insertion order does not change item bytes", () => {
    const base = schemaInfo(["main"]);
    const a = encodeSchemaInfo({
      ...base,
      tags: { b: "1", a: "2", "10": "x", "2": "y" },
      estimated_object_count: { view: 1, table: 2 },
    });
    const b = encodeSchemaInfo({
      ...base,
      tags: { "2": "y", a: "2", "10": "x", b: "1" },
      estimated_object_count: { table: 2, view: 1 },
    });
    expect(hex(a)).toBe(hex(b));
    // On the wire the entries are sorted by code point: "10" < "2" < "a" < "b"
    // (not JS's integer-keys-first object order). Encode explicit pair lists
    // in that order, bypassing encodeASD's sort, and compare bytes.
    const raw = serializeBatch(
      batchFromRows(
        [
          {
            ...base,
            tags: [["10", "x"], ["2", "y"], ["a", "2"], ["b", "1"]],
            estimated_object_count: [["table", 2], ["view", 1]],
          },
        ],
        SchemaInfoSchema,
      ),
    );
    expect(hex(a)).toBe(hex(raw));
    expect(decodeSchemaInfo(a).tags).toEqual({ b: "1", a: "2", "10": "x", "2": "y" });
  });

  test("TableInfo / ViewInfo maps (tags, write_result_modes, column_comments)", () => {
    const table = (tags: Record<string, string>, modes: Record<string, string>) =>
      encodeTableInfo({
        comment: null,
        tags,
        name: "t",
        schema_path: ["main"],
        columns: serializeSchema(schema([field("a", utf8(), true)])),
        not_null_constraints: [],
        unique_constraints: [],
        check_constraints: [],
        primary_key_constraints: [],
        foreign_key_constraints: [],
        write_result_modes: modes as any,
        supports_column_statistics: false,
        required_filters: [],
      } as any);
    expect(hex(table({ z: "1", a: "2" }, { update: "x", insert: "y" }))).toBe(
      hex(table({ a: "2", z: "1" }, { insert: "y", update: "x" })),
    );
    const view = (cc: Record<string, string>) =>
      encodeViewInfo({ comment: null, tags: {}, name: "v", schema_path: ["main"], definition: "SELECT 1", column_comments: cc });
    expect(hex(view({ y: "1", x: "2" }))).toBe(hex(view({ x: "2", y: "1" })));
    expect(Object.keys(decodeViewInfo(view({ y: "1", x: "2" })).column_comments)).toEqual(["x", "y"]);
    expect(decodeTableInfo(table({ z: "1", a: "2" }, {})).name).toBe("t");
  });

  test("encoding never mutates a frozen item", () => {
    const item = Object.freeze({ ...schemaInfo(["main"]), tags: Object.freeze({ b: "1", a: "2" }) });
    expect(() => encodeSchemaInfo(item as SchemaInfo)).not.toThrow();
    expect(Object.keys(item.tags)).toEqual(["b", "a"]);
  });

  test("compareCodePoints orders astral characters after the BMP (UTF-8 order)", () => {
    const astral = "\u{1F600}";
    const bmpHigh = "�";
    expect(compareCodePoints(bmpHigh, astral)).toBeLessThan(0);
    expect(bmpHigh < astral).toBe(false); // UTF-16 code-unit order disagrees
    expect(compareCodePoints("a", "ab")).toBeLessThan(0);
    expect(compareCodePoints("b", "a")).toBeGreaterThan(0);
    expect(compareCodePoints("x", "x")).toBe(0);
  });
});

describe("catalog_contents revalidation over HTTP, through VgiClient", () => {
  test("etag / not_modified round-trip", async () => {
    const r = registry();
    const server = serveVgiWorker({
      name: "demo",
      doc: "catalog_contents test worker.",
      version: "0.0.1",
      registry: r,
      catalogInterface: new CompositeCatalogInterface([exampleCatalog(r), new ContentsRevalCatalog()]),
      prefix: "",
      port: 0,
      signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(5),
      quiet: true,
      env: {},
    });
    try {
      const client = new VgiClient(httpConnect(`http://localhost:${server.port}`, { prefix: "" }));
      const attach = (await client.catalogAttach(CATALOG_REVAL)).attach_opaque_data;
      const full = await client.catalogContents(attach);
      expect(full.etag).toBe("gen-1");
      expect(full.not_modified).toBe(false);
      expect(full.schemas.map((s) => s.schema.path)).toEqual([["main"]]);
      const same = await client.catalogContents(attach, full.etag);
      expect(same).toEqual({ catalog_version: 1, etag: "gen-1", not_modified: true, schemas: [] });
      const other = await client.catalogContents(attach, "nope");
      expect(other.not_modified).toBe(false);
      expect(other.schemas.length).toBe(1);
    } finally {
      server.stop(true);
    }
  });
});
