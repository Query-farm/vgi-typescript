// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Catalogs for the `catalog_contents` RPC (the whole catalog in one call).
//
// The same two-schema catalog is served under three names, differing only in
// how they answer catalog_contents, plus one DDL-capable catalog:
//
//   contents_probe   advertises supports_catalog_contents and serves it (the
//                    ReadOnlyCatalogInterface default) — loaded in one RPC.
//   contents_broken  advertises it, but catalog_contents throws: the client
//                    must fall back to catalog_schemas + the per-schema RPCs.
//   contents_legacy  does not advertise it, like an older worker: the client
//                    must never call catalog_contents.
//   contents_memory  a DDL-capable in-memory catalog (its version bumps on
//                    every DDL) that advertises it, for invalidation after DDL.
//                    Every ATTACH gets its own empty `main` schema.
//
// Every kind the client seeds from catalog_contents is present at least once
// (tables, a view, scalar / aggregate / table functions, scalar and table
// macros), split over `main` and `extra`. Mirrors vgi-python's
// `vgi/_test_fixtures/catalog_contents.py`; driven by
// `vgi/test/sql/integration/catalog/catalog_contents*.test`.

import {
  Arguments,
  CatalogInterface,
  ReadOnlyCatalogInterface,
  buildCatalogAttachResult,
  type AttachOpaqueData,
  type CatalogAttachResult,
  type CatalogDescriptor,
  type FunctionRegistry,
  type SchemaContentsInfo,
  type SchemaInfo,
  type TableInfo,
  type TransactionOpaqueData,
  type ViewInfo,
  type VgiFunction,
} from "../src/index.js";
import { CatalogAlreadyExistsError, CatalogNotFoundError } from "../src/errors.js";
import { allFunctions } from "./common.js";

export const CATALOG_PROBE = "contents_probe";
export const CATALOG_BROKEN = "contents_broken";
export const CATALOG_LEGACY = "contents_legacy";
export const CATALOG_MEMORY = "contents_memory";

export const BROKEN_MESSAGE = "contents_broken: catalog_contents deliberately fails";

function exampleFunction(name: string): VgiFunction {
  const fn = allFunctions.find((f) => f.meta.name === name);
  if (!fn) throw new Error(`catalog_contents fixture: no example function named '${name}'`);
  return fn;
}

function contentsCatalog(name: string): CatalogDescriptor {
  const sequence = exampleFunction("sequence");
  return {
    name,
    defaultSchema: "main",
    comment: `catalog_contents test catalog (${name})`,
    schemas: [
      {
        path: ["main"],
        comment: "Every object kind",
        tables: [{ name: "ten", function: sequence, arguments: new Arguments([10]), comment: "Integers 0..9" }],
        views: [{ name: "answer", definition: "SELECT 42 AS answer", comment: "One row" }],
        functions: [exampleFunction("double"), exampleFunction("vgi_sum"), sequence],
        macros: [
          {
            name: "contents_triple",
            macroType: "scalar",
            parameters: ["x"],
            definition: "x * 3",
            comment: "Triple a value",
          },
          {
            name: "contents_range",
            macroType: "table",
            parameters: ["n"],
            definition: "SELECT * FROM range(n)",
            comment: "Table macro over range(n)",
          },
        ],
      },
      {
        path: ["extra"],
        comment: "A second schema, tables only",
        tables: [{ name: "five", function: sequence, arguments: new Arguments([5]), comment: "Integers 0..4" }],
      },
    ],
  };
}

/** Advertises catalog_contents but fails to serve it. */
class ContentsBrokenCatalog extends ReadOnlyCatalogInterface {
  override async catalogContents(_attachOpaqueData: AttachOpaqueData): Promise<SchemaContentsInfo[]> {
    throw new Error(BROKEN_MESSAGE);
  }
}

// ---------------------------------------------------------------------------
// contents_memory
// ---------------------------------------------------------------------------

interface MemoryState {
  version: number;
  tables: Map<string, TableInfo>;
  views: Map<string, ViewInfo>;
}

/**
 * DDL-capable in-memory catalog advertising catalog_contents. Each ATTACH has
 * private state (one `main` schema). The state lives in this process, so it
 * needs a single warm worker (`launch:` / HTTP), like vgi-python's fixture.
 */
export class ContentsMemoryCatalog extends CatalogInterface {
  private _states = new Map<string, MemoryState>();

  catalogs(): string[] {
    return [CATALOG_MEMORY];
  }

  attach(name: string): CatalogAttachResult {
    if (name !== CATALOG_MEMORY) throw new Error(`Unknown catalog: '${name}'. Available: ${CATALOG_MEMORY}`);
    const attach = new Uint8Array(16);
    crypto.getRandomValues(attach);
    const state: MemoryState = { version: 1, tables: new Map(), views: new Map() };
    this._states.set(this._key(attach), state);
    return buildCatalogAttachResult({
      attach_opaque_data: attach,
      supports_transactions: false,
      supports_time_travel: false,
      catalog_version_frozen: false,
      catalog_version: state.version,
      supports_column_statistics: false,
      resolved_data_version: null,
      resolved_implementation_version: null,
      supports_catalog_contents: true,
    });
  }

  detach(attach: AttachOpaqueData): void {
    this._states.delete(this._key(attach));
  }

  version(attach: AttachOpaqueData): number {
    return this._state(attach).version;
  }

  schemas(attach: AttachOpaqueData): SchemaInfo[] {
    this._state(attach);
    return [{ comment: null, tags: {}, attach_opaque_data: attach, path: ["main"], estimated_object_count: null }];
  }

  override schemaContentsTables(attach: AttachOpaqueData, path: string[]): TableInfo[] {
    return this._isMain(path) ? [...this._state(attach).tables.values()] : [];
  }

  override schemaContentsViews(attach: AttachOpaqueData, path: string[]): ViewInfo[] {
    return this._isMain(path) ? [...this._state(attach).views.values()] : [];
  }

  override tableGet(attach: AttachOpaqueData, schemaPath: string[], name: string): TableInfo | null {
    return this._isMain(schemaPath) ? (this._state(attach).tables.get(name) ?? null) : null;
  }

  override viewGet(attach: AttachOpaqueData, schemaPath: string[], name: string): ViewInfo | null {
    return this._isMain(schemaPath) ? (this._state(attach).views.get(name) ?? null) : null;
  }

  override tableCreate(
    attach: AttachOpaqueData,
    schemaPath: string[],
    name: string,
    columns: Uint8Array,
    onConflict: string,
    notNullConstraints?: number[],
    uniqueConstraints?: number[][],
    checkConstraints?: string[],
  ): void {
    const state = this._state(attach);
    this._requireMain(schemaPath);
    if (state.tables.has(name) && !this._replace("Table", name, onConflict)) return;
    state.tables.set(name, {
      comment: null,
      tags: {},
      name,
      schema_path: ["main"],
      columns,
      not_null_constraints: notNullConstraints ?? [],
      unique_constraints: uniqueConstraints ?? [],
      check_constraints: checkConstraints ?? [],
      primary_key_constraints: [],
      foreign_key_constraints: [],
      write_result_modes: {},
      supports_column_statistics: false,
      required_filters: [],
    });
    state.version++;
  }

  override tableDrop(attach: AttachOpaqueData, schemaPath: string[], name: string, ignoreNotFound?: boolean): void {
    const state = this._state(attach);
    if (!this._isMain(schemaPath) || !state.tables.delete(name)) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("Table", name);
    }
    state.version++;
  }

  override viewCreate(
    attach: AttachOpaqueData,
    schemaPath: string[],
    name: string,
    definition: string,
    onConflict: string,
  ): void {
    const state = this._state(attach);
    this._requireMain(schemaPath);
    if (state.views.has(name) && !this._replace("View", name, onConflict)) return;
    state.views.set(name, { comment: null, tags: {}, name, schema_path: ["main"], definition, column_comments: {} });
    state.version++;
  }

  override viewDrop(attach: AttachOpaqueData, schemaPath: string[], name: string, ignoreNotFound?: boolean): void {
    const state = this._state(attach);
    if (!this._isMain(schemaPath) || !state.views.delete(name)) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("View", name);
    }
    state.version++;
  }

  /** True to replace an existing object, false to keep it; throws on a plain conflict. */
  private _replace(kind: string, name: string, onConflict: string): boolean {
    const mode = String(onConflict ?? "").toUpperCase();
    if (mode === "IGNORE") return false;
    if (mode === "REPLACE") return true;
    throw new CatalogAlreadyExistsError(kind, name);
  }

  private _isMain(path: string[]): boolean {
    return path.length === 1 && path[0].toLowerCase() === "main";
  }

  private _requireMain(path: string[]): void {
    if (!this._isMain(path)) throw new CatalogNotFoundError("Schema", path.join("."));
  }

  // A CompositeCatalogInterface stamps its route into byte 0 after attach, so
  // the state is keyed on the rest.
  private _key(attach: AttachOpaqueData): string {
    return Buffer.from(attach.subarray(1)).toString("hex");
  }

  private _state(attach: AttachOpaqueData): MemoryState {
    const state = this._states.get(this._key(attach));
    if (!state) throw new Error(`${CATALOG_MEMORY}: not attached`);
    return state;
  }
}

/** The four catalog_contents fixture catalogs, for a CompositeCatalogInterface. */
export function createCatalogContentsCatalogs(registry: FunctionRegistry): CatalogInterface[] {
  const probe = new ReadOnlyCatalogInterface(contentsCatalog(CATALOG_PROBE), registry);
  const broken = new ContentsBrokenCatalog(contentsCatalog(CATALOG_BROKEN), registry);
  const legacy = new ReadOnlyCatalogInterface(contentsCatalog(CATALOG_LEGACY), registry);
  legacy.supportsCatalogContents = false;
  return [probe, broken, legacy, new ContentsMemoryCatalog()];
}
