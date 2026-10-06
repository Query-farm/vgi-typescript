// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Catalogs for the `catalog_contents` RPC (the whole catalog in one call).
//
// The same static two-schema catalog is served under three names, differing
// only in how they answer catalog_contents:
//
//   contents_probe   advertises supports_catalog_contents and serves it (the
//                    ReadOnlyCatalogInterface default: version-frozen, no
//                    etag) — loaded in one RPC.
//   contents_broken  advertises it, but catalog_contents throws: the client
//                    must fall back to catalog_schemas + the per-schema RPCs.
//   contents_legacy  does not advertise it, like an older worker: the client
//                    must never call catalog_contents.
//
// Three DDL-capable in-memory catalogs (version not frozen) advertise it too.
// Every ATTACH gets its own empty `main` schema, so tests sharing a warm worker
// never see each other's objects:
//
//   contents_memory  reports catalog_version 0 ("unknown") and no etag: the
//                    client's version-0 rule (first load via catalog_contents,
//                    reloads via the lazy per-schema RPCs).
//   contents_reval   a revalidating catalog with a cheap validator: the etag is
//                    "gen-<n>", n = the catalog version, bumped by every DDL.
//                    A matching if_none_match answers not_modified without
//                    building anything.
//   contents_hash    returns no etag of its own but sets catalogContentsEtag =
//                    "content-hash": the worker builds the snapshot on every
//                    call and uses its SHA-256 as the etag.
//
// The static catalog holds every kind the client seeds from catalog_contents
// at least once (tables, a view, scalar / aggregate / table functions, scalar
// and table macros), split over `main` and `extra`. Mirrors vgi-python's
// `vgi/_test_fixtures/catalog_contents.py`; driven by
// `vgi/test/sql/integration/catalog/catalog_contents*.test`.

import {
  Arguments,
  CatalogInterface,
  ReadOnlyCatalogInterface,
  buildCatalogAttachResult,
  type AttachOpaqueData,
  type CatalogAttachResult,
  type CatalogContentsResult,
  type CatalogDescriptor,
  type FunctionRegistry,
  type SchemaInfo,
  type TableInfo,
  type TransactionOpaqueData,
  type ViewInfo,
  type VgiFunction,
} from "../src/index.js";
import { CatalogAlreadyExistsError, CatalogNotFoundError } from "../src/errors.js";
import { schemaPathKey } from "../src/schema-path.js";
import { allFunctions } from "./common.js";

export const CATALOG_PROBE = "contents_probe";
export const CATALOG_BROKEN = "contents_broken";
export const CATALOG_LEGACY = "contents_legacy";
export const CATALOG_MEMORY = "contents_memory";
export const CATALOG_REVAL = "contents_reval";
export const CATALOG_HASH = "contents_hash";

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
  override async catalogContents(_attachOpaqueData: AttachOpaqueData): Promise<CatalogContentsResult> {
    throw new Error(BROKEN_MESSAGE);
  }
}

// ---------------------------------------------------------------------------
// contents_memory / contents_reval / contents_hash
// ---------------------------------------------------------------------------

interface SchemaState {
  path: string[];
  comment: string | null;
  tables: Map<string, TableInfo>;
  views: Map<string, ViewInfo>;
}

interface MemoryState {
  version: number;
  /** Keyed by schemaPathKey (case-insensitive). */
  schemas: Map<string, SchemaState>;
}

/**
 * DDL-capable in-memory catalog advertising catalog_contents. Each ATTACH of
 * `publicName` has private state (one `main` schema to start; CREATE / DROP
 * SCHEMA, tables and views). The state lives in this process, so it needs a
 * single warm worker (`launch:` / HTTP), like vgi-python's fixture.
 */
abstract class PrivateMemoryCatalog extends CatalogInterface {
  abstract readonly publicName: string;
  private _states = new Map<string, MemoryState>();

  catalogs(): string[] {
    return [this.publicName];
  }

  attach(name: string): CatalogAttachResult {
    if (name !== this.publicName) throw new Error(`Unknown catalog: '${name}'. Available: ${this.publicName}`);
    const attach = new Uint8Array(16);
    crypto.getRandomValues(attach);
    const state: MemoryState = { version: 1, schemas: new Map() };
    state.schemas.set(schemaPathKey(["main"]), { path: ["main"], comment: null, tables: new Map(), views: new Map() });
    this._states.set(this._key(attach), state);
    return buildCatalogAttachResult({
      attach_opaque_data: attach,
      supports_transactions: false,
      supports_time_travel: false,
      catalog_version_frozen: false,
      catalog_version: this.version(attach),
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
    return [...this._state(attach).schemas.values()].map((s) => ({
      comment: s.comment,
      tags: {},
      attach_opaque_data: attach,
      path: s.path,
      estimated_object_count: null,
    }));
  }

  override schemaCreate(attach: AttachOpaqueData, path: string[], comment?: string | null): void {
    const state = this._state(attach);
    const key = schemaPathKey(path);
    if (state.schemas.has(key)) throw new CatalogAlreadyExistsError("Schema", path.join("."));
    if (path.length > 1 && !state.schemas.has(schemaPathKey(path.slice(0, -1)))) {
      throw new CatalogNotFoundError("Schema", path.slice(0, -1).join("."));
    }
    state.schemas.set(key, { path: [...path], comment: comment ?? null, tables: new Map(), views: new Map() });
    state.version++;
  }

  override schemaDrop(attach: AttachOpaqueData, path: string[], ignoreNotFound?: boolean, cascade?: boolean): void {
    const state = this._state(attach);
    const schema = state.schemas.get(schemaPathKey(path));
    if (!schema) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("Schema", path.join("."));
    }
    if (!cascade && (schema.tables.size > 0 || schema.views.size > 0)) {
      throw new Error(`Schema ${path.join(".")} is not empty; use CASCADE`);
    }
    state.schemas.delete(schemaPathKey(path));
    state.version++;
  }

  override schemaContentsTables(attach: AttachOpaqueData, path: string[]): TableInfo[] {
    return [...(this._schema(attach, path)?.tables.values() ?? [])];
  }

  override schemaContentsViews(attach: AttachOpaqueData, path: string[]): ViewInfo[] {
    return [...(this._schema(attach, path)?.views.values() ?? [])];
  }

  override tableGet(attach: AttachOpaqueData, schemaPath: string[], name: string): TableInfo | null {
    return this._schema(attach, schemaPath)?.tables.get(name) ?? null;
  }

  override viewGet(attach: AttachOpaqueData, schemaPath: string[], name: string): ViewInfo | null {
    return this._schema(attach, schemaPath)?.views.get(name) ?? null;
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
    const schema = this._requireSchema(attach, schemaPath);
    if (schema.tables.has(name) && !this._replace("Table", name, onConflict)) return;
    schema.tables.set(name, {
      comment: null,
      tags: {},
      name,
      schema_path: schema.path,
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
    if (!this._schema(attach, schemaPath)?.tables.delete(name)) {
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
    const schema = this._requireSchema(attach, schemaPath);
    if (schema.views.has(name) && !this._replace("View", name, onConflict)) return;
    schema.views.set(name, { comment: null, tags: {}, name, schema_path: schema.path, definition, column_comments: {} });
    state.version++;
  }

  override viewDrop(attach: AttachOpaqueData, schemaPath: string[], name: string, ignoreNotFound?: boolean): void {
    const state = this._state(attach);
    if (!this._schema(attach, schemaPath)?.views.delete(name)) {
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

  private _schema(attach: AttachOpaqueData, path: string[]): SchemaState | undefined {
    return this._state(attach).schemas.get(schemaPathKey(path));
  }

  private _requireSchema(attach: AttachOpaqueData, path: string[]): SchemaState {
    const schema = this._schema(attach, path);
    if (!schema) throw new CatalogNotFoundError("Schema", path.join("."));
    return schema;
  }

  // A CompositeCatalogInterface stamps its route into byte 0 after attach, so
  // the state is keyed on the rest.
  private _key(attach: AttachOpaqueData): string {
    return Buffer.from(attach.subarray(1)).toString("hex");
  }

  protected _state(attach: AttachOpaqueData): MemoryState {
    const state = this._states.get(this._key(attach));
    if (!state) throw new Error(`${this.publicName}: not attached`);
    return state;
  }
}

/** Private in-memory catalog reporting version 0 and no etag (the version-0 rule). */
export class ContentsMemoryCatalog extends PrivateMemoryCatalog {
  readonly publicName = CATALOG_MEMORY;

  /** Always 0: the catalog does not track its version. */
  override version(attach: AttachOpaqueData): number {
    this._state(attach);
    return 0;
  }
}

/** Private in-memory catalog that revalidates with a generation-counter etag. */
export class ContentsRevalCatalog extends PrivateMemoryCatalog {
  readonly publicName = CATALOG_REVAL;
  /** How many snapshots this catalog built (a not_modified answer builds none). */
  builds = 0;

  /** Answer not_modified from the generation counter, building only on a miss. */
  override async catalogContents(
    attach: AttachOpaqueData,
    ifNoneMatch?: string | null,
  ): Promise<CatalogContentsResult> {
    const etag = `gen-${this._state(attach).version}`;
    if (ifNoneMatch === etag) return { etag, not_modified: true };
    this.builds++;
    const built = await super.catalogContents(attach);
    return { schemas: built.schemas, etag };
  }
}

/** Private in-memory catalog revalidated by the framework's content hash. */
export class ContentsHashCatalog extends PrivateMemoryCatalog {
  readonly publicName = CATALOG_HASH;
  override catalogContentsEtag = "content-hash" as const;
}

/** The six catalog_contents fixture catalogs, for a CompositeCatalogInterface. */
export function createCatalogContentsCatalogs(registry: FunctionRegistry): CatalogInterface[] {
  const probe = new ReadOnlyCatalogInterface(contentsCatalog(CATALOG_PROBE), registry);
  const broken = new ContentsBrokenCatalog(contentsCatalog(CATALOG_BROKEN), registry);
  const legacy = new ReadOnlyCatalogInterface(contentsCatalog(CATALOG_LEGACY), registry);
  legacy.supportsCatalogContents = false;
  return [probe, broken, legacy, new ContentsMemoryCatalog(), new ContentsRevalCatalog(), new ContentsHashCatalog()];
}
