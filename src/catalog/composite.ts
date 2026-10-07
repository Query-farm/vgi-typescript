// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// CompositeCatalogInterface — fan one worker out across multiple
// ReadOnlyCatalogInterface instances, picking which one handles each call by
// the `name` passed at attach time, then by the `attachOpaqueData` returned for all
// subsequent calls.
//
// This mirrors vgi-python's MetaWorker pattern, where one worker process
// can serve several distinct catalogs (`example`, `projection_repro`,
// `schema_reconcile`, …) from a single LOCATION.

import type {
  AttachOpaqueData,
  CatalogAttachResult,
  CatalogInfo,
  CopyFromFormatInfo,
  FunctionInfo,
  IndexInfo,
  MacroInfo,
  MacroType,
  CatalogContentsResult,
  SchemaInfo,
  TableInfo,
  TransactionOpaqueData,
  ViewInfo,
} from "./interface.js";
import { CatalogInterface } from "./interface.js";
import { OpaqueDataRejectedError } from "../crypto.js";

function bufferEquals(a: AttachOpaqueData, b: AttachOpaqueData): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

// Subprocess transport spawns a pool of worker processes; DuckDB routes calls
// across them round-robin. Each worker has its own CompositeCatalog instance,
// so we can't keep routing state in memory — the attach happens in worker A
// but later calls land on worker B with an empty route table. To route across
// workers without a shared store, we encode the backend index in the first
// byte of attach_opaque_data. Every worker decodes the same byte and routes the same
// way, so the route is implicit in the attach_opaque_data itself.
const ROUTE_BYTE = 0;

export class CompositeCatalogInterface extends CatalogInterface {
  constructor(private readonly _backends: CatalogInterface[]) {
    super();
    if (_backends.length > 256) {
      throw new Error("CompositeCatalog: at most 256 backends supported");
    }
  }

  private _route(attachOpaqueData: AttachOpaqueData): CatalogInterface {
    const idx = attachOpaqueData[ROUTE_BYTE];
    const route = this._backends[idx];
    if (!route) {
      // Routing is a check too (vgi-opaque-data-sealing.md rule 4): an
      // unroutable value gets the uniform refusal, never a description of
      // what this worker serves.
      throw new OpaqueDataRejectedError("attach_opaque_data");
    }
    return route;
  }

  /** Route to the owning backend and ask it, so the answer is per-attachment. */
  override catalogNameForAttach(attachOpaqueData: Uint8Array): string | null {
    try {
      return this._route(attachOpaqueData).catalogNameForAttach(attachOpaqueData);
    } catch {
      return null; // Unroutable attach — fall back to unscoped resolution.
    }
  }

  catalogs(): string[] {
    const all: string[] = [];
    for (const b of this._backends) all.push(...b.catalogs());
    return all;
  }

  async catalogsInfo(): Promise<CatalogInfo[]> {
    const all: CatalogInfo[] = [];
    for (const b of this._backends) {
      if (b.catalogsInfo) {
        all.push(...(await b.catalogsInfo()));
      } else {
        for (const name of b.catalogs()) {
          all.push({ name, implementation_version: null, data_version_spec: null, attach_option_specs: [], releases: [] });
        }
      }
    }
    return all;
  }

  async attach(
    name: string,
    options?: Record<string, unknown>,
    dataVersionSpec?: string | null,
    implementationVersion?: string | null,
  ): Promise<CatalogAttachResult> {
    for (let i = 0; i < this._backends.length; i++) {
      const b = this._backends[i];
      if (b.catalogs().includes(name)) {
        const result = await b.attach(name, options, dataVersionSpec, implementationVersion);
        // Stamp the route-byte so other workers in the pool can decode the
        // backend without needing in-memory state. Mutate in place so the
        // wire returns the rewritten id.
        const stamped = new Uint8Array(result.attach_opaque_data);
        stamped[ROUTE_BYTE] = i;
        result.attach_opaque_data = stamped;
        return result;
      }
    }
    throw new Error(`No worker handles catalog '${name}'`);
  }

  async detach(attachOpaqueData: AttachOpaqueData): Promise<void> {
    await this._route(attachOpaqueData).detach(attachOpaqueData);
  }

  async version(attachOpaqueData: AttachOpaqueData, transactionOpaqueData?: TransactionOpaqueData): Promise<number> {
    return await this._route(attachOpaqueData).version(attachOpaqueData, transactionOpaqueData);
  }

  async schemas(attachOpaqueData: AttachOpaqueData, transactionOpaqueData?: TransactionOpaqueData): Promise<SchemaInfo[]> {
    return await this._route(attachOpaqueData).schemas(attachOpaqueData, transactionOpaqueData);
  }

  override async schemaGet(attachOpaqueData: AttachOpaqueData, path: string[], transactionOpaqueData?: TransactionOpaqueData): Promise<SchemaInfo | null> {
    return await this._route(attachOpaqueData).schemaGet(attachOpaqueData, path, transactionOpaqueData);
  }

  override async schemaContentsTables(attachOpaqueData: AttachOpaqueData, path: string[], transactionOpaqueData?: TransactionOpaqueData): Promise<TableInfo[]> {
    return await this._route(attachOpaqueData).schemaContentsTables(attachOpaqueData, path, transactionOpaqueData);
  }

  override async schemaContentsViews(attachOpaqueData: AttachOpaqueData, path: string[], transactionOpaqueData?: TransactionOpaqueData): Promise<ViewInfo[]> {
    return await this._route(attachOpaqueData).schemaContentsViews(attachOpaqueData, path, transactionOpaqueData);
  }

  override async schemaContentsFunctions(attachOpaqueData: AttachOpaqueData, path: string[], type: string, transactionOpaqueData?: TransactionOpaqueData): Promise<FunctionInfo[]> {
    return await this._route(attachOpaqueData).schemaContentsFunctions(attachOpaqueData, path, type, transactionOpaqueData);
  }

  override async schemaContentsMacros(attachOpaqueData: AttachOpaqueData, path: string[], type: string, transactionOpaqueData?: TransactionOpaqueData): Promise<MacroInfo[]> {
    return await this._route(attachOpaqueData).schemaContentsMacros(attachOpaqueData, path, type, transactionOpaqueData);
  }

  override async schemaContentsIndexes(attachOpaqueData: AttachOpaqueData, path: string[], transactionOpaqueData?: TransactionOpaqueData): Promise<IndexInfo[]> {
    return await this._route(attachOpaqueData).schemaContentsIndexes(attachOpaqueData, path, transactionOpaqueData);
  }

  override async indexGet(attachOpaqueData: AttachOpaqueData, schemaPath: string[], name: string, transactionOpaqueData?: TransactionOpaqueData): Promise<IndexInfo | null> {
    return await this._route(attachOpaqueData).indexGet(attachOpaqueData, schemaPath, name, transactionOpaqueData);
  }

  override async tableGet(attachOpaqueData: AttachOpaqueData, schemaPath: string[], name: string, atUnit?: string, atValue?: string, transactionOpaqueData?: TransactionOpaqueData): Promise<TableInfo | null> {
    return await this._route(attachOpaqueData).tableGet(attachOpaqueData, schemaPath, name, atUnit, atValue, transactionOpaqueData);
  }

  override async tableScanFunctionGet(attachOpaqueData: AttachOpaqueData, schemaPath: string[], name: string, atUnit?: string, atValue?: string, transactionOpaqueData?: TransactionOpaqueData): Promise<any> {
    return await this._route(attachOpaqueData).tableScanFunctionGet(attachOpaqueData, schemaPath, name, atUnit, atValue, transactionOpaqueData);
  }

  override async tableScanBranchesGet(attachOpaqueData: AttachOpaqueData, schemaPath: string[], name: string, atUnit?: string, atValue?: string, transactionOpaqueData?: TransactionOpaqueData): Promise<any> {
    return await this._route(attachOpaqueData).tableScanBranchesGet(attachOpaqueData, schemaPath, name, atUnit, atValue, transactionOpaqueData);
  }

  override async tableColumnStatisticsGet(attachOpaqueData: AttachOpaqueData, schemaPath: string[], name: string, transactionOpaqueData?: TransactionOpaqueData): Promise<{ bytes: Uint8Array; cacheMaxAgeSeconds: number | null } | null> {
    return await this._route(attachOpaqueData).tableColumnStatisticsGet(attachOpaqueData, schemaPath, name, transactionOpaqueData);
  }

  override async viewGet(attachOpaqueData: AttachOpaqueData, schemaPath: string[], name: string, transactionOpaqueData?: TransactionOpaqueData): Promise<ViewInfo | null> {
    return await this._route(attachOpaqueData).viewGet(attachOpaqueData, schemaPath, name, transactionOpaqueData);
  }

  override async macroGet(attachOpaqueData: AttachOpaqueData, schemaPath: string[], name: string, transactionOpaqueData?: TransactionOpaqueData): Promise<MacroInfo | null> {
    return await this._route(attachOpaqueData).macroGet(attachOpaqueData, schemaPath, name, transactionOpaqueData);
  }

  // DDL is routed like every read: a writable backend (e.g. the
  // contents_memory fixture) behind a composite would otherwise inherit
  // CatalogInterface's read-only refusals. Every one takes attachOpaqueData
  // first.
  override async schemaCreate(...args: Parameters<CatalogInterface["schemaCreate"]>): Promise<void> {
    await (this._route(args[0]).schemaCreate as (...a: typeof args) => unknown)(...args);
  }
  override async schemaDrop(...args: Parameters<CatalogInterface["schemaDrop"]>): Promise<void> {
    await (this._route(args[0]).schemaDrop as (...a: typeof args) => unknown)(...args);
  }
  override async tableCreate(...args: Parameters<CatalogInterface["tableCreate"]>): Promise<void> {
    await (this._route(args[0]).tableCreate as (...a: typeof args) => unknown)(...args);
  }
  override async tableDrop(...args: Parameters<CatalogInterface["tableDrop"]>): Promise<void> {
    await (this._route(args[0]).tableDrop as (...a: typeof args) => unknown)(...args);
  }
  override async tableCommentSet(...args: Parameters<CatalogInterface["tableCommentSet"]>): Promise<void> {
    await (this._route(args[0]).tableCommentSet as (...a: typeof args) => unknown)(...args);
  }
  override async tableRename(...args: Parameters<CatalogInterface["tableRename"]>): Promise<void> {
    await (this._route(args[0]).tableRename as (...a: typeof args) => unknown)(...args);
  }
  override async tableColumnAdd(...args: Parameters<CatalogInterface["tableColumnAdd"]>): Promise<void> {
    await (this._route(args[0]).tableColumnAdd as (...a: typeof args) => unknown)(...args);
  }
  override async tableColumnDrop(...args: Parameters<CatalogInterface["tableColumnDrop"]>): Promise<void> {
    await (this._route(args[0]).tableColumnDrop as (...a: typeof args) => unknown)(...args);
  }
  override async tableColumnRename(...args: Parameters<CatalogInterface["tableColumnRename"]>): Promise<void> {
    await (this._route(args[0]).tableColumnRename as (...a: typeof args) => unknown)(...args);
  }
  override async tableColumnDefaultSet(...args: Parameters<CatalogInterface["tableColumnDefaultSet"]>): Promise<void> {
    await (this._route(args[0]).tableColumnDefaultSet as (...a: typeof args) => unknown)(...args);
  }
  override async tableColumnDefaultDrop(...args: Parameters<CatalogInterface["tableColumnDefaultDrop"]>): Promise<void> {
    await (this._route(args[0]).tableColumnDefaultDrop as (...a: typeof args) => unknown)(...args);
  }
  override async tableColumnTypeChange(...args: Parameters<CatalogInterface["tableColumnTypeChange"]>): Promise<void> {
    await (this._route(args[0]).tableColumnTypeChange as (...a: typeof args) => unknown)(...args);
  }
  override async tableNotNullSet(...args: Parameters<CatalogInterface["tableNotNullSet"]>): Promise<void> {
    await (this._route(args[0]).tableNotNullSet as (...a: typeof args) => unknown)(...args);
  }
  override async tableNotNullDrop(...args: Parameters<CatalogInterface["tableNotNullDrop"]>): Promise<void> {
    await (this._route(args[0]).tableNotNullDrop as (...a: typeof args) => unknown)(...args);
  }
  override async viewCreate(...args: Parameters<CatalogInterface["viewCreate"]>): Promise<void> {
    await (this._route(args[0]).viewCreate as (...a: typeof args) => unknown)(...args);
  }
  override async viewDrop(...args: Parameters<CatalogInterface["viewDrop"]>): Promise<void> {
    await (this._route(args[0]).viewDrop as (...a: typeof args) => unknown)(...args);
  }
  override async viewRename(...args: Parameters<CatalogInterface["viewRename"]>): Promise<void> {
    await (this._route(args[0]).viewRename as (...a: typeof args) => unknown)(...args);
  }
  override async viewCommentSet(...args: Parameters<CatalogInterface["viewCommentSet"]>): Promise<void> {
    await (this._route(args[0]).viewCommentSet as (...a: typeof args) => unknown)(...args);
  }
  override async macroCreate(...args: Parameters<CatalogInterface["macroCreate"]>): Promise<void> {
    await (this._route(args[0]).macroCreate as (...a: typeof args) => unknown)(...args);
  }
  override async macroDrop(...args: Parameters<CatalogInterface["macroDrop"]>): Promise<void> {
    await (this._route(args[0]).macroDrop as (...a: typeof args) => unknown)(...args);
  }

  // Routed whole, so a backend that overrides catalogContents (or keeps the
  // default) answers for its own catalog.
  override async catalogContents(
    attachOpaqueData: AttachOpaqueData,
    ifNoneMatch?: string | null,
  ): Promise<CatalogContentsResult> {
    return await this._route(attachOpaqueData).catalogContents(attachOpaqueData, ifNoneMatch);
  }

  // The backend's etag / cache settings apply, and its instance keys the
  // worker's catalog_contents cache.
  override catalogContentsOwner(attachOpaqueData: AttachOpaqueData): CatalogInterface {
    return this._route(attachOpaqueData).catalogContentsOwner(attachOpaqueData);
  }

  override async copyFromFormats(attachOpaqueData: AttachOpaqueData, transactionOpaqueData?: TransactionOpaqueData): Promise<CopyFromFormatInfo[]> {
    return await this._route(attachOpaqueData).copyFromFormats(attachOpaqueData, transactionOpaqueData);
  }

  override async transactionBegin(attachOpaqueData: AttachOpaqueData): Promise<Uint8Array | null> {
    return await this._route(attachOpaqueData).transactionBegin(attachOpaqueData);
  }

  override async transactionCommit(attachOpaqueData: AttachOpaqueData, transactionOpaqueData: TransactionOpaqueData): Promise<void> {
    await this._route(attachOpaqueData).transactionCommit(attachOpaqueData, transactionOpaqueData);
  }

  override async transactionRollback(attachOpaqueData: AttachOpaqueData, transactionOpaqueData: TransactionOpaqueData): Promise<void> {
    await this._route(attachOpaqueData).transactionRollback(attachOpaqueData, transactionOpaqueData);
  }
}
