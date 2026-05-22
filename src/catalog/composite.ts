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
  FunctionInfo,
  IndexInfo,
  MacroInfo,
  MacroType,
  SchemaInfo,
  TableInfo,
  TransactionOpaqueData,
  ViewInfo,
} from "./interface.js";
import { CatalogInterface } from "./interface.js";

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
      throw new Error(`CompositeCatalog: no backend at route-byte index ${idx} (have ${this._backends.length})`);
    }
    return route;
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

  override async schemaGet(attachOpaqueData: AttachOpaqueData, name: string, transactionOpaqueData?: TransactionOpaqueData): Promise<SchemaInfo | null> {
    return await this._route(attachOpaqueData).schemaGet(attachOpaqueData, name, transactionOpaqueData);
  }

  override async schemaContentsTables(attachOpaqueData: AttachOpaqueData, name: string, transactionOpaqueData?: TransactionOpaqueData): Promise<TableInfo[]> {
    return await this._route(attachOpaqueData).schemaContentsTables(attachOpaqueData, name, transactionOpaqueData);
  }

  override async schemaContentsViews(attachOpaqueData: AttachOpaqueData, name: string, transactionOpaqueData?: TransactionOpaqueData): Promise<ViewInfo[]> {
    return await this._route(attachOpaqueData).schemaContentsViews(attachOpaqueData, name, transactionOpaqueData);
  }

  override async schemaContentsFunctions(attachOpaqueData: AttachOpaqueData, name: string, type: string, transactionOpaqueData?: TransactionOpaqueData): Promise<FunctionInfo[]> {
    return await this._route(attachOpaqueData).schemaContentsFunctions(attachOpaqueData, name, type, transactionOpaqueData);
  }

  override async schemaContentsMacros(attachOpaqueData: AttachOpaqueData, name: string, type: string, transactionOpaqueData?: TransactionOpaqueData): Promise<MacroInfo[]> {
    return await this._route(attachOpaqueData).schemaContentsMacros(attachOpaqueData, name, type, transactionOpaqueData);
  }

  override async schemaContentsIndexes(attachOpaqueData: AttachOpaqueData, name: string, transactionOpaqueData?: TransactionOpaqueData): Promise<IndexInfo[]> {
    return await this._route(attachOpaqueData).schemaContentsIndexes(attachOpaqueData, name, transactionOpaqueData);
  }

  override async indexGet(attachOpaqueData: AttachOpaqueData, schemaName: string, name: string, transactionOpaqueData?: TransactionOpaqueData): Promise<IndexInfo | null> {
    return await this._route(attachOpaqueData).indexGet(attachOpaqueData, schemaName, name, transactionOpaqueData);
  }

  override async tableGet(attachOpaqueData: AttachOpaqueData, schemaName: string, name: string, atUnit?: string, atValue?: string, transactionOpaqueData?: TransactionOpaqueData): Promise<TableInfo | null> {
    return await this._route(attachOpaqueData).tableGet(attachOpaqueData, schemaName, name, atUnit, atValue, transactionOpaqueData);
  }

  override async tableScanFunctionGet(attachOpaqueData: AttachOpaqueData, schemaName: string, name: string, atUnit?: string, atValue?: string, transactionOpaqueData?: TransactionOpaqueData): Promise<any> {
    return await this._route(attachOpaqueData).tableScanFunctionGet(attachOpaqueData, schemaName, name, atUnit, atValue, transactionOpaqueData);
  }

  override async tableScanBranchesGet(attachOpaqueData: AttachOpaqueData, schemaName: string, name: string, atUnit?: string, atValue?: string, transactionOpaqueData?: TransactionOpaqueData): Promise<any> {
    return await this._route(attachOpaqueData).tableScanBranchesGet(attachOpaqueData, schemaName, name, atUnit, atValue, transactionOpaqueData);
  }

  override async tableColumnStatisticsGet(attachOpaqueData: AttachOpaqueData, schemaName: string, name: string, transactionOpaqueData?: TransactionOpaqueData): Promise<{ bytes: Uint8Array; cacheMaxAgeSeconds: number | null } | null> {
    return await this._route(attachOpaqueData).tableColumnStatisticsGet(attachOpaqueData, schemaName, name, transactionOpaqueData);
  }

  override async viewGet(attachOpaqueData: AttachOpaqueData, schemaName: string, name: string, transactionOpaqueData?: TransactionOpaqueData): Promise<ViewInfo | null> {
    return await this._route(attachOpaqueData).viewGet(attachOpaqueData, schemaName, name, transactionOpaqueData);
  }

  override async macroGet(attachOpaqueData: AttachOpaqueData, schemaName: string, name: string, transactionOpaqueData?: TransactionOpaqueData): Promise<MacroInfo | null> {
    return await this._route(attachOpaqueData).macroGet(attachOpaqueData, schemaName, name, transactionOpaqueData);
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
