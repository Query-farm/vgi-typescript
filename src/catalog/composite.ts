// CompositeCatalogInterface — fan one worker out across multiple
// ReadOnlyCatalogInterface instances, picking which one handles each call by
// the `name` passed at attach time, then by the `attachId` returned for all
// subsequent calls.
//
// This mirrors vgi-python's MetaWorker pattern, where one worker process
// can serve several distinct catalogs (`example`, `projection_repro`,
// `schema_reconcile`, …) from a single LOCATION.

import type {
  AttachId,
  CatalogAttachResult,
  CatalogInfo,
  FunctionInfo,
  IndexInfo,
  MacroInfo,
  MacroType,
  SchemaInfo,
  TableInfo,
  TransactionId,
  ViewInfo,
} from "./interface.js";
import { CatalogInterface } from "./interface.js";

function bufferEquals(a: AttachId, b: AttachId): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

// Subprocess transport spawns a pool of worker processes; DuckDB routes calls
// across them round-robin. Each worker has its own CompositeCatalog instance,
// so we can't keep routing state in memory — the attach happens in worker A
// but later calls land on worker B with an empty route table. To route across
// workers without a shared store, we encode the backend index in the first
// byte of attach_id. Every worker decodes the same byte and routes the same
// way, so the route is implicit in the attach_id itself.
const ROUTE_BYTE = 0;

export class CompositeCatalogInterface extends CatalogInterface {
  constructor(private readonly _backends: CatalogInterface[]) {
    super();
    if (_backends.length > 256) {
      throw new Error("CompositeCatalog: at most 256 backends supported");
    }
  }

  private _route(attachId: AttachId): CatalogInterface {
    const idx = attachId[ROUTE_BYTE];
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
          all.push({ name, implementation_version: null, data_version_spec: null });
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
        const stamped = new Uint8Array(result.attach_id);
        stamped[ROUTE_BYTE] = i;
        result.attach_id = stamped;
        return result;
      }
    }
    throw new Error(`No worker handles catalog '${name}'`);
  }

  async detach(attachId: AttachId): Promise<void> {
    await this._route(attachId).detach(attachId);
  }

  async version(attachId: AttachId, transactionId?: TransactionId): Promise<number> {
    return await this._route(attachId).version(attachId, transactionId);
  }

  async schemas(attachId: AttachId, transactionId?: TransactionId): Promise<SchemaInfo[]> {
    return await this._route(attachId).schemas(attachId, transactionId);
  }

  override async schemaGet(attachId: AttachId, name: string, transactionId?: TransactionId): Promise<SchemaInfo | null> {
    return await this._route(attachId).schemaGet(attachId, name, transactionId);
  }

  override async schemaContentsTables(attachId: AttachId, name: string, transactionId?: TransactionId): Promise<TableInfo[]> {
    return await this._route(attachId).schemaContentsTables(attachId, name, transactionId);
  }

  override async schemaContentsViews(attachId: AttachId, name: string, transactionId?: TransactionId): Promise<ViewInfo[]> {
    return await this._route(attachId).schemaContentsViews(attachId, name, transactionId);
  }

  override async schemaContentsFunctions(attachId: AttachId, name: string, type: string, transactionId?: TransactionId): Promise<FunctionInfo[]> {
    return await this._route(attachId).schemaContentsFunctions(attachId, name, type, transactionId);
  }

  override async schemaContentsMacros(attachId: AttachId, name: string, type: string, transactionId?: TransactionId): Promise<MacroInfo[]> {
    return await this._route(attachId).schemaContentsMacros(attachId, name, type, transactionId);
  }

  override async schemaContentsIndexes(attachId: AttachId, name: string, transactionId?: TransactionId): Promise<IndexInfo[]> {
    return await this._route(attachId).schemaContentsIndexes(attachId, name, transactionId);
  }

  override async indexGet(attachId: AttachId, schemaName: string, name: string, transactionId?: TransactionId): Promise<IndexInfo | null> {
    return await this._route(attachId).indexGet(attachId, schemaName, name, transactionId);
  }

  override async tableGet(attachId: AttachId, schemaName: string, name: string, atUnit?: string, atValue?: string, transactionId?: TransactionId): Promise<TableInfo | null> {
    return await this._route(attachId).tableGet(attachId, schemaName, name, atUnit, atValue, transactionId);
  }

  override async tableScanFunctionGet(attachId: AttachId, schemaName: string, name: string, atUnit?: string, atValue?: string, transactionId?: TransactionId): Promise<any> {
    return await this._route(attachId).tableScanFunctionGet(attachId, schemaName, name, atUnit, atValue, transactionId);
  }

  override async tableColumnStatisticsGet(attachId: AttachId, schemaName: string, name: string, transactionId?: TransactionId): Promise<{ bytes: Uint8Array; cacheMaxAgeSeconds: number | null } | null> {
    return await this._route(attachId).tableColumnStatisticsGet(attachId, schemaName, name, transactionId);
  }

  override async viewGet(attachId: AttachId, schemaName: string, name: string, transactionId?: TransactionId): Promise<ViewInfo | null> {
    return await this._route(attachId).viewGet(attachId, schemaName, name, transactionId);
  }

  override async macroGet(attachId: AttachId, schemaName: string, name: string, transactionId?: TransactionId): Promise<MacroInfo | null> {
    return await this._route(attachId).macroGet(attachId, schemaName, name, transactionId);
  }

  override async transactionBegin(attachId: AttachId): Promise<Uint8Array | null> {
    return await this._route(attachId).transactionBegin(attachId);
  }

  override async transactionCommit(attachId: AttachId, transactionId: TransactionId): Promise<void> {
    await this._route(attachId).transactionCommit(attachId, transactionId);
  }

  override async transactionRollback(attachId: AttachId, transactionId: TransactionId): Promise<void> {
    await this._route(attachId).transactionRollback(attachId, transactionId);
  }
}
