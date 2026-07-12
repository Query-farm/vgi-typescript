// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Versioned-tables VGI worker — TS port of
// vgi-python/vgi/_test_fixtures/versioned_tables.py.
//
// The catalog advertises data_version_spec ">=1.0.0,<4.0.0" and resolves the
// client's requested version at ATTACH. The resolved version is encoded into
// attach_opaque_data; the visible table set is filtered per version:
//
//   1.0.0 -> animals               (name, legs, sound)
//   1.1.0 -> animals (with color)  (name, legs, sound, color)
//   2.0.0 -> animals + plants
//   3.0.0 -> plants
//
// Shared catalog + scan functions, consumed by the subprocess
// (versioned-tables-worker.ts) and HTTP (versioned-tables-http-worker.ts) entries.

import {
  CatalogInterface,
  defineTableFunction,
  batchFromColumns,
  toSchema,
  serializeSchema,
  serializeBatch,
  str,
  int,
  float,
  type AttachOpaqueData,
  type TransactionOpaqueData,
  type CatalogAttachResult,
  type CatalogInfo,
  type SchemaInfo,
  type TableInfo,
} from "../src/index.js";
import type { VgiSchema } from "../src/index.js";

const CATALOG_NAME = "versioned_tables";
const DATA_VERSION_SPEC = ">=1.0.0,<4.0.0";
const SUPPORTED_VERSIONS = ["1.0.0", "1.1.0", "2.0.0", "3.0.0"] as const;
const DEFAULT_VERSION = "3.0.0";
const SOURCE_URL = "https://github.com/Query-farm/vgi-python";

// Implementation versions use distinct numbering (10.x/11.x) from data
// versions (1.x/2.x/3.x) so test assertions can't confuse the two dimensions.
const SUPPORTED_IMPL_VERSIONS = ["10.0.0", "10.1.0", "11.0.0"] as const;
const DEFAULT_IMPL_VERSION = "11.0.0";

// Newest-first release manifest (surfaced via vgi_catalogs / describe page).
const DATA_VERSION_RELEASES = [
  { version: "3.0.0", released_at: BigInt(Date.UTC(2026, 3, 15)), summary: "Removed deprecated 'animals' table; 'plants' is now the only table.", notes_url: "https://github.com/Query-farm/vgi-python/releases/tag/data-v3.0.0" },
  { version: "2.0.0", released_at: BigInt(Date.UTC(2026, 1, 1)), summary: "Added 'plants' table alongside 'animals'.", notes_url: "https://github.com/Query-farm/vgi-python/releases/tag/data-v2.0.0" },
  { version: "1.1.0", released_at: BigInt(Date.UTC(2026, 0, 10)), summary: "Added 'sound' column to 'animals'.", notes_url: null },
  { version: "1.0.0", released_at: BigInt(Date.UTC(2026, 0, 1)), summary: "Initial release.", notes_url: null },
];

// ============================================================================
// Static table data + schemas
// ============================================================================

const ANIMALS_SCHEMA_V1 = toSchema({ name: str, legs: int, sound: str });
const ANIMALS_SCHEMA_V1_1 = toSchema({ name: str, legs: int, sound: str, color: str });
const PLANTS_SCHEMA = toSchema({ name: str, kind: str, height_m: float });

const ANIMALS_ROWS = {
  name: ["chicken", "cow", "horse", "pig", "sheep"],
  legs: [2n, 4n, 4n, 4n, 4n],
  sound: ["cluck", "moo", "neigh", "oink", "baa"],
};
const ANIMALS_COLORS = ["red", "brown", "black", "pink", "white"];
const PLANTS_ROWS = {
  name: ["oak", "pine", "rose", "tomato", "wheat"],
  kind: ["tree", "tree", "flower", "vegetable", "grass"],
  height_m: [20.0, 25.0, 0.6, 1.5, 1.0],
};

// ============================================================================
// Scan functions — one per (table, version-variant), each emits once
// ============================================================================

interface EmitOnceState {
  done: boolean;
}

function emitOnce(schemaConst: VgiSchema, columns: Record<string, unknown[]>) {
  return {
    args: {},
    onBind: () => ({ outputSchema: schemaConst }),
    initialState: (): EmitOnceState => ({ done: false }),
    process: (_p: unknown, state: EmitOnceState, out: any) => {
      if (state.done) return out.finish();
      state.done = true;
      out.emit(batchFromColumns(columns, schemaConst));
    },
  };
}

const animalsScan = defineTableFunction({
  name: "versioned_tables_animals_scan",
  description: "Animals table for data_version 1.0.0",
  ...emitOnce(ANIMALS_SCHEMA_V1, ANIMALS_ROWS),
});

const animalsColorScan = defineTableFunction({
  name: "versioned_tables_animals_color_scan",
  description: "Animals table for data_version 1.1.0 (with color)",
  ...emitOnce(ANIMALS_SCHEMA_V1_1, { ...ANIMALS_ROWS, color: ANIMALS_COLORS }),
});

const plantsScan = defineTableFunction({
  name: "versioned_tables_plants_scan",
  description: "Plants table for data_version 2.0.0 and 3.0.0",
  ...emitOnce(PLANTS_SCHEMA, PLANTS_ROWS),
});

// ============================================================================
// Per-version table spec
// ============================================================================

interface VersionedTable {
  functionName: string;
  columns: VgiSchema;
}

const ANIMALS_V1: VersionedTable = { functionName: "versioned_tables_animals_scan", columns: ANIMALS_SCHEMA_V1 };
const ANIMALS_V1_1: VersionedTable = { functionName: "versioned_tables_animals_color_scan", columns: ANIMALS_SCHEMA_V1_1 };
const PLANTS: VersionedTable = { functionName: "versioned_tables_plants_scan", columns: PLANTS_SCHEMA };

const VERSION_TABLES: Record<string, Record<string, VersionedTable>> = {
  "1.0.0": { animals: ANIMALS_V1 },
  "1.1.0": { animals: ANIMALS_V1_1 },
  "2.0.0": { animals: ANIMALS_V1, plants: PLANTS },
  "3.0.0": { plants: PLANTS },
};

// ============================================================================
// npm-ish version spec resolver
// ============================================================================

const EXACT_RE = /^(\d+)\.(\d+)\.(\d+)$/;
const MAJOR_RE = /^(\d+)$/;
const MAJOR_MINOR_RE = /^(\d+)\.(\d+)$/;
const CARET_RE = /^\^(\d+)\.(\d+)\.(\d+)$/;
const TILDE_RE = /^~(\d+)\.(\d+)\.(\d+)$/;

function parseVersion(v: string): [number, number, number] {
  const m = EXACT_RE.exec(v);
  if (!m) throw new Error(`Not a valid version: '${v}'`);
  return [Number(m[1]), Number(m[2]), Number(m[3])];
}

function cmp(a: [number, number, number], b: [number, number, number]): number {
  return a[0] - b[0] || a[1] - b[1] || a[2] - b[2];
}

function resolveAgainst(spec: string | null | undefined, supported: readonly string[], def: string, label: string): string {
  if (!spec) return def;
  const sorted = [...supported].map((v) => [parseVersion(v), v] as const).sort((a, b) => cmp(a[0], b[0]));

  if (EXACT_RE.test(spec)) {
    if (supported.includes(spec)) return spec;
    throw new Error(`Unsupported ${label} '${spec}'; this worker serves ${JSON.stringify(supported)}`);
  }
  let m = MAJOR_RE.exec(spec);
  if (m) {
    const major = Number(m[1]);
    const c = sorted.filter(([t]) => t[0] === major).map(([, v]) => v);
    if (!c.length) throw new Error(`Unsupported ${label} '${spec}'; no major ${major} version available`);
    return c[c.length - 1];
  }
  m = MAJOR_MINOR_RE.exec(spec);
  if (m) {
    const pinned = `${m[1]}.${m[2]}.0`;
    if (supported.includes(pinned)) return pinned;
    throw new Error(`Unsupported ${label} '${spec}'; '${pinned}' not in ${JSON.stringify(supported)}`);
  }
  m = CARET_RE.exec(spec);
  if (m) {
    const base: [number, number, number] = [Number(m[1]), Number(m[2]), Number(m[3])];
    const c = sorted.filter(([t]) => t[0] === base[0] && cmp(t, base) >= 0).map(([, v]) => v);
    if (!c.length) throw new Error(`Unsupported ${label} '${spec}'; no match in major ${base[0]}`);
    return c[c.length - 1];
  }
  m = TILDE_RE.exec(spec);
  if (m) {
    const base: [number, number, number] = [Number(m[1]), Number(m[2]), Number(m[3])];
    const c = sorted.filter(([t]) => t[0] === base[0] && t[1] === base[1] && cmp(t, base) >= 0).map(([, v]) => v);
    if (!c.length) throw new Error(`Unsupported ${label} '${spec}'; no match in ${base[0]}.${base[1]}.x`);
    return c[c.length - 1];
  }
  throw new Error(`Unsupported ${label} '${spec}'; accepted forms: X.Y.Z, X, X.Y, ^X.Y.Z, ~X.Y.Z`);
}

// ============================================================================
// Catalog interface
// ============================================================================

const ATTACH_ID_SEP = 0x00;

class VersionedTablesCatalog extends CatalogInterface {
  catalogs(): string[] {
    return [CATALOG_NAME];
  }

  catalogsInfo(): CatalogInfo[] {
    return [{
      name: CATALOG_NAME,
      implementation_version: DEFAULT_IMPL_VERSION,
      data_version_spec: DATA_VERSION_SPEC,
      releases: DATA_VERSION_RELEASES,
      source_url: SOURCE_URL,
    }];
  }

  attach(
    name: string,
    _options?: Record<string, unknown>,
    dataVersionSpec?: string | null,
    implementationVersion?: string | null,
  ): CatalogAttachResult {
    if (name !== CATALOG_NAME) {
      throw new Error(`Unknown catalog: '${name}'. Available: ${CATALOG_NAME}`);
    }
    const resolvedImpl = resolveAgainst(implementationVersion, SUPPORTED_IMPL_VERSIONS, DEFAULT_IMPL_VERSION, "implementation_version");
    const resolved = resolveAgainst(dataVersionSpec, SUPPORTED_VERSIONS, DEFAULT_VERSION, "data_version_spec");

    // attach_opaque_data = <resolved version utf8> \x00 <16 random bytes>
    const versionBytes = new TextEncoder().encode(resolved);
    const entropy = new Uint8Array(16);
    crypto.getRandomValues(entropy);
    const aod = new Uint8Array(versionBytes.length + 1 + entropy.length);
    aod.set(versionBytes, 0);
    aod[versionBytes.length] = ATTACH_ID_SEP;
    aod.set(entropy, versionBytes.length + 1);

    return {
      attach_opaque_data: aod,
      supports_transactions: false,
      supports_time_travel: false,
      catalog_version_frozen: true,
      catalog_version: 1,
      attach_opaque_data_required: true,
      default_schema: "main",
      resolved_data_version: resolved,
      resolved_implementation_version: resolvedImpl,
    };
  }

  detach(_attachOpaqueData: AttachOpaqueData): void { /* no-op */ }

  version(_attachOpaqueData: AttachOpaqueData, _txn?: TransactionOpaqueData): number {
    return 1;
  }

  schemas(_attachOpaqueData: AttachOpaqueData, _txn?: TransactionOpaqueData): SchemaInfo[] {
    return [{ attach_opaque_data: new Uint8Array(0), name: "main", comment: null, tags: {} }];
  }

  // -- table visibility, filtered by the resolved version in attach_opaque_data --

  private tablesFor(attachOpaqueData: AttachOpaqueData): Record<string, VersionedTable> {
    const raw = attachOpaqueData as Uint8Array;
    const sep = raw.indexOf(ATTACH_ID_SEP);
    if (sep <= 0) return {};
    const version = new TextDecoder().decode(raw.subarray(0, sep));
    return VERSION_TABLES[version] ?? {};
  }

  private makeTableInfo(name: string, table: VersionedTable): TableInfo {
    return {
      comment: null,
      tags: {},
      name,
      schema_name: "main",
      columns: serializeSchema(table.columns),
      not_null_constraints: [],
      unique_constraints: [],
      check_constraints: [],
      primary_key_constraints: [],
      foreign_key_constraints: [],
      supports_insert: false,
      supports_update: false,
      supports_delete: false,
      supports_returning: false,
      supports_column_statistics: false,
      scan_function: null,
      insert_function: null,
      update_function: null,
      delete_function: null,
      cardinality_estimate: null,
      cardinality_max: null,
      column_statistics: null,
      bind_result: null,
      required_filters: [],
    };
  }

  schemaContentsTables(attachOpaqueData: AttachOpaqueData, name: string): TableInfo[] {
    if (name.toLowerCase() !== "main") return [];
    const tables = this.tablesFor(attachOpaqueData);
    return Object.keys(tables)
      .sort()
      .map((n) => this.makeTableInfo(n, tables[n]));
  }

  tableGet(attachOpaqueData: AttachOpaqueData, schemaName: string, name: string): TableInfo | null {
    if (schemaName.toLowerCase() !== "main") return null;
    const table = this.tablesFor(attachOpaqueData)[name.toLowerCase()];
    return table ? this.makeTableInfo(name.toLowerCase(), table) : null;
  }

  // Returns the wire-shape scan result { function_name, arguments,
  // required_extensions }; arguments is a serialized 1-row empty batch (no
  // args). The base tableScanBranchesGet wraps this as a single branch.
  tableScanFunctionGet(attachOpaqueData: AttachOpaqueData, schemaName: string, name: string) {
    if (schemaName.toLowerCase() !== "main") throw new Error(`Unknown schema: ${schemaName}`);
    const table = this.tablesFor(attachOpaqueData)[name.toLowerCase()];
    if (!table) throw new Error(`Table ${schemaName}.${name} not visible at this data version`);
    return {
      function_name: table.functionName,
      arguments: serializeBatch(batchFromColumns({}, toSchema({}))),
      required_extensions: [],
    };
  }
}

export { VersionedTablesCatalog, CATALOG_NAME };
export const versionedTablesFunctions = [animalsScan, animalsColorScan, plantsScan];
