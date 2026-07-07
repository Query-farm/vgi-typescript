// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0
//
// Producer for the versioned VGI HTTP landing contract (`describe.json`) from a
// worker's catalog introspection. This is the TypeScript counterpart to the
// Python reference producer (`vgi/http/describe_json.py`); both emit an
// equivalent document, guarded by the cross-language conformance harness at
// `vgi/test/landing/`. See `vgi/docs/http-landing-contract.md` for the spec.
//
// The document is JSON — it does NOT depend on the (versioned, evolving) VGI
// wire protocol, so the single shared static `landing.html` (byte-identical
// across every language worker) can render it same-origin. Table/view columns
// are lazy: the document carries only a column count, and the page fetches
// per-object detail from `buildColumnsJson`
// (`GET {prefix}/describe/{catalog}/{schema}/{table}.json`) on first expand.

import {
  deserializeBatch,
  deserializeSchema,
  isBinary,
  isBool,
  isFloat,
  isInt,
  isNull,
  isUtf8,
  type VgiDataType,
  type VgiField,
} from "../arrow/index.js";
import type { CatalogInterface } from "../catalog/interface.js";
import type {
  CatalogAttachResult,
  CatalogInfo,
  FunctionInfo,
  SchemaInfo,
  TableInfo,
  ViewInfo,
} from "../generated/vgi-client.js";
import {
  VGI_ARG_KEY,
  VGI_ARG_NAMED,
  VGI_DEFAULT_KEY,
  VGI_DOC_KEY,
  VGI_TYPE_KEY,
  VGI_TYPE_TABLE,
} from "../types.js";

export const LANDING_SCHEMA_VERSION = 1;
export const CUPOLA_BASE = "https://cupola.query-farm.services";

/**
 * Producer for the VGI landing surface, passed to `createHttpHandler`'s
 * `landingDescribe` option. Declared locally (structurally identical to
 * `@query-farm/vgi-rpc`'s `LandingDescribeProvider`) so this package does not
 * hard-couple to a specific vgi-rpc version.
 */
export interface LandingDescribeProvider {
  describe(ctx: { serverId: string; oauth: boolean }): unknown | Promise<unknown>;
  columns(catalog: string, schema: string, table: string): unknown | null | Promise<unknown | null>;
}

/** Identity of the worker software surfaced in `describe.json` → `worker`. */
export interface WorkerDescribeInfo {
  /** Worker name (e.g. class or catalog name). */
  name: string;
  /** One-line worker description. */
  doc?: string;
  /** Worker software version. */
  version?: string;
}

// `duckdb_databases().tags` keys (the reserved `vgi.*` namespace) surfaced in
// the landing page's catalog card. Mirrors the Python reference `_STRING_TAGS`.
const STRING_TAGS: Record<string, string> = {
  title: "vgi.title",
  doc_md: "vgi.doc_md",
  source_url: "vgi.source_url",
  license: "vgi.license",
  author: "vgi.author",
  copyright: "vgi.copyright",
  support_contact: "vgi.support_contact",
  support_policy_url: "vgi.support_policy_url",
};
const KEYWORDS_TAG = "vgi.keywords"; // JSON array encoded as a string in the tags MAP

// ---------------------------------------------------------------------------
// Type / field helpers
// ---------------------------------------------------------------------------

/** Human-readable DuckDB-ish type name for a VGI Arrow type. */
function typeToString(type: VgiDataType): string {
  if (isNull(type)) return "NULL";
  if (isUtf8(type)) return "VARCHAR";
  if (isBinary(type)) return "BLOB";
  if (isBool(type)) return "BOOLEAN";
  if (isInt(type)) {
    const bw = (type as { bitWidth?: number }).bitWidth ?? 32;
    return bw === 64 ? "BIGINT" : "INTEGER";
  }
  if (isFloat(type)) {
    const precision = (type as { precision?: number }).precision ?? 2;
    return precision === 1 ? "FLOAT" : "DOUBLE";
  }
  const s = (type as { toString?: () => string }).toString?.();
  return s ?? String(type.typeId);
}

function fieldMeta(field: VgiField, key: string): string | undefined {
  return field.metadata.get(key);
}

interface ColumnJson {
  name: string;
  type: string;
  comment?: string;
}

function fieldToColumn(field: VgiField): ColumnJson {
  const col: ColumnJson = { name: field.name, type: typeToString(field.type) };
  const comment = fieldMeta(field, "comment") ?? fieldMeta(field, VGI_DOC_KEY);
  if (comment) col.comment = comment;
  return col;
}

function readSchemaSafe(bytes: Uint8Array | null | undefined) {
  if (!bytes || bytes.length === 0) return null;
  try {
    return deserializeSchema(bytes);
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Function mapping
// ---------------------------------------------------------------------------

/** scalar | table | aggregate | table_in_out (mirrors Python `_function_display_type`). */
function functionDisplayType(fn: FunctionInfo): "scalar" | "table" | "aggregate" | "table_in_out" {
  const t = String(fn.function_type).toUpperCase();
  if (t === "SCALAR") return "scalar";
  if (t === "AGGREGATE") return "aggregate";
  if (fn.has_finalize) return "table_in_out";
  return "table";
}

function functionReturns(fn: FunctionInfo): string | undefined {
  const schema = readSchemaSafe(fn.output_schema);
  if (schema === null || schema.fields.length === 0) return undefined;
  const t = String(fn.function_type).toUpperCase();
  if (t === "SCALAR" || t === "AGGREGATE") {
    // Scalar/aggregate output is a single "result" column.
    return typeToString(schema.fields[0].type);
  }
  const cols = schema.fields.map((f) => `${f.name} ${typeToString(f.type)}`).join(", ");
  return `TABLE(${cols})`;
}

interface ArgJson {
  name: string;
  type: string;
  named?: boolean;
  default?: string;
  desc?: string;
}

function functionArgs(fn: FunctionInfo): ArgJson[] {
  const schema = readSchemaSafe(fn.arguments);
  if (schema === null) return [];
  const args: ArgJson[] = [];
  for (const field of schema.fields) {
    // Skip the piped input relation of a table-in-out function; it's not a
    // user-supplied argument.
    if (fieldMeta(field, VGI_TYPE_KEY) === VGI_TYPE_TABLE) continue;
    const arg: ArgJson = { name: field.name, type: typeToString(field.type) };
    if (fieldMeta(field, VGI_ARG_KEY) === VGI_ARG_NAMED) arg.named = true;
    const doc = fieldMeta(field, VGI_DOC_KEY);
    if (doc) arg.desc = doc;
    const rawDefault = fieldMeta(field, VGI_DEFAULT_KEY);
    if (rawDefault !== undefined) {
      // Stored as a JSON scalar; render the decoded value for display.
      try {
        arg.default = JSON.stringify(JSON.parse(rawDefault));
      } catch {
        arg.default = rawDefault;
      }
    }
    args.push(arg);
  }
  return args;
}

// ---------------------------------------------------------------------------
// Catalog contents
// ---------------------------------------------------------------------------

async function schemaContents(
  iface: CatalogInterface,
  aod: Uint8Array,
  name: string,
  kind: "TABLE" | "VIEW",
): Promise<TableInfo[] | ViewInfo[]> {
  try {
    if (kind === "TABLE") {
      return await iface.schemaContentsTables(aod, name);
    }
    return await iface.schemaContentsViews(aod, name);
  } catch {
    return [];
  }
}

async function schemaFunctions(iface: CatalogInterface, aod: Uint8Array, name: string): Promise<FunctionInfo[]> {
  const out: FunctionInfo[] = [];
  for (const kind of ["SCALAR_FUNCTION", "TABLE_FUNCTION", "AGGREGATE_FUNCTION"]) {
    try {
      out.push(...(await iface.schemaContentsFunctions(aod, name, kind)));
    } catch {
      // best-effort — keep whatever we collected
    }
  }
  return out;
}

interface Counts {
  schemas: number;
  tables: number;
  views: number;
  functions: number;
}

async function buildSchemas(
  iface: CatalogInterface,
  aod: Uint8Array,
): Promise<{ schemas: unknown[]; counts: Counts }> {
  let schemaInfos: SchemaInfo[];
  try {
    schemaInfos = await iface.schemas(aod);
  } catch {
    return { schemas: [], counts: { schemas: 0, tables: 0, views: 0, functions: 0 } };
  }

  const schemas: unknown[] = [];
  const counts: Counts = { schemas: 0, tables: 0, views: 0, functions: 0 };

  for (const si of schemaInfos) {
    const tablesRaw = (await schemaContents(iface, aod, si.name, "TABLE")) as TableInfo[];
    const viewsRaw = (await schemaContents(iface, aod, si.name, "VIEW")) as ViewInfo[];
    const funcsRaw = await schemaFunctions(iface, aod, si.name);

    const tables = tablesRaw.map((t) => {
      const schema = readSchemaSafe(t.columns);
      return { name: t.name, cols: schema ? schema.fields.length : 0, comment: t.comment ?? "" };
    });

    const views = viewsRaw.map((v) => ({
      name: v.name,
      cols: Object.keys(v.column_comments ?? {}).length,
      comment: v.comment ?? "",
      def: v.definition,
    }));

    const functions = funcsRaw
      .slice()
      .sort((a, b) =>
        String(a.function_type).localeCompare(String(b.function_type)) || a.name.localeCompare(b.name),
      )
      .map((fn) => {
        const obj: Record<string, unknown> = {
          name: fn.name,
          type: functionDisplayType(fn),
          doc: fn.description ?? "",
          args: functionArgs(fn),
        };
        const ret = functionReturns(fn);
        if (ret) obj.returns = ret;
        return obj;
      });

    schemas.push({ name: si.name, tables, views, functions });
    counts.schemas += 1;
    counts.tables += tables.length;
    counts.views += views.length;
    counts.functions += functions.length;
  }

  return { schemas, counts };
}

// ---------------------------------------------------------------------------
// Catalog-level metadata (tags, attach options, data versions)
// ---------------------------------------------------------------------------

function catalogTags(result: CatalogAttachResult | null): Record<string, unknown> {
  const tags = result?.tags;
  if (!tags) return {};
  const out: Record<string, unknown> = {};
  for (const [key, tag] of Object.entries(STRING_TAGS)) {
    const val = tags[tag];
    if (val) out[key] = val;
  }
  const kw = tags[KEYWORDS_TAG];
  if (kw) {
    try {
      const parsed = JSON.parse(kw);
      if (Array.isArray(parsed)) out.keywords = parsed.map((k) => String(k));
    } catch {
      // ignore unparseable keyword tag
    }
  }
  return out;
}

interface AttachOptionJson {
  name: string;
  type: string;
  default: string;
  description: string;
}

/** Decode a serialized AttachOptionSpec row into the describe shape. */
function decodeAttachOption(bytes: Uint8Array): AttachOptionJson | null {
  try {
    const batch = deserializeBatch(bytes);
    if (batch.numRows === 0) return null;
    const name = String(batch.getChild("name")?.get(0) ?? "");
    const description = String(batch.getChild("description")?.get(0) ?? "");
    const typeBytes = batch.getChild("type")?.get(0) as Uint8Array | null | undefined;
    let type = "";
    const typeSchema = readSchemaSafe(typeBytes ?? null);
    if (typeSchema && typeSchema.fields.length > 0) type = typeToString(typeSchema.fields[0].type);
    let defaultStr = "";
    const defBytes = batch.getChild("default_value")?.get(0) as Uint8Array | null | undefined;
    if (defBytes) {
      try {
        const defBatch = deserializeBatch(defBytes);
        if (defBatch.numRows > 0) {
          const v = defBatch.getChild("value")?.get(0);
          defaultStr = v == null ? "" : String(v);
        }
      } catch {
        // leave default empty
      }
    }
    return { name, type, default: defaultStr, description };
  } catch {
    return null;
  }
}

async function attachCatalog(
  iface: CatalogInterface,
  name: string,
): Promise<CatalogAttachResult | null> {
  try {
    return await iface.attach(name, {}, null, null);
  } catch {
    return null;
  }
}

async function buildCatalog(
  iface: CatalogInterface,
  name: string,
  info: CatalogInfo | undefined,
): Promise<Record<string, unknown>> {
  const result = await attachCatalog(iface, name);

  const attachOptions = (info?.attach_option_specs ?? [])
    .map((b) => decodeAttachOption(b))
    .filter((o): o is AttachOptionJson => o !== null);

  const dataVersions = (info?.releases ?? []).map((r) =>
    r.summary ? { spec: r.version, label: r.summary } : { spec: r.version },
  );

  let impl: string | null = info?.implementation_version ?? null;
  let dvs: string | null = info?.data_version_spec ?? null;
  if (result) {
    impl = result.resolved_implementation_version ?? impl;
    dvs = result.resolved_data_version ?? dvs;
  }

  let schemas: unknown[] = [];
  let counts: Counts = { schemas: 0, tables: 0, views: 0, functions: 0 };
  if (result) {
    const built = await buildSchemas(iface, result.attach_opaque_data);
    schemas = built.schemas;
    counts = built.counts;
  }

  return {
    name,
    implementation_version: impl,
    data_version_spec: dvs,
    data_versions: dataVersions,
    attach_options: attachOptions,
    tags: catalogTags(result),
    counts,
    schemas,
  };
}

async function catalogInfoByName(iface: CatalogInterface): Promise<Map<string, CatalogInfo>> {
  const map = new Map<string, CatalogInfo>();
  if (typeof iface.catalogsInfo === "function") {
    try {
      const infos = await iface.catalogsInfo();
      for (const info of infos) map.set(info.name, info);
    } catch {
      // fall back to bare names below
    }
  }
  return map;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Build the full `describe.json` document for a worker's catalog interface.
 *
 * `oauth` and `serverId` are supplied by the HTTP server (they depend on runtime
 * config, not the catalog).
 */
export async function buildDescribeJson(
  iface: CatalogInterface,
  worker: WorkerDescribeInfo,
  opts: { oauth?: boolean; serverId?: string; cupolaBase?: string } = {},
): Promise<Record<string, unknown>> {
  const infos = await catalogInfoByName(iface);
  let names: string[];
  try {
    names = iface.catalogs();
  } catch {
    names = [];
  }

  const catalogs: unknown[] = [];
  for (const name of names) {
    catalogs.push(await buildCatalog(iface, name, infos.get(name)));
  }

  return {
    landing_schema_version: LANDING_SCHEMA_VERSION,
    worker: {
      name: worker.name,
      doc: worker.doc ?? "",
      version: worker.version ?? "unknown",
      lang: "typescript",
    },
    server_id: opts.serverId ?? "",
    oauth: opts.oauth ?? false,
    cupola_base: opts.cupolaBase ?? CUPOLA_BASE,
    catalogs,
  };
}

/**
 * Build the lazy per-object column payload for one table or view.
 *
 * Returns `{ columns: [{ name, type, comment? }] }`, or `null` when the object
 * can't be found. Tables deserialize their Arrow column schema; views expose
 * their declared column comments (types are only known after binding the SQL,
 * which the worker does not do here).
 */
export async function buildColumnsJson(
  iface: CatalogInterface,
  catalog: string,
  schema: string,
  table: string,
): Promise<{ columns: ColumnJson[] } | null> {
  const result = await attachCatalog(iface, catalog);
  if (!result) return null;
  const aod = result.attach_opaque_data;

  const tables = (await schemaContents(iface, aod, schema, "TABLE")) as TableInfo[];
  for (const t of tables) {
    if (t.name === table) {
      const arrow = readSchemaSafe(t.columns);
      return { columns: (arrow?.fields ?? []).map((f) => fieldToColumn(f)) };
    }
  }

  const views = (await schemaContents(iface, aod, schema, "VIEW")) as ViewInfo[];
  for (const v of views) {
    if (v.name === table) {
      return {
        columns: Object.entries(v.column_comments ?? {}).map(([name, comment]) => ({
          name,
          type: "",
          comment,
        })),
      };
    }
  }

  return null;
}

/**
 * Build a {@link LandingDescribeProvider} to pass to `createHttpHandler` from
 * `@query-farm/vgi-rpc`. Wires the worker's catalog interface into the
 * standardized landing surface (`GET {prefix}/describe.json` and the lazy
 * per-object column endpoint).
 */
export function createLandingDescribe(
  iface: CatalogInterface,
  worker: WorkerDescribeInfo,
  opts: { cupolaBase?: string } = {},
): LandingDescribeProvider {
  return {
    describe: (ctx) => buildDescribeJson(iface, worker, { ...ctx, cupolaBase: opts.cupolaBase }),
    columns: (catalog, schema, table) => buildColumnsJson(iface, catalog, schema, table),
  };
}
