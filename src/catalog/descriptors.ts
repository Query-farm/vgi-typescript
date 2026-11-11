// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Declarative catalog descriptor types.

import type { VgiSchema, VgiDataType } from "../arrow/index.js";
import type { AttachCatalogInfo } from "./interface.js";
import type { AttachOptionSpec } from "./attach-option.js";
import type { WriteOperation, WriteResultMode } from "../write-results.js";
import type { VgiFunction } from "../functions/types.js";
import type { Arguments } from "../arguments/arguments.js";
import type { ColumnStatistics } from "../util/statistics.js";

export interface SettingDescriptor {
  name: string;
  description: string;
  type: VgiDataType;
  /** Must be a JS primitive compatible with `type` (string, number, bigint, boolean). */
  defaultValue?: string | number | bigint | boolean;
}

export interface ForeignKeyDef {
  columns: string[];
  referencedTable: string;
  referencedColumns: string[];
  referencedSchemaPath?: string[];
}

export type DefaultValue = string | number | boolean | null;

export interface TableDescriptor {
  /**
   * Canonical ConstraintInfo IPC records with provider-owned stable identities
   * (build them with `constraintRecord`). They also set the served Arrow
   * nullability: a column is served non-nullable exactly when a NOT_NULL or
   * PRIMARY_KEY record names it, so a non-nullable field nobody names is
   * served nullable.
   */
  constraints?: Uint8Array[];
  /** Opaque token that changes whenever `constraints` change. */
  definitionRevision?: Uint8Array | null;
  name: string;
  columns?: VgiSchema;
  /**
   * The table's scan function. It need not be listed in any schema's
   * `functions`: the SDK always sends it in the table's schema listing so
   * DuckDB knows its capabilities (projection/filter pushdown, rowid, ...),
   * as `table_only` (absent from `duckdb_functions()`, not callable from SQL)
   * unless `exposeScan` is set or a schema also lists it in `functions`.
   */
  function?: VgiFunction;
  /**
   * Protocol 3.3: also expose the scan `function` as an ordinary, callable
   * table function (`visibility: "public"`). Default false: the scan is sent
   * `table_only`, so it resolves this table but is absent from
   * `duckdb_functions()` and cannot be called directly. Listing the function
   * in a schema's `functions` makes it public too. Requires `function`.
   */
  exposeScan?: boolean;
  arguments?: Arguments;
  /**
   * Table in-out function backing `INSERT INTO <table>`. It receives the new
   * rows (the table's user columns) and returns a `writeResultSchema` batch
   * for the mode in its `write_options` (see `write-results.ts`). Omitted
   * means the table does not accept INSERT.
   *
   * Write functions are always sent `table_only` (protocol 3.3): DuckDB
   * learns their capabilities (`has_finalize`, the write schema) but they are
   * not callable from SQL, even when a schema also lists them in `functions`.
   */
  insertFunction?: VgiFunction;
  /**
   * Table in-out function backing `UPDATE <table>`. It receives the changed
   * columns plus the row-id column (`rowIdField()`, which the scan
   * `function` must emit) for each updated row. Requires `function`, which
   * must also support projection pushdown (see `rowIdField`). The scan need
   * not be listed in the schema's `functions`: it is always sent, as
   * `table_only` unless `exposeScan`.
   */
  updateFunction?: VgiFunction;
  /**
   * Table in-out function backing `DELETE FROM <table>`. It receives the
   * row-id column of each deleted row. Requires `function`, with the same
   * projection-pushdown conditions as `updateFunction`.
   */
  deleteFunction?: VgiFunction;
  /**
   * The richest result mode each write function produces: `count` (plain
   * DML), `rows` (`RETURNING`) or `changes` (old/new pairs). Ordered
   * `count < rows < changes`; supporting one mode promises every lower one.
   * An operation with a write function but no entry defaults to `count`.
   * Mirrors vgi-python's `Table.write_result_modes`.
   */
  writeResultModes?: Partial<Record<WriteOperation, WriteResultMode>>;
  /** @deprecated Not sent under vgi.v3 (serving throws). Use `constraints: [constraintRecord({ kind: "NOT_NULL", ... })]`. */
  notNull?: string[];
  /** @deprecated Not sent under vgi.v3 (serving throws). Use `constraints: [constraintRecord({ kind: "UNIQUE", ... })]`. */
  unique?: string[][];
  /** @deprecated Not sent under vgi.v3 (serving throws). Use `constraints: [constraintRecord({ kind: "CHECK", ... })]`. */
  check?: string[];
  /** @deprecated Not sent under vgi.v3 (serving throws). Use `constraints: [constraintRecord({ kind: "PRIMARY_KEY", ... })]`. */
  primaryKey?: string[][];
  /** @deprecated Not sent under vgi.v3 (serving throws). Use `constraints: [constraintRecord({ kind: "FOREIGN_KEY", ... })]`. */
  foreignKey?: ForeignKeyDef[];
  /**
   * Column defaults as literals. Served as canonical `vgi.column.default.v1`
   * when a string literal meets a utf8 column or a boolean a boolean column,
   * and as the legacy `default` SQL key otherwise (numeric defaults included:
   * their canonical form would show in duckdb_columns() as a folded cast).
   */
  defaults?: Record<string, DefaultValue>;
  /** Per-column comment strings, applied as field metadata `comment`. */
  columnComments?: Record<string, string>;
  /**
   * Generated (virtual) columns: map of column name → SQL expression computed
   * from other physical columns. Applied as Arrow field metadata
   * canonical SqlExpression IPC with explicit owner/context, which DuckDB reads at table
   * registration. The backing scan function should not return these columns —
   * DuckDB evaluates the expressions client-side.
   */
  generatedColumns?: Record<string, string>;
  /**
   * Per-column statistics for the optimizer. Keys are column names; values
   * are ColumnStatistics records with typed min/max and Arrow type info.
   * DuckDB uses these for plan-time filter elimination and join reordering
   * via the `catalog_table_column_statistics_get` RPC.
   */
  statistics?: Record<string, ColumnStatistics>;
  /**
   * Cache TTL for this table's column statistics. DuckDB caches the result
   * of `catalog_table_column_statistics_get` for up to this many seconds.
   * `undefined`/null means cache indefinitely; `0` means never cache
   * (always re-fetch).
   */
  statisticsCacheMaxAgeSeconds?: number | null;
  /** AND-of-OR groups of raw component paths, e.g. `[[["bbox", "xmin"]], [["ticker"], ["cik"]]]`.
   * Dots and quotes are literal. A present component prefix satisfies a path;
   * a child does not satisfy its parent. Empty/undefined means no requirements. */
  requiredFilters?: string[][][];
  supportsTimeTravel?: boolean;
  /**
   * Protocol 3.2 — when the client resolves this table's scan redirect.
   * `"bind"` (default) keeps 3.1 behaviour. `"filtered"` makes the client call
   * `catalog_table_scan_branches_get` once per scan, after collecting the
   * query's predicates on the table, and pass them as `pushdown_filters` to
   * the catalog's `tableScanBranchesPlan` (override it on a
   * `ReadOnlyCatalogInterface` subclass); zero branches then means "no rows".
   * Requires `columns` and no `function`: a `function` table inlines a fixed
   * scan redirect.
   */
  scanPlanning?: "bind" | "filtered";
  /**
   * Inline cardinality estimate/max surfaced through TableInfo. When set,
   * the C++ extension uses these directly and skips the per-bind
   * table_function_cardinality RPC. Only meaningful for function-backed
   * tables; static-column tables don't have a function to call.
   */
  inlinedCardinality?: { estimate: number | bigint; max: number | bigint };
  comment?: string;
  tags?: Record<string, string>;
}

/**
 * Protocol 3.3 `FunctionInfo.visibility` values. Null/absent on the wire means
 * `public`.
 *
 * - `public`: an ordinary catalog function, listed in `duckdb_functions()` and
 *   callable from SQL.
 * - `table_only`: sent only so the client knows the capabilities of a table's
 *   scan or write function; hidden from `duckdb_functions()` and not callable.
 */
export const FUNCTION_VISIBILITY_PUBLIC = "public";
export const FUNCTION_VISIBILITY_TABLE_ONLY = "table_only";
export type FunctionVisibility = typeof FUNCTION_VISIBILITY_PUBLIC | typeof FUNCTION_VISIBILITY_TABLE_ONLY;

export interface ViewDescriptor {
  name: string;
  /** Ordered raw output identifiers; empty uses query names. */
  aliases?: string[];
  /** Optimization barrier mode; readers must enforce or reject it. */
  security?: "regular" | "barrier";
  definition: string;
  comment?: string;
  /**
   * Per-column comments keyed by the view's output column name. Unlike tables
   * (whose column comments ride along as Arrow field metadata), a view ships
   * only its SQL `definition`, so column comments need their own channel. The
   * C++ extension aligns these by name against the bound output columns and
   * feeds them into `CreateViewInfo.column_comments_map`; names that don't
   * match a bound column are ignored.
   */
  columnComments?: Record<string, string>;
  tags?: Record<string, string>;
}

export interface MacroDescriptor {
  name: string;
  macroType: "scalar" | "table";
  parameters: string[];
  parameterDefaultValues?: Uint8Array | null;
  /** Complete canonical family; simple-definition fields must be empty when supplied. */
  overloads?: import("../generated/vgi-client.js").MacroOverload[];
  /**
   * Optional mapping of parameter name to a human/agent-facing description.
   * Keys must appear in `parameters`. Descriptions flow over the wire via the
   * canonical parameter's `documentation` field, so the DuckDB extension's
   * `vgi_function_arguments()` can surface them. Empty/omitted = no docs.
   */
  parameterDocs?: Record<string, string>;
  definition: string;
  comment?: string;
  tags?: Record<string, string>;
}

export interface IndexDescriptor {
  name: string;
  /** Complete provider-authored definition; no expression strings or options are inferred. */
  definition: Uint8Array;
  /** Effective values and persisted override names, including explicit NULL. */
  options: Uint8Array;
  comment?: string;
  tags?: Record<string, string>;
}

export interface SchemaDescriptor {
  /** Raw identifier components, from the catalog root to this schema. */
  path?: string[];
  /** @deprecated Protocol 1.x compatibility; prefer `path: [name]`. */
  name?: string;
  tables?: TableDescriptor[];
  views?: ViewDescriptor[];
  macros?: MacroDescriptor[];
  functions?: VgiFunction[];
  indexes?: IndexDescriptor[];
  comment?: string;
  tags?: Record<string, string>;
}

export function schemaDescriptorPath(schema: SchemaDescriptor): string[] {
  if (schema.path) return schema.path;
  if (schema.name) return [schema.name];
  throw new Error("SchemaDescriptor requires a non-empty path");
}

export interface SecretTypeDescriptor {
  name: string;
  description: string;
  schema: VgiSchema; // Field metadata {redact: "true"} marks redacted fields
}

export interface CatalogDescriptor {
  name: string;
  /** One raw root identifier; null means no implicit default, omitted means main. */
  defaultSchema?: string | null;
  schemas: SchemaDescriptor[];
  settings?: SettingDescriptor[];
  secretTypes?: SecretTypeDescriptor[];
  /**
   * Options the catalog accepts at `ATTACH` time
   * (`ATTACH 'x' (TYPE vgi, LOCATION '...', region 'eu', api_key '...')`).
   * Advertised in the catalog's discovery record so the DuckDB extension
   * validates and casts them (and redacts `secret` ones), and `required` ones
   * are enforced at attach. Mirrors vgi-python's
   * `ReadOnlyCatalogInterface.attach_option_specs`.
   *
   * The values reach `attach()` as its `options`; to hand them to functions,
   * override `attach()` and carry them in `attach_opaque_data`
   * (examples/attach-options-worker.ts), which every function call receives.
   */
  attachOptions?: AttachOptionSpec[];
  /**
   * Companion catalogs (lakehouse federation) the client should ATTACH when this
   * VGI catalog attaches. Surfaced via `catalog_attach.attach_catalogs`.
   */
  attachCatalogs?: AttachCatalogInfo[];
  comment?: string;
  tags?: Record<string, string>;
  /**
   * Homepage for this catalog — repo, docs, or dataset landing page. Surfaced
   * through the `catalog_catalogs` discovery record as `CatalogInfo.source_url`,
   * which DuckDB exposes via `duckdb_databases()`. Optional; omitted/`undefined`
   * advertises a null `source_url`.
   */
  sourceUrl?: string;
  /**
   * Functions this catalog asks the client to *additionally* publish into its
   * global (DuckDB `system.main`) function namespace, on top of the normal
   * schema-qualified registration. Surfaced as IPC-serialized `FunctionInfo`
   * records on `catalog_attach.global_functions`.
   *
   * Every entry must also be declared in exactly one of this catalog's
   * schemas — the `FunctionInfo.schema_path` it carries is the bind-dispatch
   * key, not a sentinel. Mirrors vgi-python's `Catalog.global_functions`.
   */
  globalFunctions?: VgiFunction[];
  /**
   * Prefix applied client-side to every `globalFunctions` entry's name when it
   * is published globally (e.g. prefix `vgi_example` + `global_scalar` →
   * `vgi_example_global_scalar`). The advertised `FunctionInfo.name` stays
   * unprefixed. Surfaced as `catalog_attach.global_function_prefix`.
   */
  globalFunctionPrefix?: string;
}
