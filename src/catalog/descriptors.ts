// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Declarative catalog descriptor types.

import type { VgiSchema, VgiDataType } from "../arrow/index.js";
import type { AttachCatalogInfo } from "./interface.js";
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
  referencedSchema?: string;
}

export type DefaultValue = string | number | boolean | null;

export interface TableDescriptor {
  name: string;
  columns?: VgiSchema;
  function?: VgiFunction;
  arguments?: Arguments;
  notNull?: string[];
  unique?: string[][];
  check?: string[];
  primaryKey?: string[][];
  foreignKey?: ForeignKeyDef[];
  defaults?: Record<string, DefaultValue>;
  /** Per-column comment strings, applied as field metadata `comment`. */
  columnComments?: Record<string, string>;
  /**
   * Generated (virtual) columns: map of column name → SQL expression computed
   * from other physical columns. Applied as Arrow field metadata
   * `generated_expression`, which the DuckDB VGI extension reads at table
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
  /**
   * Required WHERE-filter groups in conjunctive normal form — an AND (outer
   * list) of OR-groups (inner lists) of dotted-path column references that MUST
   * appear in a WHERE expression for any scan of this table. A group is
   * satisfied when any one of its paths has a filter; every group must be
   * satisfied. So `[["accession_number"], ["ticker", "cik"]]` means
   * "accession_number AND one of (ticker, cik)"; a single-path group
   * `[["country"]]` is a plain mandatory filter. Paths are top-level names
   * (`"country"`) or struct subfields (`"bbox.xmin"`, `"nested.outer.inner"`).
   * Empty/undefined (default) means no enforcement — the zero-cost fast path
   * for every existing table.
   *
   * Satisfaction is prefix-based: a present filter on a shorter path satisfies
   * any required path it is a prefix of. So a whole-struct filter on `bbox`
   * satisfies all of `bbox.xmin` / `.xmax` / `.ymin` / `.ymax`. The VGI DuckDB
   * extension's optimizer pass consults this at bind time and throws a
   * `BinderException` listing any unsatisfied groups.
   */
  requiredFilters?: string[][];
  supportsTimeTravel?: boolean;
  /**
   * Inline cardinality estimate/max surfaced through TableInfo. When set,
   * the C++ extension uses these directly and skips the per-bind
   * table_function_cardinality RPC. Only meaningful for function-backed
   * tables; static-column tables don't have a function to call.
   */
  inlinedCardinality?: { estimate: bigint; max: bigint };
  comment?: string;
  tags?: Record<string, string>;
}

export interface ViewDescriptor {
  name: string;
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
  /**
   * Optional mapping of parameter name to a human/agent-facing description.
   * Keys must appear in `parameters`. Descriptions flow over the wire via the
   * macro `arguments_schema`'s `vgi_doc` field metadata (the same channel
   * functions use for per-argument docs), so the DuckDB extension's
   * `vgi_function_arguments()` can surface them. Empty/omitted = no docs.
   */
  parameterDocs?: Record<string, string>;
  definition: string;
  comment?: string;
  tags?: Record<string, string>;
}

export interface IndexDescriptor {
  name: string;
  tableName: string;
  expressions: string[];
  /** UNIQUE / PRIMARY / NONE; defaults to NONE. */
  constraintType?: "NONE" | "UNIQUE" | "PRIMARY";
  indexType?: string;
  options?: Record<string, string>;
  comment?: string;
  tags?: Record<string, string>;
}

export interface SchemaDescriptor {
  name: string;
  tables?: TableDescriptor[];
  views?: ViewDescriptor[];
  macros?: MacroDescriptor[];
  functions?: VgiFunction[];
  indexes?: IndexDescriptor[];
  comment?: string;
  tags?: Record<string, string>;
}

export interface SecretTypeDescriptor {
  name: string;
  description: string;
  schema: VgiSchema; // Field metadata {redact: "true"} marks redacted fields
}

export interface CatalogDescriptor {
  name: string;
  defaultSchema?: string;
  schemas: SchemaDescriptor[];
  settings?: SettingDescriptor[];
  secretTypes?: SecretTypeDescriptor[];
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
}
