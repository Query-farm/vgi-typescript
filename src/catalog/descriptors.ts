// Declarative catalog descriptor types.

import type { Schema, DataType } from "@query-farm/apache-arrow";
import type { VgiFunction } from "../functions/types.js";
import type { Arguments } from "../arguments/arguments.js";
import type { ColumnStatistics } from "../util/statistics.js";

export interface SettingDescriptor {
  name: string;
  description: string;
  type: DataType;
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
  columns?: Schema;
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
  tags?: Record<string, string>;
}

export interface MacroDescriptor {
  name: string;
  macroType: "scalar" | "table";
  parameters: string[];
  parameterDefaultValues?: Uint8Array | null;
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
  schema: Schema; // Field metadata {redact: "true"} marks redacted fields
}

export interface CatalogDescriptor {
  name: string;
  defaultSchema?: string;
  schemas: SchemaDescriptor[];
  settings?: SettingDescriptor[];
  secretTypes?: SecretTypeDescriptor[];
  comment?: string;
  tags?: Record<string, string>;
}
