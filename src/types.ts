// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// VGI core types - enums and constants matching Python wire format exactly.

export enum FunctionType {
  SCALAR = "scalar",
  TABLE = "table",
  AGGREGATE = "aggregate",
}

export enum FunctionStability {
  CONSISTENT = "CONSISTENT",
  VOLATILE = "VOLATILE",
  CONSISTENT_WITHIN_QUERY = "CONSISTENT_WITHIN_QUERY",
}

export enum NullHandling {
  DEFAULT = "DEFAULT",
  SPECIAL = "SPECIAL",
}

export enum OrderPreservation {
  /** Output rows are in same order as input rows (DuckDB INSERTION_ORDER). */
  PRESERVES_ORDER = "PRESERVES_ORDER",
  /** Output order is undefined; may be reordered (DuckDB NO_ORDER). */
  NO_ORDER_GUARANTEE = "NO_ORDER_GUARANTEE",
  /** Output is in a fixed mandatory order; DuckDB serialises the pipeline
   *  (single worker) to preserve it (DuckDB FIXED_ORDER). */
  FIXED_ORDER = "FIXED_ORDER",
}

export enum OrderDependence {
  ORDER_DEPENDENT = "ORDER_DEPENDENT",
  NOT_ORDER_DEPENDENT = "NOT_ORDER_DEPENDENT",
}

export enum DistinctDependence {
  DISTINCT_DEPENDENT = "DISTINCT_DEPENDENT",
  NOT_DISTINCT_DEPENDENT = "NOT_DISTINCT_DEPENDENT",
}

export enum TableInOutPhase {
  INPUT = "INPUT",
  FINALIZE = "FINALIZE",
  // Sink+Source (TableBufferingFunction) init phases. TABLE_BUFFERING is the
  // sink-side init (persist init metadata so any pool worker can serve
  // process/combine); TABLE_BUFFERING_FINALIZE is the per-finalize_state_id
  // Source-stream init.
  TABLE_BUFFERING = "TABLE_BUFFERING",
  TABLE_BUFFERING_FINALIZE = "TABLE_BUFFERING_FINALIZE",
}

// Arrow schema metadata keys (must match Python exactly - bytes in Python, strings in TS)
export const VGI_ARG_KEY = "vgi_arg";
export const VGI_ARG_NAMED = "named";
export const VGI_TYPE_KEY = "vgi_type";
export const VGI_TYPE_TABLE = "table";
export const VGI_TYPE_ANY = "any";
export const VGI_VARARGS_KEY = "vgi_varargs";
export const VGI_VARARGS_TRUE = "true";
export const VGI_CONST_KEY = "vgi_const";
export const VGI_CONST_TRUE = "true";
// Per-argument description (UTF-8; presence-only). `vgi_doc_*` prefix reserved.
export const VGI_DOC_KEY = "vgi_doc";

// Per-argument constraint metadata for agent discovery. All presence-only
// (omitted when absent) and value-encoded as UTF-8 so the surfaced column type
// stays uniform regardless of the arg's value type:
//   vgi_default — JSON scalar (the arg's default value; optional args only)
//   vgi_choices — JSON array (the closed set of allowed values)
//   vgi_range   — interval notation built from ge/le/gt/lt (e.g. "[0, 100]",
//                 "(0, +inf)", "[1, 10)"); a discovery surface, not raw bounds
//   vgi_pattern — raw regex the value must match (open set)
// Kept byte-for-byte in sync with the C++ reader and the Python/other SDKs.
export const VGI_DEFAULT_KEY = "vgi_default";
export const VGI_CHOICES_KEY = "vgi_choices";
export const VGI_RANGE_KEY = "vgi_range";
export const VGI_PATTERN_KEY = "vgi_pattern";

export interface TableCardinality {
  estimate: number | null;
  max: number | null;
}

// Default max workers (matches Python DEFAULT_MAX_WORKERS = 99999)
export const DEFAULT_MAX_WORKERS = 99999;
