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
  PRESERVES_ORDER = "PRESERVES_ORDER",
  NO_ORDER_GUARANTEE = "NO_ORDER_GUARANTEE",
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

export interface TableCardinality {
  estimate: number | null;
  max: number | null;
}

// Default max workers (matches Python DEFAULT_MAX_WORKERS = 99999)
export const DEFAULT_MAX_WORKERS = 99999;
