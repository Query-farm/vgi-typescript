// Copyright 2025, 2026 Query Farm LLC - https://query.farm

import type { VgiBatch, VgiDataType, VgiField } from "../arrow/index.js";

export const FILTER_ENCODING = "vgi.filters.v2";
export const FILTER_VERSION = "2";
export const DUCKDB_STANDARD_V1 = "vgi.duckdb.standard.v1";
export const NO_EVALUATION_CONTEXT = "vgi.none.v1";
export const DUCKDB_SESSION_CONTEXT = "vgi.duckdb.session.v1";

export enum ComparisonOp {
  EQ = "eq",
  NE = "ne",
  LT = "lt",
  LE = "le",
  GT = "gt",
  GE = "ge",
  DISTINCT_FROM = "distinct_from",
  NOT_DISTINCT_FROM = "not_distinct_from",
}

export type ArithmeticOp = "add" | "subtract" | "multiply" | "divide" | "modulo";
export type PredicateMode = "required" | "advisory";
export type PredicateSource = "query" | "join" | "top_n" | "split_refinement" | "other";
export type StandardFilterFunction = "starts_with" | "ends_with" | "contains" | "list_contains";

export interface FunctionIdentity {
  namespace: string;
  name: string;
  version: number;
}

export interface EvaluationContext {
  profile: string;
  timeZone?: string;
  calendar?: string;
  defaultCollation?: string;
  ieeeFloatingPointOps?: boolean;
  integerDivision?: boolean;
  providerFingerprint?: string;
}

export type FilterExpression =
  | { node: "column_ref"; columnIndex: number; columnName: string; dataType?: VgiDataType }
  | { node: "field_ref"; expression: FilterExpression; fieldIndex: number; fieldName: string; dataType: VgiDataType }
  | { node: "literal"; valueRef: number; field: VgiField; value: unknown }
  | { node: "comparison"; op: ComparisonOp; left: FilterExpression; right: FilterExpression }
  | { node: "and" | "or"; children: FilterExpression[] }
  | { node: "not"; expression: FilterExpression }
  | { node: "is_null"; expression: FilterExpression; negated: boolean }
  | { node: "in"; expression: FilterExpression; set: FilterSet; negated: boolean }
  | { node: "cast"; expression: FilterExpression; typeRef: number; field: VgiField }
  | { node: "arithmetic"; op: ArithmeticOp; left: FilterExpression; right: FilterExpression }
  | { node: "negate"; expression: FilterExpression }
  | {
      node: "call";
      function: StandardFilterFunction | FunctionIdentity;
      arguments: FilterExpression[];
      options?: Record<string, unknown>;
    }
  | {
      node: "runtime_filter";
      algorithm: FunctionIdentity;
      input: FilterExpression;
      artifactRef: number;
      field: VgiField;
      artifact: unknown;
      nullHandling: "pass" | "reject";
      supported: boolean;
    };

export type FilterSet =
  | { kind: "literal"; valueRef: number; field: VgiField; values: unknown[] }
  | {
      kind: "external";
      batchIndex: number;
      columnIndex: number;
      columnName: string;
      values: unknown[];
      batch: VgiBatch;
    };

export interface FilterPredicate {
  id: string;
  revision: number;
  mode: PredicateMode;
  source: PredicateSource;
  expression: FilterExpression;
}

export interface FilterCapabilities {
  extensionFunctions?: ReadonlyArray<FunctionIdentity>;
  runtimeAlgorithms?: ReadonlyArray<FunctionIdentity>;
  evaluationContexts?: ReadonlyArray<{ profile: string; providerFingerprint?: string | null }>;
}

// Source aliases ease the protocol-2.0 transition for applications that imported
// the old names. The wire model itself is v2-only.
export type Filter = FilterExpression;
export type ExprNode = FilterExpression;
export type ConstantFilter = FilterExpression;
export type IsNullFilter = FilterExpression;
export type IsNotNullFilter = FilterExpression;
export type InFilter = FilterExpression;
export type AndFilter = FilterExpression;
export type OrFilter = FilterExpression;
export type StructFilter = FilterExpression;
export type ExpressionFilter = FilterExpression;
