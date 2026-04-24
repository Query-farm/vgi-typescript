// Metadata types for function introspection and DuckDB registration.

import {
  FunctionStability,
  NullHandling,
  OrderPreservation,
  OrderDependence,
  DistinctDependence,
} from "../types.js";

export enum CatalogFunctionType {
  SCALAR = "SCALAR",
  TABLE = "TABLE",
  AGGREGATE = "AGGREGATE",
}

export interface ParameterInfo {
  name: string;
  position: number | null;
  positionName: string | null;
  typeName: string | null;
  description: string;
  required: boolean;
  default: string | null;
  constraints: string | null;
  isTableInput: boolean;
  isVarargs: boolean;
  isConst: boolean;
}

export interface FunctionExample {
  sql: string;
  description: string;
  expectedOutput: string | null;
}

export interface ResolvedMetadata {
  name: string;
  className: string;
  functionType: CatalogFunctionType;
  description: string;
  examples: FunctionExample[];
  categories: string[];
  tags: Record<string, string>;
  parameters: ParameterInfo[];
  stability: FunctionStability;
  nullHandling: NullHandling;
  requiredSettings: string[];
  requiredSecrets: string[];
  projectionPushdown: boolean;
  filterPushdown: boolean;
  samplingPushdown: boolean;
  supportedExpressionFilters: string[];
  preservesOrder: OrderPreservation;
  maxWorkers: number | null;
  orderDependent: OrderDependence;
  distinctDependent: DistinctDependence;
}
