// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Metadata types for function introspection and DuckDB registration.

import {
  FunctionStability,
  NullHandling,
  ArgumentMonotonicity,
  OrderPreservation,
  OrderDependence,
  DistinctDependence,
} from "../types.js";

export enum CatalogFunctionType {
  SCALAR = "SCALAR",
  TABLE = "TABLE",
  TABLE_BUFFERING = "TABLE_BUFFERING",
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

export interface FilterFunctionCapability {
  namespace: string;
  name: string;
  version: number;
}

export interface EvaluationContextCapability {
  profile: string;
  providerFingerprint: string | null;
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
  argumentMonotonicity: ArgumentMonotonicity[] | null;
  requiredSettings: string[];
  requiredSecrets: string[];
  projectionPushdown: boolean;
  filterPushdown: boolean;
  samplingPushdown: boolean;
  filterSemanticProfiles: string[];
  additionalFilterFunctions: FilterFunctionCapability[];
  runtimeFilterAlgorithms: FilterFunctionCapability[];
  filterEvaluationContexts: EvaluationContextCapability[];
  preservesOrder: OrderPreservation;
  maxWorkers: number | null;
  orderDependent: OrderDependence;
  distinctDependent: DistinctDependence;
}
