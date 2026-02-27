// Extract metadata from VgiFunction configs.

import {
  FunctionStability,
  NullHandling,
  OrderPreservation,
  OrderDependence,
  DistinctDependence,
  DEFAULT_MAX_WORKERS,
} from "../types.js";
import type { VgiFunction } from "../functions/types.js";
import type { ArgumentSpec } from "../arguments/argument-spec.js";
import {
  CatalogFunctionType,
  type ResolvedMetadata,
  type ParameterInfo,
  type FunctionExample,
} from "./types.js";
import { DataType, Utf8, Binary, Int64, Int32, Float64, Float32, Bool, Null } from "apache-arrow";

function arrowTypeToName(type: DataType): string | null {
  if (type instanceof Utf8) return "VARCHAR";
  if (type instanceof Binary) return "BLOB";
  if (type instanceof Int64) return "BIGINT";
  if (type instanceof Int32) return "INTEGER";
  if (type instanceof Float64) return "DOUBLE";
  if (type instanceof Float32) return "FLOAT";
  if (type instanceof Bool) return "BOOLEAN";
  if (type instanceof Null) return null;
  return type.toString();
}

function specToParameterInfo(spec: ArgumentSpec): ParameterInfo {
  return {
    name: spec.name,
    position: typeof spec.position === "number" ? spec.position : null,
    positionName: typeof spec.position === "string" ? spec.position : null,
    typeName: arrowTypeToName(spec.arrowType),
    description: "",
    required: true,
    default: null,
    constraints: null,
    isTableInput: spec.isTableInput ?? false,
    isVarargs: spec.isVarargs ?? false,
    isConst: spec.isConst ?? false,
  };
}

export function resolveMetadata(func: VgiFunction): ResolvedMetadata {
  const meta = func.meta;

  let functionType: CatalogFunctionType;
  switch (func.kind) {
    case "scalar":
      functionType = CatalogFunctionType.SCALAR;
      break;
    case "table":
    case "table_in_out":
      functionType = CatalogFunctionType.TABLE;
      break;
    default:
      functionType = CatalogFunctionType.TABLE;
  }

  const parameters: ParameterInfo[] = func.argumentSpecs.map(specToParameterInfo);

  const examples: FunctionExample[] = (meta.examples ?? []).map((e) => ({
    sql: e.sql,
    description: e.description,
    expectedOutput: e.expectedOutput ?? null,
  }));

  const maxWorkers = meta.maxWorkers ?? DEFAULT_MAX_WORKERS;

  return {
    name: meta.name,
    className: meta.name, // In TS, no class name distinction
    functionType,
    description: meta.description ?? "",
    examples,
    categories: meta.categories ?? [],
    tags: meta.tags ?? {},
    parameters,
    stability: meta.stability ?? FunctionStability.CONSISTENT,
    nullHandling: meta.nullHandling ?? NullHandling.DEFAULT,
    requiredSettings: meta.requiredSettings ?? [],
    requiredSecrets: meta.requiredSecrets ?? [],
    projectionPushdown: meta.projectionPushdown ?? false,
    filterPushdown: meta.filterPushdown ?? false,
    preservesOrder: meta.preservesOrder ?? OrderPreservation.NO_ORDER_GUARANTEE,
    maxWorkers: maxWorkers === DEFAULT_MAX_WORKERS ? null : maxWorkers,
    orderDependent: meta.orderDependent ?? OrderDependence.NOT_ORDER_DEPENDENT,
    distinctDependent: meta.distinctDependent ?? DistinctDependence.NOT_DISTINCT_DEPENDENT,
  };
}
