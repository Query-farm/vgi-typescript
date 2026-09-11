// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Extract metadata from VgiFunction configs.

import {
  FunctionStability,
  NullHandling,
  ArgumentMonotonicity,
  OrderPreservation,
  OrderDependence,
  DistinctDependence,
  DEFAULT_MAX_WORKERS,
} from "../types.js";
import type { VgiFunction } from "../functions/types.js";
import type { ArgumentSpec } from "../arguments/argument-spec.js";
import { supportsExtensionFilterFunction } from "../filter-pushdown/capabilities.js";
import {
  CatalogFunctionType,
  type ResolvedMetadata,
  type ParameterInfo,
  type FunctionExample,
} from "./types.js";
import { type VgiDataType, TypeId, isUtf8, isBinary, isBool, isNull, isFloat, isInt } from "../arrow/index.js";
function arrowTypeToName(type: VgiDataType): string | null {
  if (isNull(type)) return null;
  if (isUtf8(type)) return "VARCHAR";
  if (isBinary(type)) return "BLOB";
  if (isBool(type)) return "BOOLEAN";
  if (isInt(type)) {
    const bw = (type as any).bitWidth ?? 32;
    return bw === 64 ? "BIGINT" : "INTEGER";
  }
  if (isFloat(type)) {
    // arrow-js: Float.precision === 0/1/2 (HALF/SINGLE/DOUBLE).
    // flechette: same precision values.
    const precision = (type as any).precision ?? 2;
    return precision === 1 ? "FLOAT" : "DOUBLE";
  }
  return String((type as any).toString?.() ?? type.typeId);
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

  if (meta.runtimeFilterAlgorithms?.length) {
    throw new Error("TypeScript SDK has no registered runtime-filter artifact evaluator to advertise");
  }
  if (meta.filterEvaluationContexts?.length) {
    throw new Error("TypeScript SDK has no isolated DuckDB session-context evaluator to advertise");
  }
  if (meta.filterSemanticProfiles?.some((value) => value !== "vgi.duckdb.standard.v1")) {
    throw new Error("TypeScript SDK supports only vgi.duckdb.standard.v1 filter semantics");
  }
  if (meta.additionalFilterFunctions?.some((value) => !supportsExtensionFilterFunction(value))) {
    throw new Error("TypeScript SDK cannot advertise an extension-filter function without an evaluator");
  }

  let functionType: CatalogFunctionType;
  switch (func.kind) {
    case "scalar":
      functionType = CatalogFunctionType.SCALAR;
      break;
    case "table":
    case "table_in_out":
      functionType = CatalogFunctionType.TABLE;
      break;
    case "table_buffering" as any:
      functionType = CatalogFunctionType.TABLE_BUFFERING;
      break;
    case "aggregate" as any:
      functionType = CatalogFunctionType.AGGREGATE;
      break;
    default:
      functionType = CatalogFunctionType.TABLE;
  }

  const argumentMonotonicity = meta.argumentMonotonicity ?? null;
  if (argumentMonotonicity !== null && functionType !== CatalogFunctionType.SCALAR) {
    throw new Error("argumentMonotonicity is only valid for scalar functions");
  }

  const parameters: ParameterInfo[] = func.argumentSpecs.map(specToParameterInfo);

  if (argumentMonotonicity !== null) {
    if (argumentMonotonicity.length !== parameters.length) {
      throw new Error(
        `argumentMonotonicity has ${argumentMonotonicity.length} entries, ` +
        `expected ${parameters.length} declaration slots`,
      );
    }
    const allowed = new Set<string>(Object.values(ArgumentMonotonicity));
    for (const [index, value] of argumentMonotonicity.entries()) {
      if (!allowed.has(value)) {
        throw new Error(`argumentMonotonicity[${index}] has unknown value ${String(value)}`);
      }
    }
  }

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
    argumentMonotonicity,
    requiredSettings: meta.requiredSettings ?? [],
    requiredSecrets: meta.requiredSecrets ?? [],
    projectionPushdown: meta.projectionPushdown ?? false,
    filterPushdown: meta.filterPushdown ?? false,
    samplingPushdown: meta.samplingPushdown ?? false,
    filterSemanticProfiles: meta.filterSemanticProfiles ?? (meta.filterPushdown ? ["vgi.duckdb.standard.v1"] : []),
    additionalFilterFunctions: meta.additionalFilterFunctions ?? [],
    runtimeFilterAlgorithms: meta.runtimeFilterAlgorithms ?? [],
    filterEvaluationContexts: (meta.filterEvaluationContexts ?? []).map((value) => ({
      profile: value.profile,
      providerFingerprint: value.providerFingerprint ?? null,
    })),
    preservesOrder: meta.preservesOrder ?? OrderPreservation.NO_ORDER_GUARANTEE,
    maxWorkers: maxWorkers === DEFAULT_MAX_WORKERS ? null : maxWorkers,
    orderDependent: meta.orderDependent ?? OrderDependence.NOT_ORDER_DEPENDENT,
    distinctDependent: meta.distinctDependent ?? DistinctDependence.NOT_DISTINCT_DEPENDENT,
  };
}
