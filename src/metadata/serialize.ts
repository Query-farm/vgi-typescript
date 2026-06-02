// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Metadata -> Arrow batch serialization.
// Must produce batches matching Python's _METADATA_SCHEMA exactly.

import {
  type VgiBatch,
  schema as makeSchema,
  field,
  utf8,
  int32,
  bool,
  list,
  struct as makeStruct,
  map as makeMap,
  batchFromColumns,
  emptyBatch,
} from "../arrow/index.js";
import type { ResolvedMetadata, ParameterInfo, FunctionExample } from "./types.js";

// ============================================================================
// Schema definitions matching Python's _METADATA_SCHEMA exactly
// ============================================================================

const EXAMPLE_STRUCT = makeStruct([
  field("sql", utf8(), false),
  field("description", utf8(), false),
  field("expected_output", utf8(), true),
]);

const PARAMETER_STRUCT = makeStruct([
  field("name", utf8(), false),
  field("position", int32(), true),
  field("position_name", utf8(), true),
  field("type_name", utf8(), true),
  field("description", utf8(), false),
  field("required", bool(), false),
  field("default", utf8(), true),
  field("constraints", utf8(), true),
  field("is_table_input", bool(), false),
  field("is_varargs", bool(), false),
  field("is_const", bool(), false),
]);

const METADATA_SCHEMA = makeSchema([
  field("name", utf8(), false),
  field("class_name", utf8(), false),
  field("function_type", utf8(), false),
  field("description", utf8(), false),
  field("examples", list(field("item", EXAMPLE_STRUCT, true)), false),
  field("categories", list(field("item", utf8(), true)), false),
  field("tags", makeMap(field("key", utf8(), false), field("value", utf8(), true), false), false),
  field("parameters", list(field("item", PARAMETER_STRUCT, true)), false),
  field("stability", utf8(), false),
  field("null_handling", utf8(), false),
  field("required_settings", list(field("item", utf8(), true)), false),
  field("required_secrets", list(field("item", utf8(), true)), false),
  field("projection_pushdown", bool(), false),
  field("filter_pushdown", bool(), false),
  field("preserves_order", utf8(), false),
  field("max_workers", int32(), true),
  field("order_dependent", utf8(), false),
  field("distinct_dependent", utf8(), false),
]);

export { METADATA_SCHEMA };

// ============================================================================
// Serialization
// ============================================================================

function metadataToRow(m: ResolvedMetadata): Record<string, any> {
  return {
    name: m.name,
    class_name: m.className,
    function_type: m.functionType,
    description: m.description,
    examples: m.examples.map((e) => ({
      sql: e.sql,
      description: e.description,
      expected_output: e.expectedOutput,
    })),
    categories: m.categories,
    tags: Object.entries(m.tags).map(([k, v]) => [k, v]),
    parameters: m.parameters.map((p) => ({
      name: p.name,
      position: p.position,
      position_name: p.positionName,
      type_name: p.typeName,
      description: p.description,
      required: p.required,
      default: p.default,
      constraints: p.constraints,
      is_table_input: p.isTableInput,
      is_varargs: p.isVarargs,
      is_const: p.isConst,
    })),
    stability: m.stability,
    null_handling: m.nullHandling,
    required_settings: m.requiredSettings,
    required_secrets: m.requiredSecrets,
    projection_pushdown: m.projectionPushdown,
    filter_pushdown: m.filterPushdown,
    sampling_pushdown: m.samplingPushdown,
    supported_expression_filters: m.supportedExpressionFilters,
    preserves_order: m.preservesOrder,
    max_workers: m.maxWorkers,
    order_dependent: m.orderDependent,
    distinct_dependent: m.distinctDependent,
  };
}

/**
 * Serialize multiple ResolvedMetadata to a single Arrow RecordBatch.
 */
export function metadatasToArrow(metadatas: ResolvedMetadata[]): VgiBatch {
  if (metadatas.length === 0) {
    return emptyBatch(METADATA_SCHEMA);
  }

  const rows = metadatas.map(metadataToRow);

  // Pivot rows -> columns keyed by field name; the facade handles complex
  // type construction (List<Struct>, Map, etc.) uniformly.
  const columns: Record<string, any[]> = {};
  for (const f of METADATA_SCHEMA.fields) {
    columns[f.name] = rows.map((r) => r[f.name]);
  }

  return batchFromColumns(columns, METADATA_SCHEMA);
}

/**
 * Deserialize Arrow RecordBatch to ResolvedMetadata array.
 */
export function arrowToMetadatas(batch: VgiBatch): ResolvedMetadata[] {
  const results: ResolvedMetadata[] = [];

  for (let i = 0; i < batch.numRows; i++) {
    const get = (name: string) => {
      const col = batch.getChild(name);
      return col ? col.get(i) : null;
    };

    // Parse examples
    const rawExamples = get("examples") as any ?? [];
    const examples: FunctionExample[] = [];
    if (rawExamples && typeof rawExamples[Symbol.iterator] === "function") {
      for (const e of rawExamples) {
        if (e) {
          examples.push({
            sql: e.sql ?? "",
            description: e.description ?? "",
            expectedOutput: e.expected_output ?? null,
          });
        }
      }
    }

    // Parse parameters
    const rawParams = get("parameters") as any ?? [];
    const parameters: ParameterInfo[] = [];
    if (rawParams && typeof rawParams[Symbol.iterator] === "function") {
      for (const p of rawParams) {
        if (p) {
          parameters.push({
            name: p.name ?? "",
            position: p.position ?? null,
            positionName: p.position_name ?? null,
            typeName: p.type_name ?? null,
            description: p.description ?? "",
            required: p.required ?? true,
            default: p.default ?? null,
            constraints: p.constraints ?? null,
            isTableInput: p.is_table_input ?? false,
            isVarargs: p.is_varargs ?? false,
            isConst: p.is_const ?? false,
          });
        }
      }
    }

    // Parse categories
    const rawCategories = get("categories") as any ?? [];
    const categories: string[] = rawCategories
      ? [...rawCategories].filter((c: any) => c != null)
      : [];

    // Parse tags (Map). arrow-js MapRow is iterable of {key,value}; flechette
    // returns [[k,v],...] arrays. Handle both.
    const rawTags = get("tags") as any;
    const tags: Record<string, string> = {};
    if (rawTags) {
      if (typeof rawTags[Symbol.iterator] === "function") {
        for (const entry of rawTags) {
          if (Array.isArray(entry)) {
            tags[String(entry[0])] = String(entry[1] ?? "");
          } else if (entry && entry.key != null) {
            tags[String(entry.key)] = String(entry.value ?? "");
          }
        }
      }
    }

    const rawRequiredSettings = get("required_settings") as any ?? [];
    const requiredSettings: string[] = rawRequiredSettings
      ? [...rawRequiredSettings].filter((s: any) => s != null)
      : [];

    const rawRequiredSecrets = get("required_secrets") as any ?? [];
    const requiredSecrets: string[] = rawRequiredSecrets
      ? [...rawRequiredSecrets].filter((s: any) => s != null)
      : [];

    results.push({
      name: (get("name") as string) ?? "",
      className: (get("class_name") as string) ?? "",
      functionType: (get("function_type") as any) ?? "TABLE",
      description: (get("description") as string) ?? "",
      examples,
      categories,
      tags,
      parameters,
      stability: (get("stability") as any) ?? "CONSISTENT",
      nullHandling: (get("null_handling") as any) ?? "DEFAULT",
      requiredSettings,
      requiredSecrets,
      projectionPushdown: (get("projection_pushdown") as boolean) ?? false,
      filterPushdown: (get("filter_pushdown") as boolean) ?? false,
      samplingPushdown: (get("sampling_pushdown") as boolean) ?? false,
      supportedExpressionFilters: (() => {
        const raw = get("supported_expression_filters") as any ?? [];
        return raw ? [...raw].filter((s: any) => s != null).map(String) : [];
      })(),
      preservesOrder: (get("preserves_order") as any) ?? "NO_ORDER_GUARANTEE",
      maxWorkers: (get("max_workers") as number | null) ?? null,
      orderDependent: (get("order_dependent") as any) ?? "NOT_ORDER_DEPENDENT",
      distinctDependent: (get("distinct_dependent") as any) ?? "NOT_DISTINCT_DEPENDENT",
    });
  }

  return results;
}
