// Metadata -> Arrow batch serialization.
// Must produce batches matching Python's _METADATA_SCHEMA exactly.

import {
  Schema,
  Field,
  RecordBatch,
  Utf8,
  Int32,
  Bool,
  List,
  Struct,
  Map_,
  vectorFromArray,
  makeData,
  Data,
} from "@query-farm/apache-arrow";
import type { ResolvedMetadata, ParameterInfo, FunctionExample } from "./types.js";

// Helper to create a Map_ type properly
function mapType(keyType: any, valueType: any): Map_ {
  const entriesStruct = new Struct([
    new Field("key", keyType, false),
    new Field("value", valueType, true),
  ]);
  return new Map_(new Field("entries", entriesStruct, false));
}

// ============================================================================
// Schema definitions matching Python's _METADATA_SCHEMA exactly
// ============================================================================

const EXAMPLE_STRUCT = new Struct([
  new Field("sql", new Utf8(), false),
  new Field("description", new Utf8(), false),
  new Field("expected_output", new Utf8(), true),
]);

const PARAMETER_STRUCT = new Struct([
  new Field("name", new Utf8(), false),
  new Field("position", new Int32(), true),
  new Field("position_name", new Utf8(), true),
  new Field("type_name", new Utf8(), true),
  new Field("description", new Utf8(), false),
  new Field("required", new Bool(), false),
  new Field("default", new Utf8(), true),
  new Field("constraints", new Utf8(), true),
  new Field("is_table_input", new Bool(), false),
  new Field("is_varargs", new Bool(), false),
  new Field("is_const", new Bool(), false),
]);

const METADATA_SCHEMA = new Schema([
  new Field("name", new Utf8(), false),
  new Field("class_name", new Utf8(), false),
  new Field("function_type", new Utf8(), false),
  new Field("description", new Utf8(), false),
  new Field("examples", new List(new Field("item", EXAMPLE_STRUCT, true)), false),
  new Field("categories", new List(new Field("item", new Utf8(), true)), false),
  new Field("tags", mapType(new Utf8(), new Utf8()), false),
  new Field("parameters", new List(new Field("item", PARAMETER_STRUCT, true)), false),
  new Field("stability", new Utf8(), false),
  new Field("null_handling", new Utf8(), false),
  new Field("required_settings", new List(new Field("item", new Utf8(), true)), false),
  new Field("required_secrets", new List(new Field("item", new Utf8(), true)), false),
  new Field("projection_pushdown", new Bool(), false),
  new Field("filter_pushdown", new Bool(), false),
  new Field("preserves_order", new Utf8(), false),
  new Field("max_workers", new Int32(), true),
  new Field("order_dependent", new Utf8(), false),
  new Field("distinct_dependent", new Utf8(), false),
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
    preserves_order: m.preservesOrder,
    max_workers: m.maxWorkers,
    order_dependent: m.orderDependent,
    distinct_dependent: m.distinctDependent,
  };
}

/**
 * Serialize multiple ResolvedMetadata to a single Arrow RecordBatch.
 */
export function metadatasToArrow(metadatas: ResolvedMetadata[]): RecordBatch {
  if (metadatas.length === 0) {
    // Return empty batch with correct schema
    const children = METADATA_SCHEMA.fields.map((f: Field) => {
      return makeData({ type: f.type, length: 0, nullCount: 0 });
    });
    const structType = new Struct(METADATA_SCHEMA.fields);
    const data = makeData({
      type: structType,
      length: 0,
      children,
      nullCount: 0,
    });
    return new RecordBatch(METADATA_SCHEMA, data);
  }

  const rows = metadatas.map(metadataToRow);

  // Build column arrays
  const columns: Record<string, any[]> = {};
  for (const field of METADATA_SCHEMA.fields) {
    columns[field.name] = rows.map((r) => r[field.name]);
  }

  // Use vectorFromArray for each column
  const children = METADATA_SCHEMA.fields.map((f: Field) => {
    const vals = columns[f.name];
    const arr = vectorFromArray(vals, f.type);
    return arr.data[0];
  });

  const structType = new Struct(METADATA_SCHEMA.fields);
  const data = makeData({
    type: structType,
    length: metadatas.length,
    children,
    nullCount: 0,
  });

  return new RecordBatch(METADATA_SCHEMA, data);
}

/**
 * Deserialize Arrow RecordBatch to ResolvedMetadata array.
 */
export function arrowToMetadatas(batch: RecordBatch): ResolvedMetadata[] {
  const results: ResolvedMetadata[] = [];

  for (let i = 0; i < batch.numRows; i++) {
    const get = (name: string) => {
      const col = batch.getChild(name);
      return col ? col.get(i) : null;
    };

    // Parse examples
    const rawExamples = get("examples") ?? [];
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
    const rawParams = get("parameters") ?? [];
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
    const rawCategories = get("categories") ?? [];
    const categories: string[] = rawCategories
      ? [...rawCategories].filter((c: any) => c != null)
      : [];

    // Parse tags
    const rawTags = get("tags");
    const tags: Record<string, string> = {};
    if (rawTags) {
      // Map type in Arrow
      if (typeof rawTags[Symbol.iterator] === "function") {
        for (const entry of rawTags) {
          if (entry && entry.key != null) {
            tags[String(entry.key)] = String(entry.value ?? "");
          }
        }
      }
    }

    const rawRequiredSettings = get("required_settings") ?? [];
    const requiredSettings: string[] = rawRequiredSettings
      ? [...rawRequiredSettings].filter((s: any) => s != null)
      : [];

    const rawRequiredSecrets = get("required_secrets") ?? [];
    const requiredSecrets: string[] = rawRequiredSecrets
      ? [...rawRequiredSecrets].filter((s: any) => s != null)
      : [];

    results.push({
      name: get("name") ?? "",
      className: get("class_name") ?? "",
      functionType: get("function_type") ?? "TABLE",
      description: get("description") ?? "",
      examples,
      categories,
      tags,
      parameters,
      stability: get("stability") ?? "CONSISTENT",
      nullHandling: get("null_handling") ?? "DEFAULT",
      requiredSettings,
      requiredSecrets,
      projectionPushdown: get("projection_pushdown") ?? false,
      filterPushdown: get("filter_pushdown") ?? false,
      preservesOrder: get("preserves_order") ?? "NO_ORDER_GUARANTEE",
      maxWorkers: get("max_workers") ?? null,
      orderDependent: get("order_dependent") ?? "NOT_ORDER_DEPENDENT",
      distinctDependent: get("distinct_dependent") ?? "NOT_DISTINCT_DEPENDENT",
    });
  }

  return results;
}
