// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// ArgumentSpec: converts function argument definitions to Arrow schema with VGI metadata keys.
// Must produce byte-identical schemas to Python's argument_specs_to_schema().

import { type VgiSchema, schema as schema_, type VgiField, field as field_, type VgiDataType, nullType, deserializeBatch } from "../arrow/index.js";
import {
  VGI_ARG_KEY,
  VGI_ARG_NAMED,
  VGI_TYPE_KEY,
  VGI_TYPE_TABLE,
  VGI_TYPE_ANY,
  VGI_VARARGS_KEY,
  VGI_VARARGS_TRUE,
  VGI_CONST_KEY,
  VGI_CONST_TRUE,
  VGI_DOC_KEY,
} from "../types.js";

export interface ArgumentSpec {
  name: string;
  position: number | string;
  arrowType: VgiDataType;
  isTableInput?: boolean;
  isAnyType?: boolean;
  isVarargs?: boolean;
  isConst?: boolean;
  doc?: string;
}

function argumentSpecSortKey(spec: ArgumentSpec): [number, number | string] {
  if (typeof spec.position === "number") {
    return [0, spec.position];
  }
  return [1, spec.position];
}

/**
 * Convert ArgumentSpecs to an Arrow Schema with VGI metadata keys.
 * Positional arguments come first (in order), named arguments follow.
 */
export function argumentSpecsToSchema(specs: ArgumentSpec[]): VgiSchema {
  const sorted = [...specs].sort((a, b) => {
    const ka = argumentSpecSortKey(a);
    const kb = argumentSpecSortKey(b);
    if (ka[0] !== kb[0]) return ka[0] - kb[0];
    if (typeof ka[1] === "number" && typeof kb[1] === "number") {
      return ka[1] - kb[1];
    }
    return String(ka[1]).localeCompare(String(kb[1]));
  });

  const fields: VgiField[] = [];
  for (const spec of sorted) {
    const metadata: Map<string, string> = new Map();

    if (typeof spec.position === "string") {
      metadata.set(VGI_ARG_KEY, VGI_ARG_NAMED);
    }

    if (spec.isTableInput) {
      metadata.set(VGI_TYPE_KEY, VGI_TYPE_TABLE);
    } else if (spec.isAnyType) {
      metadata.set(VGI_TYPE_KEY, VGI_TYPE_ANY);
    }

    if (spec.isVarargs) {
      metadata.set(VGI_VARARGS_KEY, VGI_VARARGS_TRUE);
    }

    if (spec.isConst) {
      metadata.set(VGI_CONST_KEY, VGI_CONST_TRUE);
    }

    // Per-argument description (UTF-8; presence-only — omit when empty).
    if (spec.doc) {
      metadata.set(VGI_DOC_KEY, spec.doc);
    }

    const field = field_(
      spec.name,
      spec.arrowType,
      true,
      metadata.size > 0 ? metadata : undefined
    );
    fields.push(field);
  }

  return schema_(fields);
}

// =============================================================================
// Macro Argument Schemas
// =============================================================================

/**
 * Build a macro `arguments_schema` describing macro parameters.
 *
 * Mirrors the function `arguments_schema` mechanism: one Arrow field per macro
 * parameter, in `parameters` order, each nullable. The per-parameter
 * description is carried via the same `vgi_doc` field-metadata key functions use
 * (UTF-8, presence-only — the key is omitted entirely when there is no doc). A
 * parameter's field type is the type of its default value when one is known
 * (decoded from `parameterDefaultValues`, a one-row RecordBatch serialized as
 * IPC bytes), else `nullType()`.
 *
 * @param parameters Ordered list of macro parameter names.
 * @param parameterDefaultValues Optional one-row RecordBatch (IPC bytes) whose
 *   columns are parameter names with typed default values; used to infer each
 *   parameter's field type.
 * @param parameterDocs Optional mapping of parameter name to description. Empty
 *   or missing descriptions yield no `vgi_doc` metadata on the field.
 * @returns Arrow schema with one nullable field per parameter, in order.
 */
export function macroArgumentsSchema(
  parameters: string[],
  parameterDefaultValues?: Uint8Array | null,
  parameterDocs?: Record<string, string>,
): VgiSchema {
  const docs = parameterDocs ?? {};

  // Map parameter name -> Arrow type from the typed default values, if any.
  const defaultTypes: Map<string, VgiDataType> = new Map();
  if (parameterDefaultValues && parameterDefaultValues.length > 0) {
    const batch = deserializeBatch(parameterDefaultValues);
    for (const f of batch.schema.fields) {
      defaultTypes.set(f.name, f.type);
    }
  }

  const fields: VgiField[] = [];
  for (const name of parameters) {
    const metadata: Map<string, string> = new Map();
    const doc = docs[name];
    if (doc) {
      metadata.set(VGI_DOC_KEY, doc);
    }
    const fieldType = defaultTypes.get(name) ?? nullType();
    fields.push(field_(name, fieldType, true, metadata.size > 0 ? metadata : undefined));
  }

  return schema_(fields);
}

/**
 * Extract per-parameter descriptions from a macro `arguments_schema`.
 *
 * Inverse of {@link macroArgumentsSchema}'s `vgi_doc` handling: reads the
 * `vgi_doc` field metadata (UTF-8) for each field. Fields without the key
 * (undocumented) are omitted from the result.
 *
 * @param schema A macro `arguments_schema` (one field per parameter).
 * @returns Mapping of parameter name to description, for documented parameters only.
 */
export function macroParameterDocsFromSchema(schema: VgiSchema): Record<string, string> {
  const docs: Record<string, string> = {};
  for (const field of schema.fields) {
    const doc = field.metadata.get(VGI_DOC_KEY);
    if (doc) {
      docs[field.name] = doc;
    }
  }
  return docs;
}

/**
 * Convert an Arrow Schema back to ArgumentSpecs.
 */
export function schemaToArgumentSpecs(schema: VgiSchema): ArgumentSpec[] {
  const specs: ArgumentSpec[] = [];
  let positionIndex = 0;

  for (const field of schema.fields) {
    const metadata = field.metadata;

    const isNamed = metadata.get(VGI_ARG_KEY) === VGI_ARG_NAMED;
    const position: number | string = isNamed
      ? field.name
      : positionIndex++;

    const vgiType = metadata.get(VGI_TYPE_KEY);
    const isTableInput = vgiType === VGI_TYPE_TABLE;
    const isAnyType = vgiType === VGI_TYPE_ANY;
    const isVarargs = metadata.get(VGI_VARARGS_KEY) === VGI_VARARGS_TRUE;
    const isConst = metadata.get(VGI_CONST_KEY) === VGI_CONST_TRUE;
    const doc = metadata.get(VGI_DOC_KEY) ?? "";

    specs.push({
      name: field.name,
      position,
      arrowType: field.type,
      isTableInput,
      isAnyType,
      isVarargs,
      isConst,
      doc,
    });
  }

  return specs;
}
