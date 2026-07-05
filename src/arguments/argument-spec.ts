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
  VGI_DEFAULT_KEY,
  VGI_CHOICES_KEY,
  VGI_RANGE_KEY,
  VGI_PATTERN_KEY,
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
  /**
   * Discovery-facing constraint metadata, pre-encoded to their surfaced UTF-8
   * forms (presence-only — `undefined` means the constraint is absent and the
   * corresponding field-metadata key is omitted entirely). Mirrors Python's
   * `ArgumentSpec.default_json` / `choices_json` / `range_notation` / `pattern`
   * so `argumentSpecsToSchema`/`schemaToArgumentSpecs` round-trip symmetrically.
   */
  defaultJson?: string;
  choicesJson?: string;
  rangeNotation?: string;
  pattern?: string;
}

/**
 * Raw per-argument constraint declarations, as authored on a function's
 * argument descriptor. Encoded into the pre-computed {@link ArgumentSpec}
 * constraint fields via {@link constraintSpecFields}.
 */
export interface ArgumentConstraints {
  /** Closed set of allowed values (JSON-encoded into `vgi_choices`). */
  choices?: readonly unknown[];
  /** Inclusive lower bound (`>=`). */
  ge?: number;
  /** Inclusive upper bound (`<=`). */
  le?: number;
  /** Exclusive lower bound (`>`). */
  gt?: number;
  /** Exclusive upper bound (`<`). */
  lt?: number;
  /** Regex the value must match (surfaced as `vgi_pattern`, as-is). */
  pattern?: string;
  /** Default value (JSON-encoded into `vgi_default`); optional args only. */
  default?: unknown;
}

/** `JSON.stringify`, falling back to the value's string form when unserializable. */
function safeJson(value: unknown): string {
  try {
    const encoded = JSON.stringify(value);
    // JSON.stringify returns undefined for functions/symbols/undefined.
    if (encoded === undefined) {
      return JSON.stringify(String(value));
    }
    return encoded;
  } catch {
    return JSON.stringify(String(value));
  }
}

/**
 * Build interval notation from an argument's numeric bounds.
 *
 * Inclusive bounds (`ge`/`le`) render as square brackets, exclusive bounds
 * (`gt`/`lt`) as parentheses, and an open side as `-inf`/`+inf`. Returns
 * `undefined` when the argument has no numeric bound at all.
 *
 * Examples: `ge=0,le=10` -> `"[0, 10]"`; `gt=0` -> `"(0, +inf)"`;
 * `ge=1,lt=10` -> `"[1, 10)"`.
 */
export function formatRange(
  ge?: number,
  le?: number,
  gt?: number,
  lt?: number,
): string | undefined {
  if (ge === undefined && le === undefined && gt === undefined && lt === undefined) {
    return undefined;
  }
  let low: string;
  if (gt !== undefined) {
    low = `(${gt}`;
  } else if (ge !== undefined) {
    low = `[${ge}`;
  } else {
    low = "(-inf";
  }
  let high: string;
  if (lt !== undefined) {
    high = `${lt})`;
  } else if (le !== undefined) {
    high = `${le}]`;
  } else {
    high = "+inf)";
  }
  return `${low}, ${high}`;
}

/**
 * Encode raw {@link ArgumentConstraints} into the pre-computed constraint
 * fields of an {@link ArgumentSpec} (presence-only — absent constraints yield
 * no field). Mirrors Python's `_constraint_kwargs`.
 */
export function constraintSpecFields(
  constraints: ArgumentConstraints | undefined,
): Pick<ArgumentSpec, "defaultJson" | "choicesJson" | "rangeNotation" | "pattern"> {
  const fields: Pick<
    ArgumentSpec,
    "defaultJson" | "choicesJson" | "rangeNotation" | "pattern"
  > = {};
  if (constraints === undefined) return fields;

  if ("default" in constraints && constraints.default !== undefined) {
    fields.defaultJson = safeJson(constraints.default);
  }
  if (constraints.choices !== undefined) {
    fields.choicesJson = safeJson([...constraints.choices]);
  }
  const range = formatRange(
    constraints.ge,
    constraints.le,
    constraints.gt,
    constraints.lt,
  );
  if (range !== undefined) {
    fields.rangeNotation = range;
  }
  if (constraints.pattern !== undefined) {
    fields.pattern = constraints.pattern;
  }
  return fields;
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

    // Per-argument constraint metadata (presence-only; already value-encoded).
    if (spec.defaultJson !== undefined) {
      metadata.set(VGI_DEFAULT_KEY, spec.defaultJson);
    }
    if (spec.choicesJson !== undefined) {
      metadata.set(VGI_CHOICES_KEY, spec.choicesJson);
    }
    if (spec.rangeNotation !== undefined) {
      metadata.set(VGI_RANGE_KEY, spec.rangeNotation);
    }
    if (spec.pattern !== undefined) {
      metadata.set(VGI_PATTERN_KEY, spec.pattern);
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

    // Per-argument constraint metadata (absent key -> undefined).
    const defaultJson = metadata.get(VGI_DEFAULT_KEY);
    const choicesJson = metadata.get(VGI_CHOICES_KEY);
    const rangeNotation = metadata.get(VGI_RANGE_KEY);
    const pattern = metadata.get(VGI_PATTERN_KEY);

    specs.push({
      name: field.name,
      position,
      arrowType: field.type,
      isTableInput,
      isAnyType,
      isVarargs,
      isConst,
      doc,
      defaultJson,
      choicesJson,
      rangeNotation,
      pattern,
    });
  }

  return specs;
}
