// ArgumentSpec: converts function argument definitions to Arrow schema with VGI metadata keys.
// Must produce byte-identical schemas to Python's argument_specs_to_schema().

import { Schema, Field, DataType, Null } from "@query-farm/apache-arrow";
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
} from "../types.js";

export interface ArgumentSpec {
  name: string;
  position: number | string;
  arrowType: DataType;
  isTableInput?: boolean;
  isAnyType?: boolean;
  isVarargs?: boolean;
  isConst?: boolean;
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
export function argumentSpecsToSchema(specs: ArgumentSpec[]): Schema {
  const sorted = [...specs].sort((a, b) => {
    const ka = argumentSpecSortKey(a);
    const kb = argumentSpecSortKey(b);
    if (ka[0] !== kb[0]) return ka[0] - kb[0];
    if (typeof ka[1] === "number" && typeof kb[1] === "number") {
      return ka[1] - kb[1];
    }
    return String(ka[1]).localeCompare(String(kb[1]));
  });

  const fields: Field[] = [];
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

    const field = new Field(
      spec.name,
      spec.arrowType,
      true,
      metadata.size > 0 ? metadata : undefined
    );
    fields.push(field);
  }

  return new Schema(fields);
}

/**
 * Convert an Arrow Schema back to ArgumentSpecs.
 */
export function schemaToArgumentSpecs(schema: Schema): ArgumentSpec[] {
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

    specs.push({
      name: field.name,
      position,
      arrowType: field.type,
      isTableInput,
      isAnyType,
      isVarargs,
      isConst,
    });
  }

  return specs;
}
