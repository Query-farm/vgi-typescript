// TableFunctionCardinalityRequest / TableCardinality wire serialization.

import { type VgiSchema, schema, type VgiField, field, type VgiDataType, binary, int64 } from "../../arrow/index.js";
import type { TableFunctionCardinalityRequest, TableCardinality } from "../types.js";
import { deserializeBatch } from "../../util/arrow/index.js";
import { toUint8Array } from "./shared.js";
import { deserializeBindRequest } from "./bind.js";

const TABLE_CARDINALITY_SCHEMA = schema([
  field("estimate", int64(), true),
  field("max", int64(), true),
]);

const TABLE_FUNCTION_CARDINALITY_REQUEST_SCHEMA = schema([
  field("bind_call", binary(), false),
  field("bind_opaque_data", binary(), true),
]);

export function deserializeCardinalityRequest(
  params: Record<string, any>
): TableFunctionCardinalityRequest {
  const bindCallBytes = toUint8Array(params.bind_call);
  const bindCallBatch = deserializeBatch(bindCallBytes);
  const bindParams: Record<string, any> = {};
  for (const field of bindCallBatch.schema.fields) {
    const col = bindCallBatch.getChild(field.name);
    bindParams[field.name] = col ? col.get(0) : null;
  }
  const bindCall = deserializeBindRequest(bindParams);

  return {
    bind_call: bindCall,
    bind_opaque_data: params.bind_opaque_data
      ? toUint8Array(params.bind_opaque_data)
      : null,
  };
}

export function serializeTableCardinality(
  card: TableCardinality
): Record<string, any> {
  return {
    estimate: card.estimate,
    max: card.max,
  };
}

export { TABLE_CARDINALITY_SCHEMA, TABLE_FUNCTION_CARDINALITY_REQUEST_SCHEMA };
