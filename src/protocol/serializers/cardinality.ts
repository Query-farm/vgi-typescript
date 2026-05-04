// TableFunctionCardinalityRequest / TableCardinality wire serialization.

import { Schema, Field, Binary, Int64 } from "@query-farm/apache-arrow";
import type { TableFunctionCardinalityRequest, TableCardinality } from "../types.js";
import { deserializeBatch } from "../../util/arrow/index.js";
import { toUint8Array } from "./shared.js";
import { deserializeBindRequest } from "./bind.js";

const TABLE_CARDINALITY_SCHEMA = new Schema([
  new Field("estimate", new Int64(), true),
  new Field("max", new Int64(), true),
]);

const TABLE_FUNCTION_CARDINALITY_REQUEST_SCHEMA = new Schema([
  new Field("bind_call", new Binary(), false),
  new Field("bind_opaque_data", new Binary(), true),
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
