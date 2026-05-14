// BindRequest / BindResponse wire serialization. Schemas mirror Python's
// ArrowSerializableDataclass field declaration order so the format is
// positional-compatible with the Python reader.

import { type VgiSchema, schema, type VgiField, field, type VgiBatch, type VgiDataType, utf8, binary, bool, list } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { FunctionType } from "../../types.js";
import type { BindRequest, BindResponse } from "../types.js";
import {
  serializeSchema,
  deserializeSchema,
  serializeBatch,
  deserializeBatch,
} from "../../util/arrow/index.js";
import { toUint8Array, buildSingleRowBatch } from "./shared.js";
import { serializeArguments, deserializeArguments } from "./arguments.js";

const BIND_REQUEST_SCHEMA = schema([
  field("function_name", utf8(), false),
  field("arguments", binary(), false),
  field("function_type", utf8(), false),
  field("input_schema", binary(), true),
  field("settings", binary(), true),
  field("secrets", binary(), true),
  field("attach_opaque_data", binary(), true),
  field("transaction_opaque_data", binary(), true),
  field("resolved_secrets_provided", bool(), false),
]);

const BIND_RESPONSE_SCHEMA = schema([
  field("output_schema", binary(), false),
  field("opaque_data", binary(), true),
  field("lookup_secret_types", list(field("item", utf8(), true)), false),
  field("lookup_scopes", list(field("item", utf8(), true)), false),
  field("lookup_names", list(field("item", utf8(), true)), false),
]);

export function serializeBindRequest(req: BindRequest): VgiBatch {
  const row: Record<string, any> = {
    function_name: req.function_name,
    arguments: serializeArguments(req.arguments),
    function_type: req.function_type,
    input_schema: req.input_schema ? serializeSchema(req.input_schema) : null,
    settings: req.settings ? serializeBatch(req.settings) : null,
    secrets: req.secrets ? serializeBatch(req.secrets) : null,
    attach_opaque_data: req.attach_opaque_data ?? null,
    transaction_opaque_data: req.transaction_opaque_data ?? null,
    resolved_secrets_provided: req.resolved_secrets_provided ?? false,
  };
  return buildSingleRowBatch(BIND_REQUEST_SCHEMA, row);
}

export function deserializeBindRequest(
  params: Record<string, any>
): BindRequest {
  // params come from the RPC layer - already extracted from a single-row batch
  // arguments may be null/undefined when DuckDB sends empty args
  const args = params.arguments
    ? deserializeArguments(toUint8Array(params.arguments))
    : new Arguments();

  // function_type might be a string directly or enum name
  let functionType: FunctionType;
  const ftStr = String(params.function_type);
  if (ftStr === "scalar" || ftStr === "SCALAR") {
    functionType = FunctionType.SCALAR;
  } else if (ftStr === "table" || ftStr === "TABLE") {
    functionType = FunctionType.TABLE;
  } else if (ftStr === "aggregate" || ftStr === "AGGREGATE") {
    functionType = FunctionType.AGGREGATE;
  } else {
    functionType = ftStr as FunctionType;
  }

  return {
    function_name: params.function_name,
    arguments: args,
    function_type: functionType,
    input_schema: params.input_schema
      ? deserializeSchema(toUint8Array(params.input_schema))
      : null,
    settings: params.settings
      ? deserializeBatch(toUint8Array(params.settings))
      : null,
    secrets: params.secrets
      ? deserializeBatch(toUint8Array(params.secrets))
      : null,
    attach_opaque_data: params.attach_opaque_data ? toUint8Array(params.attach_opaque_data) : null,
    transaction_opaque_data: params.transaction_opaque_data
      ? toUint8Array(params.transaction_opaque_data)
      : null,
    resolved_secrets_provided: params.resolved_secrets_provided ?? false,
  };
}

export function serializeBindResponse(
  resp: BindResponse
): Record<string, any> {
  return {
    output_schema: serializeSchema(resp.output_schema),
    opaque_data: resp.opaque_data ?? null,
    lookup_secret_types: resp.lookup_secret_types ?? [],
    lookup_scopes: resp.lookup_scopes ?? [],
    lookup_names: resp.lookup_names ?? [],
  };
}

export function deserializeBindResponse(
  params: Record<string, any>
): BindResponse {
  const toStrArray = (val: any): string[] => {
    if (!val) return [];
    const arr = Array.isArray(val) ? val : [...val];
    return arr.filter((v: any) => v != null).map(String);
  };
  return {
    output_schema: deserializeSchema(toUint8Array(params.output_schema)),
    opaque_data: params.opaque_data ? toUint8Array(params.opaque_data) : null,
    lookup_secret_types: toStrArray(params.lookup_secret_types),
    lookup_scopes: toStrArray(params.lookup_scopes),
    lookup_names: toStrArray(params.lookup_names),
  };
}

export { BIND_REQUEST_SCHEMA, BIND_RESPONSE_SCHEMA };
