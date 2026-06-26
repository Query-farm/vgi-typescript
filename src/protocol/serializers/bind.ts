// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// BindRequest / BindResponse wire serialization. Schemas mirror Python's
// ArrowSerializableDataclass field declaration order so the format is
// positional-compatible with the Python reader.

import { type VgiSchema, schema, type VgiField, field, type VgiBatch, type VgiDataType, utf8, binary, bool, list, struct } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { FunctionType } from "../../types.js";
import type { BindRequest, BindResponse, CopyFromContext } from "../types.js";
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
  // Time travel AT clause (additive, nullable, name-keyed -> wire-compatible).
  // Both null when the scan has no AT clause. See BindRequest.at_unit.
  field("at_unit", utf8(), true),
  field("at_value", utf8(), true),
]);

// COPY ... FROM context — a nullable nested struct<format, file_path,
// expected_schema>. Byte-for-byte the shape the C++ extension builds in
// vgi_rpc_types.cpp (BuildBindRequest's copy_from branch) and the Python
// BindRequest.copy_from field. Only appended to the serialized batch when the
// bind actually opens a COPY scan, so ordinary scans keep the legacy wire shape.
const COPY_FROM_STRUCT_TYPE = struct([
  field("format", utf8(), false),
  field("file_path", utf8(), false),
  field("expected_schema", binary(), false),
]);
const COPY_FROM_FIELD = field("copy_from", COPY_FROM_STRUCT_TYPE, true);

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
    at_unit: req.at_unit ?? null,
    at_value: req.at_value ?? null,
  };
  // Append the copy_from struct column only for COPY scans, so non-COPY binds
  // serialize to the exact legacy shape (field matched by name on the reader).
  if (req.copy_from) {
    const schemaWithCopyFrom = schema([...BIND_REQUEST_SCHEMA.fields, COPY_FROM_FIELD]);
    row.copy_from = {
      format: req.copy_from.format,
      file_path: req.copy_from.file_path,
      expected_schema: serializeSchema(req.copy_from.expected_schema),
    };
    return buildSingleRowBatch(schemaWithCopyFrom, row);
  }
  return buildSingleRowBatch(BIND_REQUEST_SCHEMA, row);
}

function parseCopyFromContext(raw: any): CopyFromContext | null {
  // The struct column decodes to a plain { format, file_path, expected_schema }
  // object via the codec/canonical path (expected_schema is binary -> Uint8Array).
  if (raw == null) return null;
  const format = raw.format;
  const filePath = raw.file_path;
  const schemaBytes = raw.expected_schema;
  if (format == null || filePath == null || schemaBytes == null) return null;
  return {
    format: String(format),
    file_path: String(filePath),
    expected_schema: deserializeSchema(toUint8Array(schemaBytes)),
  };
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
    // Empty string -> null (matches the C++ BuildBindRequest convention).
    at_unit: params.at_unit ? String(params.at_unit) : null,
    at_value: params.at_value ? String(params.at_value) : null,
    // COPY ... FROM context — absent for ordinary scans (params.copy_from
    // undefined -> null). The struct column decodes to a plain object.
    copy_from: parseCopyFromContext(params.copy_from),
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
