// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// BindRequest / BindResponse wire serialization. Schemas mirror Python's
// ArrowSerializableDataclass field declaration order so the format is
// positional-compatible with the Python reader.

import type { VgiSchema, VgiBatch } from "../../arrow/index.js";
import { FunctionType } from "../../types.js";
import type { ArgumentsLocation, BindRequest, BindResponse, BindRepeatability, CopyFromContext, CopyToContext } from "../types.js";
import { BindRequestSchema } from "../../generated/vgi-protocol-schemas.js";
import { nestedBindRequest, type BindRequest as WireBindRequest, type Nested } from "../../generated/vgi-protocol-types.js";
import { toUint8Array as toUint8ArrayBase } from "../../util/bytes.js";
import { invalidArgumentsRef, invalidBindRequest, unresolvedArguments } from "../arguments-ref.js";
import {
  serializeSchema,
  deserializeSchema,
  serializeBatch,
  deserializeBatch,
} from "../../util/arrow/index.js";
import { toUint8Array, buildSingleRowBatch } from "./shared.js";
import { serializeArguments, deserializeArguments } from "./arguments.js";

// The BindRequest wire record is the GENERATED `BindRequestSchema`
// (src/generated/vgi-protocol-schemas.ts), not a hand-written copy. A copy
// has to equal it exactly -- vgi-rpc-go validates a request against its
// declared parameter contract with `arrow.Schema.Equal`, which is order-,
// name-, type- AND nullability-sensitive -- and the hand-written one this file
// used to carry went stale the moment protocol 3.4 made `arguments` nullable
// and appended `arguments_location`.
const BIND_REQUEST_SCHEMA = BindRequestSchema as unknown as VgiSchema;

/**
 * The bytes each decoded bind call was received as, held in the generated
 * raw-preserving wrapper (vgi docs/large-values-3.4.md §3.3). Keyed by the
 * domain object, so a copy (`{ ...bindCall, x }`) has none and serializes from
 * its fields, while the decoded object itself -- even after the framework
 * opened its sealed attach / transaction in place -- re-encodes as exactly the
 * bytes the client sent (§3.3 rule 11: opened values live beside the wrapper,
 * never in it).
 */
const receivedBindCalls = new WeakMap<object, Nested<WireBindRequest>>();

/** The wrapper holding the bytes `bindCall` was decoded from, or `undefined` for one built from fields. */
export function receivedBindCall(bindCall: BindRequest): Nested<WireBindRequest> | undefined {
  return receivedBindCalls.get(bindCall);
}

/** The bind call's wire bytes: the bytes it was received as, else a fresh serialization of its fields. */
export function bindCallBytes(bindCall: BindRequest): Uint8Array {
  return receivedBindCalls.get(bindCall)?.bytes ?? serializeBatch(serializeBindRequest(bindCall));
}

export function serializeBindRequest(req: BindRequest): VgiBatch {
  // A bind call that arrived by reference re-serializes as that reference,
  // never as the resolved value (large-values spec §6: anything that
  // re-serializes an init request emits `arguments_location`).
  const location = req.arguments_location ?? null;
  return buildSingleRowBatch(BIND_REQUEST_SCHEMA, {
    function_name: req.function_name,
    arguments: location ? null : serializeArguments(req.arguments),
    // The wire carries the Python enum member NAME ("TABLE"), not the TS
    // enum's value ("table"); each FunctionType value upper-cases to its name.
    function_type: req.function_type == null ? req.function_type : String(req.function_type).toUpperCase(),
    input_schema: req.input_schema ? serializeSchema(req.input_schema) : null,
    settings: req.settings ? serializeBatch(req.settings) : null,
    secrets: req.secrets ? serializeBatch(req.secrets) : null,
    attach_opaque_data: req.attach_opaque_data ?? null,
    transaction_opaque_data: req.transaction_opaque_data ?? null,
    resolved_secrets_provided: req.resolved_secrets_provided ?? false,
    at_unit: req.at_unit ?? null,
    at_value: req.at_value ?? null,
    copy_from: req.copy_from
      ? {
          format: req.copy_from.format,
          file_path: req.copy_from.file_path,
          expected_schema: serializeSchema(req.copy_from.expected_schema),
        }
      : null,
    copy_to: req.copy_to
      ? { format: req.copy_to.format, file_path: req.copy_to.file_path }
      : null,
    schema_path: req.schema_path ?? null,
    argument_names: req.argument_names ?? null,
    arguments_location: location
      ? { ref: location.ref, size: BigInt(location.size), sha256: location.sha256 }
      : null,
  });
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

function parseCopyToContext(raw: any): CopyToContext | null {
  // The struct column decodes to a plain { format, file_path } object via the
  // codec/canonical path.
  if (raw == null) return null;
  const format = raw.format;
  const filePath = raw.file_path;
  if (format == null || filePath == null) return null;
  return { format: String(format), file_path: String(filePath) };
}

const SHA256_HEX = /^[0-9a-f]{64}$/;

/** Validate and normalise a received `arguments_location` (large-values spec §2). */
function parseArgumentsLocation(raw: any): ArgumentsLocation {
  const size = typeof raw.size === "bigint" ? Number(raw.size) : Number(raw.size);
  if (!Number.isSafeInteger(size) || size < 1) {
    throw invalidArgumentsRef("arguments_location.size must be a positive byte count");
  }
  const sha256 = raw.sha256 == null ? "" : String(raw.sha256);
  if (!SHA256_HEX.test(sha256)) {
    throw invalidArgumentsRef("arguments_location.sha256 must be 64 lowercase hex digits");
  }
  if (raw.ref == null) throw invalidArgumentsRef("arguments_location.ref is missing");
  return { ref: toUint8ArrayBase(raw.ref), size, sha256 };
}

/**
 * Decode a bind request from a dict of its wire columns (the generated
 * decoder's record, or `batchToScalarDict` of the one-row batch).
 *
 * Exactly one of `arguments` and `arguments_location` must be set (protocol
 * 3.4); anything else is refused with `invalid_bind_request`. When the
 * arguments came by reference the returned bind call's `arguments` is a
 * placeholder that throws until the framework resolves it at the turn's entry
 * (`resolveBindCall`, protocol/arguments-ref.ts); `arguments_location` keeps
 * the location it arrived with.
 */
export function deserializeBindRequest(
  params: Record<string, any>
): BindRequest {
  const hasArguments = params.arguments != null;
  const hasLocation = params.arguments_location != null;
  if (hasArguments === hasLocation) throw invalidBindRequest();
  const location = hasLocation ? parseArgumentsLocation(params.arguments_location) : null;
  const args = location ? unresolvedArguments() : deserializeArguments(toUint8ArrayBase(params.arguments));

  // function_type might be a string directly or enum name
  let functionType: FunctionType;
  const ftStr = String(params.function_type);
  if (ftStr === "scalar" || ftStr === "SCALAR") {
    functionType = FunctionType.SCALAR;
  } else if (ftStr === "table" || ftStr === "TABLE") {
    functionType = FunctionType.TABLE;
  } else if (ftStr === "table_buffering" || ftStr === "TABLE_BUFFERING") {
    functionType = FunctionType.TABLE_BUFFERING;
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
    // Empty string -> null, same convention as at_unit/at_value.
    schema_path: params.schema_path
      ? (Array.isArray(params.schema_path) ? params.schema_path : [...params.schema_path]).map(String)
      : null,
    argument_names: params.argument_names
      ? (Array.isArray(params.argument_names) ? params.argument_names : [...params.argument_names])
        .map((value: unknown) => value == null ? null : String(value))
      : null,
    // COPY ... FROM context — absent for ordinary scans (params.copy_from
    // undefined -> null). The struct column decodes to a plain object.
    copy_from: parseCopyFromContext(params.copy_from),
    // COPY ... TO context — absent for ordinary scans / COPY FROM.
    copy_to: parseCopyToContext(params.copy_to),
    arguments_location: location,
  };
}

/**
 * Decode a received bind call from its bytes through the generated
 * raw-preserving wrapper: the domain object re-encodes as exactly these bytes
 * ({@link bindCallBytes}) for as long as it lives, whatever the framework opens
 * on it in place.
 *
 * @param source The bind call's bytes, or a wrapper already holding them.
 * @param path The field path decode errors name (`InitRequest.bind_call`).
 */
export function decodeBindCall(source: Uint8Array | Nested<WireBindRequest>, path = "BindRequest"): BindRequest {
  const nested = source instanceof Uint8Array ? nestedBindRequest(source, path) : source;
  const bindCall = deserializeBindRequest(nested.get() as Record<string, any>);
  receivedBindCalls.set(bindCall, nested);
  return bindCall;
}

/** Validate required wire enum values, including rejecting NULL and absent values. */
export function parseBindRepeatability(value: unknown): BindRepeatability {
  if (value !== "UNKNOWN" && value !== "STABLE_WITHIN_QUERY")
    throw new Error("invalid bind repeatability");
  return value;
}

/** Only omitted provider declarations default; explicit NULL remains invalid. */
export function normalizeBindRepeatability(value: unknown): BindRepeatability {
  return parseBindRepeatability(value === undefined ? "UNKNOWN" : value);
}

export function serializeBindResponse(
  resp: BindResponse
): Record<string, any> {
  return {
    output_schema: serializeSchema(resp.output_schema),
    opaque_data: resp.opaque_data ?? null,
    projection_expression_profiles: resp.projection_expression_profiles ?? [],
    lookup_secret_types: resp.lookup_secret_types ?? [],
    lookup_scopes: resp.lookup_scopes ?? [],
    lookup_names: resp.lookup_names ?? [],
    repeatability: normalizeBindRepeatability(resp.repeatability),
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
    projection_expression_profiles: toStrArray(params.projection_expression_profiles),
    opaque_data: params.opaque_data ? toUint8Array(params.opaque_data) : null,
    lookup_secret_types: toStrArray(params.lookup_secret_types),
    lookup_scopes: toStrArray(params.lookup_scopes),
    lookup_names: toStrArray(params.lookup_names),
    repeatability: parseBindRepeatability(params.repeatability),
  };
}

// The BindResponse copy that used to sit beside the request schema had
// already drifted from the generated BindResultSchema (it called
// `opaque_data` nullable where the protocol says non-null). Use
// BindResultSchema from src/generated/vgi-protocol-schemas.ts.
export { BIND_REQUEST_SCHEMA };
