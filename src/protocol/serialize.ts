// Arrow serialization/deserialization for VGI protocol types.
// Must produce Arrow batches with identical field names, types, and metadata
// as Python's ArrowSerializableDataclass output.

import {
  Schema,
  Field,
  RecordBatch,
  DataType,
  Utf8,
  Binary,
  Int64,
  Int32,
  Float64,
  Bool,
  List,
  Null,
  vectorFromArray,
  Struct,
  makeData,
  RecordBatchStreamWriter,
  RecordBatchReader,
} from "@query-farm/apache-arrow";
import { Arguments } from "../arguments/arguments.js";
import { FunctionType, TableInOutPhase } from "../types.js";
import type {
  BindRequest,
  BindResponse,
  InitRequest,
  GlobalInitResponse,
  TableFunctionCardinalityRequest,
  TableCardinality,
} from "./types.js";
import { OrderByDirection, OrderByNullOrder } from "./types.js";
import {
  serializeSchema,
  deserializeSchema,
  serializeBatch,
  deserializeBatch,
  emptyBatch,
  batchFromColumns,
} from "../util/arrow.js";
import { toUint8Array as toUint8ArrayBase } from "../util/bytes.js";

// ============================================================================
// Schema definitions matching Python's ArrowSerializableDataclass ARROW_SCHEMA
// ============================================================================

// BindRequest schema:
//   function_name: string
//   arguments: binary (Arguments serialized as Arrow IPC)
//   function_type: dictionary(int16, string) -> we use utf8 since TS arrow dict is complex
//   input_schema: binary (nullable)
//   settings: binary (nullable)
//   secrets: binary (nullable)
//   attach_id: binary (nullable)
//   transaction_id: binary (nullable)
const BIND_REQUEST_SCHEMA = new Schema([
  new Field("function_name", new Utf8(), false),
  new Field("arguments", new Binary(), false),
  new Field("function_type", new Utf8(), false),
  new Field("input_schema", new Binary(), true),
  new Field("settings", new Binary(), true),
  new Field("secrets", new Binary(), true),
  new Field("attach_id", new Binary(), true),
  new Field("transaction_id", new Binary(), true),
  new Field("resolved_secrets_provided", new Bool(), false),
]);

// BindResponse schema:
//   output_schema: binary
//   opaque_data: binary (nullable)
//   lookup_secret_types: list<utf8> (nullable)
//   lookup_scopes: list<utf8> (nullable)
//   lookup_names: list<utf8> (nullable)
const BIND_RESPONSE_SCHEMA = new Schema([
  new Field("output_schema", new Binary(), false),
  new Field("opaque_data", new Binary(), true),
  new Field("lookup_secret_types", new List(new Field("item", new Utf8(), true)), false),
  new Field("lookup_scopes", new List(new Field("item", new Utf8(), true)), false),
  new Field("lookup_names", new List(new Field("item", new Utf8(), true)), false),
]);

// InitRequest schema:
//   bind_call: binary (nested BindRequest)
//   output_schema: binary
//   bind_opaque_data: binary (nullable)
//   projection_ids: list<int32> (nullable)
//   pushdown_filters: binary (nullable)
//   phase: utf8 (nullable) - dictionary(int16,string) in Python, simplified to utf8
//   execution_id: binary (nullable)
//   init_opaque_data: binary (nullable)
// vgi-python's InitRequest (vgi/protocol.py) — outer wire shape:
//   bind_call: binary (nested IPC of BindRequest)
//   output_schema: binary (nullable serialized Schema)
//   bind_opaque_data: binary (nullable)
//   projection_ids: list<int64> (nullable)
//   pushdown_filters: large_binary (nullable) — nested IPC of filter batch
//   join_keys: list<large_binary> (non-null list of IPC-serialized batches)
//   phase: utf8 (nullable) — dictionary(int16,string) in Python, accepts utf8 for back-compat
//   execution_id: binary (nullable)
//   init_opaque_data: binary (nullable)
// ORDER matches vgi-python's dataclass field declaration order so serialization
// is positional-compatible with the Python reader.
const INIT_REQUEST_SCHEMA = new Schema([
  new Field("bind_call", new Binary(), false),
  new Field("output_schema", new Binary(), false),
  new Field("bind_opaque_data", new Binary(), true),
  new Field("projection_ids", new List(new Field("item", new Int64(), false)), true),
  new Field("pushdown_filters", new Binary(), true),
  new Field("join_keys", new List(new Field("item", new Binary(), true)), false),
  new Field("phase", new Utf8(), true),
  new Field("execution_id", new Binary(), true),
  new Field("init_opaque_data", new Binary(), true),
  // Order pushdown hints from DuckDB's RowGroupPruner (all null when no hint).
  new Field("order_by_column_name", new Utf8(), true),
  new Field("order_by_direction", new Utf8(), true),
  new Field("order_by_null_order", new Utf8(), true),
  new Field("order_by_limit", new Int64(), true),
  // TABLESAMPLE pushdown hints from DuckDB's SamplingPushdown optimizer.
  new Field("tablesample_percentage", new Float64(), true),
  new Field("tablesample_seed", new Int64(), true),
]);

// GlobalInitResponse schema:
//   execution_id: binary
//   opaque_data: binary (nullable)
//   max_workers: int64
const GLOBAL_INIT_RESPONSE_SCHEMA = new Schema([
  new Field("execution_id", new Binary(), false),
  new Field("opaque_data", new Binary(), true),
  new Field("max_workers", new Int64(), false),
]);

// TableCardinality schema:
//   estimate: int64 (nullable)
//   max: int64 (nullable)
const TABLE_CARDINALITY_SCHEMA = new Schema([
  new Field("estimate", new Int64(), true),
  new Field("max", new Int64(), true),
]);

// TableFunctionCardinalityRequest schema:
//   bind_call: binary
//   bind_opaque_data: binary (nullable)
const TABLE_FUNCTION_CARDINALITY_REQUEST_SCHEMA = new Schema([
  new Field("bind_call", new Binary(), false),
  new Field("bind_opaque_data", new Binary(), true),
]);

// ============================================================================
// Arguments serialization (matching Python's Arguments ArrowSerializableDataclass)
// ============================================================================

function serializeArguments(args: Arguments): Uint8Array {
  // Arguments are serialized as a single-row batch with one "args" Struct column.
  // The struct has fields: "positional_0", "positional_1", ... and "named_<name>".
  // This matches Python's Arguments.serialize_to_bytes() format.

  const structFields: Field[] = [];
  const structValues: Record<string, any> = {};

  // Positional args
  for (let i = 0; i < args.positional.length; i++) {
    const val = args.positional[i];
    const fieldName = `positional_${i}`;
    structFields.push(new Field(fieldName, inferScalarType(val), true));
    structValues[fieldName] = val;
  }

  // Named args
  for (const [name, val] of args.named) {
    const fieldName = `named_${name}`;
    structFields.push(new Field(fieldName, inferScalarType(val), true));
    structValues[fieldName] = val;
  }

  // Build the "args" struct column
  const structType = new Struct(structFields);
  const argsField = new Field("args", structType, true);
  const schema = new Schema([argsField]);

  if (structFields.length === 0) {
    // Empty struct: create batch with empty struct column
    const structData = makeData({ type: structType, length: 1, children: [], nullCount: 0 });
    const outerStructType = new Struct(schema.fields);
    const data = makeData({ type: outerStructType, length: 1, children: [structData], nullCount: 0 });
    const batch = new RecordBatch(schema, data);
    return serializeBatch(batch);
  }

  // Build struct children
  const children = structFields.map((f) => {
    const val = structValues[f.name];
    let coerced = [val];
    if (DataType.isInt(f.type) && (f.type as any).bitWidth === 64) {
      coerced = [typeof val === "number" ? BigInt(val) : val];
    }
    return vectorFromArray(coerced, f.type).data[0];
  });

  const structData = makeData({ type: structType, length: 1, children, nullCount: 0 });
  const outerStructType = new Struct(schema.fields);
  const data = makeData({ type: outerStructType, length: 1, children: [structData], nullCount: 0 });
  const batch = new RecordBatch(schema, data);
  return serializeBatch(batch);
}

function inferScalarType(val: any): DataType {
  if (val === null || val === undefined) return new Null();
  if (typeof val === "string") return new Utf8();
  if (typeof val === "boolean") return new Bool();
  if (typeof val === "number") return new Int64();
  if (typeof val === "bigint") return new Int64();
  if (val instanceof Uint8Array || val instanceof ArrayBuffer) return new Binary();
  return new Utf8(); // fallback
}

export function deserializeArguments(bytes: Uint8Array): Arguments {
  if (!bytes || bytes.length === 0) return new Arguments();

  // Ensure we have a clean copy (not a view into a larger buffer)
  const cleanBytes = bytes.byteOffset !== 0 || bytes.byteLength !== bytes.buffer.byteLength
    ? new Uint8Array(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength))
    : bytes;

  const reader = RecordBatchReader.from(cleanBytes);
  const batches = [...reader];

  const positional: any[] = [];
  const named = new Map<string, any>();

  if (batches.length === 0 || batches[0].numRows === 0) {
    return new Arguments(positional, named);
  }

  const batch = batches[0];
  // Arguments are serialized as a single "args" Struct column.
  // The struct has fields named "positional_0", "positional_1", etc.
  // for positional args, and "named_<name>" for named args.
  const argsCol = batch.getChild("args");
  if (argsCol) {
    // Extract per-positional types from the struct's children
    const argsStructType = argsCol.type as any;
    const structSchema = argsStructType.children
      ? new Schema(argsStructType.children)
      : batch.schema;

    const structVal = argsCol.get(0);
    if (structVal && typeof structVal === "object") {
      // Convert struct scalar to a plain object
      const dict: Record<string, any> = structVal.toJSON
        ? structVal.toJSON()
        : Object.assign({}, structVal);
      for (const [key, value] of Object.entries(dict)) {
        if (key.startsWith("positional_")) {
          const idx = parseInt(key.slice("positional_".length), 10);
          while (positional.length <= idx) positional.push(null);
          positional[idx] = value;
        } else if (key.startsWith("named_")) {
          const name = key.slice("named_".length);
          named.set(name, value);
        }
      }
    }
    return new Arguments(positional, named, structSchema);
  }

  // Fallback: flat columns (legacy format - each field is a column)
  const schema = batch.schema;
  if (!schema || schema.fields.length === 0) {
    return new Arguments(positional, named);
  }

  for (const field of schema.fields) {
    const col = batch.getChild(field.name);
    const val = col ? col.get(0) : null;

    if (field.name.startsWith("positional_")) {
      const idx = parseInt(field.name.slice("positional_".length), 10);
      while (positional.length <= idx) positional.push(null);
      positional[idx] = val;
    } else if (field.name.startsWith("named_")) {
      const name = field.name.slice("named_".length);
      named.set(name, val);
    } else {
      // Try numeric field name (old format)
      const metadata = field.metadata;
      const isNamed = metadata.get("vgi_arg") === "named";
      if (isNamed) {
        named.set(field.name, val);
      } else {
        const idx = parseInt(field.name, 10);
        if (!isNaN(idx)) {
          while (positional.length <= idx) positional.push(null);
          positional[idx] = val;
        }
      }
    }
  }

  return new Arguments(positional, named, schema);
}

// ============================================================================
// BindRequest serialization
// ============================================================================

export function serializeBindRequest(req: BindRequest): RecordBatch {
  const row: Record<string, any> = {
    function_name: req.functionName,
    arguments: serializeArguments(req.arguments),
    function_type: req.functionType,
    input_schema: req.inputSchema ? serializeSchema(req.inputSchema) : null,
    settings: req.settings ? serializeBatch(req.settings) : null,
    secrets: req.secrets ? serializeBatch(req.secrets) : null,
    attach_id: req.attachId ?? null,
    transaction_id: req.transactionId ?? null,
    resolved_secrets_provided: req.resolvedSecretsProvided ?? false,
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
    functionName: params.function_name,
    arguments: args,
    functionType,
    inputSchema: params.input_schema
      ? deserializeSchema(toUint8Array(params.input_schema))
      : null,
    settings: params.settings
      ? deserializeBatch(toUint8Array(params.settings))
      : null,
    secrets: params.secrets
      ? deserializeBatch(toUint8Array(params.secrets))
      : null,
    attachId: params.attach_id ? toUint8Array(params.attach_id) : null,
    transactionId: params.transaction_id
      ? toUint8Array(params.transaction_id)
      : null,
    resolvedSecretsProvided: params.resolved_secrets_provided ?? false,
  };
}

// ============================================================================
// BindResponse serialization
// ============================================================================

export function serializeBindResponse(
  resp: BindResponse
): Record<string, any> {
  return {
    output_schema: serializeSchema(resp.outputSchema),
    opaque_data: resp.opaqueData ?? null,
    lookup_secret_types: resp.lookupSecretTypes ?? [],
    lookup_scopes: resp.lookupScopes ?? [],
    lookup_names: resp.lookupNames ?? [],
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
    outputSchema: deserializeSchema(toUint8Array(params.output_schema)),
    opaqueData: params.opaque_data ? toUint8Array(params.opaque_data) : null,
    lookupSecretTypes: toStrArray(params.lookup_secret_types),
    lookupScopes: toStrArray(params.lookup_scopes),
    lookupNames: toStrArray(params.lookup_names),
  };
}

// ============================================================================
// InitRequest serialization
// ============================================================================

export function serializeInitRequest(req: InitRequest): RecordBatch {
  const bindCallBatch = serializeBindRequest(req.bindCall);
  const bindCallBytes = serializeBatch(bindCallBatch);

  const row: Record<string, any> = {
    bind_call: bindCallBytes,
    output_schema: serializeSchema(req.outputSchema),
    bind_opaque_data: req.bindOpaqueData ?? null,
    projection_ids: req.projectionIds?.map((n) => BigInt(n)) ?? null,
    pushdown_filters: req.pushdownFilters
      ? serializeBatch(req.pushdownFilters)
      : null,
    join_keys: (req.joinKeys ?? []).map((b) => serializeBatch(b)),
    phase: req.phase ?? null,
    execution_id: req.executionId ?? null,
    init_opaque_data: req.initOpaqueData ?? null,
    order_by_column_name: req.orderByColumnName ?? null,
    order_by_direction: req.orderByDirection ?? null,
    order_by_null_order: req.orderByNullOrder ?? null,
    order_by_limit: req.orderByLimit ?? null,
    tablesample_percentage: req.tablesamplePercentage ?? null,
    tablesample_seed: req.tablesampleSeed ?? null,
  };
  return buildSingleRowBatch(INIT_REQUEST_SCHEMA, row);
}

export function deserializeInitRequest(
  params: Record<string, any>
): InitRequest {
  const bindCallBytes = toUint8Array(params.bind_call);
  const bindCallBatch = deserializeBatch(bindCallBytes);
  // Extract the single row as params
  const bindParams: Record<string, any> = {};
  for (const field of bindCallBatch.schema.fields) {
    const col = bindCallBatch.getChild(field.name);
    bindParams[field.name] = col ? col.get(0) : null;
  }

  const bindCall = deserializeBindRequest(bindParams);

  // Parse projection_ids - may be a list/array of Int32
  let projectionIds: number[] | null = null;
  if (params.projection_ids != null) {
    const raw = params.projection_ids;
    if (Array.isArray(raw)) {
      projectionIds = raw.map(Number);
    } else if (raw && typeof raw[Symbol.iterator] === "function") {
      projectionIds = [...raw].map(Number);
    }
  }

  // Parse phase
  let phase: TableInOutPhase | null = null;
  if (params.phase != null) {
    const phaseStr = String(params.phase);
    if (phaseStr === "INPUT") {
      phase = TableInOutPhase.INPUT;
    } else if (phaseStr === "FINALIZE") {
      phase = TableInOutPhase.FINALIZE;
    }
  }

  // Parse join_keys - list of IPC-serialized RecordBatches
  const joinKeys: RecordBatch[] = [];
  if (params.join_keys != null) {
    const raw = params.join_keys;
    const iter: Iterable<any> = Array.isArray(raw)
      ? raw
      : typeof raw[Symbol.iterator] === "function"
      ? raw
      : [];
    for (const entry of iter) {
      if (entry == null) continue;
      try {
        joinKeys.push(deserializeBatch(toUint8Array(entry)));
      } catch {
        // Skip malformed batches rather than failing init.
      }
    }
  }

  return {
    bindCall,
    outputSchema: deserializeSchema(toUint8Array(params.output_schema)),
    bindOpaqueData: params.bind_opaque_data
      ? toUint8Array(params.bind_opaque_data)
      : null,
    projectionIds,
    pushdownFilters: params.pushdown_filters
      ? deserializeBatch(toUint8Array(params.pushdown_filters))
      : null,
    joinKeys,
    phase,
    executionId: params.execution_id
      ? toUint8Array(params.execution_id)
      : null,
    initOpaqueData: params.init_opaque_data
      ? toUint8Array(params.init_opaque_data)
      : null,
    orderByColumnName: parseEnum(params.order_by_column_name) ?? null,
    orderByDirection: parseDirection(params.order_by_direction),
    orderByNullOrder: parseNullOrder(params.order_by_null_order),
    orderByLimit: parseBigInt(params.order_by_limit),
    tablesamplePercentage: parseNumber(params.tablesample_percentage),
    tablesampleSeed: parseBigInt(params.tablesample_seed),
  };
}

function parseEnum(v: any): string | null {
  if (v === null || v === undefined) return null;
  return String(v);
}
function parseDirection(v: any): OrderByDirection | null {
  if (v === null || v === undefined) return null;
  const s = String(v);
  if (s === "ASC") return OrderByDirection.ASC;
  if (s === "DESC") return OrderByDirection.DESC;
  return null;
}
function parseNullOrder(v: any): OrderByNullOrder | null {
  if (v === null || v === undefined) return null;
  const s = String(v);
  if (s === "NULLS_FIRST") return OrderByNullOrder.NULLS_FIRST;
  if (s === "NULLS_LAST") return OrderByNullOrder.NULLS_LAST;
  return null;
}
function parseBigInt(v: any): bigint | null {
  if (v === null || v === undefined) return null;
  if (typeof v === "bigint") return v;
  if (typeof v === "number") return BigInt(v);
  try {
    return BigInt(String(v));
  } catch {
    return null;
  }
}
function parseNumber(v: any): number | null {
  if (v === null || v === undefined) return null;
  if (typeof v === "number") return v;
  if (typeof v === "bigint") return Number(v);
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
}

// ============================================================================
// GlobalInitResponse serialization
// ============================================================================

export function serializeGlobalInitResponse(
  resp: GlobalInitResponse
): Record<string, any> {
  return {
    execution_id: resp.executionId,
    opaque_data: resp.opaqueData ?? null,
    max_workers: resp.maxWorkers,
  };
}

export function deserializeGlobalInitResponse(
  params: Record<string, any>
): GlobalInitResponse {
  return {
    executionId: toUint8Array(params.execution_id),
    opaqueData: params.opaque_data
      ? toUint8Array(params.opaque_data)
      : null,
    maxWorkers: Number(params.max_workers),
  };
}

// ============================================================================
// TableFunctionCardinalityRequest serialization
// ============================================================================

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
    bindCall,
    bindOpaqueData: params.bind_opaque_data
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

// ============================================================================
// Helpers
// ============================================================================

function toUint8Array(val: any): Uint8Array {
  if (typeof val === "string") return new TextEncoder().encode(val);
  const result = toUint8ArrayBase(val);
  if (result.length === 0 && val != null) {
    throw new Error(`Cannot convert ${typeof val} to Uint8Array`);
  }
  return result;
}

function buildSingleRowBatch(
  schema: Schema,
  values: Record<string, any>
): RecordBatch {
  const children = schema.fields.map((f: Field) => {
    let val = values[f.name];
    // Coerce int64
    if (DataType.isInt(f.type) && (f.type as any).bitWidth === 64) {
      if (typeof val === "number") val = BigInt(val);
    }
    const arr = vectorFromArray([val], f.type);
    return arr.data[0];
  });

  const structType = new Struct(schema.fields);
  const data = makeData({
    type: structType,
    length: 1,
    children,
    nullCount: 0,
  });

  return new RecordBatch(schema, data);
}
