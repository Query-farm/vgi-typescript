// InitRequest / GlobalInitResponse wire serialization. Schema mirrors
// vgi-python's InitRequest dataclass — outer wire shape:
//   bind_call: binary (nested IPC of BindRequest)
//   output_schema: binary (nullable serialized Schema)
//   bind_opaque_data: binary (nullable)
//   projection_ids: list<int64> (nullable)
//   pushdown_filters: large_binary (nullable) — nested IPC of filter batch
//   join_keys: list<large_binary> (non-null list of IPC-serialized batches)
//   phase: utf8 (nullable) — dictionary(int16,string) in Python
//   execution_id: binary (nullable)
//   init_opaque_data: binary (nullable)
// Order pushdown hints + TABLESAMPLE pushdown hints follow.

import {
  Schema,
  Field,
  RecordBatch,
  Utf8,
  Binary,
  Int64,
  Float64,
  List,
} from "@query-farm/apache-arrow";
import { TableInOutPhase } from "../../types.js";
import {
  OrderByDirection,
  OrderByNullOrder,
  type InitRequest,
  type GlobalInitResponse,
} from "../types.js";
import {
  serializeSchema,
  deserializeSchema,
  serializeBatch,
  deserializeBatch,
} from "../../util/arrow/index.js";
import { toUint8Array, buildSingleRowBatch } from "./shared.js";
import { serializeBindRequest, deserializeBindRequest } from "./bind.js";

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

const GLOBAL_INIT_RESPONSE_SCHEMA = new Schema([
  new Field("execution_id", new Binary(), false),
  new Field("opaque_data", new Binary(), true),
  new Field("max_workers", new Int64(), false),
]);

export function serializeInitRequest(req: InitRequest): RecordBatch {
  const bindCallBatch = serializeBindRequest(req.bind_call);
  const bindCallBytes = serializeBatch(bindCallBatch);

  const row: Record<string, any> = {
    bind_call: bindCallBytes,
    output_schema: serializeSchema(req.output_schema),
    bind_opaque_data: req.bind_opaque_data ?? null,
    projection_ids: req.projection_ids?.map((n) => BigInt(n)) ?? null,
    pushdown_filters: req.pushdown_filters
      ? serializeBatch(req.pushdown_filters)
      : null,
    join_keys: (req.join_keys ?? []).map((b) => serializeBatch(b)),
    phase: req.phase ?? null,
    execution_id: req.execution_id ?? null,
    init_opaque_data: req.init_opaque_data ?? null,
    order_by_column_name: req.order_by_column_name ?? null,
    order_by_direction: req.order_by_direction ?? null,
    order_by_null_order: req.order_by_null_order ?? null,
    order_by_limit: req.order_by_limit ?? null,
    tablesample_percentage: req.tablesample_percentage ?? null,
    tablesample_seed: req.tablesample_seed ?? null,
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
    bind_call: bindCall,
    output_schema: deserializeSchema(toUint8Array(params.output_schema)),
    bind_opaque_data: params.bind_opaque_data
      ? toUint8Array(params.bind_opaque_data)
      : null,
    projection_ids: projectionIds,
    pushdown_filters: params.pushdown_filters
      ? deserializeBatch(toUint8Array(params.pushdown_filters))
      : null,
    join_keys: joinKeys,
    phase,
    execution_id: params.execution_id
      ? toUint8Array(params.execution_id)
      : null,
    init_opaque_data: params.init_opaque_data
      ? toUint8Array(params.init_opaque_data)
      : null,
    order_by_column_name: parseEnum(params.order_by_column_name) ?? null,
    order_by_direction: parseDirection(params.order_by_direction),
    order_by_null_order: parseNullOrder(params.order_by_null_order),
    order_by_limit: parseBigInt(params.order_by_limit),
    tablesample_percentage: parseNumber(params.tablesample_percentage),
    tablesample_seed: parseBigInt(params.tablesample_seed),
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

export function serializeGlobalInitResponse(
  resp: GlobalInitResponse
): Record<string, any> {
  return {
    execution_id: resp.execution_id,
    opaque_data: resp.opaque_data ?? null,
    max_workers: resp.max_workers,
  };
}

export function deserializeGlobalInitResponse(
  params: Record<string, any>
): GlobalInitResponse {
  return {
    execution_id: toUint8Array(params.execution_id),
    opaque_data: params.opaque_data
      ? toUint8Array(params.opaque_data)
      : null,
    max_workers: Number(params.max_workers),
  };
}

export { INIT_REQUEST_SCHEMA, GLOBAL_INIT_RESPONSE_SCHEMA };
