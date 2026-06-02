// Copyright 2025, 2026 Query Farm LLC - https://query.farm
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
  type VgiBatch,
  schema as makeSchema,
  field,
  utf8,
  binary,
  int64,
  float64,
  list,
} from "../../arrow/index.js";
// RecordBatch type alias kept for the joinKeys closure below — it's a runtime
// value that lands in BindRequest, not a wire shape.
type RecordBatch = VgiBatch;
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
import { decodeDictValue } from "../../util/arrow/index.js";

const INIT_REQUEST_SCHEMA = makeSchema([
  field("bind_call", binary(), false),
  field("output_schema", binary(), false),
  field("bind_opaque_data", binary(), true),
  field("projection_ids", list(field("item", int64(), false)), true),
  field("pushdown_filters", binary(), true),
  field("join_keys", list(field("item", binary(), true)), false),
  field("phase", utf8(), true),
  field("execution_id", binary(), true),
  field("init_opaque_data", binary(), true),
  // Order pushdown hints from DuckDB's RowGroupPruner (all null when no hint).
  field("order_by_column_name", utf8(), true),
  field("order_by_direction", utf8(), true),
  field("order_by_null_order", utf8(), true),
  field("order_by_limit", int64(), true),
  // TABLESAMPLE pushdown hints from DuckDB's SamplingPushdown optimizer.
  field("tablesample_percentage", float64(), true),
  field("tablesample_seed", int64(), true),
  // Buffered-table finalize stream identifier — C++ appends this last.
  field("finalize_state_id", binary(), true),
]);

const GLOBAL_INIT_RESPONSE_SCHEMA = makeSchema([
  field("execution_id", binary(), false),
  field("opaque_data", binary(), true),
  field("max_workers", int64(), false),
]);

export function serializeInitRequest(req: InitRequest): VgiBatch {
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
    finalize_state_id: req.finalize_state_id ?? null,
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

  // Parse phase. DuckDB sends this as a Dictionary on the wire; vgi-rpc's
  // row extractor doesn't auto-decode dictionaries, so values come in as raw
  // Arrow Data objects. decodeDictValue resolves them to the underlying string.
  let phase: TableInOutPhase | null = null;
  const phaseRaw = decodeDictValue(params.phase);
  if (phaseRaw != null) {
    const phaseStr = String(phaseRaw);
    if (phaseStr === "INPUT") {
      phase = TableInOutPhase.INPUT;
    } else if (phaseStr === "FINALIZE") {
      phase = TableInOutPhase.FINALIZE;
    } else if (phaseStr === "TABLE_BUFFERING") {
      phase = TableInOutPhase.TABLE_BUFFERING;
    } else if (phaseStr === "TABLE_BUFFERING_FINALIZE") {
      phase = TableInOutPhase.TABLE_BUFFERING_FINALIZE;
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
    finalize_state_id: params.finalize_state_id
      ? toUint8Array(params.finalize_state_id)
      : null,
    execution_id: params.execution_id
      ? toUint8Array(params.execution_id)
      : null,
    init_opaque_data: params.init_opaque_data
      ? toUint8Array(params.init_opaque_data)
      : null,
    order_by_column_name: parseEnum(decodeDictValue(params.order_by_column_name)) ?? null,
    order_by_direction: parseDirection(decodeDictValue(params.order_by_direction)),
    order_by_null_order: parseNullOrder(decodeDictValue(params.order_by_null_order)),
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
