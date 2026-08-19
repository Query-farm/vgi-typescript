// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// TableFunctionPlanRequest / PlanResponse / ScanSplit wire serialization.
//
// Field ORDER and nullability are wire-significant: they mirror vgi/protocol.py
// exactly, and the extension rejects a response whose schema disagrees.

import { type VgiSchema, schema, field, binary, bool, int64, list } from "../../arrow/index.js";
import {
  ScanSplitSchema,
  TableFunctionPlanResultSchema,
} from "../../generated/vgi-protocol-schemas.js";
import type { BindRequest } from "../types.js";
import { deserializeBatch, batchToScalarDict } from "../../util/arrow/index.js";
import { toUint8Array } from "./shared.js";
import { deserializeBindRequest } from "./bind.js";

/**
 * One named, independently redeemable unit of scan work.
 *
 * A worker sets `payload` and nothing else; the framework stamps `token` from
 * it, so an author cannot forget the consistency anchor or mis-bind the
 * fingerprint. The client sends the TOKEN back, never the raw payload.
 */
export interface ScanSplit {
  /** The worker's own bytes NAMING this unit of work. */
  payload: Uint8Array;
  /** Framework-stamped envelope. Set by the framework, never by a worker. */
  token?: Uint8Array;
  estimatedRows?: number | bigint | null;
  /** True if `estimatedRows` is exact — unlocks COUNT(*) from statistics. */
  rowsExact?: boolean;
  /**
   * Load-bearing for engines that bin-pack (DataFusion weight, Trino
   * SplitWeight). Omitting it degrades them to round-robin by count; a greedily
   * claiming client needs no cost model at all.
   */
  estimatedBytes?: number | bigint | null;
  /** 2-row (min, max) batch in the existing `vgi_partition_values` encoding. */
  partitionBounds?: Uint8Array | null;
  columnStatistics?: Uint8Array | null;
  locationIds?: number[] | null;
  startPosition?: Uint8Array | null;
  /** Absent means UNBOUNDED — a shard read forever. */
  endPosition?: Uint8Array | null;
}

/** What a table function's `plan` hook returns. */
export interface PlanResult {
  /**
   * One entry per unit of work. EMPTY is legal and means "no work": a
   * fully-pruned scan reaches it, and the client must produce an empty result
   * rather than an error.
   */
  splits: ScanSplit[];
  /**
   * Continued enumeration. More than one MUST partition the remaining
   * enumeration disjointly and exhaustively — the client dedups by token
   * regardless, because violating it produces duplicate ROWS.
   */
  nextCursors?: Uint8Array[];
  /** NORMATIVE cap on redemption concurrency, not advisory. */
  maxWorkers?: number | null;
  estimatedTotalSplits?: number | bigint | null;
  estimatedTotalRows?: number | bigint | null;
  estimatedTotalBytes?: number | bigint | null;
  /**
   * The counter a stale token is detected against, and the anchor the framework
   * stamps into every token in this plan.
   */
  catalogVersion?: number | bigint | null;
  /**
   * `catalog` (the default) or `transaction`. A transaction-scoped plan is not
   * cacheable and is not redeemable after commit or rollback.
   */
  scope?: "catalog" | "transaction";
  cacheMaxAgeSeconds?: number | null;
}

/** The request a client sends to divide a scan. */
export interface TableFunctionPlanRequest {
  bind_call: BindRequest;
  bind_opaque_data: Uint8Array | null;
  projection_ids: number[] | null;
  pushdown_filters: Uint8Array | null;
  /** The primary sizing lever: every engine is byte-driven. */
  target_split_bytes: bigint | null;
  /**
   * The parallelism FLOOR — a small but expensive table still needs one reader
   * per thread, which a byte target alone would not give it.
   */
  min_splits: bigint | null;
  /**
   * A place in the ENUMERATION of splits — NOT a place in the data. A cursor
   * lives for one plan call; a position is checkpointed and must survive
   * restarts, upgrades and key rotation.
   */
  cursor: Uint8Array | null;
}

/**
 * The ScanSplit shape, from codegen rather than hand-written.
 *
 * It used to be spelled out here, and hand-written is how four SDKs ended up
 * disagreeing about which of these columns were binary and which were
 * large_binary — a disagreement the client surfaced as "the worker bypassed the
 * framework", on every scan.
 */
export const SCAN_SPLIT_SCHEMA: VgiSchema = ScanSplitSchema as VgiSchema;

/**
 * The PlanResponse shape, taken from codegen rather than hand-written: it is
 * regenerated from the canonical protocol, so it cannot drift.
 */
export const PLAN_RESPONSE_SCHEMA: VgiSchema = TableFunctionPlanResultSchema as VgiSchema;

/**
 * Decode a plan request.
 *
 * A client sends only the sizing inputs it actually has — the DuckDB extension
 * sends seven columns, because it has no basis to invent a byte target it did
 * not set — so every field but `bind_call` is read defensively.
 */
export function deserializePlanRequest(params: Record<string, any>): TableFunctionPlanRequest {
  const bindCallBytes = toUint8Array(params.bind_call);
  const bindCall = deserializeBindRequest(batchToScalarDict(deserializeBatch(bindCallBytes)));

  let projectionIds: number[] | null = null;
  if (params.projection_ids != null) {
    const raw = params.projection_ids;
    if (Array.isArray(raw)) projectionIds = raw.map(Number);
    else if (raw && typeof raw[Symbol.iterator] === "function") projectionIds = [...raw].map(Number);
  }

  const asBigInt = (v: unknown): bigint | null =>
    v == null ? null : typeof v === "bigint" ? v : BigInt(Number(v));

  return {
    bind_call: bindCall,
    bind_opaque_data: params.bind_opaque_data ? toUint8Array(params.bind_opaque_data) : null,
    projection_ids: projectionIds,
    pushdown_filters: params.pushdown_filters ? toUint8Array(params.pushdown_filters) : null,
    target_split_bytes: asBigInt(params.target_split_bytes),
    min_splits: asBigInt(params.min_splits),
    cursor: params.cursor ? toUint8Array(params.cursor) : null,
  };
}

/**
 * The raw bind fields the split-token fingerprint is derived from.
 *
 * Read from the UNDECODED bind_call dict rather than the parsed BindRequest,
 * because the fingerprint must be reproducible byte-for-byte between the plan
 * that minted a token and the init that redeems it. A decoded-then-re-encoded
 * argument list is not guaranteed to round-trip to identical bytes; the wire
 * bytes are.
 */
export function fingerprintInputs(bindCallBytes: Uint8Array): {
  schemaName: string;
  functionName: string;
  args: Uint8Array;
  settings: Uint8Array;
} {
  const dict = batchToScalarDict(deserializeBatch(bindCallBytes));
  return {
    schemaName: dict.schema_name == null ? "" : String(dict.schema_name),
    functionName: String(dict.function_name ?? ""),
    args: dict.arguments ? toUint8Array(dict.arguments) : new Uint8Array(0),
    settings: dict.settings ? toUint8Array(dict.settings) : new Uint8Array(0),
  };
}

/** Build the row dict for one `ScanSplit`. */
export function scanSplitRow(split: ScanSplit, token: Uint8Array): Record<string, any> {
  const asBigInt = (v: number | bigint | null | undefined): bigint | null =>
    v == null ? null : typeof v === "bigint" ? v : BigInt(v);
  return {
    payload: split.payload,
    token,
    estimated_rows: asBigInt(split.estimatedRows),
    rows_exact: split.rowsExact === true,
    estimated_bytes: asBigInt(split.estimatedBytes),
    partition_bounds: split.partitionBounds ?? null,
    column_statistics: split.columnStatistics ?? null,
    location_ids: split.locationIds ?? null,
    start_position: split.startPosition ?? null,
    end_position: split.endPosition ?? null,
  };
}

/** Build the row dict for a `PlanResponse`, given already-serialized splits. */
export function planResponseRow(result: PlanResult, splitBlobs: Uint8Array[]): Record<string, any> {
  const asBigInt = (v: number | bigint | null | undefined): bigint | null =>
    v == null ? null : typeof v === "bigint" ? v : BigInt(v);
  return {
    splits: splitBlobs,
    next_cursors: result.nextCursors ?? [],
    execution_id: null,
    init_opaque_data: new Uint8Array(0),
    max_workers: asBigInt(result.maxWorkers),
    estimated_total_splits: asBigInt(result.estimatedTotalSplits),
    estimated_total_rows: asBigInt(result.estimatedTotalRows),
    estimated_total_bytes: asBigInt(result.estimatedTotalBytes),
    catalog_version: asBigInt(result.catalogVersion),
    scope: result.scope ?? "catalog",
    locations: null,
    partitioning: [],
    sort_order: [],
    cache_max_age_seconds: asBigInt(result.cacheMaxAgeSeconds),
    start_position: new Uint8Array(0),
    end_position: new Uint8Array(0),
  };
}
