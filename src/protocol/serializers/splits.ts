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
import { OrderByDirection, OrderByNullOrder } from "../types.js";
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
  /** One-row exact transformed-key tuple aligned with `PlanResult.keyGrouping`. */
  keyValue?: Uint8Array | null;
  /** Zero-based range owned by this split under `PlanResult.rangePartitioning`. */
  rangeIndex?: number | bigint | null;
  /** Two-row (min, max) pruning batch; never an exact distribution claim. */
  columnBounds?: Uint8Array | null;
  /**
   * Two-row (min, max) batch in the legacy `vgi_partition_values` encoding.
   * @deprecated Protocol 3 producers should use `columnBounds`.
   */
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
   * enumeration disjointly and exhaustively. No client checks this: a dedup was
   * tried and removed (it needed a set holding a copy of every token, it compared
   * token bytes so it could never work on a keyed worker where each mint uses a
   * fresh nonce, and the most a client can do with a duplicate is refuse anyway).
   * Violating it returns DUPLICATE ROWS, silently.
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
  /** Hoisted host list indexed by each split's `locationIds`. */
  locations?: string[] | null;
  /** Serialized `KeyGrouping` shared by every page of this plan. */
  keyGrouping?: Uint8Array | null;
  /** Serialized `RangePartitioning` shared by every page of this plan. */
  rangePartitioning?: Uint8Array | null;
  /** @deprecated Protocol 3 producers should use `keyGrouping` or `rangePartitioning`. */
  partitioning?: Uint8Array[] | null;
  /** Serialized `SortField` records describing order within each split. */
  sortOrder?: Uint8Array[] | null;
  cacheMaxAgeSeconds?: number | null;
  /** Actual exclusive lower data position selected by the worker. */
  startPosition?: Uint8Array | null;
  /** Data frontier resolved by the worker for this plan. */
  endPosition?: Uint8Array | null;
}

/** The request a client sends to divide a scan. */
export interface TableFunctionPlanRequest {
  bind_call: BindRequest;
  bind_opaque_data: Uint8Array | null;
  projection_ids: number[] | null;
  pushdown_filters: Uint8Array | null;
  join_keys: Uint8Array[] | null;
  row_limit: bigint | null;
  /** The primary sizing lever: every engine is byte-driven. */
  target_split_bytes: bigint | null;
  /**
   * The parallelism FLOOR — a small but expensive table still needs one reader
   * per thread, which a byte target alone would not give it.
   */
  min_splits: bigint | null;
  /** Maximum number of splits to enumerate on this response page. */
  max_splits_per_response: bigint | null;
  /**
   * A place in the ENUMERATION of splits — NOT a place in the data. A cursor
   * lives for one plan call; a position is checkpointed and must survive
   * restarts, upgrades and key rotation.
   */
  cursor: Uint8Array | null;
  /** Narrower filters supplied on a continuation request. */
  refined_filters: Uint8Array | null;
  /** False while the client may still send further filter refinement. */
  filters_complete: boolean;
  start_position: Uint8Array | null;
  end_position: Uint8Array | null;
  order_by_column_name: string | null;
  order_by_direction: OrderByDirection | null;
  order_by_null_order: OrderByNullOrder | null;
  order_by_limit: bigint | null;
  tablesample_percentage: number | null;
  tablesample_seed: bigint | null;
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
  const asBytes = (v: unknown): Uint8Array | null => v == null ? null : toUint8Array(v);
  const asBytesList = (v: unknown): Uint8Array[] | null => {
    if (v == null) return null;
    const values = Array.isArray(v)
      ? v
      : v && typeof (v as any)[Symbol.iterator] === "function"
        ? [...(v as Iterable<unknown>)]
        : [];
    return values.filter((entry) => entry != null).map(toUint8Array);
  };
  const enumValue = <T extends string>(v: unknown, values: readonly T[]): T | null => {
    if (v == null) return null;
    const text = String(v) as T;
    return values.includes(text) ? text : null;
  };

  return {
    bind_call: bindCall,
    bind_opaque_data: asBytes(params.bind_opaque_data),
    projection_ids: projectionIds,
    pushdown_filters: asBytes(params.pushdown_filters),
    join_keys: asBytesList(params.join_keys),
    row_limit: asBigInt(params.row_limit),
    target_split_bytes: asBigInt(params.target_split_bytes),
    min_splits: asBigInt(params.min_splits),
    max_splits_per_response: asBigInt(params.max_splits_per_response),
    cursor: asBytes(params.cursor),
    refined_filters: asBytes(params.refined_filters),
    filters_complete: params.filters_complete == null ? true : Boolean(params.filters_complete),
    start_position: asBytes(params.start_position),
    end_position: asBytes(params.end_position),
    order_by_column_name: params.order_by_column_name == null ? null : String(params.order_by_column_name),
    order_by_direction: enumValue(params.order_by_direction, Object.values(OrderByDirection)),
    order_by_null_order: enumValue(params.order_by_null_order, Object.values(OrderByNullOrder)),
    order_by_limit: asBigInt(params.order_by_limit),
    tablesample_percentage: params.tablesample_percentage == null ? null : Number(params.tablesample_percentage),
    tablesample_seed: asBigInt(params.tablesample_seed),
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
  schemaPath: string[];
  functionName: string;
  args: Uint8Array;
  settings: Uint8Array;
} {
  const dict = batchToScalarDict(deserializeBatch(bindCallBytes));
  return {
    schemaPath: dict.schema_path == null
      ? []
      : (Array.isArray(dict.schema_path) ? dict.schema_path : [...dict.schema_path]).map(String),
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
    // Cleared, not forwarded. The payload is sealed INTO the token, and shipping
    // the plaintext in the field beside the ciphertext made the seal decorative.
    // No client reads it — the C++ side pulls `token` alone — and redemption
    // recovers the payload from inside the envelope.
    payload: new Uint8Array(0),
    token,
    estimated_rows: asBigInt(split.estimatedRows),
    rows_exact: split.rowsExact === true,
    estimated_bytes: asBigInt(split.estimatedBytes),
    key_value: split.keyValue ?? null,
    range_index: asBigInt(split.rangeIndex),
    column_bounds: split.columnBounds ?? null,
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
    locations: result.locations ?? null,
    key_grouping: result.keyGrouping ?? null,
    range_partitioning: result.rangePartitioning ?? null,
    partitioning: result.partitioning ?? null,
    sort_order: result.sortOrder ?? null,
    cache_max_age_seconds: asBigInt(result.cacheMaxAgeSeconds),
    start_position: result.startPosition ?? null,
    end_position: result.endPosition ?? null,
  };
}
