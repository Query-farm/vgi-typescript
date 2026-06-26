// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Protocol-level types for the VGI worker. Snake_case to match the Python
// side and the generated vgi-client.ts wire interfaces. These are the
// *deserialized* shapes handlers see — fields that cross the wire as `binary`
// here hold the parsed Arrow object (Schema / RecordBatch / Arguments) rather
// than raw bytes, so the handler doesn't have to deserialize itself.
//
// Domain-layer types (FunctionMeta, TableFunctionConfig, TableProcessParams,
// StreamHandlers, VgiClientOptions, etc.) remain camelCase as JS-idiomatic
// public API surfaces. Conversions happen at the layer boundary in
// src/protocol/serialize.ts and the function/catalog dispatch sites.

import type { VgiSchema, VgiBatch } from "../arrow/index.js";
import type { Arguments } from "../arguments/arguments.js";
import { FunctionType, TableInOutPhase, type TableCardinality } from "../types.js";

/**
 * Context for a `COPY ... FROM` read, threaded onto {@link BindRequest}.
 *
 * Present only when the bind/init opens a COPY-FROM scan (`null`/absent
 * otherwise — set by the VGI extension's `copy_from_bind`). `InitRequest`
 * embeds the same `BindRequest` as `bind_call`, so process()/init also reach it
 * via `initCall.bind_call.copy_from`. The handler's options arrive through the
 * normal `BindRequest.arguments` (built from the COPY options), so they are not
 * duplicated here. Mirrors vgi-python's `protocol.CopyFromContext`.
 */
export interface CopyFromContext {
  /** The `FORMAT` name resolved at COPY bind time. */
  format: string;
  /** The source path from the `COPY ... FROM 'path'` statement. */
  file_path: string;
  /**
   * The COPY target's column schema (name + type, in target order). The worker
   * must bind its output to, and emit columns whose types match, this schema
   * exactly — DuckDB inserts no cast between the scan and the INSERT. Holds the
   * parsed Arrow Schema (the wire field is binary; deserialized here).
   */
  expected_schema: VgiSchema;
}

export interface BindRequest {
  function_name: string;
  arguments: Arguments;
  function_type: FunctionType;
  input_schema: VgiSchema | null;
  settings: VgiBatch | null;
  secrets: VgiBatch | null;
  attach_opaque_data: Uint8Array | null;
  transaction_opaque_data: Uint8Array | null;
  resolved_secrets_provided: boolean;
  /**
   * COPY ... FROM context — `null`/absent unless this bind/init opens a
   * COPY-FROM scan. Additive + name-keyed, so ordinary scans (which omit it on
   * the wire) deserialize to `null`. Mirrors vgi-python's `BindRequest.copy_from`.
   */
  copy_from?: CopyFromContext | null;
  /**
   * Time travel: the AT (TIMESTAMP|VERSION ...) clause for this scan, threaded
   * from DuckDB's per-reference bind. Both `null` when the scan has no AT clause.
   * For inline-bound (function-backed) tables the actual on_bind RPC runs once at
   * attach with no AT, so the per-scan AT is carried on the bind request embedded
   * in each InitRequest — read it at init via `init_call.bind_call.at_unit` (or
   * `TableProcessParams.atUnit`). Mirrors vgi-python's `BindRequest.at_unit`.
   */
  at_unit?: string | null;
  at_value?: string | null;
}

export interface BindResponse {
  output_schema: VgiSchema;
  opaque_data: Uint8Array | null;
  lookup_secret_types?: string[];
  lookup_scopes?: string[];
  lookup_names?: string[];
}

export interface InitRequest {
  bind_call: BindRequest;
  output_schema: VgiSchema;
  bind_opaque_data: Uint8Array | null;
  projection_ids: number[] | null;
  pushdown_filters: VgiBatch | null;
  /**
   * Join-key value batches, one per join-keys column. Keyed by the column
   * name inside each batch's schema. Populated when DuckDB promotes
   * IN/OR lists or join predicates to batched join-keys pushdowns.
   */
  join_keys: VgiBatch[];
  phase: TableInOutPhase | null;
  /**
   * Buffered-table finalize stream: which finalize_state_id this stream
   * serves. Set when phase=TABLE_BUFFERING_FINALIZE; null otherwise. Opaque
   * bytes the worker's combine() chose.
   */
  finalize_state_id: Uint8Array | null;
  execution_id: Uint8Array | null;
  init_opaque_data: Uint8Array | null;
  // Order pushdown hints from DuckDB's RowGroupPruner (null when no hint).
  order_by_column_name: string | null;
  order_by_direction: OrderByDirection | null;
  order_by_null_order: OrderByNullOrder | null;
  order_by_limit: bigint | null;
  // TABLESAMPLE pushdown hints.
  tablesample_percentage: number | null;
  tablesample_seed: bigint | null;
}

export enum OrderByDirection {
  ASC = "ASC",
  DESC = "DESC",
}

export enum OrderByNullOrder {
  NULLS_FIRST = "NULLS_FIRST",
  NULLS_LAST = "NULLS_LAST",
}

export interface GlobalInitResponse {
  max_workers: number;
  execution_id: Uint8Array;
  opaque_data: Uint8Array | null;
}

export interface TableFunctionCardinalityRequest {
  bind_call: BindRequest;
  bind_opaque_data: Uint8Array | null;
}

export { TableCardinality };
