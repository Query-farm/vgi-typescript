// Client option types for VgiClient.

import type { RecordBatch } from "@query-farm/apache-arrow";
import type { Arguments } from "../arguments/arguments.js";
import type { AttachOptionValue } from "../catalog/attach-options.js";
import type { BindResponse } from "../protocol/types.js";
import { OrderByDirection, OrderByNullOrder } from "../protocol/types.js";

export type { AttachOptionValue };
export { OrderByDirection, OrderByNullOrder };

/** ORDER BY pushdown hint passed at init time. */
export interface OrderByPushdown {
  columnName: string;
  direction: OrderByDirection;
  nullOrder: OrderByNullOrder;
  /** Optional LIMIT to combine with ORDER BY. */
  limit?: bigint | number | null;
}

/** TABLESAMPLE pushdown hint passed at init time. */
export interface TablesamplePushdown {
  /** Sample percentage in [0, 100]. */
  percentage: number;
  /** Optional sampling seed. */
  seed?: bigint | number | null;
}

/** Callback invoked after a successful bind, before init. */
export type BindResultCallback = (response: BindResponse) => void;

/**
 * Options bag for VgiClient.catalogAttach.
 *
 * Pass `options` as a plain key→value map — the client serializes to an
 * Arrow RecordBatch with per-value type inference (see AttachOptionValue).
 * For Arrow types that inference can't express (Decimal, Timestamp, exact
 * int width, nested struct), use `optionsBytes` to supply pre-serialized
 * bytes. Providing both throws.
 *
 * `dataVersionSpec` / `implementationVersion` are sent to versioned
 * catalogs for attach-time validation; workers that aren't versioned
 * ignore them.
 */
export interface CatalogAttachOptions {
  options?: Record<string, AttachOptionValue>;
  optionsBytes?: Uint8Array;
  dataVersionSpec?: string | null;
  implementationVersion?: string | null;
}

/** Conflict resolution strategy for create operations. */
export type OnCreateConflict = "error" | "ignore" | "replace";

/** DuckDB catalog function type filter (sent as uppercase wire values). */
export type CatalogFunctionType = "SCALAR_FUNCTION" | "TABLE_FUNCTION";

/** Macro type filter for schema contents listing (uppercase wire values). */
export type CatalogMacroType = "SCALAR_MACRO" | "TABLE_MACRO";

/** Options for constructing a VgiClient. */
export interface VgiClientOptions {
  /** Pre-existing attach ID to bind this client to a specific catalog. */
  attachId?: Uint8Array;
}

/** Options for calling a table function. */
export interface TableFunctionOptions {
  /** Name of the function to call. */
  functionName: string;
  /** Positional and named arguments. */
  arguments?: Arguments;
  /** Column indices to project (filter pushdown). */
  projectionIds?: number[];
  /** Filter pushdown batch. */
  pushdownFilters?: RecordBatch;
  /** DuckDB settings to pass to the function. */
  settings?: RecordBatch;
  /** Transaction ID for transactional catalogs. */
  transactionId?: Uint8Array;
  /** Attach ID to bind this call to a specific catalog attach. Overrides the client-level attachId. */
  attachId?: Uint8Array;
  /** ORDER BY pushdown hint from DuckDB's RowGroupPruner. */
  orderBy?: OrderByPushdown;
  /** TABLESAMPLE pushdown hint from DuckDB's SamplingPushdown optimizer. */
  tablesample?: TablesamplePushdown;
  /** Join-key value batches, one per join-keys column. */
  joinKeys?: RecordBatch[];
  /** Invoked after bind, before init. Receives the bind response. */
  onBind?: BindResultCallback;
}

/** Options for calling a scalar function. */
export interface ScalarFunctionOptions {
  /** Name of the function to call. */
  functionName: string;
  /** Input batches to process. */
  input: Iterable<RecordBatch> | AsyncIterable<RecordBatch>;
  /** Positional and named arguments. */
  arguments?: Arguments;
  /** DuckDB settings to pass to the function. */
  settings?: RecordBatch;
  /** DuckDB secrets to pass to the function. */
  secrets?: RecordBatch;
  /** Transaction ID for transactional catalogs. */
  transactionId?: Uint8Array;
  /** Attach ID to bind this call to a specific catalog attach. Overrides the client-level attachId. */
  attachId?: Uint8Array;
  /** Invoked after bind, before init. Receives the bind response. */
  onBind?: BindResultCallback;
}

/** Options for calling a table-in-out function. */
export interface TableInOutFunctionOptions {
  /** Name of the function to call. */
  functionName: string;
  /** Input batches to process. */
  input: Iterable<RecordBatch> | AsyncIterable<RecordBatch>;
  /** Positional and named arguments. */
  arguments?: Arguments;
  /** Column indices to project (filter pushdown). */
  projectionIds?: number[];
  /** Filter pushdown batch. */
  pushdownFilters?: RecordBatch;
  /** DuckDB settings to pass to the function. */
  settings?: RecordBatch;
  /** Transaction ID for transactional catalogs. */
  transactionId?: Uint8Array;
  /** Attach ID to bind this call to a specific catalog attach. Overrides the client-level attachId. */
  attachId?: Uint8Array;
  /** ORDER BY pushdown hint from DuckDB's RowGroupPruner. */
  orderBy?: OrderByPushdown;
  /** TABLESAMPLE pushdown hint from DuckDB's SamplingPushdown optimizer. */
  tablesample?: TablesamplePushdown;
  /** Join-key value batches, one per join-keys column. */
  joinKeys?: RecordBatch[];
  /** Invoked after bind, before init. Receives the bind response. */
  onBind?: BindResultCallback;
}
