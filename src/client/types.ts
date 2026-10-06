// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Client option types for VgiClient.

import type { VgiBatch } from "../arrow/index.js";
import type { Arguments } from "../arguments/arguments.js";
import type { AttachOptionValue } from "../catalog/attach-options.js";
import type { BindResponse } from "../protocol/types.js";
import type { SchemaContentsInfo, TransactionOpaqueData } from "../catalog/interface.js";
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
  /** Engine capabilities serialized into the protocol's named inner record. */
  clientCapabilities?: import("../generated/vgi-client.js").ClientCapabilities | null;
}

/** Conflict resolution strategy for create operations. */
export type OnCreateConflict = "error" | "ignore" | "replace";

/** DuckDB catalog function type filter (sent as uppercase wire values). */
export type CatalogFunctionType = "SCALAR_FUNCTION" | "AGGREGATE_FUNCTION" | "TABLE_FUNCTION";

/** Macro type filter for schema contents listing (uppercase wire values). */
export type CatalogMacroType = "SCALAR_MACRO" | "TABLE_MACRO";

/** Options for constructing a VgiClient. */
export interface VgiClientOptions {
  /** Pre-existing attach ID to bind this client to a specific catalog. */
  attachOpaqueData?: Uint8Array;
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
  pushdownFilters?: VgiBatch;
  /** DuckDB settings to pass to the function. */
  settings?: VgiBatch;
  /** Transaction ID for transactional catalogs. */
  transactionOpaqueData?: Uint8Array;
  /** Attach ID to bind this call to a specific catalog attach. Overrides the client-level attachOpaqueData. */
  attachOpaqueData?: Uint8Array;
  /** ORDER BY pushdown hint from DuckDB's RowGroupPruner. */
  orderBy?: OrderByPushdown;
  /** TABLESAMPLE pushdown hint from DuckDB's SamplingPushdown optimizer. */
  tablesample?: TablesamplePushdown;
  /** Join-key value batches, one per join-keys column. */
  joinKeys?: VgiBatch[];
  /** Invoked after bind, before init. Receives the bind response. */
  onBind?: BindResultCallback;
}

/** Options for calling a scalar function. */
export interface ScalarFunctionOptions {
  /** Name of the function to call. */
  functionName: string;
  /** Input batches to process. */
  input: Iterable<VgiBatch> | AsyncIterable<VgiBatch>;
  /** Positional and named arguments. */
  arguments?: Arguments;
  /** DuckDB settings to pass to the function. */
  settings?: VgiBatch;
  /** DuckDB secrets to pass to the function. */
  secrets?: VgiBatch;
  /** Transaction ID for transactional catalogs. */
  transactionOpaqueData?: Uint8Array;
  /** Attach ID to bind this call to a specific catalog attach. Overrides the client-level attachOpaqueData. */
  attachOpaqueData?: Uint8Array;
  /** Invoked after bind, before init. Receives the bind response. */
  onBind?: BindResultCallback;
}

/** Options for calling a table-in-out function. */
export interface TableInOutFunctionOptions {
  /** Name of the function to call. */
  functionName: string;
  /** Input batches to process. */
  input: Iterable<VgiBatch> | AsyncIterable<VgiBatch>;
  /** Positional and named arguments. */
  arguments?: Arguments;
  /** Column indices to project (filter pushdown). */
  projectionIds?: number[];
  /** Filter pushdown batch. */
  pushdownFilters?: VgiBatch;
  /** DuckDB settings to pass to the function. */
  settings?: VgiBatch;
  /** Transaction ID for transactional catalogs. */
  transactionOpaqueData?: Uint8Array;
  /** Attach ID to bind this call to a specific catalog attach. Overrides the client-level attachOpaqueData. */
  attachOpaqueData?: Uint8Array;
  /** ORDER BY pushdown hint from DuckDB's RowGroupPruner. */
  orderBy?: OrderByPushdown;
  /** TABLESAMPLE pushdown hint from DuckDB's SamplingPushdown optimizer. */
  tablesample?: TablesamplePushdown;
  /** Join-key value batches, one per join-keys column. */
  joinKeys?: VgiBatch[];
  /** Invoked after bind, before init. Receives the bind response. */
  onBind?: BindResultCallback;
  /**
   * Whether the function declares a FINALIZE stage (`FunctionInfo.has_finalize`).
   * Defaults to `true`, preserving the unconditional FINALIZE-phase `init()` every
   * caller got before this option existed. Pass `false` for a function known to have
   * no finalize — every blended row-transform function (`defineRowTransformFunction`,
   * `FunctionInfo.input_from_args`) — to skip the FINALIZE `init()` entirely: the
   * worker rejects an unexpected FINALIZE for a function that never advertised one.
   * The DuckDB extension avoids it the same way (it registers no final callback).
   * Mirrors vgi-python's `table_in_out_function(has_finalize=...)`.
   */
  hasFinalize?: boolean;
}

/**
 * A whole-catalog snapshot returned by `VgiClient.loadCatalog`: every schema
 * with all of its tables, views, functions, macros and indexes.
 *
 * Hold on to it and pass it back as `LoadCatalogOptions.previous` to
 * revalidate: when it carries an `etag` the client sends that as
 * `if_none_match`, and a `not_modified` answer returns this snapshot's
 * contents unchanged.
 */
export interface CatalogSnapshot {
  /**
   * Version the snapshot was taken at, as reported by `catalog_contents`.
   * `null` when it was assembled from the per-schema RPCs (which carry no
   * version); 0 means the worker does not track versions.
   */
  catalog_version: number | null;
  /** Validator to revalidate with; null = the worker does not revalidate. */
  etag: string | null;
  /**
   * How the contents were obtained: one `catalog_contents` call, or
   * `catalog_schemas` + per-schema `catalog_schema_contents_*` calls.
   */
  source: "catalog_contents" | "per_schema";
  /** True when this load was a `not_modified` revalidation of `previous`. */
  not_modified: boolean;
  /**
   * Set when the catalog advertised `catalog_contents` but the call failed
   * (or returned a malformed answer) and the client fell back to the
   * per-schema RPCs: the failure's message.
   */
  fallback_error: string | null;
  /** One entry per schema, parents before children. */
  schemas: SchemaContentsInfo[];
}

/** Options for `VgiClient.loadCatalog`. */
export interface LoadCatalogOptions {
  /**
   * A snapshot from an earlier load of the same attach. Its `etag` (if any)
   * is sent as `if_none_match`; a `not_modified` answer keeps its contents.
   */
  previous?: CatalogSnapshot | null;
  /**
   * Set false to never call `catalog_contents` (the client-side equivalent of
   * DuckDB's `SET vgi_catalog_contents = false`). Default true: it is used
   * whenever the attach result advertised `supports_catalog_contents`.
   */
  useCatalogContents?: boolean;
  /**
   * Enumerate inside this transaction. `catalog_contents` returns only the
   * committed catalog (it takes no transaction), so a transaction-scoped load
   * always uses the per-schema RPCs.
   */
  transactionOpaqueData?: TransactionOpaqueData;
}
