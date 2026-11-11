// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Writable-table result modes shared by catalogs and write functions.
// Port of vgi-python's `vgi/write_results.py`, plus the `write_options` decoder
// its write functions use.
//
// A writable table advertises, per operation (insert / update / delete), the
// richest result mode its write function can produce. Modes are ordered
// `count < rows < changes`, and a table that supports one mode supports every
// lower one. DuckDB asks for one mode per statement, in the `write_options`
// named argument of the write function's bind:
//
//   count    -- plain INSERT/UPDATE/DELETE: one row, `count BIGINT NOT NULL`
//   rows     -- `... RETURNING`: the written rows, in the table's schema
//   changes  -- old/new pairs: `old STRUCT(<table>)`, `new STRUCT(<table>)`
//
// UPDATE and DELETE identify rows by the table's row-id column (the one field
// carrying `is_row_id` metadata), which the scan function must emit.

import {
  type VgiBatch,
  type VgiField,
  type VgiSchema,
  batchFromColumns,
  deserializeBatch,
  field,
  int64,
  iterRows,
  schema,
  struct,
} from "./arrow/index.js";
import type { BindRequest } from "./protocol/types.js";
import { toUint8Array } from "./util/bytes.js";

export type WriteOperation = "insert" | "update" | "delete";
export type WriteResultMode = "count" | "rows" | "changes";

export const WRITE_OPERATIONS: readonly WriteOperation[] = ["insert", "update", "delete"];
export const WRITE_RESULT_MODES: readonly WriteResultMode[] = ["count", "rows", "changes"];
const MODE_RANK: Record<string, number> = { count: 0, rows: 1, changes: 2 };

/** Field metadata key that marks a table's row-id column. */
export const ROW_ID_METADATA_KEY = "is_row_id";

/**
 * A row-id field for a writable table's columns and its scan's output:
 * `rowIdField()` is DuckDB's conventional `rowid BIGINT`.
 *
 * DuckDB reads the row id as a *virtual* column, which it only requests from a
 * scan that supports projection pushdown. So a table with UPDATE or DELETE
 * needs a scan function that (1) emits this field, (2) is declared with
 * `projectionPushdown: true` and honours `params.outputSchema`, and (3) is
 * listed in its schema's `functions`, which is how DuckDB learns (2). Missing
 * any of these, UPDATE/DELETE fail with "Virtual columns require projection
 * pushdown". Row ids must be unique and stable for the life of a statement.
 */
export function rowIdField(name = "rowid", type = int64()): VgiField {
  return field(name, type, false, new Map([[ROW_ID_METADATA_KEY, ""]]));
}

/** Return a validated copy of an operation -> maximum-mode mapping. */
export function validateWriteResultModes(
  modes: Partial<Record<string, string>>,
): Partial<Record<WriteOperation, WriteResultMode>> {
  const out: Partial<Record<WriteOperation, WriteResultMode>> = {};
  const unknown = Object.keys(modes).filter((op) => !WRITE_OPERATIONS.includes(op as WriteOperation)).sort();
  if (unknown.length) throw new Error(`Unknown write operation(s): ${unknown.join(", ")}`);
  for (const [operation, mode] of Object.entries(modes)) {
    if (mode === undefined) continue;
    if (!(mode in MODE_RANK)) throw new Error(`Unknown write result mode '${mode}' for '${operation}'`);
    out[operation as WriteOperation] = mode as WriteResultMode;
  }
  return out;
}

/** Whether a table whose maximum is `maximum` supports `requested`. */
export function supportsWriteResultMode(maximum: string, requested: string): boolean {
  return maximum in MODE_RANK && requested in MODE_RANK && MODE_RANK[maximum]! >= MODE_RANK[requested]!;
}

/** Validate a wire value and narrow it to a `WriteResultMode`. */
export function coerceWriteResultMode(value: unknown): WriteResultMode {
  if (typeof value !== "string" || !(value in MODE_RANK)) {
    throw new Error(`Unknown write result mode: ${JSON.stringify(value)}`);
  }
  return value as WriteResultMode;
}

/**
 * The table's user columns: `columns` without the row-id field. Write results
 * (`rows`, and both halves of `changes`) use this schema.
 */
export function userColumns(columns: VgiSchema): VgiSchema {
  return schema(columns.fields.filter((f) => !f.metadata?.has(ROW_ID_METADATA_KEY)));
}

/** The exact output schema a write function must bind for `mode`. */
export function writeResultSchema(mode: WriteResultMode, tableSchema: VgiSchema): VgiSchema {
  if (mode === "count") return schema([field("count", int64(), false)]);
  if (mode === "rows") return tableSchema;
  const rowType = struct(tableSchema.fields as VgiField[]);
  return schema([field("old", rowType, true), field("new", rowType, true)]);
}

/** A `count` result batch. */
export function writeCountBatch(count: number | bigint): VgiBatch {
  return batchFromColumns({ count: [BigInt(count)] }, writeResultSchema("count", schema([])));
}

/** A `rows` result batch: the written rows as plain objects keyed by column. */
export function writeRowsBatch(tableSchema: VgiSchema, rows: readonly Record<string, unknown>[]): VgiBatch {
  const columns: Record<string, unknown[]> = {};
  for (const f of tableSchema.fields) columns[f.name] = rows.map((r) => r[f.name] ?? null);
  return batchFromColumns(columns, tableSchema);
}

/**
 * A `changes` result batch, one OLD/NEW pair per written row: `old` is null
 * for an insert, `new` is null for a delete.
 */
export function writeChangesBatch(
  tableSchema: VgiSchema,
  oldRows: readonly (Record<string, unknown> | null)[],
  newRows: readonly (Record<string, unknown> | null)[],
): VgiBatch {
  if (oldRows.length !== newRows.length) throw new Error("oldRows and newRows must have equal length");
  const pick = (row: Record<string, unknown> | null) =>
    row == null ? null : Object.fromEntries(tableSchema.fields.map((f) => [f.name, row[f.name] ?? null]));
  return batchFromColumns(
    { old: oldRows.map(pick), new: newRows.map(pick) },
    writeResultSchema("changes", tableSchema),
  );
}

/** The result batch for `mode`, from the written rows' before/after images. */
export function writeResultBatch(
  mode: WriteResultMode,
  tableSchema: VgiSchema,
  oldRows: readonly (Record<string, unknown> | null)[],
  newRows: readonly (Record<string, unknown> | null)[],
): VgiBatch {
  if (mode === "count") return writeCountBatch(newRows.length);
  if (mode === "changes") return writeChangesBatch(tableSchema, oldRows, newRows);
  // RETURNING shows the row as written: the new image, or the old one for a delete.
  return writeRowsBatch(
    tableSchema,
    newRows.map((row, i) => (row ?? oldRows[i]) as Record<string, unknown>),
  );
}

/** What DuckDB asked a write function for, decoded from `write_options`. */
export interface WriteOptions {
  resultMode: WriteResultMode;
  /**
   * @deprecated Always `"throw"`. Protocol 3.3 removed `on_conflict` from the
   * `write_options` batch: DuckDB rewrites every `INSERT ... ON CONFLICT` and
   * `INSERT OR REPLACE` into a MERGE before any catalog hook runs, so a write
   * function never received anything else.
   */
  onConflict: string;
  /** @deprecated Always empty: protocol 3.3 removed `on_conflict_columns` (see `onConflict`). */
  onConflictColumns: string[];
}

/**
 * Decode the `write_options` named argument of a write function's bind (a
 * serialized one-row batch). Missing options mean a plain `count` write.
 * Protocol 3.3: the batch carries only `result_mode`.
 */
export function writeOptions(bindCall: Pick<BindRequest, "arguments">): WriteOptions {
  const out: WriteOptions = { resultMode: "count", onConflict: "throw", onConflictColumns: [] };
  const raw = bindCall.arguments?.get("write_options", null);
  if (raw == null) return out;
  const row = [...iterRows(deserializeBatch(toUint8Array(raw)))][0] as Record<string, unknown> | undefined;
  if (!row) return out;
  if (row.result_mode != null) out.resultMode = coerceWriteResultMode(row.result_mode);
  return out;
}
