// CatalogInterface abstract class with ~35 methods.
// Matching Python's vgi/catalog/catalog_interface.py

import { CatalogReadOnlyError } from "../errors.js";

export type AttachId = Uint8Array;
export type TransactionId = Uint8Array;

// Wire-encoded string union (matches Python's CatalogMacroType enum names).
import type { MacroType } from "../generated/vgi-client.js";
export type { MacroType };

// CatalogAttachResult is the generated snake_case wire shape.
// The `supports_column_statistics` field is a catalog-level opt-in for the
// catalog_table_column_statistics_get RPC — defaults to true when unspecified;
// individual tables still need to set TableInfo.supports_column_statistics
// before DuckDB will actually call it.
import type { CatalogAttachResult } from "../generated/vgi-client.js";
export type { CatalogAttachResult };

// ============================================================================
// Serializable info types
// ============================================================================

// These serialize themselves to Arrow IPC bytes for catalog responses

// SchemaInfo is the generated interface (snake_case wire shape).
// Codecs live in ../generated/vgi-client.ts — no class needed.
import type { SchemaInfo } from "../generated/vgi-client.js";
export type { SchemaInfo };
export { encodeSchemaInfo, decodeSchemaInfo } from "../generated/vgi-client.js";

// TableInfo is the generated interface (snake_case wire shape).
import type { TableInfo } from "../generated/vgi-client.js";
export type { TableInfo };
export { encodeTableInfo, decodeTableInfo } from "../generated/vgi-client.js";

// ViewInfo is the generated interface (snake_case wire shape).
import type { ViewInfo } from "../generated/vgi-client.js";
export type { ViewInfo };
export { encodeViewInfo, decodeViewInfo } from "../generated/vgi-client.js";

// FunctionInfo is the generated interface (snake_case wire shape).
// FunctionInfoOptions was the camelCase constructor bag for the old class —
// callers now use object literals matching the wire shape directly.
import type { FunctionInfo } from "../generated/vgi-client.js";
export type { FunctionInfo };
/** @deprecated Use the FunctionInfo interface (snake_case) directly. */
export type FunctionInfoOptions = FunctionInfo;
export { encodeFunctionInfo, decodeFunctionInfo } from "../generated/vgi-client.js";

// MacroInfo is the generated interface (snake_case wire shape).
import type { MacroInfo } from "../generated/vgi-client.js";
export type { MacroInfo };
export { encodeMacroInfo, decodeMacroInfo } from "../generated/vgi-client.js";

// ============================================================================
// CatalogInterface abstract class
// ============================================================================

export abstract class CatalogInterface {
  // Required overrides
  abstract catalogs(): string[];
  abstract attach(
    name: string,
    options?: any
  ): CatalogAttachResult;
  abstract detach(attachId: AttachId): void;
  abstract version(
    attachId: AttachId,
    transactionId?: TransactionId
  ): number;
  abstract schemas(
    attachId: AttachId,
    transactionId?: TransactionId
  ): SchemaInfo[];

  // Default implementations (throw CatalogReadOnlyError)
  create(name: string, onConflict: string, options?: any): void {
    throw new CatalogReadOnlyError("create");
  }
  drop(name: string): void {
    throw new CatalogReadOnlyError("drop");
  }
  schemaGet(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): SchemaInfo | null {
    const all = this.schemas(attachId, transactionId);
    return all.find((s) => s.name === name) ?? null;
  }
  schemaCreate(
    attachId: AttachId,
    name: string,
    comment?: string | null,
    tags?: any,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("schema_create");
  }
  schemaDrop(
    attachId: AttachId,
    name: string,
    ignoreNotFound?: boolean,
    cascade?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("schema_drop");
  }
  schemaContentsTables(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): TableInfo[] {
    return [];
  }
  schemaContentsViews(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): ViewInfo[] {
    return [];
  }
  schemaContentsFunctions(
    attachId: AttachId,
    name: string,
    type: string,
    transactionId?: TransactionId
  ): FunctionInfo[] {
    return [];
  }
  tableGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    atUnit?: string,
    atValue?: string,
    transactionId?: TransactionId
  ): TableInfo | null {
    return null;
  }
  tableCreate(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columns: Uint8Array,
    onConflict: string,
    notNullConstraints?: number[],
    uniqueConstraints?: number[][],
    checkConstraints?: string[],
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_create");
  }
  tableDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_drop");
  }
  tableScanFunctionGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    atUnit?: string,
    atValue?: string,
    transactionId?: TransactionId
  ): any {
    throw new CatalogReadOnlyError("table_scan_function_get");
  }
  /**
   * Return serialized column statistics for a table, or null if none are
   * available. The result is the IPC bytes of a ColumnStatistics RecordBatch
   * (see src/util/statistics.ts for the schema) wrapped with an optional
   * cache TTL. Callers typically override this to pull stats from a
   * descriptor. Returning null signals "no stats" so DuckDB falls back to
   * the function-level `table_function_statistics` path.
   */
  tableColumnStatisticsGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    transactionId?: TransactionId,
  ): { bytes: Uint8Array; cacheMaxAgeSeconds: number | null } | null {
    return null;
  }
  tableCommentSet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_comment_set");
  }
  tableRename(
    attachId: AttachId,
    schemaName: string,
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_rename");
  }
  tableColumnAdd(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    columnType: string,
    defaultValue?: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_column_add");
  }
  tableColumnDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_column_drop");
  }
  tableColumnRename(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_column_rename");
  }
  tableColumnDefaultSet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    defaultValue: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_column_default_set");
  }
  tableColumnDefaultDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_column_default_drop");
  }
  tableColumnTypeChange(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    newType: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_column_type_change");
  }
  tableNotNullSet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_not_null_set");
  }
  tableNotNullDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_not_null_drop");
  }
  viewGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    transactionId?: TransactionId
  ): ViewInfo | null {
    return null;
  }
  viewCreate(
    attachId: AttachId,
    schemaName: string,
    name: string,
    definition: string,
    onConflict: string,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("view_create");
  }
  viewDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("view_drop");
  }
  viewRename(
    attachId: AttachId,
    schemaName: string,
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("view_rename");
  }
  viewCommentSet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("view_comment_set");
  }
  macroGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    transactionId?: TransactionId
  ): MacroInfo | null {
    return null;
  }
  macroCreate(
    attachId: AttachId,
    schemaName: string,
    name: string,
    macroType: MacroType,
    parameters: string[],
    definition: string,
    onConflict: string,
    parameterDefaultValues?: Uint8Array | null,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("macro_create");
  }
  macroDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("macro_drop");
  }
  schemaContentsMacros(
    attachId: AttachId,
    name: string,
    type: string,
    transactionId?: TransactionId
  ): MacroInfo[] {
    return [];
  }
  transactionBegin(attachId: AttachId): Uint8Array | null {
    return null;
  }
  transactionCommit(
    attachId: AttachId,
    transactionId: TransactionId
  ): void {}
  transactionRollback(
    attachId: AttachId,
    transactionId: TransactionId
  ): void {}
}

