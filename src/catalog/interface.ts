// CatalogInterface abstract class with ~35 methods.
// Matching Python's vgi/catalog/catalog_interface.py

import { CatalogReadOnlyError } from "../errors.js";

export type AttachOpaqueData = Uint8Array;
export type TransactionOpaqueData = Uint8Array;

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

// CatalogInfo — advertised by workers per catalog (name + versions).
import type { CatalogInfo } from "../generated/vgi-client.js";
export type { CatalogInfo };
export { encodeCatalogInfo, decodeCatalogInfo } from "../generated/vgi-client.js";

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

// IndexInfo wire shape — declarative indexes a worker advertises and DuckDB
// surfaces under duckdb_indexes(). Constraint type maps to is_unique /
// is_primary on the DuckDB side.
import type { IndexInfo, IndexConstraintType } from "../generated/vgi-client.js";
export type { IndexInfo, IndexConstraintType };
export { encodeIndexInfo, decodeIndexInfo } from "../generated/vgi-client.js";

// ============================================================================
// ScanFunctionResult — typed result for catalog_table_scan_function_get.
// Mirrors vgi-python's ScanFunctionResult (catalog_interface.py:380).
// ============================================================================

import { deserializeBatch, serializeBatch, batchFromColumns } from "../util/arrow/index.js";
import { schema as schema_, field as field_, type VgiDataType, type VgiField } from "../arrow/index.js";
import { toUint8Array } from "../util/bytes.js";
import { ScanBranchSchema } from "../generated/vgi-protocol-schemas.js";

/**
 * Result from `tableScanFunctionGet` — tells the VGI DuckDB extension which
 * DuckDB function to call to obtain the data for a table.
 */
export interface ScanFunctionResult {
  /** The DuckDB function to call (e.g. "read_parquet"). */
  functionName: string;
  /** Positional arguments, decoded from the inner Arrow batch. */
  positionalArguments: unknown[];
  /** Named arguments, decoded from the inner Arrow batch. */
  namedArguments: Record<string, unknown>;
  /** DuckDB extensions to load before calling the function. */
  requiredExtensions: string[];
}

/**
 * Decode a ScanFunctionResult from a wire-shape inner result dict.
 *
 * The wire shape (per ScanFunctionResultSchema) is
 * `{ function_name: utf8, arguments: binary, required_extensions: list<utf8> }`,
 * where `arguments` is a serialized single-row batch with one column per
 * argument: `arg_<index>` for positional args, the bare name for named args.
 */
export function decodeScanFunctionResult(inner: Record<string, unknown>): ScanFunctionResult {
  const argsBytes = inner.arguments;
  const positionalArguments: unknown[] = [];
  const namedArguments: Record<string, unknown> = {};

  if (argsBytes != null) {
    const batch = deserializeBatch(toUint8Array(argsBytes));
    for (const field of batch.schema.fields) {
      const col = batch.getChild(field.name);
      const value = col ? col.get(0) : null;
      if (field.name.startsWith("arg_")) {
        const idx = parseInt(field.name.slice(4), 10);
        while (positionalArguments.length <= idx) positionalArguments.push(null);
        positionalArguments[idx] = value;
      } else {
        namedArguments[field.name] = value;
      }
    }
  }

  const exts = inner.required_extensions;
  const requiredExtensions: string[] = exts == null
    ? []
    : (Array.isArray(exts) ? exts : [...(exts as Iterable<unknown>)])
        .filter((v) => v != null)
        .map(String);

  return {
    functionName: String(inner.function_name ?? ""),
    positionalArguments,
    namedArguments,
    requiredExtensions,
  };
}

/**
 * Synthesise a one-branch `ScanBranchesResult` wire dict from a legacy
 * `tableScanFunctionGet` wire result. Mirrors vgi-python's default
 * `table_scan_branches_get` (catalog_interface.py): a single-source worker is
 * automatically compatible with the branches-aware C++ side.
 *
 * Each branch is its own IPC stream carried in a `list<binary>` column. The
 * legacy result's `arguments` field is already the nested-IPC form
 * `ScanBranch` expects, so it passes through untouched.
 */
export function singleBranchResult(
  legacy: { function_name: string; arguments: unknown; required_extensions?: string[] },
): { branches: Uint8Array[]; required_extensions: string[] } {
  const branchBatch = batchFromColumns(
    {
      function_name: [legacy.function_name],
      arguments: [toUint8Array(legacy.arguments)],
      branch_filter: [null],
      writable: [false],
    },
    ScanBranchSchema as any,
  );
  return {
    branches: [serializeBatch(branchBatch)],
    required_extensions: legacy.required_extensions ?? [],
  };
}

/**
 * One physical source backing a multi-branch scan, in the shape the
 * `buildScanBranchesResult` helper consumes. Mirrors vgi-python's
 * `ScanBranch` (catalog_interface.py).
 *
 * Each scalar argument carries an explicit Arrow `type` so the nested
 * arguments batch is built with the wire type the C++ binder expects
 * (e.g. utf8 for file paths, int64 for counts).
 */
export interface ScanBranchInput {
  /** DuckDB function to call for this branch (e.g. "sequence", "read_parquet"). */
  functionName: string;
  /** Positional scalar arguments, each with its Arrow type. */
  positionalArguments?: { value: unknown; type: VgiDataType }[];
  /** Named scalar arguments, each with its Arrow type. */
  namedArguments?: Record<string, { value: unknown; type: VgiDataType }>;
  /** Optional SQL filter text AND'd into every scan of this branch. */
  branchFilter?: string | null;
  /** Declares this branch as the INSERT target (at most one per table). */
  writable?: boolean;
}

/**
 * Serialize a single branch's arguments to the nested-IPC form `ScanBranch`
 * expects: a 1-row batch with one column per argument — `arg_<index>` for
 * positional args, the bare name for named args. Mirrors Python's
 * `ScanBranch.to_row_dict` argument handling.
 */
function serializeBranchArguments(branch: ScanBranchInput): Uint8Array {
  const fields: VgiField[] = [];
  const values: Record<string, unknown[]> = {};

  const positional = branch.positionalArguments ?? [];
  positional.forEach((arg, index) => {
    const name = `arg_${index}`;
    fields.push(field_(name, arg.type, true));
    values[name] = [arg.value];
  });

  for (const [name, arg] of Object.entries(branch.namedArguments ?? {})) {
    fields.push(field_(name, arg.type, true));
    values[name] = [arg.value];
  }

  const batchSchema = schema_(fields);
  // batchFromColumns handles the zero-column case by producing a 1-row
  // empty batch — same as Python's RecordBatch.from_pylist([{}]).
  return serializeBatch(batchFromColumns(values, batchSchema));
}

/**
 * Build a multi-branch `ScanBranchesResult` wire dict from explicit branch
 * definitions. Each branch becomes its own 1-row `ScanBranchSchema` IPC
 * stream carried in the `branches` list<binary> column. Mirrors vgi-python's
 * `ScanBranchesResult` construction in the test fixture's
 * `table_scan_branches_get`.
 *
 * An empty `branches` list is serialized verbatim — the C++ side loud-fails
 * on it (that asymmetry is exercised by multi_branch_empty_branches.test).
 */
export function buildScanBranchesResult(
  branches: ScanBranchInput[],
  requiredExtensions: string[] = [],
): { branches: Uint8Array[]; required_extensions: string[] } {
  const serializedBranches = branches.map((branch) => {
    const branchBatch = batchFromColumns(
      {
        function_name: [branch.functionName],
        arguments: [serializeBranchArguments(branch)],
        branch_filter: [branch.branchFilter ?? null],
        writable: [branch.writable ?? false],
      },
      ScanBranchSchema as any,
    );
    return serializeBatch(branchBatch);
  });
  return {
    branches: serializedBranches,
    required_extensions: requiredExtensions,
  };
}

// ============================================================================
// CatalogInterface abstract class
// ============================================================================

/**
 * Catalog method return type — sync `T` or `Promise<T>`. The framework's
 * dispatch handlers `await` every catalog call, so async overrides work
 * out of the box. Default impls return sync values.
 */
type Awaitable<T> = T | Promise<T>;

function isPromise<T>(v: Awaitable<T>): v is Promise<T> {
  return v != null && typeof (v as any).then === "function";
}

export abstract class CatalogInterface {
  // Required overrides
  abstract catalogs(): string[];
  /**
   * Attach this catalog. The framework already validates ATTACH options
   * (name + type) against the specs the worker advertises in
   * `catalogsInfo().attach_option_specs`, so `options` arrives as a
   * validated, decoded `{name: value}` dict with typed column values.
   * Use `{}` if no options were supplied.
   */
  abstract attach(
    name: string,
    options?: Record<string, unknown>,
    dataVersionSpec?: string | null,
    implementationVersion?: string | null,
  ): Awaitable<CatalogAttachResult>;
  /**
   * List the catalogs this worker exposes with their version metadata. The
   * `implementation_version` / `data_version_spec` come from the CatalogInfo
   * returned by the read-only catalogs() signature below — a versioned worker
   * overrides `catalogs()` / `catalogsInfo()` to advertise real values; the
   * default derives them from the registered descriptor(s).
   */
  catalogsInfo?(): Awaitable<import("../generated/vgi-client.js").CatalogInfo[]>;
  abstract detach(attachOpaqueData: AttachOpaqueData): Awaitable<void>;
  abstract version(
    attachOpaqueData: AttachOpaqueData,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<number>;
  abstract schemas(
    attachOpaqueData: AttachOpaqueData,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<SchemaInfo[]>;

  // Default implementations (throw CatalogReadOnlyError)
  create(name: string, onConflict: string, options?: any): Awaitable<void> {
    throw new CatalogReadOnlyError("create");
  }
  drop(name: string): Awaitable<void> {
    throw new CatalogReadOnlyError("drop");
  }
  schemaGet(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<SchemaInfo | null> {
    const all = this.schemas(attachOpaqueData, transactionOpaqueData);
    if (isPromise(all)) {
      return all.then((arr) => arr.find((s) => s.name === name) ?? null);
    }
    return all.find((s) => s.name === name) ?? null;
  }
  schemaCreate(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    comment?: string | null,
    tags?: any,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("schema_create");
  }
  schemaDrop(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    ignoreNotFound?: boolean,
    cascade?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("schema_drop");
  }
  schemaContentsTables(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<TableInfo[]> {
    return [];
  }
  schemaContentsViews(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<ViewInfo[]> {
    return [];
  }
  schemaContentsFunctions(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    type: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<FunctionInfo[]> {
    return [];
  }
  tableGet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    atUnit?: string,
    atValue?: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<TableInfo | null> {
    return null;
  }
  tableCreate(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columns: Uint8Array,
    onConflict: string,
    notNullConstraints?: number[],
    uniqueConstraints?: number[][],
    checkConstraints?: string[],
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("table_create");
  }
  tableDrop(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("table_drop");
  }
  tableScanFunctionGet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    atUnit?: string,
    atValue?: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<any> {
    throw new CatalogReadOnlyError("table_scan_function_get");
  }
  /**
   * Return the scan branches for a (possibly multi-source) table. Default
   * delegates to `tableScanFunctionGet` and wraps the single result as a
   * one-branch list, so every single-source catalog is compatible with the
   * branches-aware C++ extension. Override for genuine multi-source tables.
   */
  tableScanBranchesGet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    atUnit?: string,
    atValue?: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<any> {
    const legacy = this.tableScanFunctionGet(
      attachOpaqueData, schemaName, name, atUnit, atValue, transactionOpaqueData,
    );
    return isPromise(legacy)
      ? legacy.then((r) => singleBranchResult(r))
      : singleBranchResult(legacy);
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
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Awaitable<{ bytes: Uint8Array; cacheMaxAgeSeconds: number | null } | null> {
    return null;
  }
  tableCommentSet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("table_comment_set");
  }
  tableRename(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("table_rename");
  }
  tableColumnAdd(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnName: string,
    columnType: string,
    defaultValue?: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("table_column_add");
  }
  tableColumnDrop(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("table_column_drop");
  }
  tableColumnRename(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnName: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("table_column_rename");
  }
  tableColumnDefaultSet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnName: string,
    defaultValue: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("table_column_default_set");
  }
  tableColumnDefaultDrop(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("table_column_default_drop");
  }
  tableColumnTypeChange(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnName: string,
    newType: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("table_column_type_change");
  }
  tableNotNullSet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("table_not_null_set");
  }
  tableNotNullDrop(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("table_not_null_drop");
  }
  viewGet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<ViewInfo | null> {
    return null;
  }
  viewCreate(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    definition: string,
    onConflict: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("view_create");
  }
  viewDrop(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("view_drop");
  }
  viewRename(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("view_rename");
  }
  viewCommentSet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("view_comment_set");
  }
  macroGet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<MacroInfo | null> {
    return null;
  }
  macroCreate(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    macroType: MacroType,
    parameters: string[],
    definition: string,
    onConflict: string,
    parameterDefaultValues?: Uint8Array | null,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("macro_create");
  }
  macroDrop(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<void> {
    throw new CatalogReadOnlyError("macro_drop");
  }
  schemaContentsMacros(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    type: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<MacroInfo[]> {
    return [];
  }
  schemaContentsIndexes(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<IndexInfo[]> {
    return [];
  }
  indexGet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): Awaitable<IndexInfo | null> {
    return null;
  }
  transactionBegin(attachOpaqueData: AttachOpaqueData): Awaitable<Uint8Array | null> {
    return null;
  }
  transactionCommit(
    attachOpaqueData: AttachOpaqueData,
    transactionOpaqueData: TransactionOpaqueData
  ): Awaitable<void> {}
  transactionRollback(
    attachOpaqueData: AttachOpaqueData,
    transactionOpaqueData: TransactionOpaqueData
  ): Awaitable<void> {}
}

