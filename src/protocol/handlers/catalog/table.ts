// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import { encodeValidatedTableInfo, validateColumnMetadataIpc } from "../../../catalog/column-metadata.js";
import { validateColumnExpressionsIpc } from "../../../catalog/column-expressions.js";
import { MethodNotImplementedError } from "@query-farm/vgi-rpc";
// Catalog table handlers: table_get/create/drop, scan_function_get,
// column_statistics_get, comment_set, rename, plus all column_* mutations.

import type { VgiService } from "../../../generated/vgi-service.js";
import {
  CatalogTableConstraintsApplyResultSchema,
  CatalogTableGetResultSchema,
  ScanBranchesResultSchema,
  ScanFunctionResultSchema,
} from "../../../generated/vgi-protocol-schemas.js";
import { toUint8Array } from "../../../util/bytes.js";
import { requireCreateColumns } from "../../../catalog/unconstrained-columns.js";
import { CONSTRAINT_CREATE, CONSTRAINT_ENFORCE, CONSTRAINT_MUTATE, readConstraintRecord } from "../../../catalog/constraints.js";
import { decodeCatalogObjectRef } from "../../../generated/vgi-protocol-types.js";
import {
  wrapResult,
} from "../shared.js";
import {
  type GetCatalog,
  catalogHandler,
} from "./shared.js";

function requireFlatColumnPath(value: unknown, operation: string): string[] {
  // The raw RPC codec returns Arrow list scalars as iterable vectors.
  const path = value != null && typeof value === "object" && Symbol.iterator in value
    ? Array.from(value as Iterable<unknown>) : value;
  if (!Array.isArray(path) || path.length !== 1 ||
      typeof path[0] !== "string" || !path[0] || path[0].includes("\0")) {
    throw new MethodNotImplementedError(`${operation}: nested column paths require a negotiated capability`);
  }
  return path as string[];
}

export function catalogTableHandlers(getCatalog: GetCatalog, signingKey?: Uint8Array): Partial<VgiService> {
  // catalog_table_get
  const catalogTableGet = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const info = await cat.tableGet(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.at_unit,
      params.at_value,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return wrapResult({
      items: info ? [encodeValidatedTableInfo(info)] : [],
    }, CatalogTableGetResultSchema);
  });

  // catalog_table_create
  const catalogTableCreate = catalogHandler(signingKey, async (params) => {
    if (params.constraint_definitions == null) throw new Error("constraint_definitions is required");
    const definitions = Array.from(params.constraint_definitions as Iterable<unknown>, (d) => toUint8Array(d as Uint8Array));
    if (params.comment != null || params.tags != null || params.options != null || params.layout != null) {
      throw new MethodNotImplementedError("catalog_table_create: table metadata/options/layout profiles are not supported");
    }
    const attach = toUint8Array(params.attach_opaque_data);
    const cat = getCatalog();
    if (definitions.length > 0) {
      // DuckDB sends definitions only under a negotiated create profile; refuse
      // here too when the catalog states it does not implement one.
      const caps = await cat.catalogCapabilities(attach);
      if (caps != null && !(caps.ddl.includes(CONSTRAINT_CREATE) && caps.write.includes(CONSTRAINT_ENFORCE))) {
        throw new MethodNotImplementedError(
          "catalog_table_create: this catalog does not enforce constraints (NOT NULL, PRIMARY KEY, UNIQUE, CHECK, " +
            "FOREIGN KEY); it advertises neither vgi.constraint.create.v1 nor vgi.constraint.enforce.v1",
        );
      }
    }
    validateColumnMetadataIpc(toUint8Array(params.columns));
    validateColumnExpressionsIpc(toUint8Array(params.columns), params.schema_path);
    requireCreateColumns(toUint8Array(params.columns), definitions);
    await cat.tableCreate(
      attach,
      params.schema_path,
      params.name,
      toUint8Array(params.columns),
      params.on_conflict,
      definitions,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });

  // catalog_table_constraints_apply (ALTER TABLE ... SET/DROP NOT NULL and
  // every other canonical constraint change).
  const catalogTableConstraintsApply = catalogHandler(signingKey, async (params) => {
    const attach = toUint8Array(params.attach_opaque_data);
    const cat = getCatalog();
    const caps = await cat.catalogCapabilities(attach);
    if (caps != null && !caps.ddl.includes(CONSTRAINT_MUTATE)) {
      throw new MethodNotImplementedError("catalog_table_constraints_apply: this catalog does not advertise vgi.constraint.mutate.v1");
    }
    const table = toUint8Array(params.table);
    const expected = toUint8Array(params.expected_revision);
    const changes = Array.from(params.changes as Iterable<unknown>, (c) => toUint8Array(c as Uint8Array));
    if (expected.length === 0 || changes.length === 0) throw new Error("catalog_table_constraints_apply: empty revision or change list");
    const snapshot = await cat.tableConstraintsApply({
      attach_opaque_data: attach,
      table,
      expected_revision: expected,
      changes,
      transaction_opaque_data: params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : null,
    });
    const want = decodeCatalogObjectRef(table);
    const got = decodeCatalogObjectRef(toUint8Array(snapshot.table));
    if (got.name !== want.name || JSON.stringify([...got.schema_path]) !== JSON.stringify([...want.schema_path]))
      throw new Error("catalog_table_constraints_apply: the catalog returned a snapshot of a different table");
    const revision = toUint8Array(snapshot.definition_revision);
    if (revision.length === 0 || (revision.length === expected.length && revision.every((b, i) => b === expected[i])))
      throw new Error(`catalog_table_constraints_apply: table '${want.name}' must return a new, nonempty definition_revision`);
    for (const record of snapshot.constraints) {
      const view = readConstraintRecord(toUint8Array(record));
      if (view.table !== want.name || JSON.stringify(view.schemaPath) !== JSON.stringify([...want.schema_path]))
        throw new Error(`catalog_table_constraints_apply: constraint '${view.id}' belongs to another table`);
    }
    return wrapResult({ table, definition_revision: revision, constraints: snapshot.constraints.map((c) => toUint8Array(c)) },
      CatalogTableConstraintsApplyResultSchema);
  });

  // catalog_table_drop
  const catalogTableDrop = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    await cat.tableDrop(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.ignore_not_found,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });

  // catalog_table_column_statistics_get
  const catalogTableColumnStatisticsGet = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const stats = await cat.tableColumnStatisticsGet(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined,
    );
    return { result: stats?.bytes ?? null };
  });

  // catalog_table_scan_function_get
  const catalogTableScanFunctionGet = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const scanResult = await cat.tableScanFunctionGet(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.at_unit,
      params.at_value,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return wrapResult(scanResult, ScanFunctionResultSchema);
  });

  // catalog_table_scan_branches_get — branches-aware variant of
  // scan_function_get. The branches-aware C++ extension calls this for every
  // table scan; single-source catalogs synthesise a one-branch result.
  const catalogTableScanBranchesGet = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    const branchesResult = await cat.tableScanBranchesGet(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.at_unit,
      params.at_value,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return wrapResult(branchesResult, ScanBranchesResultSchema);
  });

  // catalog_table_comment_set
  const catalogTableCommentSet = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    await cat.tableCommentSet(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.comment,
      params.ignore_not_found,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });

  // catalog_table_rename
  const catalogTableRename = catalogHandler(signingKey, async (params) => {
    const cat = getCatalog();
    await cat.tableRename(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.new_name,
      params.ignore_not_found,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });

  // catalog_table_column_add
  const catalogTableColumnAdd = catalogHandler(signingKey, async (params) => {
    params.column_path = requireFlatColumnPath(params.column_path, "catalog_table_column_add");
    validateColumnMetadataIpc(toUint8Array(params.column_definition));
    validateColumnExpressionsIpc(toUint8Array(params.column_definition), params.schema_path);
    const cat = getCatalog();
    await cat.tableColumnAdd(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.column_path,
      toUint8Array(params.column_definition),
      params.ignore_not_found,
      params.if_column_not_exists,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });
  // catalog_table_column_drop
  const catalogTableColumnDrop = catalogHandler(signingKey, async (params) => {
    params.column_path = requireFlatColumnPath(params.column_path, "catalog_table_column_drop");
    const cat = getCatalog();
    await cat.tableColumnDrop(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.column_path,
      params.ignore_not_found,
      params.if_column_exists,
      params.cascade,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });
  // catalog_table_column_rename
  const catalogTableColumnRename = catalogHandler(signingKey, async (params) => {
    params.column_path = requireFlatColumnPath(params.column_path, "catalog_table_column_rename");
    const cat = getCatalog();
    await cat.tableColumnRename(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.column_path,
      params.new_column_name,
      params.ignore_not_found,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });
  // catalog_table_column_comment_set
  const catalogTableColumnCommentSet = catalogHandler(signingKey, async (params) => {
    params.column_path = requireFlatColumnPath(params.column_path, "catalog_table_column_comment_set");
    const cat = getCatalog();
    await cat.tableColumnCommentSet(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.column_path,
      params.comment,
      params.ignore_not_found,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });
  // catalog_table_column_default_set
  const catalogTableColumnDefaultSet = catalogHandler(signingKey, async (params) => {
    params.column_path = requireFlatColumnPath(params.column_path, "catalog_table_column_default_set");
    const cat = getCatalog();
    await cat.tableColumnDefaultSet(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.column_path,
      toUint8Array(params.expression),
      params.ignore_not_found,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });
  // catalog_table_column_default_drop
  const catalogTableColumnDefaultDrop = catalogHandler(signingKey, async (params) => {
    params.column_path = requireFlatColumnPath(params.column_path, "catalog_table_column_default_drop");
    const cat = getCatalog();
    await cat.tableColumnDefaultDrop(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.column_path,
      params.ignore_not_found,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });
  // catalog_table_column_type_change
  const catalogTableColumnTypeChange = catalogHandler(signingKey, async (params) => {
    params.column_path = requireFlatColumnPath(params.column_path, "catalog_table_column_type_change");
    const cat = getCatalog();
    await cat.tableColumnTypeChange(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.column_path,
      toUint8Array(params.column_definition),
      params.expression == null ? null : toUint8Array(params.expression),
      params.ignore_not_found,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });
  // catalog_table_not_null_set
  const catalogTableNotNullSet = catalogHandler(signingKey, async (params) => {
    params.column_path = requireFlatColumnPath(params.column_path, "catalog_table_not_null_set");
    const cat = getCatalog();
    await cat.tableNotNullSet(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.column_path,
      params.ignore_not_found,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });
  // catalog_table_not_null_drop
  const catalogTableNotNullDrop = catalogHandler(signingKey, async (params) => {
    params.column_path = requireFlatColumnPath(params.column_path, "catalog_table_not_null_drop");
    const cat = getCatalog();
    await cat.tableNotNullDrop(
      toUint8Array(params.attach_opaque_data),
      params.schema_path,
      params.name,
      params.column_path,
      params.ignore_not_found,
      params.transaction_opaque_data ? toUint8Array(params.transaction_opaque_data) : undefined
    );
    return {};
  });
  return {
    catalogTableConstraintsApply,
    catalogTableGet,
    catalogTableCreate,
    catalogTableDrop,
    catalogTableColumnStatisticsGet,
    catalogTableScanFunctionGet,
    catalogTableScanBranchesGet,
    catalogTableCommentSet,
    catalogTableRename,
    catalogTableColumnAdd,
    catalogTableColumnDrop,
    catalogTableColumnRename,
    catalogTableColumnCommentSet,
    catalogTableColumnDefaultSet,
    catalogTableColumnDefaultDrop,
    catalogTableColumnTypeChange,
    catalogTableNotNullSet,
    catalogTableNotNullDrop,
  };
}
