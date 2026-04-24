// VgiClient — high-level client for calling VGI worker functions and catalog API.
// Works with any RpcClient (subprocess or HTTP transport).

import { Schema, Field, RecordBatch, Utf8, Binary, List } from "@query-farm/apache-arrow";
import type { RpcClient, StreamSession } from "vgi-rpc";
import {
  serializeBindRequest,
  deserializeBindResponse,
  serializeInitRequest,
  deserializeGlobalInitResponse,
} from "../protocol/serialize.js";
import type {
  BindRequest,
  BindResponse,
  InitRequest,
  GlobalInitResponse,
} from "../protocol/types.js";
import { FunctionType, TableInOutPhase } from "../types.js";
import { Arguments } from "../arguments/arguments.js";
import {
  batchFromRows,
  batchFromColumns,
  iterRows,
} from "../util/arrow.js";
import {
  type SchemaInfo,
  decodeSchemaInfo,
  type TableInfo,
  decodeTableInfo,
  type ViewInfo,
  decodeViewInfo,
  type FunctionInfo,
  decodeFunctionInfo,
  type MacroInfo,
  decodeMacroInfo,
  MacroType,
  type CatalogAttachResult,
  type AttachId,
  type TransactionId,
} from "../catalog/interface.js";
import { wrapRequest, unwrapResult } from "./protocol.js";
import { toUint8Array } from "../util/bytes.js";
import type {
  VgiClientOptions,
  TableFunctionOptions,
  ScalarFunctionOptions,
  TableInOutFunctionOptions,
  OnCreateConflict,
  CatalogFunctionType,
  CatalogMacroType,
} from "./types.js";

/** Error thrown by VgiClient when an RPC call fails or returns unexpected data. */
export class VgiClientError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "VgiClientError";
  }
}

/**
 * High-level client for calling VGI worker functions and catalog API.
 *
 * Works with any RpcClient transport (subprocess or HTTP):
 * ```ts
 * import { subprocessConnect } from "vgi-rpc";
 * import { VgiClient, Arguments } from "vgi";
 *
 * const rpc = subprocessConnect(["./my-worker"]);
 * const client = new VgiClient(rpc);
 *
 * for await (const rows of client.tableFunctionRows({
 *   functionName: "sequence",
 *   arguments: new Arguments([10]),
 * })) {
 *   console.log(rows);
 * }
 * client.close();
 * ```
 */
export class VgiClient {
  private readonly rpc: RpcClient;
  private readonly attachId: Uint8Array | null;

  constructor(rpc: RpcClient, options?: VgiClientOptions) {
    this.rpc = rpc;
    this.attachId = options?.attachId ?? null;
  }

  // ==========================================================================
  // Private helpers
  // ==========================================================================

  private async _doBind(
    functionName: string,
    functionType: FunctionType,
    args: Arguments,
    inputSchema: Schema | null,
    settings: RecordBatch | null,
    secrets: RecordBatch | null,
    transactionId: Uint8Array | null,
  ): Promise<{ request: BindRequest; response: BindResponse }> {
    const request: BindRequest = {
      functionName,
      arguments: args,
      functionType,
      inputSchema,
      settings: settings ?? null,
      secrets: secrets ?? null,
      attachId: this.attachId,
      transactionId: transactionId ?? null,
      resolvedSecretsProvided: false,
    };

    const requestBatch = serializeBindRequest(request);
    const rpcResult = await this.rpc.call("bind", wrapRequest(requestBatch));
    if (!rpcResult) throw new VgiClientError("bind returned null");
    const inner = unwrapResult(rpcResult);
    const response = deserializeBindResponse(inner);
    return { request, response };
  }

  private async _doInit(
    bindRequest: BindRequest,
    bindResponse: BindResponse,
    opts?: {
      projectionIds?: number[] | null;
      pushdownFilters?: RecordBatch | null;
      phase?: TableInOutPhase | null;
      executionId?: Uint8Array | null;
    },
  ): Promise<{ session: StreamSession; initResponse: GlobalInitResponse }> {
    const initRequest: InitRequest = {
      bindCall: bindRequest,
      outputSchema: bindResponse.outputSchema,
      bindOpaqueData: bindResponse.opaqueData,
      projectionIds: opts?.projectionIds ?? null,
      pushdownFilters: opts?.pushdownFilters ?? null,
      joinKeys: [],
      phase: opts?.phase ?? null,
      orderByColumnName: null,
      orderByDirection: null,
      orderByNullOrder: null,
      orderByLimit: null,
      tablesamplePercentage: null,
      tablesampleSeed: null,
      executionId: opts?.executionId ?? null,
      initOpaqueData: null,
    };

    const initBatch = serializeInitRequest(initRequest);
    const session = await this.rpc.stream("init", wrapRequest(initBatch));

    const headerDict = session.header;
    if (!headerDict) throw new VgiClientError("init did not return a header");
    const initResponse = deserializeGlobalInitResponse(headerDict);

    return { session, initResponse };
  }

  // ==========================================================================
  // Table function — RecordBatch API
  // ==========================================================================

  /** Call a table function, yielding output as RecordBatch instances. */
  async *tableFunction(
    opts: TableFunctionOptions,
  ): AsyncGenerator<RecordBatch> {
    const args = opts.arguments ?? new Arguments();

    const { request: bindReq, response: bindResp } = await this._doBind(
      opts.functionName,
      FunctionType.TABLE,
      args,
      null,
      opts.settings ?? null,
      null,
      opts.transactionId ?? null,
    );

    const { session } = await this._doInit(bindReq, bindResp, {
      projectionIds: opts.projectionIds,
      pushdownFilters: opts.pushdownFilters,
    });

    const outputSchema = bindResp.outputSchema;
    try {
      for await (const rows of session) {
        if (rows.length > 0) {
          yield batchFromRows(rows, outputSchema);
        }
      }
    } finally {
      session.close();
    }
  }

  // ==========================================================================
  // Scalar function — RecordBatch API
  // ==========================================================================

  /** Call a scalar function, yielding output as RecordBatch instances. */
  async *scalarFunction(
    opts: ScalarFunctionOptions,
  ): AsyncGenerator<RecordBatch> {
    // Peek first batch to get input schema
    const inputIter = toAsyncIterator(opts.input);
    const first = await inputIter.next();
    if (first.done) return;

    const firstBatch: RecordBatch = first.value;
    const inputSchema = firstBatch.schema;

    const args = opts.arguments ?? new Arguments();

    const { request: bindReq, response: bindResp } = await this._doBind(
      opts.functionName,
      FunctionType.SCALAR,
      args,
      inputSchema,
      opts.settings ?? null,
      opts.secrets ?? null,
      opts.transactionId ?? null,
    );

    const { session } = await this._doInit(bindReq, bindResp);

    const outputSchema = bindResp.outputSchema;
    try {
      // Exchange first batch
      const firstRows = [...iterRows(firstBatch)];
      const outRows = await session.exchange(firstRows);
      if (outRows.length > 0) {
        yield batchFromRows(outRows, outputSchema);
      }

      // Exchange remaining batches
      for await (const batch of { [Symbol.asyncIterator]: () => inputIter }) {
        const rows = [...iterRows(batch)];
        const result = await session.exchange(rows);
        if (result.length > 0) {
          yield batchFromRows(result, outputSchema);
        }
      }
    } finally {
      session.close();
    }
  }

  // ==========================================================================
  // Table-in-out function — RecordBatch API
  // ==========================================================================

  /** Call a table-in-out function, yielding output as RecordBatch instances. */
  async *tableInOutFunction(
    opts: TableInOutFunctionOptions,
  ): AsyncGenerator<RecordBatch> {
    // Peek first batch to get input schema
    const inputIter = toAsyncIterator(opts.input);
    const first = await inputIter.next();
    if (first.done) return;

    const firstBatch: RecordBatch = first.value;
    const inputSchema = firstBatch.schema;

    const args = opts.arguments ?? new Arguments();

    const { request: bindReq, response: bindResp } = await this._doBind(
      opts.functionName,
      FunctionType.TABLE,  // table-in-out uses TABLE function type at the bind level
      args,
      inputSchema,
      opts.settings ?? null,
      null,
      opts.transactionId ?? null,
    );

    // Phase 1: INPUT
    const { session: inputSession, initResponse } = await this._doInit(
      bindReq,
      bindResp,
      {
        projectionIds: opts.projectionIds,
        pushdownFilters: opts.pushdownFilters,
        phase: TableInOutPhase.INPUT,
      },
    );

    const outputSchema = bindResp.outputSchema;
    try {
      // Exchange first batch
      const firstRows = [...iterRows(firstBatch)];
      const outRows = await inputSession.exchange(firstRows);
      if (outRows.length > 0) {
        yield batchFromRows(outRows, outputSchema);
      }

      // Exchange remaining batches
      for await (const batch of { [Symbol.asyncIterator]: () => inputIter }) {
        const rows = [...iterRows(batch)];
        const result = await inputSession.exchange(rows);
        if (result.length > 0) {
          yield batchFromRows(result, outputSchema);
        }
      }
    } finally {
      inputSession.close();
    }

    // Phase 2: FINALIZE
    const { session: finalizeSession } = await this._doInit(
      bindReq,
      bindResp,
      {
        projectionIds: opts.projectionIds,
        pushdownFilters: opts.pushdownFilters,
        phase: TableInOutPhase.FINALIZE,
        executionId: initResponse.executionId,
      },
    );

    try {
      for await (const rows of finalizeSession) {
        if (rows.length > 0) {
          yield batchFromRows(rows, outputSchema);
        }
      }
    } finally {
      finalizeSession.close();
    }
  }

  // ==========================================================================
  // Row convenience APIs
  // ==========================================================================

  /** Call a table function, yielding output as row objects. */
  async *tableFunctionRows(
    opts: TableFunctionOptions,
  ): AsyncGenerator<Record<string, any>[]> {
    for await (const batch of this.tableFunction(opts)) {
      yield [...iterRows(batch)];
    }
  }

  /** Call a scalar function, yielding output as row objects. */
  async *scalarFunctionRows(
    opts: ScalarFunctionOptions,
  ): AsyncGenerator<Record<string, any>[]> {
    for await (const batch of this.scalarFunction(opts)) {
      yield [...iterRows(batch)];
    }
  }

  /** Call a table-in-out function, yielding output as row objects. */
  async *tableInOutFunctionRows(
    opts: TableInOutFunctionOptions,
  ): AsyncGenerator<Record<string, any>[]> {
    for await (const batch of this.tableInOutFunction(opts)) {
      yield [...iterRows(batch)];
    }
  }

  // ==========================================================================
  // Catalog API
  // ==========================================================================

  /** List all available catalog names. */
  async catalogs(): Promise<string[]> {
    const result = await this.rpc.call("catalog_catalogs", {});
    if (!result) return [];
    const inner = unwrapResult(result);
    const items = inner.items;
    if (!items) return [];
    return Array.isArray(items) ? items : [...items];
  }

  /** Attach a catalog by name. Returns connection details including the attachId. */
  async catalogAttach(
    name: string,
    options?: Uint8Array,
  ): Promise<CatalogAttachResult> {
    const schema = new Schema([
      new Field("name", new Utf8(), false),
      new Field("options", new Binary(), true),
    ]);
    const innerBatch = batchFromColumns(
      { name: [name], options: [options ?? null] },
      schema,
    );
    const result = await this.rpc.call("catalog_attach", wrapRequest(innerBatch));
    if (!result) throw new VgiClientError("catalog_attach returned null");
    const inner = unwrapResult(result);
    return {
      attachId: toUint8Array(inner.attach_id),
      supportsTransactions: inner.supports_transactions ?? false,
      supportsTimeTravel: inner.supports_time_travel ?? false,
      catalogVersionFrozen: inner.catalog_version_frozen ?? false,
      catalogVersion: Number(inner.catalog_version ?? 0),
      attachIdRequired: inner.attach_id_required ?? true,
      defaultSchema: inner.default_schema ?? "main",
      settings: inner.settings
        ? (Array.isArray(inner.settings) ? inner.settings : [...inner.settings]).map(toUint8Array)
        : [],
      comment: inner.comment ?? null,
      tags: deserializeTags(inner.tags),
    };
  }

  /** Detach a previously-attached catalog. */
  async catalogDetach(attachId: AttachId): Promise<void> {
    await this.rpc.call("catalog_detach", { attach_id: attachId });
  }

  /** Create a new catalog. */
  async catalogCreate(
    name: string,
    onConflict: OnCreateConflict,
    options?: Uint8Array,
  ): Promise<void> {
    await this.rpc.call("catalog_create", {
      name,
      on_conflict: onConflict,
      options: options ?? null,
    });
  }

  /** Drop a catalog by name. */
  async catalogDrop(name: string): Promise<void> {
    await this.rpc.call("catalog_drop", { name });
  }

  /** Get the current catalog version number. */
  async catalogVersion(
    attachId: AttachId,
    transactionId?: TransactionId,
  ): Promise<number> {
    const result = await this.rpc.call("catalog_version", {
      attach_id: attachId,
      transaction_id: transactionId ?? null,
    });
    if (!result) throw new VgiClientError("catalog_version returned null");
    const inner = unwrapResult(result);
    return Number(inner.version ?? 0);
  }

  /** Begin a new transaction. Returns the transaction ID. */
  async transactionBegin(attachId: AttachId): Promise<Uint8Array> {
    const result = await this.rpc.call("catalog_transaction_begin", {
      attach_id: attachId,
    });
    if (!result) throw new VgiClientError("transaction_begin returned null");
    const inner = unwrapResult(result);
    if (!inner.transaction_id) {
      throw new VgiClientError("transaction_begin returned no transaction_id");
    }
    return toUint8Array(inner.transaction_id);
  }

  /** Commit a transaction. */
  async transactionCommit(
    attachId: AttachId,
    transactionId: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_transaction_commit", {
      attach_id: attachId,
      transaction_id: transactionId,
    });
  }

  /** Rollback a transaction. */
  async transactionRollback(
    attachId: AttachId,
    transactionId: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_transaction_rollback", {
      attach_id: attachId,
      transaction_id: transactionId,
    });
  }

  /** List schemas in an attached catalog. */
  async schemas(
    attachId: AttachId,
    transactionId?: TransactionId,
  ): Promise<SchemaInfo[]> {
    const result = await this.rpc.call("catalog_schemas", {
      attach_id: attachId,
      transaction_id: transactionId ?? null,
    });
    if (!result) return [];
    const inner = unwrapResult(result);
    return deserializeInfoList(inner.items, decodeSchemaInfo);
  }

  /** Get a schema by name, or null if not found. */
  async schemaGet(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId,
  ): Promise<SchemaInfo | null> {
    const result = await this.rpc.call("catalog_schema_get", {
      attach_id: attachId,
      name,
      transaction_id: transactionId ?? null,
    });
    if (!result) return null;
    const inner = unwrapResult(result);
    const items = deserializeInfoList(inner.items, decodeSchemaInfo);
    return items.length > 0 ? items[0] : null;
  }

  /** Create a new schema. */
  async schemaCreate(
    attachId: AttachId,
    name: string,
    comment?: string | null,
    tags?: Uint8Array | null,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_schema_create", {
      attach_id: attachId,
      name,
      comment: comment ?? null,
      tags: tags ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Drop a schema by name. */
  async schemaDrop(
    attachId: AttachId,
    name: string,
    ignoreNotFound?: boolean,
    cascade?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_schema_drop", {
      attach_id: attachId,
      name,
      ignore_not_found: ignoreNotFound ?? null,
      cascade: cascade ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** List tables in a schema. */
  async schemaContentsTables(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId,
  ): Promise<TableInfo[]> {
    const result = await this.rpc.call("catalog_schema_contents_tables", {
      attach_id: attachId,
      name,
      transaction_id: transactionId ?? null,
    });
    if (!result) return [];
    const inner = unwrapResult(result);
    return deserializeInfoList(inner.items, decodeTableInfo);
  }

  /** List views in a schema. */
  async schemaContentsViews(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId,
  ): Promise<ViewInfo[]> {
    const result = await this.rpc.call("catalog_schema_contents_views", {
      attach_id: attachId,
      name,
      transaction_id: transactionId ?? null,
    });
    if (!result) return [];
    const inner = unwrapResult(result);
    return deserializeInfoList(inner.items, decodeViewInfo);
  }

  /** List functions in a schema, filtered by type. */
  async schemaContentsFunctions(
    attachId: AttachId,
    name: string,
    type: CatalogFunctionType,
    transactionId?: TransactionId,
  ): Promise<FunctionInfo[]> {
    const result = await this.rpc.call("catalog_schema_contents_functions", {
      attach_id: attachId,
      name,
      type,
      transaction_id: transactionId ?? null,
    });
    if (!result) return [];
    const inner = unwrapResult(result);
    return deserializeInfoList(inner.items, decodeFunctionInfo);
  }

  /** Get a table by name, or null if not found. */
  async tableGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    transactionId?: TransactionId,
  ): Promise<TableInfo | null> {
    const result = await this.rpc.call("catalog_table_get", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      transaction_id: transactionId ?? null,
    });
    if (!result) return null;
    const inner = unwrapResult(result);
    const items = deserializeInfoList(inner.items, decodeTableInfo);
    return items.length > 0 ? items[0] : null;
  }

  /** Create a new table. */
  async tableCreate(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columns: Uint8Array,
    onConflict: OnCreateConflict,
    notNullConstraints?: number[],
    uniqueConstraints?: number[][],
    checkConstraints?: string[],
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_table_create", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      columns,
      on_conflict: onConflict,
      not_null_constraints: notNullConstraints ?? null,
      unique_constraints: uniqueConstraints ?? null,
      check_constraints: checkConstraints ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Drop a table by name. */
  async tableDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_table_drop", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Get the scan function for a table (used for time-travel and table scans). */
  async tableScanFunctionGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    atUnit?: string | null,
    atValue?: string | null,
    transactionId?: TransactionId,
  ): Promise<Record<string, any>> {
    const result = await this.rpc.call("catalog_table_scan_function_get", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      at_unit: atUnit ?? null,
      at_value: atValue ?? null,
      transaction_id: transactionId ?? null,
    });
    if (!result) throw new VgiClientError("table_scan_function_get returned null");
    return unwrapResult(result);
  }

  /** Set or clear the comment on a table. */
  async tableCommentSet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_table_comment_set", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      comment: comment ?? null,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Rename a table. */
  async tableRename(
    attachId: AttachId,
    schemaName: string,
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_table_rename", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      new_name: newName,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Add a column to a table. */
  async tableColumnAdd(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    columnType: string,
    defaultValue?: string | null,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_table_column_add", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      column_name: columnName,
      column_type: columnType,
      default_value: defaultValue ?? null,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Drop a column from a table. */
  async tableColumnDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_table_column_drop", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      column_name: columnName,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Rename a column in a table. */
  async tableColumnRename(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_table_column_rename", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      column_name: columnName,
      new_name: newName,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Set the default value for a column. */
  async tableColumnDefaultSet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    defaultValue: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_table_column_default_set", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      column_name: columnName,
      default_value: defaultValue,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Remove the default value from a column. */
  async tableColumnDefaultDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_table_column_default_drop", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      column_name: columnName,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Change the type of a column. */
  async tableColumnTypeChange(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    newType: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_table_column_type_change", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      column_name: columnName,
      new_type: newType,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Set a NOT NULL constraint on a column. */
  async tableNotNullSet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_table_not_null_set", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      column_name: columnName,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Remove a NOT NULL constraint from a column. */
  async tableNotNullDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_table_not_null_drop", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      column_name: columnName,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Get a view by name, or null if not found. */
  async viewGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    transactionId?: TransactionId,
  ): Promise<ViewInfo | null> {
    const result = await this.rpc.call("catalog_view_get", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      transaction_id: transactionId ?? null,
    });
    if (!result) return null;
    const inner = unwrapResult(result);
    const items = deserializeInfoList(inner.items, decodeViewInfo);
    return items.length > 0 ? items[0] : null;
  }

  /** Create a new view. */
  async viewCreate(
    attachId: AttachId,
    schemaName: string,
    name: string,
    definition: string,
    onConflict: OnCreateConflict,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_view_create", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      definition,
      on_conflict: onConflict,
      transaction_id: transactionId ?? null,
    });
  }

  /** Drop a view by name. */
  async viewDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_view_drop", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Rename a view. */
  async viewRename(
    attachId: AttachId,
    schemaName: string,
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_view_rename", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      new_name: newName,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Set or clear the comment on a view. */
  async viewCommentSet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_view_comment_set", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      comment: comment ?? null,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  // ==========================================================================
  // Macro Catalog API
  // ==========================================================================

  /** Get a macro by name, or null if not found. */
  async macroGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    transactionId?: TransactionId,
  ): Promise<MacroInfo | null> {
    const result = await this.rpc.call("catalog_macro_get", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      transaction_id: transactionId ?? null,
    });
    if (!result) return null;
    const inner = unwrapResult(result);
    const items = deserializeInfoList(inner.items, decodeMacroInfo);
    return items.length > 0 ? items[0] : null;
  }

  /** List macros in a schema, filtered by type. */
  async schemaContentsMacros(
    attachId: AttachId,
    name: string,
    type: CatalogMacroType,
    transactionId?: TransactionId,
  ): Promise<MacroInfo[]> {
    const result = await this.rpc.call("catalog_schema_contents_macros", {
      attach_id: attachId,
      name,
      type,
      transaction_id: transactionId ?? null,
    });
    if (!result) return [];
    const inner = unwrapResult(result);
    return deserializeInfoList(inner.items, decodeMacroInfo);
  }

  /** Create a new macro. */
  async macroCreate(
    attachId: AttachId,
    schemaName: string,
    name: string,
    macroType: MacroType,
    parameters: string[],
    definition: string,
    onConflict: OnCreateConflict,
    parameterDefaultValues?: Uint8Array | null,
    transactionId?: TransactionId,
  ): Promise<void> {
    const schema = new Schema([
      new Field("attach_id", new Binary(), true),
      new Field("schema_name", new Utf8(), false),
      new Field("name", new Utf8(), false),
      new Field("macro_type", new Utf8(), false),
      new Field("parameters", new List(new Field("item", new Utf8(), false)), false),
      new Field("definition", new Utf8(), false),
      new Field("on_conflict", new Utf8(), false),
      new Field("parameter_default_values", new Binary(), true),
      new Field("transaction_id", new Binary(), true),
    ]);
    const innerBatch = batchFromColumns(
      {
        attach_id: [attachId],
        schema_name: [schemaName],
        name: [name],
        macro_type: [macroType],
        parameters: [parameters],
        definition: [definition],
        on_conflict: [onConflict],
        parameter_default_values: [parameterDefaultValues ?? null],
        transaction_id: [transactionId ?? null],
      },
      schema,
    );
    await this.rpc.call("catalog_macro_create", wrapRequest(innerBatch));
  }

  /** Drop a macro by name. */
  async macroDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId,
  ): Promise<void> {
    await this.rpc.call("catalog_macro_drop", {
      attach_id: attachId,
      schema_name: schemaName,
      name,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_id: transactionId ?? null,
    });
  }

  /** Close the underlying RPC connection. */
  close(): void {
    this.rpc.close();
  }
}

// ==========================================================================
// Internal helpers
// ==========================================================================

/**
 * Deserialize a List<Binary> of serialized info batches into typed info objects.
 */
function deserializeInfoList<T>(
  items: any,
  deserializeFn: (bytes: Uint8Array) => T,
): T[] {
  if (!items) return [];
  const arr: any[] = Array.isArray(items) ? items : [...items];
  return arr
    .filter((b: any) => b != null)
    .map((b: any) => deserializeFn(toUint8Array(b)));
}

/**
 * Deserialize Arrow Map entries into a plain Record<string, string>.
 */
function deserializeTags(mapVal: any): Record<string, string> {
  const tags: Record<string, string> = {};
  if (!mapVal) return tags;
  const entries = typeof mapVal[Symbol.iterator] === "function" ? mapVal : [];
  for (const entry of entries) {
    if (Array.isArray(entry)) {
      tags[String(entry[0])] = String(entry[1]);
    } else if (entry && typeof entry === "object") {
      tags[String(entry.key ?? entry[0] ?? "")] = String(entry.value ?? entry[1] ?? "");
    }
  }
  return tags;
}

/**
 * Convert sync/async iterable to async iterator.
 */
function toAsyncIterator<T>(
  input: Iterable<T> | AsyncIterable<T>,
): AsyncIterator<T> {
  if (Symbol.asyncIterator in (input as any)) {
    return (input as AsyncIterable<T>)[Symbol.asyncIterator]();
  }
  const syncIter = (input as Iterable<T>)[Symbol.iterator]();
  return {
    async next() {
      return syncIter.next();
    },
  };
}
