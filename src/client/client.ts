// VgiClient — high-level client for calling VGI worker functions and catalog API.
// Works with any RpcClient (subprocess or HTTP transport).

import { type VgiSchema, schema as schema_, type VgiField, field, type VgiBatch, type VgiDataType, utf8, binary, list } from "../arrow/index.js";
import { type RpcClient, type StreamSession } from "vgi-rpc";
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
} from "../util/arrow/index.js";
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
  type MacroType,
  type CatalogAttachResult,
  type AttachOpaqueData,
  type TransactionOpaqueData,
  decodeCatalogInfo,
  type CatalogInfo,
  type ScanFunctionResult,
  decodeScanFunctionResult,
} from "../catalog/interface.js";
import { wrapRequest, unwrapResult } from "./protocol.js";
import { toUint8Array } from "../util/bytes.js";
import { serializeAttachOptions } from "../catalog/attach-options.js";
import { VgiClientError, wrapRpcWithErrorEnrichment } from "./errors.js";
import { deserializeInfoList, deserializeTags, toAsyncIterator } from "./helpers.js";

export { VgiClientError };
import type {
  VgiClientOptions,
  TableFunctionOptions,
  ScalarFunctionOptions,
  TableInOutFunctionOptions,
  OnCreateConflict,
  CatalogFunctionType,
  CatalogMacroType,
  CatalogAttachOptions,
  OrderByPushdown,
  TablesamplePushdown,
  AttachOptionValue,
} from "./types.js";

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
  private readonly defaultAttachOpaqueData: Uint8Array | null;

  /**
   * Construct a VgiClient wrapping an `RpcClient` transport.
   *
   * `options.attachOpaqueData` sets a client-wide default attach ID used by every
   * function call that doesn't supply its own. Per-call `attachOpaqueData` on
   * `TableFunctionOptions`/`ScalarFunctionOptions`/`TableInOutFunctionOptions`
   * takes precedence — useful when a single client talks to multiple
   * attached catalogs.
   */
  constructor(rpc: RpcClient, options?: VgiClientOptions) {
    this.rpc = wrapRpcWithErrorEnrichment(rpc);
    this.defaultAttachOpaqueData = options?.attachOpaqueData ?? null;
  }

  // ==========================================================================
  // Private helpers
  // ==========================================================================

  private async _doBind(
    functionName: string,
    functionType: FunctionType,
    args: Arguments,
    inputSchema: VgiSchema | null,
    settings: VgiBatch | null,
    secrets: VgiBatch | null,
    transactionOpaqueData: Uint8Array | null,
    attachOpaqueData: Uint8Array | null = null,
    onBind?: ((response: BindResponse) => void) | null,
  ): Promise<{ request: BindRequest; response: BindResponse }> {
    const request: BindRequest = {
      function_name: functionName,
      arguments: args,
      function_type: functionType,
      input_schema: inputSchema,
      settings: settings ?? null,
      secrets: secrets ?? null,
      attach_opaque_data: attachOpaqueData ?? this.defaultAttachOpaqueData,
      transaction_opaque_data: transactionOpaqueData ?? null,
      resolved_secrets_provided: false,
    };

    const requestBatch = serializeBindRequest(request);
    const rpcResult = await this.rpc.call("bind", wrapRequest(requestBatch));
    if (!rpcResult) throw new VgiClientError("bind returned null");
    const inner = unwrapResult(rpcResult);
    const response = deserializeBindResponse(inner);
    if (onBind) onBind(response);
    return { request, response };
  }

  private async _doInit(
    bindRequest: BindRequest,
    bindResponse: BindResponse,
    opts?: {
      projectionIds?: number[] | null;
      pushdownFilters?: VgiBatch | null;
      phase?: TableInOutPhase | null;
      executionId?: Uint8Array | null;
      orderBy?: OrderByPushdown | null;
      tablesample?: TablesamplePushdown | null;
      joinKeys?: VgiBatch[] | null;
    },
  ): Promise<{ session: StreamSession; initResponse: GlobalInitResponse }> {
    const ob = opts?.orderBy ?? null;
    const ts = opts?.tablesample ?? null;
    const initRequest: InitRequest = {
      bind_call: bindRequest,
      output_schema: bindResponse.output_schema,
      bind_opaque_data: bindResponse.opaque_data,
      projection_ids: opts?.projectionIds ?? null,
      pushdown_filters: opts?.pushdownFilters ?? null,
      join_keys: opts?.joinKeys ?? [],
      phase: opts?.phase ?? null,
      order_by_column_name: ob?.columnName ?? null,
      order_by_direction: ob?.direction ?? null,
      order_by_null_order: ob?.nullOrder ?? null,
      order_by_limit: ob?.limit == null ? null : BigInt(ob.limit),
      tablesample_percentage: ts?.percentage ?? null,
      tablesample_seed: ts?.seed == null ? null : BigInt(ts.seed),
      execution_id: opts?.executionId ?? null,
      init_opaque_data: null,
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
  ): AsyncGenerator<VgiBatch> {
    const args = opts.arguments ?? new Arguments();

    const { request: bindReq, response: bindResp } = await this._doBind(
      opts.functionName,
      FunctionType.TABLE,
      args,
      null,
      opts.settings ?? null,
      null,
      opts.transactionOpaqueData ?? null,
      opts.attachOpaqueData ?? null,
      opts.onBind ?? null,
    );

    const { session } = await this._doInit(bindReq, bindResp, {
      projectionIds: opts.projectionIds,
      pushdownFilters: opts.pushdownFilters,
      orderBy: opts.orderBy,
      tablesample: opts.tablesample,
      joinKeys: opts.joinKeys,
    });

    const outputSchema = bindResp.output_schema;
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
  // Scalar function
  // ==========================================================================

  /** Call a scalar function, yielding output as row objects. */
  async *scalarFunctionRows(
    opts: ScalarFunctionOptions,
  ): AsyncGenerator<Record<string, any>[]> {
    // Peek first batch to get input schema
    const inputIter = toAsyncIterator(opts.input);
    const first = await inputIter.next();
    if (first.done) {
      throw new VgiClientError(
        `scalarFunction(${opts.functionName}): input iterator yielded no batches; ` +
          `at least one batch is required to determine the input schema`,
      );
    }

    const firstBatch: VgiBatch = first.value;
    const inputSchema = firstBatch.schema;

    const args = opts.arguments ?? new Arguments();

    const { request: bindReq, response: bindResp } = await this._doBind(
      opts.functionName,
      FunctionType.SCALAR,
      args,
      inputSchema,
      opts.settings ?? null,
      opts.secrets ?? null,
      opts.transactionOpaqueData ?? null,
      opts.attachOpaqueData ?? null,
      opts.onBind ?? null,
    );

    const { session } = await this._doInit(bindReq, bindResp);

    try {
      // Exchange first batch
      const outRows = await session.exchange([...iterRows(firstBatch)]);
      if (outRows.length > 0) yield outRows;

      // Exchange remaining batches
      for await (const batch of { [Symbol.asyncIterator]: () => inputIter }) {
        const result = await session.exchange([...iterRows(batch)]);
        if (result.length > 0) yield result;
      }
    } finally {
      session.close();
    }
  }

  /** Call a scalar function, yielding output as RecordBatch instances. */
  async *scalarFunction(
    opts: ScalarFunctionOptions,
  ): AsyncGenerator<VgiBatch> {
    // Same path as scalarFunctionRows, but with the output schema captured
    // here so we can pack rows back into batches without a re-bind.
    const inputIter = toAsyncIterator(opts.input);
    const first = await inputIter.next();
    if (first.done) {
      throw new VgiClientError(
        `scalarFunction(${opts.functionName}): input iterator yielded no batches; ` +
          `at least one batch is required to determine the input schema`,
      );
    }

    const firstBatch: VgiBatch = first.value;
    const inputSchema = firstBatch.schema;

    const args = opts.arguments ?? new Arguments();

    const { request: bindReq, response: bindResp } = await this._doBind(
      opts.functionName,
      FunctionType.SCALAR,
      args,
      inputSchema,
      opts.settings ?? null,
      opts.secrets ?? null,
      opts.transactionOpaqueData ?? null,
      opts.attachOpaqueData ?? null,
      opts.onBind ?? null,
    );

    const { session } = await this._doInit(bindReq, bindResp);
    const outputSchema = bindResp.output_schema;
    try {
      const outRows = await session.exchange([...iterRows(firstBatch)]);
      if (outRows.length > 0) yield batchFromRows(outRows, outputSchema);

      for await (const batch of { [Symbol.asyncIterator]: () => inputIter }) {
        const result = await session.exchange([...iterRows(batch)]);
        if (result.length > 0) yield batchFromRows(result, outputSchema);
      }
    } finally {
      session.close();
    }
  }

  // ==========================================================================
  // Table-in-out function
  // ==========================================================================

  /** Call a table-in-out function, yielding output as row objects. */
  async *tableInOutFunctionRows(
    opts: TableInOutFunctionOptions,
  ): AsyncGenerator<Record<string, any>[]> {
    const inputIter = toAsyncIterator(opts.input);
    const first = await inputIter.next();
    if (first.done) {
      throw new VgiClientError(
        `tableInOutFunction(${opts.functionName}): input iterator yielded no batches; ` +
          `at least one batch is required to determine the input schema`,
      );
    }

    const firstBatch: VgiBatch = first.value;
    const inputSchema = firstBatch.schema;

    const args = opts.arguments ?? new Arguments();

    const { request: bindReq, response: bindResp } = await this._doBind(
      opts.functionName,
      FunctionType.TABLE, // table-in-out uses TABLE function type at the bind level
      args,
      inputSchema,
      opts.settings ?? null,
      null,
      opts.transactionOpaqueData ?? null,
      opts.attachOpaqueData ?? null,
      opts.onBind ?? null,
    );

    // Phase 1: INPUT. We keep the init response around for its `execution_id`,
    // which we must echo back to pair the FINALIZE phase with this call.
    const { session: inputSession, initResponse } = await this._doInit(
      bindReq,
      bindResp,
      {
        projectionIds: opts.projectionIds,
        pushdownFilters: opts.pushdownFilters,
        phase: TableInOutPhase.INPUT,
        orderBy: opts.orderBy,
        tablesample: opts.tablesample,
        joinKeys: opts.joinKeys,
      },
    );

    try {
      const outRows = await inputSession.exchange([...iterRows(firstBatch)]);
      if (outRows.length > 0) yield outRows;

      for await (const batch of { [Symbol.asyncIterator]: () => inputIter }) {
        const result = await inputSession.exchange([...iterRows(batch)]);
        if (result.length > 0) yield result;
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
        executionId: initResponse.execution_id,
        orderBy: opts.orderBy,
        tablesample: opts.tablesample,
        joinKeys: opts.joinKeys,
      },
    );

    try {
      for await (const rows of finalizeSession) {
        if (rows.length > 0) yield rows;
      }
    } finally {
      finalizeSession.close();
    }
  }

  /** Call a table-in-out function, yielding output as RecordBatch instances. */
  async *tableInOutFunction(
    opts: TableInOutFunctionOptions,
  ): AsyncGenerator<VgiBatch> {
    const inputIter = toAsyncIterator(opts.input);
    const first = await inputIter.next();
    if (first.done) {
      throw new VgiClientError(
        `tableInOutFunction(${opts.functionName}): input iterator yielded no batches; ` +
          `at least one batch is required to determine the input schema`,
      );
    }

    const firstBatch: VgiBatch = first.value;
    const inputSchema = firstBatch.schema;

    const args = opts.arguments ?? new Arguments();

    const { request: bindReq, response: bindResp } = await this._doBind(
      opts.functionName,
      FunctionType.TABLE,
      args,
      inputSchema,
      opts.settings ?? null,
      null,
      opts.transactionOpaqueData ?? null,
      opts.attachOpaqueData ?? null,
      opts.onBind ?? null,
    );

    const { session: inputSession, initResponse } = await this._doInit(
      bindReq,
      bindResp,
      {
        projectionIds: opts.projectionIds,
        pushdownFilters: opts.pushdownFilters,
        phase: TableInOutPhase.INPUT,
        orderBy: opts.orderBy,
        tablesample: opts.tablesample,
        joinKeys: opts.joinKeys,
      },
    );

    const outputSchema = bindResp.output_schema;
    try {
      const outRows = await inputSession.exchange([...iterRows(firstBatch)]);
      if (outRows.length > 0) yield batchFromRows(outRows, outputSchema);

      for await (const batch of { [Symbol.asyncIterator]: () => inputIter }) {
        const result = await inputSession.exchange([...iterRows(batch)]);
        if (result.length > 0) yield batchFromRows(result, outputSchema);
      }
    } finally {
      inputSession.close();
    }

    const { session: finalizeSession } = await this._doInit(
      bindReq,
      bindResp,
      {
        projectionIds: opts.projectionIds,
        pushdownFilters: opts.pushdownFilters,
        phase: TableInOutPhase.FINALIZE,
        executionId: initResponse.execution_id,
        orderBy: opts.orderBy,
        tablesample: opts.tablesample,
        joinKeys: opts.joinKeys,
      },
    );

    try {
      for await (const rows of finalizeSession) {
        if (rows.length > 0) yield batchFromRows(rows, outputSchema);
      }
    } finally {
      finalizeSession.close();
    }
  }

  // ==========================================================================
  // Row convenience for table function
  // ==========================================================================

  /** Call a table function, yielding output as row objects. */
  async *tableFunctionRows(
    opts: TableFunctionOptions,
  ): AsyncGenerator<Record<string, any>[]> {
    const args = opts.arguments ?? new Arguments();
    const { request: bindReq, response: bindResp } = await this._doBind(
      opts.functionName,
      FunctionType.TABLE,
      args,
      null,
      opts.settings ?? null,
      null,
      opts.transactionOpaqueData ?? null,
      opts.attachOpaqueData ?? null,
      opts.onBind ?? null,
    );
    const { session } = await this._doInit(bindReq, bindResp, {
      projectionIds: opts.projectionIds,
      pushdownFilters: opts.pushdownFilters,
      orderBy: opts.orderBy,
      tablesample: opts.tablesample,
      joinKeys: opts.joinKeys,
    });
    try {
      for await (const rows of session) {
        if (rows.length > 0) yield rows;
      }
    } finally {
      session.close();
    }
  }

  // ==========================================================================
  // Catalog API
  // ==========================================================================

  /**
   * List all catalogs with their advertised version metadata.
   *
   * Each entry has `{name, implementation_version?, data_version_spec?}`.
   * Versioned workers populate the version fields; read-only workers
   * leave both null.
   */
  async catalogsInfo(): Promise<CatalogInfo[]> {
    const result = await this.rpc.call("catalog_catalogs", {});
    if (!result) return [];
    const inner = unwrapResult(result);
    return deserializeInfoList(inner.items, decodeCatalogInfo);
  }

  /** List all available catalog names (shorthand for `catalogsInfo().map(c => c.name)`). */
  async catalogs(): Promise<string[]> {
    const infos = await this.catalogsInfo();
    return infos.map((i) => i.name);
  }

  /**
   * Attach a catalog by name. Returns connection details including the
   * attachOpaqueData.
   *
   * `opts.options` is a plain key→value map; column types are inferred from
   * the value at runtime (see AttachOptionValue for the mapping). Use
   * `opts.optionsBytes` instead when you need Arrow types the inference
   * can't express (Decimal, Timestamp, Int32 vs Int64, nested structs).
   * Providing both throws.
   *
   * Versioned catalogs (see vgi-example-versioned-worker) validate
   * `dataVersionSpec` / `implementationVersion` at attach time and echo
   * back the resolved values on the result — callers can read those from
   * `result.resolved_data_version` / `result.resolved_implementation_version`.
   *
   * @example
   * ```ts
   * const result = await client.catalogAttach("my_catalog", {
   *   options: { region: "us-east-1", maxCachedRows: 1000n, readOnly: true },
   *   dataVersionSpec: "1.0.0",
   * });
   * ```
   */
  async catalogAttach(
    name: string,
    opts?: CatalogAttachOptions,
  ): Promise<CatalogAttachResult> {
    if (opts?.options != null && opts?.optionsBytes != null) {
      throw new VgiClientError(
        "catalogAttach: cannot specify both `options` and `optionsBytes`",
      );
    }
    let wireOptions: Uint8Array | null;
    if (opts?.optionsBytes != null) {
      // Escape hatch: raw pre-serialized bytes. Same zero-byte guard as
      // before — pyarrow's IPC reader rejects 0-byte input on a non-null
      // binary field.
      wireOptions = opts.optionsBytes.byteLength === 0 ? null : opts.optionsBytes;
    } else {
      wireOptions = serializeAttachOptions(opts?.options);
    }

    const schema = schema_([
      field("name", utf8(), false),
      field("options", binary(), true),
      field("data_version_spec", utf8(), true),
      field("implementation_version", utf8(), true),
    ]);
    const innerBatch = batchFromColumns(
      {
        name: [name],
        options: [wireOptions],
        data_version_spec: [opts?.dataVersionSpec ?? null],
        implementation_version: [opts?.implementationVersion ?? null],
      },
      schema,
    );
    const result = await this.rpc.call("catalog_attach", wrapRequest(innerBatch));
    if (!result) throw new VgiClientError("catalog_attach returned null");
    const inner = unwrapResult(result);
    return {
      attach_opaque_data: toUint8Array(inner.attach_opaque_data),
      supports_transactions: inner.supports_transactions ?? false,
      supports_time_travel: inner.supports_time_travel ?? false,
      catalog_version_frozen: inner.catalog_version_frozen ?? false,
      catalog_version: Number(inner.catalog_version ?? 0),
      attach_opaque_data_required: inner.attach_opaque_data_required ?? true,
      default_schema: inner.default_schema ?? "main",
      settings: inner.settings
        ? (Array.isArray(inner.settings) ? inner.settings : [...inner.settings]).map(toUint8Array)
        : [],
      secret_types: inner.secret_types
        ? (Array.isArray(inner.secret_types) ? inner.secret_types : [...inner.secret_types]).map(toUint8Array)
        : [],
      comment: inner.comment ?? null,
      tags: deserializeTags(inner.tags),
      supports_column_statistics: inner.supports_column_statistics ?? false,
      resolved_data_version: inner.resolved_data_version ?? null,
      resolved_implementation_version: inner.resolved_implementation_version ?? null,
    };
  }

  /** Detach a previously-attached catalog. */
  async catalogDetach(attachOpaqueData: AttachOpaqueData): Promise<void> {
    await this.rpc.call("catalog_detach", { attach_opaque_data: attachOpaqueData });
  }

  /**
   * Create a new catalog.
   *
   * `options` is a plain key→value map; column types are inferred from the
   * value at runtime (see AttachOptionValue). Use `optionsBytes` instead
   * when you need Arrow types the inference can't express. Providing both
   * throws.
   */
  async catalogCreate(
    name: string,
    onConflict: OnCreateConflict,
    options?:
      | Uint8Array
      | { options?: Record<string, AttachOptionValue>; optionsBytes?: Uint8Array },
  ): Promise<void> {
    let wireOptions: Uint8Array | null;
    if (options == null) {
      wireOptions = null;
    } else if (options instanceof Uint8Array) {
      // Backwards-compatible escape hatch: caller pre-serialized the options batch.
      wireOptions = options.byteLength === 0 ? null : options;
    } else {
      if (options.options != null && options.optionsBytes != null) {
        throw new VgiClientError(
          "catalogCreate: cannot specify both `options` and `optionsBytes`",
        );
      }
      if (options.optionsBytes != null) {
        wireOptions = options.optionsBytes.byteLength === 0 ? null : options.optionsBytes;
      } else {
        wireOptions = serializeAttachOptions(options.options);
      }
    }
    await this.rpc.call("catalog_create", {
      name,
      on_conflict: onConflict,
      options: wireOptions,
    });
  }

  /** Drop a catalog by name. */
  async catalogDrop(name: string): Promise<void> {
    await this.rpc.call("catalog_drop", { name });
  }

  /** Get the current catalog version number. */
  async catalogVersion(
    attachOpaqueData: AttachOpaqueData,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<number> {
    const result = await this.rpc.call("catalog_version", {
      attach_opaque_data: attachOpaqueData,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
    if (!result) throw new VgiClientError("catalog_version returned null");
    const inner = unwrapResult(result);
    return Number(inner.version ?? 0);
  }

  /** Begin a new transaction. Returns the transaction ID. */
  async transactionBegin(attachOpaqueData: AttachOpaqueData): Promise<Uint8Array> {
    const result = await this.rpc.call("catalog_transaction_begin", {
      attach_opaque_data: attachOpaqueData,
    });
    if (!result) throw new VgiClientError("transaction_begin returned null");
    const inner = unwrapResult(result);
    if (!inner.transaction_opaque_data) {
      throw new VgiClientError("transaction_begin returned no transaction_opaque_data");
    }
    return toUint8Array(inner.transaction_opaque_data);
  }

  /** Commit a transaction. */
  async transactionCommit(
    attachOpaqueData: AttachOpaqueData,
    transactionOpaqueData: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_transaction_commit", {
      attach_opaque_data: attachOpaqueData,
      transaction_opaque_data: transactionOpaqueData,
    });
  }

  /** Rollback a transaction. */
  async transactionRollback(
    attachOpaqueData: AttachOpaqueData,
    transactionOpaqueData: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_transaction_rollback", {
      attach_opaque_data: attachOpaqueData,
      transaction_opaque_data: transactionOpaqueData,
    });
  }

  /** List schemas in an attached catalog. */
  async schemas(
    attachOpaqueData: AttachOpaqueData,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<SchemaInfo[]> {
    const result = await this.rpc.call("catalog_schemas", {
      attach_opaque_data: attachOpaqueData,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
    if (!result) return [];
    const inner = unwrapResult(result);
    return deserializeInfoList(inner.items, decodeSchemaInfo);
  }

  /** Get a schema by name, or null if not found. */
  async schemaGet(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<SchemaInfo | null> {
    const result = await this.rpc.call("catalog_schema_get", {
      attach_opaque_data: attachOpaqueData,
      name,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
    if (!result) return null;
    const inner = unwrapResult(result);
    const items = deserializeInfoList(inner.items, decodeSchemaInfo);
    return items.length > 0 ? items[0] : null;
  }

  /** Create a new schema. */
  async schemaCreate(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    opts?: {
      onConflict?: OnCreateConflict;
      comment?: string | null;
      tags?: Record<string, string> | null;
      transactionOpaqueData?: TransactionOpaqueData;
    },
  ): Promise<void> {
    const tagsMap = opts?.tags
      ? new Map(Object.entries(opts.tags))
      : null;
    await this.rpc.call("catalog_schema_create", {
      attach_opaque_data: attachOpaqueData,
      name,
      on_conflict: opts?.onConflict ?? "error",
      comment: opts?.comment ?? null,
      tags: tagsMap,
      transaction_opaque_data: opts?.transactionOpaqueData ?? null,
    });
  }

  /** Drop a schema by name. */
  async schemaDrop(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    ignoreNotFound?: boolean,
    cascade?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_schema_drop", {
      attach_opaque_data: attachOpaqueData,
      name,
      ignore_not_found: ignoreNotFound ?? false,
      cascade: cascade ?? false,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /** List tables in a schema. */
  async schemaContentsTables(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<TableInfo[]> {
    const result = await this.rpc.call("catalog_schema_contents_tables", {
      attach_opaque_data: attachOpaqueData,
      name,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
    if (!result) return [];
    const inner = unwrapResult(result);
    return deserializeInfoList(inner.items, decodeTableInfo);
  }

  /** List views in a schema. */
  async schemaContentsViews(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<ViewInfo[]> {
    const result = await this.rpc.call("catalog_schema_contents_views", {
      attach_opaque_data: attachOpaqueData,
      name,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
    if (!result) return [];
    const inner = unwrapResult(result);
    return deserializeInfoList(inner.items, decodeViewInfo);
  }

  /** List functions in a schema, filtered by type. */
  async schemaContentsFunctions(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    type: CatalogFunctionType,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<FunctionInfo[]> {
    const result = await this.rpc.call("catalog_schema_contents_functions", {
      attach_opaque_data: attachOpaqueData,
      name,
      type,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
    if (!result) return [];
    const inner = unwrapResult(result);
    return deserializeInfoList(inner.items, decodeFunctionInfo);
  }

  /** Get a table by name, or null if not found. */
  async tableGet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<TableInfo | null> {
    const result = await this.rpc.call("catalog_table_get", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
    if (!result) return null;
    const inner = unwrapResult(result);
    const items = deserializeInfoList(inner.items, decodeTableInfo);
    return items.length > 0 ? items[0] : null;
  }

  /** Create a new table. */
  async tableCreate(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columns: Uint8Array,
    onConflict: OnCreateConflict,
    notNullConstraints?: number[],
    uniqueConstraints?: number[][],
    checkConstraints?: string[],
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_table_create", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      columns,
      on_conflict: onConflict,
      not_null_constraints: notNullConstraints ?? null,
      unique_constraints: uniqueConstraints ?? null,
      check_constraints: checkConstraints ?? null,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /** Drop a table by name. */
  async tableDrop(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    cascade?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_table_drop", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      ignore_not_found: ignoreNotFound ?? false,
      cascade: cascade ?? false,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /**
   * Get the scan function for a table — tells DuckDB which function to call
   * to read the table data (e.g. `read_parquet` with a path argument). Used
   * by the VGI extension during query planning.
   */
  async tableScanFunctionGet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    atUnit?: string | null,
    atValue?: string | null,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<ScanFunctionResult> {
    const result = await this.rpc.call("catalog_table_scan_function_get", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      at_unit: atUnit ?? null,
      at_value: atValue ?? null,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
    if (!result) throw new VgiClientError("table_scan_function_get returned null");
    return decodeScanFunctionResult(unwrapResult(result));
  }

  /** Set or clear the comment on a table. */
  async tableCommentSet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_table_comment_set", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      comment: comment ?? null,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /** Rename a table. */
  async tableRename(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_table_rename", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      new_name: newName,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /** Add a column to a table. */
  async tableColumnAdd(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnName: string,
    columnType: string,
    defaultValue?: string | null,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_table_column_add", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      column_name: columnName,
      column_type: columnType,
      default_value: defaultValue ?? null,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /** Drop a column from a table. */
  async tableColumnDrop(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_table_column_drop", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      column_name: columnName,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /** Rename a column in a table. */
  async tableColumnRename(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnName: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_table_column_rename", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      column_name: columnName,
      new_name: newName,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /** Set the default value for a column. */
  async tableColumnDefaultSet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnName: string,
    defaultValue: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_table_column_default_set", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      column_name: columnName,
      default_value: defaultValue,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /** Remove the default value from a column. */
  async tableColumnDefaultDrop(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_table_column_default_drop", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      column_name: columnName,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /**
   * Change the type of a column.
   *
   * `columnDefinition` is a serialized Arrow Schema with a single field whose
   * name identifies the target column and whose type is the new column type.
   * `expression` is an optional SQL expression used to convert existing values.
   */
  async tableColumnTypeChange(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnDefinition: Uint8Array,
    expression?: string | null,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_table_column_type_change", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      column_definition: columnDefinition,
      expression: expression ?? null,
      ignore_not_found: ignoreNotFound ?? false,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /** Set a NOT NULL constraint on a column. */
  async tableNotNullSet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_table_not_null_set", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      column_name: columnName,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /** Remove a NOT NULL constraint from a column. */
  async tableNotNullDrop(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_table_not_null_drop", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      column_name: columnName,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /** Get a view by name, or null if not found. */
  async viewGet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<ViewInfo | null> {
    const result = await this.rpc.call("catalog_view_get", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
    if (!result) return null;
    const inner = unwrapResult(result);
    const items = deserializeInfoList(inner.items, decodeViewInfo);
    return items.length > 0 ? items[0] : null;
  }

  /** Create a new view. */
  async viewCreate(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    definition: string,
    onConflict: OnCreateConflict,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_view_create", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      definition,
      on_conflict: onConflict,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /** Drop a view by name. */
  async viewDrop(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    cascade?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_view_drop", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      ignore_not_found: ignoreNotFound ?? false,
      cascade: cascade ?? false,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /** Rename a view. */
  async viewRename(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_view_rename", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      new_name: newName,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /** Set or clear the comment on a view. */
  async viewCommentSet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_view_comment_set", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      comment: comment ?? null,
      ignore_not_found: ignoreNotFound ?? null,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  // ==========================================================================
  // Macro Catalog API
  // ==========================================================================

  /** Get a macro by name, or null if not found. */
  async macroGet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<MacroInfo | null> {
    const result = await this.rpc.call("catalog_macro_get", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
    if (!result) return null;
    const inner = unwrapResult(result);
    const items = deserializeInfoList(inner.items, decodeMacroInfo);
    return items.length > 0 ? items[0] : null;
  }

  /** List macros in a schema, filtered by type. */
  async schemaContentsMacros(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    type: CatalogMacroType,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<MacroInfo[]> {
    const result = await this.rpc.call("catalog_schema_contents_macros", {
      attach_opaque_data: attachOpaqueData,
      name,
      type,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
    if (!result) return [];
    const inner = unwrapResult(result);
    return deserializeInfoList(inner.items, decodeMacroInfo);
  }

  /** Create a new macro. */
  async macroCreate(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    macroType: MacroType,
    parameters: string[],
    definition: string,
    onConflict: OnCreateConflict,
    parameterDefaultValues?: Uint8Array | null,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    const schema = schema_([
      field("attach_opaque_data", binary(), true),
      field("schema_name", utf8(), false),
      field("name", utf8(), false),
      field("macro_type", utf8(), false),
      field("parameters", list(field("item", utf8(), false)), false),
      field("definition", utf8(), false),
      field("on_conflict", utf8(), false),
      field("parameter_default_values", binary(), true),
      field("transaction_opaque_data", binary(), true),
    ]);
    const innerBatch = batchFromColumns(
      {
        attach_opaque_data: [attachOpaqueData],
        schema_name: [schemaName],
        name: [name],
        macro_type: [macroType],
        parameters: [parameters],
        definition: [definition],
        on_conflict: [onConflict],
        parameter_default_values: [parameterDefaultValues ?? null],
        transaction_opaque_data: [transactionOpaqueData ?? null],
      },
      schema,
    );
    await this.rpc.call("catalog_macro_create", wrapRequest(innerBatch));
  }

  /** Drop a macro by name. */
  async macroDrop(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<void> {
    await this.rpc.call("catalog_macro_drop", {
      attach_opaque_data: attachOpaqueData,
      schema_name: schemaName,
      name,
      ignore_not_found: ignoreNotFound ?? false,
      transaction_opaque_data: transactionOpaqueData ?? null,
    });
  }

  /** Close the underlying RPC connection. */
  close(): void {
    this.rpc.close();
  }
}

