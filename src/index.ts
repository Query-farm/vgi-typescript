// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// VGI TypeScript - Public API

// Function factories (primary API)
export { defineScalarFunction, type ScalarFunctionConfig, type ScalarBindParameters, type ScalarParameterDef } from "./functions/scalar.js";
export { defineTableFunction, type TableFunctionConfig, type TableBindParams, type TableProcessParams } from "./functions/table.js";
export {
  defineAggregate,
  GROUP_COLUMN_NAME,
  type AggregateFunctionConfig,
  type AggregateBindParams,
  type AggregateUpdateParams,
  type AggregateFinalizeParams,
} from "./functions/aggregate.js";
export { defineTableInOutFunction, type TableInOutConfig, type TableInOutBindParams, type TableInOutProcessParams } from "./functions/table-in-out.js";
export {
  defineTableBufferingFunction,
  type TableBufferingConfig,
  type TableBufferingBindParams,
  type TableBufferingParams,
  type TableBufferingVgiFunction,
} from "./functions/table-buffering.js";

// Worker
export { Worker, type WorkerConfig } from "./worker.js";

// Function types
export type { VgiFunction, FunctionMeta, StreamHandlers, FunctionExample, HandlerState } from "./functions/types.js";

// Catalog
export { CatalogInterface, type CatalogAttachResult, type SchemaInfo, encodeSchemaInfo, decodeSchemaInfo, type TableInfo, encodeTableInfo, decodeTableInfo, type ViewInfo, encodeViewInfo, decodeViewInfo, type CatalogInfo, encodeCatalogInfo, decodeCatalogInfo, type FunctionInfo, encodeFunctionInfo, decodeFunctionInfo, type FunctionInfoOptions, type MacroInfo, encodeMacroInfo, decodeMacroInfo, type MacroType, type AttachOpaqueData, type TransactionOpaqueData, buildScanBranchesResult, type ScanBranchInput } from "./catalog/interface.js";
export type { CatalogDescriptor, SchemaDescriptor, TableDescriptor, ViewDescriptor, MacroDescriptor, SettingDescriptor, SecretTypeDescriptor, ForeignKeyDef, DefaultValue } from "./catalog/descriptors.js";
export { ReadOnlyCatalogInterface } from "./catalog/read-only.js";
export { CompositeCatalogInterface } from "./catalog/composite.js";
export { FunctionRegistry } from "./functions/registry.js";

// Core types
export {
  FunctionType,
  FunctionStability,
  NullHandling,
  OrderPreservation,
  OrderDependence,
  DistinctDependence,
  TableInOutPhase,
  DEFAULT_MAX_WORKERS,
} from "./types.js";

// Protocol types
export type {
  BindRequest,
  BindResponse,
  InitRequest,
  GlobalInitResponse,
  TableCardinality,
} from "./protocol/types.js";

// Arguments
export { Arguments } from "./arguments/arguments.js";
export { type ArgumentSpec, argumentSpecsToSchema, schemaToArgumentSpecs } from "./arguments/argument-spec.js";

// Errors
export {
  VgiError,
  RowCountMismatchError,
  FunctionNotFoundError,
  CatalogReadOnlyError,
  CatalogNotFoundError,
  CatalogAlreadyExistsError,
  ArgumentValidationError,
  NoCatalogError,
} from "./errors.js";

// Metadata
export type { ResolvedMetadata, ParameterInfo } from "./metadata/types.js";
export { CatalogFunctionType } from "./metadata/types.js";
export { resolveMetadata } from "./metadata/resolve.js";
export { metadatasToArrow, arrowToMetadatas } from "./metadata/serialize.js";

// Filter pushdown
export {
  PushdownFilters,
  FilteringOutputCollector,
  deserializeFilters,
  buildJoinKeysLookup,
  ComparisonOp,
  type Filter,
  type ConstantFilter,
  type IsNullFilter,
  type IsNotNullFilter,
  type InFilter,
  type AndFilter,
  type OrFilter,
  type StructFilter,
  formatPushedFilters,
  reprPushedFilters,
} from "./filter-pushdown/index.js";

// State serializer
export { arrowStateSerializer, EXCHANGE_STATE_SCHEMA, serializeUserState, deserializeUserState } from "./protocol/state-serializer.js";

// Protocol assembly (for embedding the worker dispatcher in a custom HTTP server).
export { buildVgiProtocol, type ProtocolConfig } from "./protocol/dispatch.js";

// Wire schemas — needed by workers that synthesize TableInfo.bind_result /
// TableInfo.scan_function bytes to short-circuit per-scan RPCs.
export { BindResultSchema, ScanFunctionResultSchema } from "./generated/vgi-protocol-schemas.js";

// Attach-option specs (for catalogs that advertise typed ATTACH options).
export { type AttachOptionSpec, serializeAttachOptionSpec, serializeAttachOptionSpecs } from "./catalog/attach-option.js";

// Column statistics (for inlining on TableInfo.column_statistics).
export { type ColumnStatistics, serializeColumnStatistics } from "./util/statistics.js";

// Byte utilities
export { toUint8Array } from "./util/bytes.js";

// Arrow utilities
export {
  iterRows,
  batchFromRows,
  batchFromColumns,
  emptyBatch,
  filterBatch,
  projectSchema,
  projectBatch,
  batchToScalarDict,
  batchToSecretDict,
  safeNumber,
  serializeSchema,
  deserializeSchema,
  serializeBatch,
  deserializeBatch,
} from "./util/arrow/index.js";

// Storage
export {
  type FunctionStorage,
  FunctionStorageSqlite,
  BoundStorage,
  UnknownInvocationError,
  resolveStorageFromEnv,
  setStorage,
  storage as functionStorage,
} from "./functions/storage.js";
export { FunctionStorageCfDo, type FetchLike } from "./functions/storage-cf-do.js";

// Client
export { VgiClient, VgiClientError } from "./client/client.js";
export type {
  VgiClientOptions,
  TableFunctionOptions,
  ScalarFunctionOptions,
  TableInOutFunctionOptions,
  OnCreateConflict,
  CatalogFunctionType as ClientCatalogFunctionType,
  CatalogAttachOptions,
  AttachOptionValue,
} from "./client/types.js";

// Re-export from vgi-rpc for convenience
export { str, bytes, int, int32, float, float32, bool, toSchema, OutputCollector, AuthContext } from "@query-farm/vgi-rpc";
export { subprocessConnect, httpConnect } from "@query-farm/vgi-rpc";
export type { RpcClient, StreamSession, LogMessage } from "@query-farm/vgi-rpc";
