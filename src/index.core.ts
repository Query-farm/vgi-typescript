// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// VGI TypeScript - Public API (runtime-agnostic core)
//
// Everything exported from the package EXCEPT the node-only `Worker` (which
// imports the AF_UNIX launcher's `serveUnix` from @query-farm/vgi-rpc). This
// module must stay free of static node-only value imports so it can be bundled
// for Cloudflare Workers / browsers via worker-cf-entry.ts. The node/bun barrel
// (`./index.ts`) re-exports this plus `Worker`.

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
export {
  defineCopyFromFunction,
  type CopyFromFunctionConfig,
  type CopyFromOption,
  type CopyFromReadParams,
} from "./functions/copy-from.js";

// Function types
export type { VgiFunction, FunctionMeta, StreamHandlers, FunctionExample, HandlerState } from "./functions/types.js";

// Catalog
export { CatalogInterface, type CatalogAttachResult, type SchemaInfo, encodeSchemaInfo, decodeSchemaInfo, type TableInfo, encodeTableInfo, decodeTableInfo, type ViewInfo, encodeViewInfo, decodeViewInfo, type CatalogInfo, encodeCatalogInfo, decodeCatalogInfo, type FunctionInfo, encodeFunctionInfo, decodeFunctionInfo, type FunctionInfoOptions, type MacroInfo, encodeMacroInfo, decodeMacroInfo, type MacroType, type CopyFromFormatInfo, encodeCopyFromFormatInfo, type AttachOpaqueData, type TransactionOpaqueData, buildScanBranchesResult, type ScanBranchInput } from "./catalog/interface.js";
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
  CopyFromContext,
  InitRequest,
  GlobalInitResponse,
  TableCardinality,
} from "./protocol/types.js";

// Arguments
export { Arguments } from "./arguments/arguments.js";
export { type ArgumentSpec, argumentSpecsToSchema, schemaToArgumentSpecs, macroArgumentsSchema, macroParameterDocsFromSchema } from "./arguments/argument-spec.js";

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
} from "./util/arrow/index.js";

export {
  type SecretFields,
  type SecretsDict,
  secretType,
  secretsOfType,
  secretForScope,
  secretForScopeOfType,
} from "./secrets/helpers.js";

export {
  serializeSchema,
  deserializeSchema,
  serializeBatch,
  deserializeBatch,
} from "./util/arrow/index.js";

// Arrow type system (phase 3): typed factories, precise descriptors, the
// rich/raw value mapping, the codec registry, and branded raw scalar types
// with their validating constructors. This is the statically-typed author API.
// NOTE: the arrow factory names `int`, `int32`, `float32`, `bool` are NOT
// re-exported here because the package root already exports vgi-rpc argument
// builders of the same names. Authors needing those specific arrow factories
// (and the full typed factory set) import them from the arrow facade module
// (`@query-farm/vgi` -> "./src/arrow/index.js"). The non-colliding and
// unit-precise factories below cover the common phase-3 typed/raw-mode cases.
export {
  // typed type factories (non-colliding subset)
  schema, field,
  nullType,
  int8, int16, int64,
  uint8, uint16, uint32, uint64,
  float16, float64,
  utf8, binary, fixedSizeBinary,
  decimal, decimal128, decimal256,
  date, dateDay, dateMillisecond,
  time, timeSecond, timeMillisecond, timeMicrosecond, timeNanosecond,
  timestamp, duration, interval,
  timestampSeconds, timestampMillis, timestampMicros, timestampNanos,
  durationSeconds, durationMillis, durationMicros, durationNanos,
  list, fixedSizeList, struct, map,
  dictionary, union, sparseUnion, denseUnion,
  TimeUnit, DateUnit, IntervalUnit, UnionMode,
  // codec registry
  codecFor,
  // branded raw constructors / unwrappers
  asDate32, asDate64Ms,
  asTime32S, asTime32Ms, asTime64Us, asTime64Ns,
  asTimestampSeconds, asTimestampMillis, asTimestampMicros, asTimestampNanos,
  asDurationSeconds, asDurationMillis, asDurationMicros, asDurationNanos,
  asUnscaledDecimal, asInt64, asUint64,
  fromBranded, fromBrandedNumber, fromBrandedBigInt,
  // backend-agnostic predicates + ids
  TypeId,
  isNull, isInt, isFloat, isBinary, isUtf8, isBool, isDecimal, isDate,
  isTime, isTimestamp, isInterval, isList, isStruct, isUnion,
  isFixedSizeBinary, isFixedSizeList, isMap, isDuration, isDictionary, isBatch,
  readCanonicalValue, columnFromArray, decodeDictValue, backend,
} from "./arrow/index.js";

export type {
  // backend-agnostic Arrow types
  VgiTypeId, VgiDataType, VgiField, VgiSchema, VgiColumn, VgiColumnData,
  VgiBatch, VgiBackendInfo, TaggedUnion,
  // precise nominal descriptors
  NullDescriptor, BoolType,
  IntType, Int8Type, Int16Type, Int32Type, Int64Type,
  Uint8Type, Uint16Type, Uint32Type, Uint64Type,
  FloatType, Utf8Type, LargeUtf8Type, BinaryType, LargeBinaryType,
  FixedSizeBinaryType, DecimalType,
  Date32Type, Date64Type, Time32Type, Time64Type,
  TimestampType, DurationType, IntervalType,
  ListType, FixedSizeListType, StructType, MapType, DictionaryType, UnionType,
  TypedField, TUnit,
  // rich/raw mapping
  RichValue, RawValue, ValueFor, Repr,
  // codec
  Codec,
  // branded scalar aliases
  Branded,
  Date32, Date64Ms,
  Time32S, Time32Ms, Time64Us, Time64Ns,
  TimestampSeconds, TimestampMillis, TimestampMicros, TimestampNanos,
  DurationSeconds, DurationMillis, DurationMicros, DurationNanos,
  UnscaledDecimal, Int64Raw, Uint64Raw,
} from "./arrow/index.js";

// Typed scalar compute helper types (phase 3).
export type { ScalarComputeRow, ScalarOutputValue, ScalarComputeResult } from "./functions/scalar.js";

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
export { subprocessConnect, httpConnect, tcpConnect } from "@query-farm/vgi-rpc";
export type { RpcClient, StreamSession, LogMessage } from "@query-farm/vgi-rpc";
