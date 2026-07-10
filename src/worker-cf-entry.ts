// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Cloudflare Workers entrypoint for the VGI worker.
//
// Imports the same building blocks as the Bun HTTP worker (Protocol, registry,
// catalog interface, state serializer) but exposes the handler as `default.fetch`
// — the shape `wrangler dev` and the Workers runtime expect.
//
// Build picks the flechette Arrow backend automatically because this entry is
// referenced under the `workerd`/`worker`/`browser` conditional export, which
// resolves `#arrow-impl` -> impl-flechette.
//
// Consumers wire their own catalog and function registry; the helper here is
// `createVgiFetch` rather than a hard-coded singleton, so a CF Worker module
// can compose multiple registries the same way the Bun worker does.

export { createVgiFetch } from "./http/fetch.js";
export type { VgiFetchOptions } from "./http/fetch.js";

// Re-export the public types CF Worker authors need to wire a registry +
// catalog. They import from "vgi/worker-cf" rather than from "vgi" directly
// so the build-condition resolution picks the flechette Arrow backend.
export {
  defineScalarFunction,
  defineTableFunction,
  defineAggregate,
  defineTableInOutFunction,
  FunctionRegistry,
  ReadOnlyCatalogInterface,
  CompositeCatalogInterface,
  CatalogInterface,
  Arguments,
  FunctionType,
  serializeAttachOptionSpec,
  serializeAttachOptionSpecs,
  serializeColumnStatistics,
  BindResultSchema,
  ScanFunctionResultSchema,
} from "./index.core.js";

export type {
  AttachOptionSpec,
  ColumnStatistics,
  CatalogAttachResult,
  CatalogDescriptor,
  CatalogInfo,
  TableInfo,
  AttachOpaqueData,
  TransactionOpaqueData,
  SchemaInfo,
  VgiFunction,
  TableProcessParams,
} from "./index.core.js";

export type { ProtocolConfig } from "./protocol/dispatch.js";

// Landing surface. CF workers pass the provider as `landingDescribe` so
// `GET /` serves the standardized VGI landing page for the worker's catalog.
export { createLandingDescribe } from "./http/describe-json.js";
export type { LandingDescribeProvider, WorkerDescribeInfo } from "./http/describe-json.js";

// Backend-agnostic Arrow facade. Same source resolves to arrow-js on
// Node/Bun and flechette under workerd/worker/browser via the package's
// `#arrow-impl` conditional import. Worker authors should always go
// through these factories instead of importing arrow-js / flechette
// directly.
export {
  // Type guards
  TypeId,
  isNull,
  isInt,
  isFloat,
  isBinary,
  isUtf8,
  isBool,
  isDecimal,
  isDate,
  isTime,
  isTimestamp,
  isInterval,
  isList,
  isStruct,
  isUnion,
  isFixedSizeBinary,
  isFixedSizeList,
  isMap,
  isDuration,
  isDictionary,
  isBatch,
  // Active backend's surface
  backend,
  schema, field,
  nullType, bool,
  int, int8, int16, int32, int64,
  uint8, uint16, uint32, uint64,
  float16, float32, float64,
  utf8, binary, fixedSizeBinary,
  decimal, decimal128, decimal256,
  date, dateDay, dateMillisecond,
  time, timeSecond, timeMillisecond, timeMicrosecond, timeNanosecond,
  timestamp, duration, interval,
  list, fixedSizeList, struct, map,
  dictionary,
  union, sparseUnion, denseUnion,
  TimeUnit, DateUnit, IntervalUnit, UnionMode,
  emptyBatch, batchFromRows, batchFromColumns, columnFromArray,
  iterRows, batchToScalarDict, batchToSecretDict, safeNumber, decodeDictValue,
  filterBatch, projectSchema, projectBatch,
  serializeSchema, deserializeSchema, serializeBatch, deserializeBatch,
  buildStatisticsBatch,
} from "./arrow/index.js";

export type {
  VgiTypeId,
  VgiDataType,
  VgiField,
  VgiSchema,
  VgiColumn,
  VgiColumnData,
  VgiBatch,
  VgiBackendInfo,
} from "./arrow/index.js";

// State serializer (HMAC-signed token round-trip across stateless requests)
export { arrowStateSerializer, EXCHANGE_STATE_SCHEMA } from "./protocol/state-serializer.js";

// Re-export from vgi-rpc for convenience.
export { OutputCollector, AuthContext } from "@query-farm/vgi-rpc";
export type { LogMessage } from "@query-farm/vgi-rpc";
