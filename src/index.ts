// VGI TypeScript - Public API

// Function factories (primary API)
export { defineScalarFunction, type ScalarFunctionConfig, type ScalarBindParameters, type ScalarParameterDef } from "./functions/scalar.js";
export { defineTableFunction, type TableFunctionConfig, type TableBindParams, type TableProcessParams } from "./functions/table.js";
export { defineTableInOutFunction, type TableInOutConfig, type TableInOutBindParams, type TableInOutProcessParams } from "./functions/table-in-out.js";

// Worker
export { Worker, type WorkerConfig } from "./worker.js";

// Function types
export type { VgiFunction, FunctionMeta, StreamHandlers, FunctionExample } from "./functions/types.js";

// Catalog
export { CatalogInterface, type CatalogAttachResult, SchemaInfo, TableInfo, ViewInfo, FunctionInfo, type AttachId } from "./catalog/interface.js";
export type { CatalogDescriptor, SchemaDescriptor, TableDescriptor, ViewDescriptor } from "./catalog/descriptors.js";
export { ReadOnlyCatalogInterface } from "./catalog/read-only.js";

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
  ComparisonOp,
  type Filter,
  type ConstantFilter,
  type IsNullFilter,
  type IsNotNullFilter,
  type InFilter,
  type AndFilter,
  type OrFilter,
  type StructFilter,
} from "./util/filter-pushdown.js";

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
} from "./util/arrow.js";

// Storage
export {
  type FunctionStorage,
  FunctionStorageSqlite,
  BoundStorage,
  UnknownInvocationError,
  storage as functionStorage,
} from "./storage/function-storage.js";

// Re-export from vgi-rpc for convenience
export { str, bytes, int, int32, float, float32, bool, toSchema, OutputCollector } from "vgi-rpc";
