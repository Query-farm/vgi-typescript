// VGI Client — browser-safe entry point.
//
// Import from "vgi/client" instead of "vgi" when targeting browsers, Deno,
// or any environment that cannot load Node.js built-ins. This entry point
// excludes server-side code (Worker, Protocol, Storage) that depends on
// node:fs, node:os, and process.
//
// Usage:
//   import { VgiClient, Arguments } from "vgi/client";
//   import { httpConnect } from "vgi-rpc";      // or your own RpcClient
//
//   const rpc = httpConnect("https://my-vgi-server/");
//   const client = new VgiClient(rpc);

// Client
export { VgiClient, VgiClientError } from "./client/client.js";
export type {
  VgiClientOptions,
  TableFunctionOptions,
  ScalarFunctionOptions,
  TableInOutFunctionOptions,
  OnCreateConflict,
  CatalogFunctionType,
} from "./client/types.js";

// Arguments
export { Arguments } from "./arguments/arguments.js";
export {
  type ArgumentSpec,
  argumentSpecsToSchema,
  schemaToArgumentSpecs,
} from "./arguments/argument-spec.js";

// Core enums
export {
  FunctionType,
  FunctionStability,
  NullHandling,
  OrderPreservation,
  OrderDependence,
  DistinctDependence,
  TableInOutPhase,
} from "./types.js";

// Protocol types (needed for advanced client use)
export type {
  BindRequest,
  BindResponse,
  InitRequest,
  GlobalInitResponse,
  TableCardinality,
} from "./protocol/types.js";

// Catalog info types
export {
  SchemaInfo,
  TableInfo,
  ViewInfo,
  FunctionInfo,
  type CatalogAttachResult,
  type AttachId,
  type TransactionId,
} from "./catalog/interface.js";

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
  safeNumber,
  serializeSchema,
  deserializeSchema,
  serializeBatch,
  deserializeBatch,
} from "./util/arrow.js";

// Byte utilities
export { toUint8Array } from "./util/bytes.js";

// Re-export transport types from vgi-rpc (type-only — zero runtime cost).
// Users import the actual connect functions (httpConnect, pipeConnect) from
// "vgi-rpc" directly so their bundler can tree-shake the server-side code.
export type { RpcClient, StreamSession, LogMessage } from "vgi-rpc";
