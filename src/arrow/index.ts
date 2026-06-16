// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Public facade. Internal vgi-typescript code imports Arrow operations
// from here; the active backend is selected at module-resolution time
// via the `#arrow-impl` subpath in package.json's `imports` field.
//
// Selection conditions:
//   workerd / worker / browser  -> impl-flechette (Cloudflare Workers, etc.)
//   default                     -> impl-arrowjs   (Bun worker, Node)

// Backend-agnostic types (defined in this package, satisfied structurally
// by both arrow-js and flechette native values).
export type {
  VgiTypeId,
  VgiDataType,
  VgiField,
  VgiSchema,
  VgiColumn,
  VgiColumnData,
  VgiBatch,
  VgiBackendInfo,
} from "./types.js";

// Backend-agnostic predicates.
export {
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
} from "./predicates.js";

// Active backend's surface (factories, IPC, build, iterate, utilities).
export {
  backend,
  // schema / field / type factories
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
  // batch utilities
  emptyBatch, batchFromRows, batchFromColumns, columnFromArray,
  iterRows, batchToScalarDict, batchToSecretDict, safeNumber, decodeDictValue,
  filterBatch, projectSchema, projectBatch,
  // canonical single-value read (backend-agnostic, lossless)
  readCanonicalValue,
  // IPC
  serializeSchema, deserializeSchema, serializeBatch, deserializeBatch,
  // Column statistics
  buildStatisticsBatch,
} from "#arrow-impl";

export type { ColumnStatistics } from "#arrow-impl";
