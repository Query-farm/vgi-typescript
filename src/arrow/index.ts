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
  TaggedUnion,
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
  typeSignature,
} from "./predicates.js";

// Active backend's surface (IPC, build, iterate, utilities).
export {
  backend,
  // batch utilities
  emptyBatch, batchFromRows, batchFromColumns, columnFromArray,
  iterRows, batchToScalarDict, batchToSecretDict, safeNumber, decodeDictValue,
  filterBatch, projectSchema, projectBatch,
  // per-record-batch metadata attachment
  withBatchMetadata,
  // canonical single-value read (backend-agnostic, lossless)
  readCanonicalValue,
  // IPC
  serializeSchema, deserializeSchema, serializeBatch, deserializeBatch,
  // Column statistics
  buildStatisticsBatch,
} from "#arrow-impl";

export type { ColumnStatistics } from "#arrow-impl";

// Type factories — exported from the TYPED facade layer (schema-types.ts) so
// every factory carries a PRECISE nominal static descriptor (Date32Type,
// TimestampType<'us'>, StructType<...>, …). Runtime values are identical to the
// backend factories; the typed layer only refines the static return type, which
// is what makes the rich/raw value mapping (RichValue/RawValue/ValueFor) and
// the statically-typed define* author API work. Both backends report the same
// static types from here.
export {
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
  timestampSeconds, timestampMillis, timestampMicros, timestampNanos,
  durationSeconds, durationMillis, durationMicros, durationNanos,
  list, fixedSizeList, struct, map,
  dictionary,
  union, sparseUnion, denseUnion,
  TimeUnit, DateUnit, IntervalUnit, UnionMode,
} from "./schema-types.js";

// Precise nominal type descriptors (the static face of the factory returns).
export type {
  NullDescriptor,
  BoolType,
  IntType, Int8Type, Int16Type, Int32Type, Int64Type,
  Uint8Type, Uint16Type, Uint32Type, Uint64Type,
  FloatType, Utf8Type, LargeUtf8Type, BinaryType, LargeBinaryType,
  FixedSizeBinaryType, DecimalType,
  Date32Type, Date64Type, Time32Type, Time64Type,
  TimestampType, DurationType, IntervalType,
  ListType, FixedSizeListType, StructType, MapType, DictionaryType, UnionType,
  TypedField, TUnit,
} from "./codec/type-descriptors.js";

// Rich/raw value mapping.
export type { RichValue, RawValue, ValueFor, Repr } from "./codec/repr.js";

// Codec registry (single value rich<->canonical<->raw converters for manual use).
export { codecFor } from "./codec/registry.js";
export type { Codec } from "./codec/registry.js";

// Branded raw scalar types + validating constructors / unwrappers.
export type {
  Branded,
  Date32, Date64Ms,
  Time32S, Time32Ms, Time64Us, Time64Ns,
  TimestampSeconds, TimestampMillis, TimestampMicros, TimestampNanos,
  DurationSeconds, DurationMillis, DurationMicros, DurationNanos,
  UnscaledDecimal, Int64 as Int64Raw, Uint64 as Uint64Raw,
} from "./codec/branded.js";
export {
  asDate32, asDate64Ms,
  asTime32S, asTime32Ms, asTime64Us, asTime64Ns,
  asTimestampSeconds, asTimestampMillis, asTimestampMicros, asTimestampNanos,
  asDurationSeconds, asDurationMillis, asDurationMicros, asDurationNanos,
  asUnscaledDecimal, asInt64, asUint64,
  fromBranded, fromBrandedNumber, fromBrandedBigInt,
} from "./codec/branded.js";
