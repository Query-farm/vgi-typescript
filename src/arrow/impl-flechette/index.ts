// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// flechette backend for the vgi-typescript Arrow facade.
// Selected via `#arrow-impl` resolution under the workerd / worker / browser
// conditions. The Bun worker keeps using the arrow-js backend (default).

import type { VgiBackendInfo } from "../types.js";

export const backend: VgiBackendInfo = { name: "flechette" };

// Schema / Field / DataType factories
export {
  schema,
  field,
  nullType,
  bool,
  int,
  int8, int16, int32, int64,
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
} from "./schema.js";

// Batch construction / iteration / utilities
export { emptyBatch } from "./empty.js";
export { batchFromRows, batchFromColumns, columnFromArray } from "./build.js";
export {
  iterRows,
  batchToScalarDict,
  batchToSecretDict,
  safeNumber,
  decodeDictValue,
} from "./iterate.js";
export { filterBatch } from "./filter.js";
export { projectSchema, projectBatch } from "./project.js";
export { readCanonicalValue } from "./canonical.js";
export { withBatchMetadata } from "./metadata.js";

// IPC
export {
  serializeSchema,
  deserializeSchema,
  serializeBatch,
  deserializeBatch,
} from "./ipc.js";

// Column statistics — sparse-union min/max via the codec/canonical path.
export { buildStatisticsBatch, type ColumnStatistics } from "./statistics.js";
