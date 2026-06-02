// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Column statistics: types and sparse-union wire serialization.
// Mirrors vgi-python's serialize_column_statistics in catalog_interface.py.
//
// The wire format is a single RecordBatch with schema
//   column_name: utf8
//   min: sparse_union<T0, T1, ...>
//   max: sparse_union<T0, T1, ...>
//   has_null: bool
//   has_not_null: bool
//   distinct_count: int64
//   contains_unicode: bool
//   max_string_length: uint64
// where Ti are the distinct Arrow types present among the stats' min/max values.
// A row's type_id picks which union child holds its real min/max; the other
// children carry null at that slot (sparse-union invariant).
//
// SparseUnion data construction differs significantly between arrow-js
// (typeIds buffer + per-type children built via makeData) and flechette
// (typeIdForValue closure on columnFromArray), so the actual batch
// assembly is delegated to the active backend via the facade.

import {
  type ColumnStatistics,
  buildStatisticsBatch,
  serializeBatch,
} from "../arrow/index.js";

export type { ColumnStatistics };

/**
 * Serialize column statistics to IPC bytes per vgi-python's wire format.
 * Returns an empty-stats batch when `stats` is empty (matching Python).
 */
export function serializeColumnStatistics(
  stats: ColumnStatistics[],
  _cacheMaxAgeSeconds?: number | null,
): Uint8Array {
  return serializeBatch(buildStatisticsBatch(stats));
}
