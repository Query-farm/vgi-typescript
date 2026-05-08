// flechette impl of column-statistics serialization.
//
// Non-empty stats (with real per-column min/max) require a sparse_union
// column with a per-row typeIds buffer — left as a follow-up because the
// shape we ship today (NDV-only stats from sqlite_stat1) skips min/max,
// and the empty-stats batch alone is enough to opt the C++ extension into
// the inlined-stats path.

import type { VgiBatch, VgiDataType } from "../types.js";
import { emptyBatch } from "./empty.js";
import {
  bool,
  field,
  int64,
  int8,
  schema,
  sparseUnion,
  uint64,
  utf8,
} from "./schema.js";

export interface ColumnStatistics {
  columnName: string;
  arrowType: VgiDataType;
  min: any;
  max: any;
  hasNull: boolean;
  hasNotNull: boolean;
  distinctCount: bigint | number | null;
  containsUnicode: boolean | null;
  maxStringLength: bigint | number | null;
}

// Canonical 8-column ColumnStatistics schema with a one-child sparse_union.
// flechette can't build a 0-length sparse-union whose child is Null (its
// columnBuilder rejects "Unsupported data type"); use Int8 as a structurally-
// equivalent placeholder. A 0-row batch carries no values, so the child type
// only matters for IPC schema serialization — the C++ extension treats any
// 0-row stats blob as "stats are authoritative, just empty" regardless of
// the union's child shape.
function emptyStatsSchema() {
  const placeholderChild = field("0", int8(), true);
  const minMaxUnion = sparseUnion([placeholderChild], [0]);
  return schema([
    field("column_name", utf8(), false),
    field("min", minMaxUnion, true),
    field("max", minMaxUnion, true),
    field("has_null", bool(), true),
    field("has_not_null", bool(), true),
    field("distinct_count", int64(), true),
    field("contains_unicode", bool(), true),
    field("max_string_length", uint64(), true),
  ]);
}

export function buildStatisticsBatch(stats: ColumnStatistics[]): VgiBatch {
  if (stats.length === 0) {
    return emptyBatch(emptyStatsSchema());
  }
  // Non-empty stats: needs sparse_union column with a per-row typeIds buffer.
  // flechette's `union(...)` plus `columnFromArray(values, unionType, {
  // typeIdForValue })` could implement this; deferred until we actually emit
  // populated min/max bounds (current NDV-only path uses the empty batch).
  throw new Error(
    "buildStatisticsBatch with non-empty stats is not yet implemented for the " +
      "flechette backend. If you hit this from a CF Workers deployment, file an " +
      "issue and we'll wire up the SparseUnion column builder.",
  );
}
