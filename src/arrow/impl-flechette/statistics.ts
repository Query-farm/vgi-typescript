// flechette impl of column-statistics serialization. CF Workers don't
// typically request column statistics — implement on demand. Throwing a
// clear "not implemented" beats silent corruption.

import type { VgiBatch, VgiDataType } from "../types.js";

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

export function buildStatisticsBatch(_stats: ColumnStatistics[]): VgiBatch {
  // Wire format requires a sparse_union column with a per-row typeIds buffer.
  // flechette's `union(...)` plus `columnFromArray(values, unionType, {
  // typeIdForValue })` could implement this; left as a follow-up since CF
  // Workers consumers haven't needed table_function_statistics yet.
  throw new Error(
    "buildStatisticsBatch is not yet implemented for the flechette backend. " +
      "If you hit this from a CF Workers deployment, file an issue and we'll wire " +
      "up the SparseUnion column builder.",
  );
}
