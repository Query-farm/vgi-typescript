// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// flechette impl of column-statistics serialization.
//
// The empty-stats batch (NDV-only path, no min/max) ships a one-child
// sparse_union placeholder. Non-empty stats build a real sparse_union<T0,..>
// min/max column whose per-row type code selects the child holding that row's
// value — routed through the codec (rich -> canonical) and the canonical writer
// so a date/timestamp/decimal min/max is represented identically to ordinary
// column data and identically to the arrow-js backend.

import {
  columnFromArray as f_columnFromArray,
  union as f_union,
  field as f_field,
  Table,
  UnionMode,
  type Column,
} from "@query-farm/flechette";
import type { VgiBatch, VgiDataType } from "../types.js";
import { emptyBatch } from "./empty.js";
import { codecFor } from "../codec/registry.js";
import { prepareForFlechette } from "./canonical.js";
import { toFlechetteType } from "./normalize-type.js";
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

const COLUMN_OPTS = {
  useBigInt: true,
  useBigIntTimestamp: true,
  useDecimalInt: true,
} as const;

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

// Stable, backend-agnostic type key. flechette types are plain objects whose
// `.toString()` is "[object Object]" for every type (collapsing date32/date64/
// timestamp/decimal into ONE union child), so dispatch on the structural fields
// instead: typeId + unit + bitWidth + precision/scale + byteWidth.
function typeKey(t: VgiDataType): string {
  const a = t as any;
  return [
    t.typeId,
    a.unit ?? "",
    a.bitWidth ?? "",
    a.precision ?? "",
    a.scale ?? "",
    a.byteWidth ?? "",
  ].join(":");
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

/**
 * Build a flechette Column for `type` from RICH values, routed through the codec
 * (rich -> canonical) and the same per-type preparation the public canonical
 * writer uses. Keeps stat values byte-identical to ordinary column data.
 */
function buildTypedColumn(values: any[], type: VgiDataType): Column<any> {
  const codec = codecFor(type);
  const canonical = values.map((v) => codec.richToCanonical(v));
  const prepared = prepareForFlechette(type, canonical);
  return f_columnFromArray(prepared, toFlechetteType(type) as any, COLUMN_OPTS);
}

/**
 * Build the sparse-union min/max Column. `rowTypeCodes[i]` selects which child
 * (one per distinct Arrow type) holds row i's value; the builder fills the
 * other children with null at that slot (sparse-union invariant). Values are
 * routed through codec rich -> canonical -> flechette prep so each child sees
 * exactly what it would for ordinary column data.
 */
function buildUnionColumn(
  values: any[],
  rowTypeCodes: number[],
  typeOrder: VgiDataType[],
): Column<any> {
  const typeIds = typeOrder.map((_t, i) => i);
  const children = typeOrder.map((t, code) =>
    f_field(String(code), toFlechetteType(t) as any, true),
  );
  // Per-row prepared value (only the selected child consumes it; non-selected
  // children get null from the SparseUnionBuilder).
  const prepared = values.map((v, i) => {
    if (v == null) return null;
    const type = typeOrder[rowTypeCodes[i]];
    const codec = codecFor(type);
    return prepareForFlechette(type, [codec.richToCanonical(v)])[0];
  });
  const unionType = f_union(
    UnionMode.Sparse,
    children as any,
    typeIds,
    (_value: any, index: number) => rowTypeCodes[index],
  );
  return f_columnFromArray(prepared, unionType as any, COLUMN_OPTS);
}

export function buildStatisticsBatch(stats: ColumnStatistics[]): VgiBatch {
  if (stats.length === 0) {
    return emptyBatch(emptyStatsSchema());
  }

  // Group distinct Arrow types in stable insertion order to assign type codes.
  const typeOrder: VgiDataType[] = [];
  const typeIdMap = new Map<string, number>();
  const rowTypeCodes: number[] = [];
  for (const s of stats) {
    const key = typeKey(s.arrowType);
    let code = typeIdMap.get(key);
    if (code === undefined) {
      code = typeOrder.length;
      typeIdMap.set(key, code);
      typeOrder.push(s.arrowType);
    }
    rowTypeCodes.push(code);
  }

  const minCol = buildUnionColumn(
    stats.map((s) => s.min),
    rowTypeCodes,
    typeOrder,
  );
  const maxCol = buildUnionColumn(
    stats.map((s) => s.max),
    rowTypeCodes,
    typeOrder,
  );

  const minMaxUnionType = minCol.type;

  const childCols: Column<any>[] = [
    buildTypedColumn(stats.map((s) => s.columnName), utf8()),
    minCol,
    maxCol,
    buildTypedColumn(stats.map((s) => s.hasNull), bool()),
    buildTypedColumn(stats.map((s) => s.hasNotNull), bool()),
    buildTypedColumn(
      stats.map((s) => (s.distinctCount == null ? null : BigInt(s.distinctCount))),
      int64(),
    ),
    buildTypedColumn(stats.map((s) => s.containsUnicode), bool()),
    buildTypedColumn(
      stats.map((s) => (s.maxStringLength == null ? null : BigInt(s.maxStringLength))),
      uint64(),
    ),
  ];

  const fields = [
    f_field("column_name", toFlechetteType(utf8()) as any, false),
    f_field("min", minMaxUnionType as any, true),
    f_field("max", minMaxUnionType as any, true),
    f_field("has_null", toFlechetteType(bool()) as any, true),
    f_field("has_not_null", toFlechetteType(bool()) as any, true),
    f_field("distinct_count", toFlechetteType(int64()) as any, true),
    f_field("contains_unicode", toFlechetteType(bool()) as any, true),
    f_field("max_string_length", toFlechetteType(uint64()) as any, true),
  ];
  const flechSchema = { version: 5, endianness: 0, fields, metadata: null };
  return new Table(flechSchema as any, childCols) as unknown as VgiBatch;
}
