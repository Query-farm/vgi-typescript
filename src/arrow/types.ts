// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Backend-agnostic Arrow type surface used throughout vgi-typescript.
//
// The interfaces below are the *structural contract* every Arrow backend
// must satisfy. arrow-js (`Schema`/`RecordBatch`/`Vector`/`DataType`) and
// flechette (`Schema`/`Table`/`Column` plus plain-object types) both
// already match these shapes, so backends pass native values through
// without wrapping.

/** Numeric Arrow type discriminator. Values match the Arrow Type enum
 *  (Null=1, Int=2, Float=3, Binary=4, Utf8=5, Bool=6, Decimal=7,
 *  Date=8, Time=9, Timestamp=10, Interval=11, List=12, Struct=13,
 *  Union=14, FixedSizeBinary=15, FixedSizeList=16, Map=17, Duration=18,
 *  LargeBinary=19, LargeUtf8=20, Dictionary=-1). Both backends agree. */
export type VgiTypeId = number;

export interface VgiDataType {
  readonly typeId: VgiTypeId;
}

export interface VgiField {
  readonly name: string;
  readonly type: VgiDataType;
  readonly nullable: boolean;
  /** Always defined (possibly empty) so callers don't need null-checks.
   *  Both arrow-js Field.metadata and flechette's field.metadata are a
   *  Map; the field-factory in each backend ensures presence. */
  readonly metadata: Map<string, string>;
}

export interface VgiSchema {
  readonly fields: readonly VgiField[];
  readonly metadata: Map<string, string>;
}

/** Column view. Generic dropped: arrow-js parameterizes on DataType, flechette
 *  on value type, so the facade exposes an erased shape. Callers cast at the
 *  use site (the column's static schema makes the value type known there). */
export interface VgiColumn {
  readonly type: VgiDataType;
  readonly length: number;
  get(index: number): unknown;
  [Symbol.iterator](): Iterator<unknown>;
}

export interface VgiBatch {
  readonly schema: VgiSchema;
  readonly numRows: number;
  getChild(name: string): VgiColumn | null;
  getChildAt(index: number): VgiColumn | null;
}

export interface VgiBackendInfo {
  readonly name: "arrow-js" | "flechette";
}

/** Low-level handle for a single column's underlying Arrow Data. The shape
 *  differs per backend (arrow-js: a `Data` instance; flechette: the inner
 *  `Column.data[0]`-style object), so this is treated opaquely; it's only
 *  meant to be passed back into facade builders that know how to consume it. */
export type VgiColumnData = unknown;

/** A decoded union-typed value: which member is active (`tag`) and its `value`.
 *
 *  DuckDB `UNION` / Arrow union values are *tagged* — the discriminator (which
 *  member is present) lives in the union's per-row type code, not in the member
 *  value. A plain scalar read returns only the member value and drops that tag,
 *  so union values are decoded into this wrapper instead: `tag` is the active
 *  member's field name (or `null` for a null union) and `value` is its decoded
 *  canonical value. Mirrors vgi-python's `vgi.arguments.TaggedUnion`. */
export interface TaggedUnion {
  readonly tag: string | null;
  readonly value: unknown;
}
