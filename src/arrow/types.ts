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

/**
 * Column view over one column of a batch.
 *
 * The value type is erased by default: arrow-js parameterizes on DataType and
 * flechette on value type, so the facade cannot name one without picking a
 * backend. Supply `T` at the use site — the column's declared type makes it
 * known there — and the cast disappears:
 *
 * ```ts
 * const ns = batch.getChildAt(0)! as Iterable<bigint | null>;
 * for (const v of ns) { ... }        // v: bigint | null
 * ```
 *
 * A type parameter here would read better, but it cannot be had cheaply: both
 * backends' native batches are assigned to VgiBatch structurally, and a
 * concrete `get(): unknown` does not satisfy a generic `get(): T`. Adding one
 * would mean casting at ~25 internal sites to remove one cast in user code.
 *
 * **These are the backend's own values, not codec output.** `get()` and
 * iteration return whatever the Arrow implementation stores, which for some
 * types is not the value the SDK documents:
 *
 * | Arrow type       | here                          | {@link iterRows}      |
 * | ---------------- | ----------------------------- | --------------------- |
 * | `int64`          | `bigint`                      | `bigint`   (same)     |
 * | `utf8`, `float64`| `string`, `number`            | same                  |
 * | `decimal128`     | backend limbs (`DecimalBigNum`)| `bigint`, unscaled    |
 * | `timestamp[us]`  | millisecond `number`          | microsecond `bigint`  |
 * | `date32`         | millisecond `number`          | `Date`                |
 *
 * `repr: "raw"` on a function selects the *codec's* representation and has no
 * effect on this path.
 *
 * So: reach for `getChildAt` on integer, float, boolean, string and binary
 * columns, where it is the cheapest correct thing. For temporal, decimal and
 * nested types go through {@link iterRows}, which runs the codec.
 */
export interface VgiColumn {
  readonly type: VgiDataType;
  readonly length: number;
  get(index: number): unknown;
  [Symbol.iterator](): Iterator<unknown>;
}

export interface VgiBatch {
  readonly schema: VgiSchema;
  readonly numRows: number;
  /** See {@link VgiColumn} — this does NOT run the codec. */
  getChild(name: string): VgiColumn | null;
  /** See {@link VgiColumn} — this does NOT run the codec. */
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
