// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// arrow-js shape compatibility for the flechette backend.
//
// The facade's job is that a worker written once runs on either backend. Most
// of that is handled by re-exporting equivalent factories, but a few *methods
// and properties* are read directly off the objects the facade hands back —
// `batch.getChild(name)`, `batch.schema.fields[i].type` — and there the two
// libraries genuinely disagree:
//
//   arrow-js                      flechette
//   ----------------------------  ---------------------------------------
//   Vector#isValid(i)             Column has no validity accessor at all
//                                 (Batch#isValid exists, Column doesn't)
//   RecordBatch#slice(a, b)       Table has no row-range slice
//   Int type: `isSigned`          Int type: `signed`
//
// Worker code that reads any of those works on arrow-js and throws on
// flechette. Rather than push `backend.name === "flechette"` conditionals into
// shared code and into every example, the gap is closed here, once, on the
// flechette side — which is what the facade is for.
//
// `Column` and `Table` are classes flechette exports, so the first two are
// prototype augmentations (installed once, on import, and idempotent). Arrow
// *types* are plain object literals with no shared prototype, so `isSigned`
// has to be attached per instance — see `aliasIntSigned`, applied by the IPC
// decode path and by the facade's own type constructors.

import {
  Column,
  Table,
  batchType,
  interval as f_interval,
  IntervalUnit,
} from "@query-farm/flechette";

// ---------------------------------------------------------------------------
// Column#isValid — arrow-js's Vector#isValid(index)
// ---------------------------------------------------------------------------

/**
 * Per-row null check on a column, matching arrow-js's `Vector#isValid`.
 *
 * flechette pushes validity down to `Batch`, and a `Column` is a list of
 * batches with an `offsets` prefix-sum, so the column index has to be mapped
 * to (batch, batch-local index) the same way `Column#at` does. The common
 * single-batch case is dispatched without the search.
 */
function columnIsValid(this: any, index: number): boolean {
  const data: any[] = this.data;
  if (data.length === 1) return data[0].isValid(index);
  const offsets: Int32Array = this.offsets;
  // offsets is a sorted prefix sum of batch lengths; find the owning batch.
  let lo = 0;
  let hi = data.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (offsets[mid + 1] <= index) lo = mid + 1;
    else hi = mid;
  }
  const batch = data[lo];
  return batch ? batch.isValid(index - offsets[lo]) : false;
}

// ---------------------------------------------------------------------------
// Table#slice — arrow-js's RecordBatch#slice(begin, end)
// ---------------------------------------------------------------------------

/**
 * Row-range slice of a table, matching arrow-js's `RecordBatch#slice`.
 *
 * flechette has no row-slicing primitive: `Batch#slice` returns a plain value
 * array, not a batch, so there is nothing to rebuild a zero-copy view from.
 * We materialize the range through the same `columnFromValues` path the build
 * side uses, which keeps the schema (names, nullability, field metadata) and
 * the per-column types intact — the properties callers actually depend on.
 *
 * The dominant caller is `slice(0, 0)`: "give me an empty batch with this
 * exact schema", used to close a stream. That degenerates to a 0-row table and
 * costs nothing.
 */
function tableSlice(this: any, begin = 0, end?: number): any {
  const numRows: number = this.numRows;
  const lo = clampIndex(begin, numRows);
  const hi = end === undefined ? numRows : clampIndex(end, numRows);
  const n = Math.max(0, hi - lo);
  const children = this.children.map((col: any) =>
    new Column([sliceBatchless(col, lo, n)], col.type),
  );
  const out: any = new Table(this.schema, children);
  // Preserve the row-object factory (plain object vs. row proxy) the source
  // table was built with; `getFactory` reads it lazily, so a post-construction
  // assignment is enough.
  out.factory = this.factory;
  return out;
}

function clampIndex(i: number, len: number): number {
  const v = i < 0 ? len + i : i;
  return v < 0 ? 0 : v > len ? len : v;
}

/**
 * A minimal `Batch`-shaped view over an extracted value range.
 *
 * Values are read through `Column#at` (so dictionary decoding, decimals and
 * nested types all come out already resolved) and re-served through the same
 * `at`/`isValid`/`value`/`length`/`nullCount` surface the rest of flechette
 * consumes. That is enough for `Table#toArray`, `getChild(...).at(i)` and the
 * facade's own iteration; it is deliberately not an IPC-encodable batch, which
 * a slice is never used as.
 */
function sliceBatchless(col: any, offset: number, length: number): any {
  const values: any[] = new Array(length);
  const valid: boolean[] = new Array(length);
  let nullCount = 0;
  for (let i = 0; i < length; i++) {
    const v = col.at(offset + i);
    values[i] = v;
    const ok = v !== null && v !== undefined;
    valid[i] = ok;
    if (!ok) nullCount++;
  }
  return {
    type: col.type,
    length,
    nullCount,
    values,
    at: (i: number) => values[i] ?? null,
    value: (i: number) => values[i],
    isValid: (i: number) => valid[i] === true,
    slice: (s: number, e: number) => values.slice(s, e),
    [Symbol.iterator]: () => values[Symbol.iterator](),
  };
}

// ---------------------------------------------------------------------------
// Int type: `isSigned` alias for flechette's `signed`
// ---------------------------------------------------------------------------

/** Arrow `Type.Int` — the enum value is identical in both libraries. */
const TYPE_INT = 2;

/**
 * Attach arrow-js's `isSigned` to a flechette Int type, recursively through
 * children / dictionary / index types.
 *
 * Worker code branches on signedness to pick a promoted result type
 * (`examples/scalar.ts`'s `promoteForAddition`: `isSigned` false ⇒ Uint64).
 * On flechette that read is `undefined`, so a signed BIGINT promotes to UINT64
 * and the codec then rejects every negative value with
 * `codec[uint64]: value out of uint64 range` — a wrong *answer*, not just a
 * missing method, which is why this matters beyond ergonomics.
 *
 * The alias is a non-enumerable getter so it never shows up in structural
 * comparisons, IPC encoding or `Object.keys` — flechette keeps writing
 * `signed`, and this is purely a read-side view of it. Mutating in place is
 * safe: these objects are freshly built per decode.
 */
export function aliasIntSigned<T>(type: T): T {
  const t = type as any;
  if (t == null || typeof t !== "object") return type;
  if (t.typeId === TYPE_INT && !("isSigned" in t)) {
    Object.defineProperty(t, "isSigned", {
      get(this: any) {
        return this.signed;
      },
      enumerable: false,
      configurable: true,
    });
  }
  if (Array.isArray(t.children)) {
    for (const child of t.children) aliasIntSigned(child?.type ?? child);
  }
  if (t.dictionary) aliasIntSigned(t.dictionary);
  if (t.indices) aliasIntSigned(t.indices);
  return type;
}

/** Apply {@link aliasIntSigned} to every field type of a decoded schema. */
export function aliasSchemaIntSigned<T>(schema: T): T {
  const fields = (schema as any)?.fields;
  if (Array.isArray(fields)) {
    for (const f of fields) aliasIntSigned(f?.type);
  }
  return schema;
}

// ---------------------------------------------------------------------------
// MonthDayNano interval values — arrow-js's four-int32 representation
// ---------------------------------------------------------------------------

/**
 * Read a MonthDayNano interval as arrow-js does: an `Int32Array` of four words
 * `[months, days, nanos_lo, nanos_hi]`.
 *
 * flechette returns `Float64Array.of(months, days, Number(nanos))` instead —
 * a different length, a different element type, and a lossy nanosecond field
 * (`readInt64` coerces to a JS number and throws past 2^53). Worker code that
 * decodes an INTERVAL argument branches on the four-word layout, so on
 * flechette it fell through to a scalar path and died in `BigInt(NaN)` with
 * "RangeError: Not an integer".
 *
 * Reading the raw 16 bytes directly also drops the precision loss: the nanos
 * word pair is copied verbatim rather than routed through a double.
 */
function intervalMonthDayNanoValue(this: any, index: number): Int32Array {
  const bytes: Uint8Array = this.values;
  const base = index << 4;
  const out = new Int32Array(4);
  for (let w = 0; w < 4; w++) {
    const p = base + (w << 2);
    out[w] =
      bytes[p] | (bytes[p + 1] << 8) | (bytes[p + 2] << 16) | (bytes[p + 3] << 24);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Install (idempotent — this module is imported for its side effects).
// ---------------------------------------------------------------------------

const CP = Column.prototype as any;
if (typeof CP.isValid !== "function") CP.isValid = columnIsValid;

const TP = Table.prototype as any;
if (typeof TP.slice !== "function") TP.slice = tableSlice;

// The batch class is not exported by name; `batchType` resolves it from a type.
const IntervalMonthDayNanoBatch: any = batchType(
  f_interval(IntervalUnit.MONTH_DAY_NANO) as any,
  {},
);
if (IntervalMonthDayNanoBatch?.prototype && !IntervalMonthDayNanoBatch.prototype.__vgiArrowJsInterval) {
  IntervalMonthDayNanoBatch.prototype.value = intervalMonthDayNanoValue;
  IntervalMonthDayNanoBatch.prototype.__vgiArrowJsInterval = true;
}
