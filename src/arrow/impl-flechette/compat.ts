// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// arrow-js shape compatibility for the flechette backend.
//
// The facade's job is that a worker written once runs on either backend. Most
// of that is handled by re-exporting equivalent factories, but a few *methods
// and values* are read directly off the objects the facade hands back —
// `batch.getChild(name)`, `batch.slice(…)`, an INTERVAL cell — and there the
// two libraries genuinely disagree:
//
//   arrow-js                       flechette
//   -----------------------------  --------------------------------------
//   Vector#isValid(i)              Column has no validity accessor at all
//                                  (Batch#isValid exists, Column doesn't)
//   RecordBatch#slice(a, b)        Table has no row-range slice
//   MonthDayNano → Int32Array(4)   → Float64Array(3), nanos via a double
//
// Worker code that touches any of those works on arrow-js and breaks on
// flechette. Rather than push `backend.name === "flechette"` conditionals into
// shared code and into every example, the gap is closed here, once, on the
// flechette side — which is what the facade is for.
//
// `Column` and `Table` are classes flechette exports, so these are prototype
// augmentations, installed on import and idempotent. The Int `isSigned` alias
// lives in ./int-signed.ts instead: Arrow types are plain object literals with
// no shared prototype, so it has to be applied per instance.

import {
  Column,
  Table,
  batchType,
  columnFromArray as f_columnFromArray,
  interval as f_interval,
  IntervalUnit,
} from "@query-farm/flechette";

import { prepareForFlechette } from "./canonical.js";

/** Extraction options the rest of the backend builds columns with. */
const COLUMN_OPTS = {
  useBigInt: true,
  useBigIntTimestamp: true,
  useDecimalInt: true,
} as const;

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
  if (data.length === 1) return batchIsValid(data[0], index);
  const offsets: Int32Array = this.offsets;
  // offsets is a sorted prefix sum of batch lengths; find the owning batch.
  let lo = 0;
  let hi = data.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if (offsets[mid + 1] <= index) lo = mid + 1;
    else hi = mid;
  }
  return batchIsValid(data[lo], index - offsets[lo]);
}

/**
 * `Batch#isValid` guarded for the all-valid case.
 *
 * flechette only ever calls `Batch#isValid` from `Batch#at`, and the batch
 * constructor short-circuits `at` when there are no nulls — so `isValid` is
 * never reached with an absent validity bitmap *internally*, and it does not
 * guard for one: `decodeBit(undefined, i)` reads `undefined[…]` and returns
 * false. Exposing it through `Column#isValid` walks straight into that, and a
 * fully-populated column reads back as entirely NULL — which is how every
 * aggregate returned NULL instead of a sum.
 */
function batchIsValid(batch: any, index: number): boolean {
  if (!batch) return false;
  if (!batch.nullCount || !batch.validity || batch.validity.length === 0) return true;
  return batch.isValid(index);
}

// ---------------------------------------------------------------------------
// Table#slice — arrow-js's RecordBatch#slice(begin, end)
// ---------------------------------------------------------------------------

/**
 * Row-range slice of a table, matching arrow-js's `RecordBatch#slice`.
 *
 * flechette has no row-slicing primitive: `Batch#slice` returns a plain value
 * array, not a batch, so there is nothing to rebuild a zero-copy view from.
 * The range is therefore materialized back through `columnFromArray` — the
 * same builder the write path uses — which yields real batches with real
 * validity/offset/value buffers. That matters: a sliced batch is routinely
 * handed straight back to `tableToIPC` (an exchange round replying with
 * `batch.slice(0, 0)` to close a stream), and a hand-rolled batch-shaped
 * object would fault inside the encoder.
 *
 * Values are read through `Column#at`, so they come out decoded; they are run
 * back through `prepareForFlechette` to restore the shapes the builder wants
 * (Date objects, Maps, three-word intervals).
 *
 * The dominant caller is `slice(0, 0)`: "give me an empty batch with this
 * exact schema". That degenerates to `columnFromArray([], type)` and costs
 * nothing.
 */
function tableSlice(this: any, begin = 0, end?: number): any {
  const numRows: number = this.numRows;
  const lo = clampIndex(begin, numRows);
  const hi = end === undefined ? numRows : clampIndex(end, numRows);
  const n = Math.max(0, hi - lo);
  const children = this.children.map((col: any) => {
    const values: unknown[] = new Array(n);
    for (let i = 0; i < n; i++) values[i] = col.at(lo + i) ?? null;
    return f_columnFromArray(prepareForFlechette(col.type, values), col.type, COLUMN_OPTS);
  });
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
 * word pair is copied verbatim rather than routed through a double. The
 * matching write-side conversion lives in ./canonical.ts.
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
if (
  IntervalMonthDayNanoBatch?.prototype &&
  !IntervalMonthDayNanoBatch.prototype.__vgiArrowJsInterval
) {
  IntervalMonthDayNanoBatch.prototype.value = intervalMonthDayNanoValue;
  IntervalMonthDayNanoBatch.prototype.__vgiArrowJsInterval = true;
}
