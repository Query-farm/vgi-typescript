// Microbenchmarks for scalar function compute paths.
//
// Establishes the baseline cost of the existing untyped `compute` path
// against a few alternatives, so any future typed-helper layer can be
// measured against numbers rather than intuition.
//
// Variants:
//   inline_pure         — for-loop on a plain bigint[], no Arrow at all.
//                         Floor: what raw JS multiplication costs.
//   col_get_only        — loops calling col.get(i) and stores it.
//                         Isolates the cost of going through Vector accessor.
//   untyped_current     — the exact pattern in examples/scalar.ts: gather
//                         column into a JS array via .push(), then .map()
//                         with defensive BigInt coercion. What we have today.
//   untyped_lean        — same shape but skips the array materialisation:
//                         loops col.get directly into a preallocated output.
//                         Shows headroom if we keep the untyped API but
//                         tighten its hot loop.
//   rowWise_sim         — minimal simulation of what `rowWise` would cost:
//                         decoder indirect call + null guard + row-object
//                         mutation + user-fn closure invocation per row.
//                         Models the proposed helper overhead.
//
// Each variant produces the same output (we cross-check). The numbers are
// "what does it take to multiply N int64s by a constant int64?" — the
// simplest possible scalar function, so the per-row overhead dominates.
//
// Run: bun run scripts/bench-scalar.ts

import {
  batchFromColumns,
  schema as makeSchema,
  field,
  int64,
  float64,
  struct,
  type VgiBatch,
  type VgiColumn,
} from "../src/arrow/index.js";

// ---------------------------------------------------------------------------
// Variants
// ---------------------------------------------------------------------------

function inlinePure(values: bigint[], factor: bigint): bigint[] {
  const n = values.length;
  const out = new Array<bigint>(n);
  for (let i = 0; i < n; i++) out[i] = values[i] * factor;
  return out;
}

function colGetOnly(batch: VgiBatch): bigint[] {
  const col = batch.getChildAt(0)!;
  const n = batch.numRows;
  const out: bigint[] = new Array(n);
  for (let i = 0; i < n; i++) out[i] = col.get(i) as bigint;
  return out;
}

// The exact pattern in examples/scalar.ts:multiply today.
function untypedCurrent(batch: VgiBatch, consts: { factor: unknown }): (bigint | null)[] {
  const factor = typeof consts.factor === "bigint" ? consts.factor : BigInt(consts.factor as number);
  const col = batch.getChildAt(0)!;
  const values: unknown[] = [];
  for (let i = 0; i < col.length; i++) values.push(col.get(i));
  return values.map((v: unknown) => {
    if (v === null || v === undefined) return null;
    const bigV = typeof v === "bigint" ? v : BigInt(v as number);
    return bigV * factor;
  });
}

// Same logic, no intermediate array, no defensive coercions (we know consts
// arrives as bigint and the column delivers bigint). This is what the
// current API could look like if we just tightened the body.
function untypedLean(batch: VgiBatch, consts: { factor: bigint }): (bigint | null)[] {
  const col = batch.getChildAt(0)!;
  const n = batch.numRows;
  const out: (bigint | null)[] = new Array(n);
  const f = consts.factor;
  for (let i = 0; i < n; i++) {
    const v = col.get(i);
    out[i] = v == null ? null : (v as bigint) * f;
  }
  return out;
}

// What a row-wise helper costs. Per-row work: col.get, null guard,
// decoder pointer call (identity here), object mutation, user-fn closure
// invocation. Models the helper layer without the type plumbing.
function rowWiseSim(batch: VgiBatch, consts: { factor: bigint }): (bigint | null)[] {
  const col = batch.getChildAt(0)!;
  const n = batch.numRows;
  const out: (bigint | null)[] = new Array(n);
  const row: { value: bigint } = { value: 0n };
  const decoder: (v: unknown) => unknown = (v) => v; // identity decoder
  const fn = (r: { value: bigint }, c: { factor: bigint }) => r.value * c.factor;
  for (let i = 0; i < n; i++) {
    const v = col.get(i);
    if (v == null) { out[i] = null; continue; }
    row.value = decoder(v) as bigint;
    out[i] = fn(row, consts);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Bench harness
// ---------------------------------------------------------------------------

function time(fn: () => unknown, iters: number): number[] {
  // Warmup
  for (let i = 0; i < 10; i++) fn();
  // Forces V8 to settle on a tier; skip the first two real samples too.
  const times: number[] = [];
  for (let i = 0; i < iters; i++) {
    const t0 = performance.now();
    fn();
    times.push(performance.now() - t0);
  }
  return times.sort((a, b) => a - b);
}

function stats(times: number[]) {
  const n = times.length;
  const sum = times.reduce((a, b) => a + b, 0);
  return {
    min: times[0],
    p50: times[Math.floor(n * 0.5)],
    p95: times[Math.floor(n * 0.95)],
    p99: times[Math.floor(n * 0.99)],
    mean: sum / n,
  };
}

function fmt(stats: { min: number; p50: number; p95: number; p99: number; mean: number }, rows: number) {
  const nsPerRow = (n: number) => ((n * 1e6) / rows).toFixed(0);
  return {
    min_ms: stats.min.toFixed(3),
    p50_ms: stats.p50.toFixed(3),
    p95_ms: stats.p95.toFixed(3),
    p99_ms: stats.p99.toFixed(3),
    mean_ms: stats.mean.toFixed(3),
    ns_per_row: nsPerRow(stats.p50),
  };
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

const SIZES = [2_048, 10_000, 100_000];
const ITERS_BY_SIZE: Record<number, number> = {
  2_048: 5000,
  10_000: 2000,
  100_000: 300,
};

function header(title: string, n: number, iters: number) {
  console.log(`\n========================================================`);
  console.log(`  ${title} — ${n.toLocaleString()} rows × ${iters} iters`);
  console.log(`========================================================`);
}

console.log(`Bun ${Bun.version} — node ${process.versions.node ?? "n/a"}`);
console.log(`Platform: ${process.platform}/${process.arch}`);

// ---------------------------------------------------------------------------
// Bench 1 — multiply (int64): the original. BigInt arithmetic dominates.
// ---------------------------------------------------------------------------

for (const n of SIZES) {
  const iters = ITERS_BY_SIZE[n];
  header("multiply int64", n, iters);

  const sample: bigint[] = new Array(n);
  for (let i = 0; i < n; i++) sample[i] = BigInt(i);
  const batch = batchFromColumns({ value: sample }, makeSchema([field("value", int64(), true)]));
  const consts = { factor: 2n };

  // Correctness cross-check on the first 10 rows.
  const ref = inlinePure(sample, consts.factor);
  const a = untypedCurrent(batch, consts);
  const b = untypedLean(batch, consts);
  const c = colGetOnly(batch);
  const d = rowWiseSim(batch, consts);
  for (let i = 0; i < Math.min(10, n); i++) {
    if (ref[i] !== a[i] || ref[i] !== b[i] || ref[i] !== d[i] || sample[i] !== c[i]) {
      throw new Error(`mismatch at i=${i}: ref=${ref[i]} a=${a[i]} b=${b[i]} c=${c[i]} d=${d[i]}`);
    }
  }

  console.table({
    inline_pure:     fmt(stats(time(() => inlinePure(sample, consts.factor), iters)), n),
    col_get_only:    fmt(stats(time(() => colGetOnly(batch),                   iters)), n),
    untyped_current: fmt(stats(time(() => untypedCurrent(batch, consts),       iters)), n),
    untyped_lean:    fmt(stats(time(() => untypedLean(batch, consts),          iters)), n),
    rowWise_sim:     fmt(stats(time(() => rowWiseSim(batch, consts),           iters)), n),
  });
}

// ---------------------------------------------------------------------------
// Bench 2 — multiply (float64): BigInt-free path. Tests whether the helper
// overhead becomes relatively *larger* once the arithmetic stops dominating.
// ---------------------------------------------------------------------------

function inlinePureF64(values: number[], factor: number): number[] {
  const n = values.length;
  const out = new Array<number>(n);
  for (let i = 0; i < n; i++) out[i] = values[i] * factor;
  return out;
}

function colGetOnlyF64(batch: VgiBatch): number[] {
  const col = batch.getChildAt(0)!;
  const n = batch.numRows;
  const out: number[] = new Array(n);
  for (let i = 0; i < n; i++) out[i] = col.get(i) as number;
  return out;
}

function untypedCurrentF64(batch: VgiBatch, consts: { factor: number }): (number | null)[] {
  const f = consts.factor;
  const col = batch.getChildAt(0)!;
  const values: unknown[] = [];
  for (let i = 0; i < col.length; i++) values.push(col.get(i));
  return values.map((v: unknown) => (v == null ? null : (v as number) * f));
}

function untypedLeanF64(batch: VgiBatch, consts: { factor: number }): (number | null)[] {
  const col = batch.getChildAt(0)!;
  const n = batch.numRows;
  const out: (number | null)[] = new Array(n);
  const f = consts.factor;
  for (let i = 0; i < n; i++) {
    const v = col.get(i);
    out[i] = v == null ? null : (v as number) * f;
  }
  return out;
}

function rowWiseSimF64(batch: VgiBatch, consts: { factor: number }): (number | null)[] {
  const col = batch.getChildAt(0)!;
  const n = batch.numRows;
  const out: (number | null)[] = new Array(n);
  const row: { value: number } = { value: 0 };
  const decoder: (v: unknown) => unknown = (v) => v;
  const fn = (r: { value: number }, c: { factor: number }) => r.value * c.factor;
  for (let i = 0; i < n; i++) {
    const v = col.get(i);
    if (v == null) { out[i] = null; continue; }
    row.value = decoder(v) as number;
    out[i] = fn(row, consts);
  }
  return out;
}

for (const n of SIZES) {
  const iters = ITERS_BY_SIZE[n];
  header("multiply float64", n, iters);

  const sample: number[] = new Array(n);
  for (let i = 0; i < n; i++) sample[i] = i * 0.5;
  const batch = batchFromColumns({ value: sample }, makeSchema([field("value", float64(), true)]));
  const consts = { factor: 2.0 };

  const ref = inlinePureF64(sample, consts.factor);
  const a = untypedCurrentF64(batch, consts);
  const b = untypedLeanF64(batch, consts);
  const d = rowWiseSimF64(batch, consts);
  for (let i = 0; i < Math.min(10, n); i++) {
    if (ref[i] !== a[i] || ref[i] !== b[i] || ref[i] !== d[i]) {
      throw new Error(`f64 mismatch at i=${i}`);
    }
  }

  console.table({
    inline_pure:     fmt(stats(time(() => inlinePureF64(sample, consts.factor), iters)), n),
    col_get_only:    fmt(stats(time(() => colGetOnlyF64(batch),                   iters)), n),
    untyped_current: fmt(stats(time(() => untypedCurrentF64(batch, consts),       iters)), n),
    untyped_lean:    fmt(stats(time(() => untypedLeanF64(batch, consts),          iters)), n),
    rowWise_sim:     fmt(stats(time(() => rowWiseSimF64(batch, consts),           iters)), n),
  });
}

// ---------------------------------------------------------------------------
// Bench 3 — sum_values with 5 int64 varargs columns. This is the path
// where the helper would otherwise allocate per-row arrays. Compares:
//   * untyped_current   — body iterates batch.getChildAt(col) inline (the
//                         existing examples/scalar.ts:sum_values pattern).
//   * rowWise_naive     — fresh `values: bigint[]` array allocated per row.
//   * rowWise_tight     — single `values` array reused across rows (mutated
//                         in place). What a smart helper implementation does.
// ---------------------------------------------------------------------------

const VARARG_COLS = 5;

function untypedCurrentSumN(batch: VgiBatch): (bigint | null)[] {
  const numCols = batch.schema.fields.length;
  const numRows = batch.numRows;
  const result: (bigint | null)[] = new Array(numRows);
  for (let row = 0; row < numRows; row++) {
    let sum: bigint = 0n;
    let first = true;
    let hasNull = false;
    for (let col = 0; col < numCols; col++) {
      const child = batch.getChildAt(col);
      const v = child ? child.get(row) : null;
      if (v == null) { hasNull = true; break; }
      const b = typeof v === "bigint" ? v : BigInt(v as number);
      if (first) { sum = b; first = false; }
      else sum += b;
    }
    result[row] = hasNull ? null : sum;
  }
  return result;
}

function rowWiseNaiveSumN(batch: VgiBatch): (bigint | null)[] {
  // Pre-resolve column readers once per batch (compileSpec equivalent).
  const numCols = batch.schema.fields.length;
  const cols: VgiColumn[] = new Array(numCols);
  for (let c = 0; c < numCols; c++) cols[c] = batch.getChildAt(c)!;

  const n = batch.numRows;
  const out: (bigint | null)[] = new Array(n);
  const fn = ({ values }: { values: bigint[] }) => {
    let s = 0n;
    for (let k = 0; k < values.length; k++) s += values[k];
    return s;
  };

  for (let i = 0; i < n; i++) {
    let hasNull = false;
    const values: bigint[] = new Array(numCols);  // <-- new array per row
    for (let k = 0; k < numCols; k++) {
      const v = cols[k].get(i);
      if (v == null) { hasNull = true; break; }
      values[k] = v as bigint;
    }
    if (hasNull) { out[i] = null; continue; }
    out[i] = fn({ values });
  }
  return out;
}

function rowWiseTightSumN(batch: VgiBatch): (bigint | null)[] {
  const numCols = batch.schema.fields.length;
  const cols: VgiColumn[] = new Array(numCols);
  for (let c = 0; c < numCols; c++) cols[c] = batch.getChildAt(c)!;

  const n = batch.numRows;
  const out: (bigint | null)[] = new Array(n);
  const row: { values: bigint[] } = { values: new Array(numCols) }; // <-- one allocation
  const fn = ({ values }: { values: bigint[] }) => {
    let s = 0n;
    for (let k = 0; k < values.length; k++) s += values[k];
    return s;
  };

  for (let i = 0; i < n; i++) {
    let hasNull = false;
    for (let k = 0; k < numCols; k++) {
      const v = cols[k].get(i);
      if (v == null) { hasNull = true; break; }
      row.values[k] = v as bigint;  // <-- mutates the reused array
    }
    if (hasNull) { out[i] = null; continue; }
    out[i] = fn(row);
  }
  return out;
}

for (const n of SIZES) {
  const iters = ITERS_BY_SIZE[n];
  header(`sum_values ×${VARARG_COLS} int64 varargs`, n, iters);

  // Build N columns of int64 with deterministic values.
  const columns: Record<string, bigint[]> = {};
  const fields = [];
  for (let c = 0; c < VARARG_COLS; c++) {
    const name = `c${c}`;
    const arr: bigint[] = new Array(n);
    for (let i = 0; i < n; i++) arr[i] = BigInt(i + c);
    columns[name] = arr;
    fields.push(field(name, int64(), true));
  }
  const batch = batchFromColumns(columns, makeSchema(fields));

  const a = untypedCurrentSumN(batch);
  const b = rowWiseNaiveSumN(batch);
  const c = rowWiseTightSumN(batch);
  for (let i = 0; i < Math.min(10, n); i++) {
    if (a[i] !== b[i] || a[i] !== c[i]) {
      throw new Error(`varargs mismatch at i=${i}: a=${a[i]} b=${b[i]} c=${c[i]}`);
    }
  }

  console.table({
    untyped_current: fmt(stats(time(() => untypedCurrentSumN(batch), iters)), n),
    rowWise_naive:   fmt(stats(time(() => rowWiseNaiveSumN(batch),   iters)), n),
    rowWise_tight:   fmt(stats(time(() => rowWiseTightSumN(batch),   iters)), n),
  });
}

// ---------------------------------------------------------------------------
// Bench 4 — geo_distance with two {lat, lon} struct columns. Compares the
// existing extractStructLatLon pattern against a typed-helper-style
// version that lets the struct cell quack as `{lat, lon}` directly.
// ---------------------------------------------------------------------------

const POINT_TYPE = struct([field("lat", float64(), true), field("lon", float64(), true)]);

function untypedCurrentGeo(batch: VgiBatch): (number | null)[] {
  const p1Col = batch.getChildAt(0)!;
  const p2Col = batch.getChildAt(1)!;
  const n = batch.numRows;
  const out: (number | null)[] = new Array(n);
  for (let i = 0; i < n; i++) {
    const p1 = p1Col.get(i);
    const p2 = p2Col.get(i);
    if (p1 == null || p2 == null) { out[i] = null; continue; }
    // Mirrors extractStructLatLon: defensive property + .get fallback.
    const a1: any = p1;
    const a2: any = p2;
    const la1 = Number(a1.lat ?? a1.get?.("lat"));
    const lo1 = Number(a1.lon ?? a1.get?.("lon"));
    const la2 = Number(a2.lat ?? a2.get?.("lat"));
    const lo2 = Number(a2.lon ?? a2.get?.("lon"));
    out[i] = Math.sqrt((la2 - la1) ** 2 + (lo2 - lo1) ** 2);
  }
  return out;
}

function rowWiseSimGeo(batch: VgiBatch): (number | null)[] {
  const p1Col = batch.getChildAt(0)!;
  const p2Col = batch.getChildAt(1)!;
  const n = batch.numRows;
  const out: (number | null)[] = new Array(n);
  // Helper-style: a user fn that trusts the typed shape `{lat: number; lon: number}`.
  const fn = (
    { p1, p2 }: { p1: { lat: number; lon: number }; p2: { lat: number; lon: number } },
  ) => Math.sqrt((p2.lat - p1.lat) ** 2 + (p2.lon - p1.lon) ** 2);
  const row: { p1: any; p2: any } = { p1: null, p2: null };
  for (let i = 0; i < n; i++) {
    const p1 = p1Col.get(i);
    const p2 = p2Col.get(i);
    if (p1 == null || p2 == null) { out[i] = null; continue; }
    row.p1 = p1;
    row.p2 = p2;
    out[i] = fn(row);
  }
  return out;
}

for (const n of SIZES) {
  const iters = ITERS_BY_SIZE[n];
  header("geo_distance struct", n, iters);

  // Build two struct columns of {lat, lon}.
  const p1Data: { lat: number; lon: number }[] = new Array(n);
  const p2Data: { lat: number; lon: number }[] = new Array(n);
  for (let i = 0; i < n; i++) {
    p1Data[i] = { lat: i * 0.1, lon: i * 0.2 };
    p2Data[i] = { lat: i * 0.1 + 1, lon: i * 0.2 + 1 };
  }
  const batch = batchFromColumns(
    { p1: p1Data, p2: p2Data },
    makeSchema([field("p1", POINT_TYPE, true), field("p2", POINT_TYPE, true)]),
  );

  const a = untypedCurrentGeo(batch);
  const b = rowWiseSimGeo(batch);
  for (let i = 0; i < Math.min(10, n); i++) {
    if (a[i] !== b[i]) throw new Error(`geo mismatch at i=${i}: ${a[i]} vs ${b[i]}`);
  }

  console.table({
    untyped_current: fmt(stats(time(() => untypedCurrentGeo(batch), iters)), n),
    rowWise_sim:     fmt(stats(time(() => rowWiseSimGeo(batch),     iters)), n),
  });
}
