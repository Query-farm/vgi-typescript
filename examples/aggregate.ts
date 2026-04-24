// Example aggregate function implementations.
// Ports vgi_count, vgi_sum, vgi_avg from vgi-python/vgi/examples/aggregate.py.

import { Schema, Field, Int64, Float64, Utf8, Decimal, DataType, Null, RecordBatch } from "@query-farm/apache-arrow";
import { defineAggregate } from "../src/functions/aggregate.js";
import { batchFromColumns } from "../src/util/arrow.js";
import type { VgiFunction } from "../src/index.js";

// ============================================================================
// vgi_count — nullary aggregate (no input columns). Counts rows per group.
// NullHandling.SPECIAL means DuckDB calls update() for every row including
// ones with NULL inputs (there are none here since the aggregate has no input
// columns, but the flag is the Python parity for counting).
// ============================================================================

interface CountState { count: bigint; }

const vgi_count = defineAggregate<Record<string, never>, CountState>({
  name: "vgi_count",
  description: "Count rows",
  outputType: new Int64(),
  nullHandling: "SPECIAL",
  initialState: () => ({ count: 0n }),
  update: ({ groupIds, ensureState }) => {
    // No column args — just tick per row. Every seen group gets state, since
    // NullHandling.SPECIAL means DuckDB sends rows even for NULL inputs.
    for (const gid of groupIds) {
      ensureState(gid).count += 1n;
    }
  },
  combine: (src, tgt) => ({ count: src.count + tgt.count }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (bigint | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.count : 0n;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

// ============================================================================
// vgi_sum — single int64 input. NullHandling.DEFAULT means DuckDB skips NULL
// inputs, so update() never sees them (all-NULL group = state stays at initial
// = group never added to states map = finalize sees undefined = returns NULL).
// ============================================================================

interface SumState { total: bigint; }

const vgi_sum = defineAggregate<{ value: number }, SumState>({
  name: "vgi_sum",
  description: "Sum integer values",
  args: { value: new Int64() },
  outputType: new Int64(),
  nullHandling: "DEFAULT",
  initialState: () => ({ total: 0n }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol != null && !valueCol.isValid(i)) continue;
      const v = valueCol?.get(i);
      if (v == null) continue;
      // Allocate lazily so all-NULL groups get no state, and finalize()
      // returns SQL NULL for those groups.
      const s = ensureState(groupIds[i]);
      s.total += typeof v === "bigint" ? v : BigInt(v);
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (bigint | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.total : null;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

// ============================================================================
// vgi_avg — two-field state (sum + count), NULL-skipping, returns FLOAT64.
// ============================================================================

interface AvgState { total: number; count: bigint; }

const vgi_avg = defineAggregate<{ value: number }, AvgState>({
  name: "vgi_avg",
  description: "Average of integer values",
  args: { value: new Int64() },
  outputType: new Float64(),
  nullHandling: "DEFAULT",
  initialState: () => ({ total: 0, count: 0n }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol != null && !valueCol.isValid(i)) continue;
      const v = valueCol?.get(i);
      if (v == null) continue;
      const s = ensureState(groupIds[i]);
      s.total += typeof v === "bigint" ? Number(v) : Number(v);
      s.count += 1n;
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total, count: src.count + tgt.count }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (number | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      if (s == null || s.count === 0n) return null;
      return s.total / Number(s.count);
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

// ============================================================================
// vgi_sum_all — varargs aggregate. Sums every value across any number of
// numeric columns. Single `columns` varargs param means the call site may
// pass 1..N columns and update() receives each as a separate entry in the
// columns array.
// ============================================================================

interface SumAllState { total: number; }

const vgi_sum_all = defineAggregate<{ columns: number }, SumAllState>({
  name: "vgi_sum_all",
  description: "Sum all numeric columns",
  args: { columns: new Float64() },
  varargs: ["columns"],
  outputType: new Float64(),
  nullHandling: "DEFAULT",
  initialState: () => ({ total: 0 }),
  update: ({ groupIds, columns, ensureState }) => {
    const n = groupIds.length;
    // Precompute per-column decoders. DECIMAL columns arrive as raw
    // integers; scale them back to float. Integer columns are converted
    // via Number(). Matches Python's implicit DECIMAL → float on
    // pa.Array.to_pylist().
    const decoders = columns.map((col: any) => makeNumericDecoder(col?.type));
    for (let i = 0; i < n; i++) {
      let rowTotal = 0;
      let anyNonNull = false;
      for (let k = 0; k < columns.length; k++) {
        const col = columns[k];
        if (col == null || !col.isValid(i)) continue;
        const v = col.get(i);
        if (v == null) continue;
        anyNonNull = true;
        rowTotal += decoders[k](v);
      }
      if (anyNonNull) {
        ensureState(groupIds[i]).total += rowTotal;
      }
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (number | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.total : null;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

// Build a numeric decoder matching the column's Arrow type. Handles the
// common cases vgi_sum_all / vgi_weighted_sum / vgi_percentile need:
// - Int/BigInt: Number() coerce.
// - Decimal(p,s): Arrow stores the unscaled integer; scale back by 10^-s
//   to recover the logical float (pyarrow's to_pylist does this implicitly,
//   which is why the Python versions of these aggregates don't need to).
// - Float: passthrough.
function makeNumericDecoder(type: DataType | undefined): (v: any) => number {
  if (type && DataType.isDecimal(type)) {
    const scale = (type as Decimal).scale;
    const divisor = Math.pow(10, scale);
    return (v: any) => {
      if (typeof v === "bigint") return Number(v) / divisor;
      if (v instanceof Uint8Array) {
        // 128-bit decimal stored as 16 bytes — assemble as BigInt
        let n = 0n;
        for (let i = v.length - 1; i >= 0; i--) n = (n << 8n) | BigInt(v[i]);
        // Sign-extend: if top bit of MSB is set, it's negative
        if (v.length > 0 && (v[v.length - 1] & 0x80)) {
          n -= 1n << BigInt(v.length * 8);
        }
        return Number(n) / divisor;
      }
      return Number(v) / divisor;
    };
  }
  return (v: any) => (typeof v === "bigint" ? Number(v) : Number(v));
}

// ============================================================================
// vgi_listagg — order-dependent string concatenation with comma separator.
// Accumulates strings as they arrive, joined by ",". Null inputs are skipped
// (NullHandling.DEFAULT). combine() appends source.values after target.values
// to preserve left-to-right ordering within a thread.
// ============================================================================

interface ListAggState { values: string; }

const vgi_listagg = defineAggregate<{ value: string }, ListAggState>({
  name: "vgi_listagg",
  description: "Concatenate strings with comma separator",
  args: { value: new Utf8() },
  outputType: new Utf8(),
  nullHandling: "DEFAULT",
  initialState: () => ({ values: "" }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol == null || !valueCol.isValid(i)) continue;
      const v = valueCol.get(i);
      if (v == null) continue;
      const s = ensureState(groupIds[i]);
      s.values = s.values ? s.values + "," + String(v) : String(v);
    }
  },
  combine: (src, tgt) => {
    if (src.values && tgt.values) return { values: tgt.values + "," + src.values };
    return { values: tgt.values || src.values };
  },
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (string | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.values : null;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

// ============================================================================
// vgi_weighted_sum — multi-arg aggregate. Sum of value*weight per group.
// ============================================================================

interface WeightedSumState { total: number; }

const vgi_weighted_sum = defineAggregate<{ value: number; weight: number }, WeightedSumState>({
  name: "vgi_weighted_sum",
  description: "Weighted sum of values",
  args: { value: new Float64(), weight: new Float64() },
  outputType: new Float64(),
  nullHandling: "DEFAULT",
  initialState: () => ({ total: 0 }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const weightCol = columns[1];
    const decV = makeNumericDecoder(valueCol?.type);
    const decW = makeNumericDecoder(weightCol?.type);
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol == null || weightCol == null) continue;
      if (!valueCol.isValid(i) || !weightCol.isValid(i)) continue;
      const v = valueCol.get(i);
      const w = weightCol.get(i);
      if (v == null || w == null) continue;
      ensureState(groupIds[i]).total += decV(v) * decW(w);
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (number | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.total : null;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

// ============================================================================
// vgi_generic_sum — ANY-typed input, return type mirrors input. The onBind
// hook reads the input column's Arrow type from the bind input_schema and
// advertises the same type as output so `typeof(vgi_generic_sum(i::BIGINT))`
// is BIGINT rather than the default DOUBLE. State keeps a double total
// internally for precision; finalize() coerces back to the declared type.
// ============================================================================

interface GenericSumState { total: number; }

const vgi_generic_sum = defineAggregate<{ value: any }, GenericSumState>({
  name: "vgi_generic_sum",
  description: "Sum any numeric type",
  args: { value: new Null() },  // Null = ANY
  outputType: new Float64(),
  nullHandling: "DEFAULT",
  onBind: (params) => {
    // Output type mirrors the input column's declared type.
    if (params.inputSchema && params.inputSchema.fields.length > 0) {
      return params.inputSchema.fields[0].type;
    }
    return new Float64();
  },
  initialState: () => ({ total: 0 }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const dec = makeNumericDecoder(valueCol?.type);
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol == null || !valueCol.isValid(i)) continue;
      const v = valueCol.get(i);
      if (v == null) continue;
      ensureState(groupIds[i]).total += dec(v);
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const outField = outputSchema.fields[0];
    const isInt = DataType.isInt(outField.type) && (outField.type as any).bitWidth === 64;
    const results: (any | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      if (s == null) return null;
      return isInt ? BigInt(Math.round(s.total)) : s.total;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

// ============================================================================
// vgi_percentile(value, percentile) — approximate percentile. Demonstrates
// const-parameter folding: `percentile` is constant-folded by DuckDB at bind
// time and arrives through bindParams.args rather than as a per-row column.
// Internally we keep all values as a sorted array, then index at finalize.
// ============================================================================

interface PercentileState { values: number[]; }

const vgi_percentile = defineAggregate<{ value: number; percentile: number }, PercentileState>({
  name: "vgi_percentile",
  description: "Approximate percentile (demonstrates ConstParam)",
  args: { value: new Float64(), percentile: new Float64() },
  constParams: ["percentile"],
  argDefaults: { percentile: 0.5 },
  outputType: new Float64(),
  nullHandling: "DEFAULT",
  initialState: () => ({ values: [] }),
  update: ({ groupIds, columns, ensureState }) => {
    // Only one column reaches update — `value`. `percentile` is const-folded
    // away by DuckDB at bind (stored in bindParams.args for finalize).
    const valueCol = columns[0];
    const dec = makeNumericDecoder(valueCol?.type);
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol == null || !valueCol.isValid(i)) continue;
      const v = valueCol.get(i);
      if (v == null) continue;
      ensureState(groupIds[i]).values.push(dec(v));
    }
  },
  combine: (src, tgt) => ({ values: tgt.values.concat(src.values) }),
  finalize: ({ groupIds, states, outputSchema, args }) => {
    const pct = args.percentile ?? 0.5;
    const results: (number | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      if (s == null || s.values.length === 0) return null;
      const sorted = [...s.values].sort((a, b) => a - b);
      const idx = Math.min(Math.floor(pct * sorted.length), sorted.length - 1);
      return sorted[idx];
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

export const aggregateFunctions: VgiFunction[] = [
  vgi_count, vgi_sum, vgi_avg, vgi_sum_all, vgi_listagg, vgi_weighted_sum, vgi_generic_sum,
  vgi_percentile,
];
