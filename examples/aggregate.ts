// Example aggregate function implementations.
// Ports vgi_count, vgi_sum, vgi_avg from vgi-python/vgi/examples/aggregate.py.

import { Schema, Field, Int64, Float64, RecordBatch } from "@query-farm/apache-arrow";
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

export const aggregateFunctions: VgiFunction[] = [vgi_count, vgi_sum, vgi_avg];
