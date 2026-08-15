// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// sum is the aggregate example for the vgi-typescript documentation.
//
// An aggregate folds many rows into one value per GROUP BY group. It runs in
// four phases, and the split is what lets DuckDB parallelise it:
//
//   - initialState — the identity value for a group (0 for a sum).
//   - update       — fold a batch of rows into per-group state. Runs in every
//                    worker, over that worker's share of the rows.
//   - combine      — merge two partial states for the same group.
//   - finalize     — turn state into one output row per group.
//
//   bun run sum.ts
//   # then, in a Haybarn shell:
//   ATTACH 'agg' (TYPE vgi, LOCATION 'bun run /abs/path/sum.ts');
//   SELECT category, agg.vgi_sum(value) FROM t GROUP BY category;

import { Worker, defineAggregate, batchFromColumns, int } from "@query-farm/vgi";

// Per-group accumulator. Unlike the Go and Python SDKs there is nothing to
// register: state stays inside this process, so it is an ordinary JS object.
interface SumState {
  total: bigint;
}

export const vgiSum = defineAggregate<{ value: bigint }, SumState>({
  name: "vgi_sum",
  description: "Sums a BIGINT column per group",
  args: { value: int },
  outputType: int,

  // DEFAULT means DuckDB skips NULL inputs, so update() never sees one. That
  // is what makes SUM over an all-NULL group return NULL rather than 0 — see
  // the lazy ensureState below.
  nullHandling: "DEFAULT",

  initialState: () => ({ total: 0n }),

  update: ({ groupIds, columns, ensureState }) => {
    const values = columns[0];
    for (let i = 0; i < groupIds.length; i++) {
      const v = values?.get(i);
      if (v == null) continue;
      // Allocate only when a row genuinely contributes. A group that never
      // reaches ensureState has no state at finalize, and emits NULL.
      ensureState(groupIds[i]).total += typeof v === "bigint" ? v : BigInt(v);
    }
  },

  // Must be associative and commutative: DuckDB decides how many workers run
  // and in what order their partials merge.
  combine: (src, tgt) => ({ total: src.total + tgt.total }),

  // Emits exactly one row per group id, in the order given.
  finalize: ({ groupIds, states, outputSchema }) => {
    const results = groupIds.map((gid) => states.get(gid)?.total ?? null);
    return batchFromColumns({ result: results }, outputSchema);
  },
});

export const worker = new Worker({
  catalog: {
    name: "agg",
    comment: "Documentation example: a distributed aggregate",
    schemas: [{ name: "main", functions: [vgiSum] }],
  },
});

if (import.meta.main) worker.run();
