// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// rowcount is the buffering example for the vgi-typescript documentation.
//
// A buffering function is for the case where output depends on the WHOLE input
// — a global sort, a top-k, a full reduction. It runs in three phases:
//
//   - process  (sink)   — called per input batch, in parallel across DuckDB
//     threads. Stash what you need and return a state id.
//   - combine           — called once, on the coordinator, with every state id
//     the sink produced. Reduce them into the ids the source will drain.
//   - finalize (source) — called per finalize id, streaming the result out.
//
// The phases can run in different worker processes, so nothing may live in a
// module-level variable between them. State goes in params.storage, which is
// scoped to this execution and shared across the workers serving it.
//
//   bun run rowcount.ts
//   # then, in a Haybarn shell:
//   ATTACH 'buffers' (TYPE vgi, LOCATION 'bun run /abs/path/rowcount.ts');
//   SELECT * FROM buffers.row_count((SELECT * FROM big_table));

import {
  Worker,
  defineTableBufferingFunction,
  batchFromColumns,
  toSchema,
  int,
} from "@query-farm/vgi";

const countSchema = toSchema({ count: int });

const enc = new TextEncoder();
const NS = enc.encode("rowcount");
const KEY = enc.encode("");

// The finalize cursor. `emitted` makes the source phase a one-shot: it emits
// the total on the first tick and finishes on the second.
interface DrainState {
  emitted: boolean;
}

export const rowCount = defineTableBufferingFunction<Record<string, never>, DrainState>({
  name: "row_count",
  description: "Counts every row of the input relation",

  onBind: (params) => {
    if (!params.bindCall.input_schema) {
      throw new Error("row_count requires a table argument");
    }
    // Output is one BIGINT, whatever the input looked like.
    return { outputSchema: countSchema };
  },

  // The sink runs in parallel across DuckDB threads. stateAppend is an
  // append-only log, so concurrent appends cannot lose each other the way a
  // read-modify-write would. Return the state id this batch contributed to.
  process: async (batch, params) => {
    const n = new BigInt64Array([BigInt(batch.numRows)]);
    await params.storage.stateAppend(NS, KEY, new Uint8Array(n.buffer));
    return params.executionId;
  },

  // Runs once, on the coordinator. Here there is a single bucket to drain, so
  // it just names the execution; a top-k would reduce the partials first.
  combine: async (_stateIds, params) => [params.executionId],

  initialFinalizeState: () => ({ emitted: false }),

  // The source phase. Sum the log and emit one row.
  finalize: async (params, _finalizeId, state, out) => {
    if (state.emitted) return out.finish();

    let total = 0n;
    // -1 starts before the first entry; the limit is a page size, not a cap.
    let afterId = -1;
    for (;;) {
      const rows = await params.storage.stateLogScan(NS, KEY, afterId, 256);
      if (rows.length === 0) break;
      for (const [logId, value] of rows) {
        total += new BigInt64Array(
          value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength),
        )[0];
        afterId = logId;
      }
    }

    state.emitted = true;
    out.emit(batchFromColumns({ count: [total] }, countSchema));
  },
});

export const worker = new Worker({
  catalog: {
    name: "buffers",
    comment: "Documentation example: a buffering (sink → combine → source) function",
    schemas: [{ name: "main", functions: [rowCount] }],
  },
});

if (import.meta.main) worker.run();
