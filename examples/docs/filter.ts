// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// filter is the table-in-out example for the vgi-typescript documentation.
//
// A table-in-out function consumes a relation and streams a transformed
// relation back, batch by batch. Unlike a scalar it may change the row count,
// and unlike a buffering function it never holds the whole input — each process
// call emits what it can from the batch in hand, which is what keeps memory
// flat over an arbitrarily large scan.
//
//   bun run filter.ts
//   # then, in a Haybarn shell:
//   ATTACH 'filters' (TYPE vgi, LOCATION 'bun run /abs/path/filter.ts');
//   SELECT * FROM filters.filter_positive((SELECT * FROM t));

import {
  Worker,
  defineTableInOutFunction,
  batchFromColumns,
  type TableInOutBindParams,
} from "@query-farm/vgi";

export const filterPositive = defineTableInOutFunction({
  name: "filter_positive",
  description: "Keeps only the rows whose `value` column is greater than zero",

  // No `args` entry declares the input relation. A table-in-out function's
  // TABLE argument is implicit: it arrives as the stream of batches process()
  // is called with, and its schema is on params.bindCall.input_schema.
  onBind: (params: TableInOutBindParams) => {
    const input = params.bindCall.input_schema;
    if (!input) throw new Error("filter_positive requires a table argument");
    // Output shape matches input shape — this function drops rows, not columns.
    return { outputSchema: input };
  },

  // Called once per input batch. Emit zero or more batches; returning without
  // emitting is how a batch is dropped entirely.
  process: (params, _state, batch, out) => {
    const values = batch.getChild("value");
    if (!values) throw new Error("expected a `value` column");

    // The column's width comes from the caller's relation, not from this
    // function, so `value` may arrive as a JS number (int32 and narrower) or a
    // bigint (int64). Comparing against 0 works for both — JS allows mixed
    // number/bigint *comparison*, just not mixed arithmetic — and the values
    // are handed back untouched, so the codec rebuilds the original type.
    const kept: (number | bigint)[] = [];
    for (let i = 0; i < batch.numRows; i++) {
      // get() returns `unknown` for the same reason — narrow it here.
      const v = values.get(i) as number | bigint | null;
      if (v != null && v > 0) kept.push(v);
    }

    // An empty batch is legal but pointless — skip the round trip.
    if (kept.length === 0) return;
    out.emit(batchFromColumns({ value: kept }, params.outputSchema));
  },
});

export const worker = new Worker({
  catalog: {
    name: "filters",
    comment: "Documentation example: a streaming table-in-out function",
    schemas: [{ name: "main", functions: [filterPositive] }],
  },
});

if (import.meta.main) worker.run();
