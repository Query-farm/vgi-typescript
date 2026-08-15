// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// calc is the worker built across the vgi-typescript tutorial: one scalar
// function and one table function in a single catalog.
//
// The scalar `double` transforms a column in place. The table function `series`
// *generates* rows from an argument, so it is called in a FROM clause rather
// than an expression. One worker can serve any mix of shapes.
//
//   bun run calc.ts
//   # then, in a Haybarn shell:
//   ATTACH 'calc' (TYPE vgi, LOCATION 'bun run /abs/path/calc.ts');
//   SELECT calc.double(21);
//   SELECT * FROM calc.series(3);

import {
  Worker,
  defineScalarFunction,
  defineTableFunction,
  batchFromColumns,
  toSchema,
  int,
} from "@query-farm/vgi";

// ── scalar: double(n) ───────────────────────────────────────────────────────

export const double = defineScalarFunction({
  name: "double",
  description: "Doubles a BIGINT",
  params: { n: int },
  returns: int,
  compute: (batch) => {
    // Columns are erased `Iterable<unknown>`; cast at the use site, where the
    // declared params make the value type known.
    const ns = batch.getChildAt(0)! as Iterable<bigint | null>;
    return Array.from(ns, (v) => (v == null ? null : v * 2n));
  },
});

// ── table: series(count) ────────────────────────────────────────────────────

// The output schema is fixed, so it can be built once at module scope. A table
// function whose columns depend on its arguments would build it in onBind.
const seriesSchema = toSchema({ n: int });

const BATCH_SIZE = 1024;

export const series = defineTableFunction({
  name: "series",
  description: "Generates the integers 0..count-1",

  args: { count: int },
  argDocs: { count: "How many numbers to generate" },
  // Enforced at bind, not just advertised: series(-1) fails before any row is
  // produced, rather than looping or silently returning nothing.
  argConstraints: { count: { ge: 0 } },

  onBind: () => ({ outputSchema: seriesSchema }),

  // Runs once per scan, after bind. Arguments are fixed for the whole scan, so
  // this is where they are read — decoding them per batch would be waste.
  //
  // BigInt() is not decoration. A table function's `args` arrive as JS numbers
  // even for an int64 argument, while the same type reaches a *scalar*
  // function's columns as bigint. Normalize once here and the rest of the
  // function can do bigint arithmetic without a runtime "Invalid mix of BigInt
  // and other type" surprise on the first batch.
  initialState: ({ args }) => ({ i: 0n, count: BigInt(args.count) }),

  // process is the pull loop: DuckDB calls it repeatedly and consumes lazily.
  // Emit what you can, then finish() to signal end-of-stream. Nothing has to be
  // materialized up front, which is what makes a table function a *generator*.
  process: (_params, state, out) => {
    if (state.i >= state.count) return out.finish();
    const end = state.count - state.i > BigInt(BATCH_SIZE)
      ? state.i + BigInt(BATCH_SIZE)
      : state.count;
    const ns: bigint[] = [];
    for (let k = state.i; k < end; k++) ns.push(k);
    out.emit(batchFromColumns({ n: ns }, seriesSchema));
    state.i = end;
  },
});

export const worker = new Worker({
  catalog: {
    name: "calc",
    comment: "Tutorial worker: a scalar and a table function",
    schemas: [{ name: "main", functions: [double, series] }],
  },
});

if (import.meta.main) worker.run();
