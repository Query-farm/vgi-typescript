// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// calcscalar is the worker built in step 1 of the vgi-typescript tutorial: one
// scalar function, served over stdio, callable from DuckDB as calc.double().
//
// A scalar function is the simplest shape — one row in, one value out, with no
// state and no finalize phase. DuckDB hands the worker a whole Arrow column and
// expects a column of the same length back.
//
//   bun run calcscalar.ts
//   # then, in a Haybarn shell:
//   ATTACH 'calc' (TYPE vgi, LOCATION 'bun run /abs/path/calcscalar.ts');
//   SELECT calc.double(21);

import { Worker, defineScalarFunction, int } from "@query-farm/vgi";

// `int` is the Int64 type alias. compute() therefore reads and returns bigint —
// DuckDB's BIGINT does not fit in a JS number, so the SDK never narrows it.
export const double = defineScalarFunction({
  name: "double",
  description: "Doubles a BIGINT",
  params: { n: int },
  returns: int,

  // compute runs once per input BATCH, not per row. Arguments are positional:
  // column 0 is the first argument, whatever the params key is called.
  compute: (batch) => {
    // A column is an erased `Iterable<unknown>` — the facade cannot know the
    // value type, because arrow-js and flechette parameterize differently. The
    // cast at the use site is the intended pattern, and under `strict` it is
    // required: without it `Array.from`'s mapper gets `unknown`.
    const ns = batch.getChildAt(0)! as Iterable<bigint | null>;
    // One output value per input row. null in → null out.
    return Array.from(ns, (v) => (v == null ? null : v * 2n));
  },
});

// Functions are served through a catalog, and the catalog's name is the name
// DuckDB ATTACHes. They must match: attaching under any other name fails.
export const worker = new Worker({
  catalog: { name: "calc", schemas: [{ name: "main", functions: [double] }] },
});

if (import.meta.main) worker.run();
