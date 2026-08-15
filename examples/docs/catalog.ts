// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// catalog is the catalog example for the vgi-typescript documentation.
//
// A worker does not have to be a bag of functions. It can present itself as a
// database: a named catalog you ATTACH, holding schemas that hold tables and
// views, queried with ordinary qualified names.
//
// The table here is *function-backed*: the descriptor names a table function
// plus the arguments to call it with, so `SELECT * FROM cat.data.cities` runs
// the function with those arguments baked in. The user never passes them, and
// never sees the function.
//
//   bun run catalog.ts
//   # then, in a Haybarn shell:
//   ATTACH 'cat' (TYPE vgi, LOCATION 'bun run /abs/path/catalog.ts');
//   SELECT * FROM cat.data.cities;
//   SELECT * FROM cat.data.big_cities;   -- a view over the table

import {
  Worker,
  Arguments,
  defineTableFunction,
  batchFromColumns,
  toSchema,
  int,
  str,
} from "@query-farm/vgi";

// The table's shape. Declaring it on the descriptor lets DuckDB describe the
// table without calling the worker at all.
const citiesSchema = toSchema({ name: str, population: int });

interface City {
  name: string;
  pop: bigint;
}

// Stands in for whatever the worker actually fronts — a remote API, a file
// format, a device.
const CITIES: City[] = [
  { name: "Charlottesville", pop: 51_000n },
  { name: "Richmond", pop: 230_000n },
  { name: "Virginia Beach", pop: 457_000n },
];

const BATCH_SIZE = 1024;

// The scan behind the table. It is an ordinary table function — nothing about
// it knows it is backing a catalog table.
export const citiesScan = defineTableFunction({
  name: "cities_scan",
  description: "Scans the cities table, optionally filtered by population",
  args: { min_population: int },
  argDocs: { min_population: "Only return cities at least this large" },
  argConstraints: { min_population: { ge: 0 } },

  onBind: () => ({ outputSchema: citiesSchema }),

  // Filtering here, in the state factory, means it happens once per scan
  // rather than once per batch.
  //
  // This materializes the whole filtered result, which is honest for three
  // rows and the wrong shape for three million. A real scan keeps a *cursor*
  // — an offset, a page token, an open iterator — and fetches inside process().
  initialState: ({ args }) => ({
    rows: CITIES.filter((c) => c.pop >= BigInt(args.min_population)),
    i: 0,
  }),

  process: (_params, state, out) => {
    if (state.i >= state.rows.length) return out.finish();
    const end = Math.min(state.i + BATCH_SIZE, state.rows.length);
    const slice = state.rows.slice(state.i, end);
    out.emit(
      batchFromColumns(
        { name: slice.map((c) => c.name), population: slice.map((c) => c.pop) },
        citiesSchema,
      ),
    );
    state.i = end;
  },
});

export const worker = new Worker({
  catalog: {
    name: "cat",
    comment: "Documentation example: a worker presented as a database",
    schemas: [
      {
        name: "data",
        functions: [citiesScan],
        tables: [
          {
            name: "cities",
            comment: "Every city the worker knows about",
            columns: citiesSchema,
            // Function-backed: `arguments` are bound at scan time, so the user
            // writes `SELECT * FROM cat.data.cities` with no arguments at all.
            function: citiesScan,
            arguments: new Arguments([0]),
            notNull: ["name"],
            columnComments: { population: "Most recent estimate" },
          },
        ],
        views: [
          {
            name: "big_cities",
            comment: "Cities with a population of at least 100,000",
            // Pure SQL that DuckDB evaluates — no worker round trip for the
            // view itself, only for the table it reads.
            //
            // `cat` is hardcoded, which would normally be fragile. It is not:
            // the name in ATTACH must match this catalog's name, so `cat` is
            // the only name this view is ever read under.
            definition: "SELECT * FROM cat.data.cities WHERE population >= 100000",
          },
        ],
      },
    ],
  },
});

if (import.meta.main) worker.run();
