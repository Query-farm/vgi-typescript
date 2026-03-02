// Shared function and catalog registration for example workers.
// Used by both the IPC worker (worker.ts) and HTTP worker (http-worker.ts).

import { Schema, Field, Int64 } from "apache-arrow";
import { type CatalogDescriptor, type MacroDescriptor, Arguments, serializeBatch, batchFromColumns } from "../src/index.js";
import { scalarFunctions } from "./scalar.js";
import { tableFunctions } from "./table.js";
import { tableInOutFunctions } from "./table_in_out.js";

// Find the sequence function for table-backed catalog entries
const sequenceFunction = tableFunctions.find((f) => f.meta.name === "sequence");

export const allFunctions = [
  ...scalarFunctions,
  ...tableFunctions,
  ...tableInOutFunctions,
];

// Serialize parameter default values for vgi_clamp: lo=0, hi=100
const clampDefaultsSchema = new Schema([
  new Field("lo", new Int64(), true),
  new Field("hi", new Int64(), true),
]);
const clampDefaults = serializeBatch(
  batchFromColumns({ lo: [0n], hi: [100n] }, clampDefaultsSchema)
);

export const catalog: CatalogDescriptor = {
  name: "example",
  defaultSchema: "main",
  schemas: [
    {
      name: "main",
      comment: "Example functions for testing VGI",
      functions: allFunctions,
      views: [
        {
          name: "first_ten",
          definition: "SELECT * FROM sequence(10)",
          comment: "First 10 integers",
        },
        {
          name: "even_numbers",
          definition: "SELECT * FROM sequence(100) WHERE n % 2 = 0",
          comment: "Even numbers from 0 to 98",
        },
      ],
      macros: [
        {
          name: "vgi_multiply",
          macroType: "scalar",
          parameters: ["x", "y"],
          definition: "x * y",
          comment: "Multiply two values",
        },
        {
          name: "vgi_clamp",
          macroType: "scalar",
          parameters: ["val", "lo", "hi"],
          parameterDefaultValues: clampDefaults,
          definition: "GREATEST(lo, LEAST(hi, val))",
          comment: "Clamp a value between lo and hi (defaults: 0..100)",
        },
        {
          name: "vgi_range_table",
          macroType: "table",
          parameters: ["n"],
          definition: "SELECT * FROM range(n)",
          comment: "Table macro returning range of values",
        },
      ],
    },
    {
      name: "data",
      comment: "Example tables backed by functions",
      tables: [
        {
          name: "large_sequence",
          function: sequenceFunction,
          arguments: new Arguments([1_000_000]),
          comment: "A large sequence of integers from 0 to 1,000,000",
        },
        {
          name: "numbers",
          function: sequenceFunction,
          arguments: new Arguments([100]),
          comment: "First 100 integers",
        },
      ],
      views: [
        {
          name: "small_numbers",
          definition: "SELECT n AS value FROM numbers WHERE n < 10",
          comment: "Numbers less than 10",
        },
      ],
    },
  ],
};
