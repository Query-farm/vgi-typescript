// Shared function and catalog registration for example workers.
// Used by both the IPC worker (worker.ts) and HTTP worker (http-worker.ts).

import { type CatalogDescriptor, Arguments } from "../src/index.js";
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

export const catalog: CatalogDescriptor = {
  name: "example",
  defaultSchema: "main",
  schemas: [
    {
      name: "main",
      comment: "Example functions for testing VGI",
      functions: allFunctions,
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
    },
  ],
};
