// Example worker entry point.
// Registers ALL example functions and the catalog, matching the Python vgi-example-worker.

import { Worker, type CatalogDescriptor, Arguments } from "../src/index.js";
import { scalarFunctions } from "./scalar.js";
import { tableFunctions } from "./table.js";
import { tableInOutFunctions } from "./table_in_out.js";

// Find the sequence function for table-backed catalog entries
const sequenceFunction = tableFunctions.find((f) => f.meta.name === "sequence");

const allFunctions = [
  ...scalarFunctions,
  ...tableFunctions,
  ...tableInOutFunctions,
];

const catalog: CatalogDescriptor = {
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

const worker = new Worker({
  functions: allFunctions,
  catalog,
});

worker.run();
