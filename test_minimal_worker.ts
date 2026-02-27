// Minimal worker with just one function for debugging
import { Worker, type CatalogDescriptor, Arguments } from "./src/index.js";
import { scalarFunctions } from "./examples/scalar.js";

// Just register the first scalar function
const worker = new Worker({
  functions: [scalarFunctions[0]],
});

worker.run();
