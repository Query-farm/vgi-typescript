// Example: Scalar function calls via VgiClient.
// Demonstrates calling upper_case() and multiply() scalar functions.

import { resolve } from "path";
import { Schema, Field, Utf8, Int64, RecordBatch } from "@query-farm/apache-arrow";
import { subprocessConnect } from "vgi-rpc";
import { VgiClient, Arguments, batchFromRows } from "../src/index.js";

const WORKER = [resolve(import.meta.dirname!, "../bin/vgi-example-worker")];

async function main() {
  const rpc = subprocessConnect(WORKER);
  const client = new VgiClient(rpc);

  try {
    // --- upper_case ---
    console.log("=== upper_case ===");
    const inputSchema = new Schema([new Field("value", new Utf8(), true)]);
    const inputBatch = batchFromRows(
      [{ value: "hello" }, { value: "world" }, { value: "vgi" }],
      inputSchema,
    );

    for await (const rows of client.scalarFunctionRows({
      functionName: "upper_case",
      input: [inputBatch],
    })) {
      for (const row of rows) {
        console.log(`  ${JSON.stringify(row)}`);
      }
    }

    // --- multiply(factor=3) ---
    console.log("\n=== multiply(factor=3) ===");
    const mulSchema = new Schema([new Field("value", new Int64(), true)]);
    const mulBatch = batchFromRows(
      [{ value: 10n }, { value: 20n }, { value: 30n }],
      mulSchema,
    );

    for await (const batch of client.scalarFunction({
      functionName: "multiply",
      input: [mulBatch],
      arguments: new Arguments([3]),
    })) {
      console.log(`  batch: ${batch.numRows} rows`);
      for (let i = 0; i < batch.numRows; i++) {
        const col = batch.getChild(batch.schema.fields[0].name);
        console.log(`  result[${i}] = ${col?.get(i)}`);
      }
    }

    console.log("\nDone!");
  } finally {
    client.close();
  }
}

main().catch(console.error);
