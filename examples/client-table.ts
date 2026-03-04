// Example: Table function calls via VgiClient.
// Demonstrates calling sequence() and named_params_echo() table functions.

import { resolve } from "path";
import { subprocessConnect } from "vgi-rpc";
import { VgiClient, Arguments } from "../src/index.js";

const WORKER = [resolve(import.meta.dirname!, "../bin/vgi-example-worker")];

async function main() {
  const rpc = subprocessConnect(WORKER);
  const client = new VgiClient(rpc);

  try {
    // --- sequence(10) ---
    console.log("=== sequence(10) ===");
    const allRows: Record<string, any>[] = [];
    for await (const rows of client.tableFunctionRows({
      functionName: "sequence",
      arguments: new Arguments([10]),
    })) {
      allRows.push(...rows);
    }
    console.log(`Got ${allRows.length} rows`);
    for (const row of allRows) {
      console.log(`  n = ${row.n}`);
    }

    // --- sequence(5, increment=3) using RecordBatch API ---
    console.log("\n=== sequence(5, increment=3) — RecordBatch API ===");
    let totalRows = 0;
    for await (const batch of client.tableFunction({
      functionName: "sequence",
      arguments: new Arguments([5], new Map([["increment", 3]])),
    })) {
      console.log(`  batch: ${batch.numRows} rows, schema: [${batch.schema.fields.map(f => f.name).join(", ")}]`);
      totalRows += batch.numRows;
    }
    console.log(`Total: ${totalRows} rows`);

    // --- named_params_echo(3, greeting='hi', multiplier=10) ---
    console.log("\n=== named_params_echo(3, greeting='hi', multiplier=10) ===");
    for await (const rows of client.tableFunctionRows({
      functionName: "named_params_echo",
      arguments: new Arguments(
        [3],
        new Map([["greeting", "hi"], ["multiplier", 10]]),
      ),
    })) {
      for (const row of rows) {
        console.log(`  id=${row.id} greeting=${row.greeting} value=${row.value} float_value=${row.float_value} enabled=${row.enabled}`);
      }
    }

    console.log("\nDone!");
  } finally {
    client.close();
  }
}

main().catch(console.error);
