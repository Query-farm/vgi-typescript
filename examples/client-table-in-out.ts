// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Example: Table-in-out function calls via VgiClient.
// Demonstrates calling echo() and buffer_input() table-in-out functions.

import { resolve } from "path";
import { Schema, Field, Int64, Utf8 } from "@query-farm/apache-arrow";
import { subprocessConnect } from "@query-farm/vgi-rpc";
import { VgiClient, batchFromRows } from "../src/index.js";

const WORKER = [resolve(import.meta.dirname!, "../bin/vgi-example-worker")];

async function main() {
  const rpc = subprocessConnect(WORKER);
  const client = new VgiClient(rpc);

  try {
    const inputSchema = new Schema([
      new Field("a", new Int64(), true),
      new Field("b", new Utf8(), true),
    ]);

    const batch1 = batchFromRows(
      [
        { a: 1n, b: "hello" },
        { a: 2n, b: "world" },
      ],
      inputSchema,
    );
    const batch2 = batchFromRows(
      [
        { a: 3n, b: "foo" },
        { a: 4n, b: "bar" },
      ],
      inputSchema,
    );

    // --- echo (passthrough) ---
    console.log("=== echo ===");
    for await (const rows of client.tableInOutFunctionRows({
      functionName: "echo",
      input: [batch1, batch2],
    })) {
      for (const row of rows) {
        console.log(`  a=${row.a} b=${row.b}`);
      }
    }

    // --- buffer_input (buffers all input, emits on finalize) ---
    console.log("\n=== buffer_input ===");
    const allRows: Record<string, any>[] = [];
    for await (const rows of client.tableInOutFunctionRows({
      functionName: "buffer_input",
      input: [batch1, batch2],
    })) {
      allRows.push(...rows);
    }
    console.log(`Got ${allRows.length} rows from finalize:`);
    for (const row of allRows) {
      console.log(`  a=${row.a} b=${row.b}`);
    }

    console.log("\nDone!");
  } finally {
    client.close();
  }
}

main().catch(console.error);
