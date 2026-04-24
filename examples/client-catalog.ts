// Example: Catalog introspection via VgiClient.
// Demonstrates listing catalogs, schemas, and functions.

import { resolve } from "path";
import { subprocessConnect } from "vgi-rpc";
import { VgiClient } from "../src/index.js";

const WORKER = [resolve(import.meta.dirname!, "../bin/vgi-example-worker")];

async function main() {
  const rpc = subprocessConnect(WORKER);
  const client = new VgiClient(rpc);

  try {
    // --- List catalogs ---
    console.log("=== Catalogs ===");
    const catalogs = await client.catalogs();
    console.log(`  ${catalogs.join(", ")}`);

    // --- Attach ---
    console.log("\n=== Attach 'example' ===");
    const attachResult = await client.catalogAttach("example");
    console.log(`  attachId: ${attachResult.attach_id.length} bytes`);
    console.log(`  defaultSchema: ${attachResult.default_schema}`);
    console.log(`  supportsTransactions: ${attachResult.supports_transactions}`);

    const attachId = attachResult.attach_id;

    // --- Version ---
    console.log("\n=== Catalog version ===");
    const version = await client.catalogVersion(attachId);
    console.log(`  version: ${version}`);

    // --- List schemas ---
    console.log("\n=== Schemas ===");
    const schemas = await client.schemas(attachId);
    for (const s of schemas) {
      console.log(`  ${s.name}: ${s.comment ?? "(no comment)"}`);
    }

    // --- List functions (scalar) ---
    console.log("\n=== Scalar functions in 'main' ===");
    const scalarFuncs = await client.schemaContentsFunctions(attachId, "main", "SCALAR_FUNCTION");
    for (const f of scalarFuncs) {
      console.log(`  ${f.name}: ${f.description}`);
    }

    // --- List functions (table) ---
    console.log("\n=== Table functions in 'main' ===");
    const tableFuncs = await client.schemaContentsFunctions(attachId, "main", "TABLE_FUNCTION");
    for (const f of tableFuncs) {
      console.log(`  ${f.name}: ${f.description}`);
    }

    // --- List tables in 'data' schema ---
    console.log("\n=== Tables in 'data' ===");
    const tables = await client.schemaContentsTables(attachId, "data");
    for (const t of tables) {
      console.log(`  ${t.name}: ${t.comment ?? "(no comment)"}`);
    }

    // --- Detach ---
    await client.catalogDetach(attachId);
    console.log("\nDetached. Done!");
  } finally {
    client.close();
  }
}

main().catch(console.error);
