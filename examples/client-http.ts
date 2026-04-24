// Example: VgiClient over HTTP transport.
//
// This demonstrates using VgiClient with httpConnect, which works in any
// runtime: Bun, Node.js, Deno, and browsers. The VgiClient itself is
// transport-agnostic — swap subprocessConnect for httpConnect and everything
// works identically.
//
// For browser or Deno usage, import from "vgi/client" instead of "vgi" to
// avoid pulling in server-side code:
//
//   import { VgiClient, Arguments } from "vgi/client";
//   import { httpConnect } from "vgi-rpc";
//
// NOTE: Streaming function calls (tableFunction, scalarFunction, etc.) require
// the HTTP server to support the VGI streaming protocol. This example shows
// catalog operations which use unary (request/response) calls and work with
// any HTTP transport.

import { resolve } from "path";
import { httpConnect } from "vgi-rpc";
import { VgiClient } from "../src/client-entry.js";

async function main() {
  // Start the HTTP worker (Bun-specific; in other runtimes you'd connect
  // to an already-running server).
  const proc = Bun.spawn(
    ["bun", "run", resolve(import.meta.dirname!, "http-worker.ts")],
    { stdout: "pipe", stderr: "inherit" },
  );

  // Read the port from the worker's stdout.
  const reader = proc.stdout.getReader();
  let portLine = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) throw new Error("HTTP worker exited before printing port");
    portLine += new TextDecoder().decode(value);
    if (portLine.includes("\n")) break;
  }
  reader.releaseLock();
  const port = portLine.match(/PORT:(\d+)/)?.[1];
  if (!port) throw new Error(`Could not parse port from: ${portLine}`);

  // Connect via HTTP — works in any runtime that has fetch().
  const rpc = httpConnect(`http://localhost:${port}`);
  const client = new VgiClient(rpc);

  try {
    // --- List catalogs ---
    console.log("=== Catalogs via HTTP ===");
    const catalogs = await client.catalogs();
    console.log(`  ${catalogs.join(", ")}`);

    // --- Attach ---
    console.log("\n=== Attach 'example' ===");
    const attachResult = await client.catalogAttach("example");
    console.log(`  attachId: ${attachResult.attach_id.length} bytes`);
    console.log(`  defaultSchema: ${attachResult.default_schema}`);

    const attachId = attachResult.attach_id;

    // --- Version ---
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
    const scalarFuncs = await client.schemaContentsFunctions(
      attachId,
      "main",
      "SCALAR_FUNCTION",
    );
    for (const f of scalarFuncs) {
      console.log(`  ${f.name}: ${f.description}`);
    }

    // --- List functions (table) ---
    console.log("\n=== Table functions in 'main' ===");
    const tableFuncs = await client.schemaContentsFunctions(
      attachId,
      "main",
      "TABLE_FUNCTION",
    );
    for (const f of tableFuncs) {
      console.log(`  ${f.name}: ${f.description}`);
    }

    // --- List tables ---
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
    proc.kill();
  }
}

main().catch(console.error);
