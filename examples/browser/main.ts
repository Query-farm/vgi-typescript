// Browser test: VgiClient over HTTP against the live trains service.

// Import httpConnect directly from vgi-rpc client source to avoid bundling
// server-side code (HMAC auth, subprocess) that depends on node:crypto.
import { httpConnect } from "../../../vgi-rpc-typescript/src/client/connect.js";
import { VgiClient, Arguments } from "../../src/client-entry.js";

const SERVICE_URL = "https://trains.vgi.query-farm.services";
const out = document.getElementById("output")!;

function log(text: string) {
  out.textContent += text + "\n";
}

async function main() {
  out.textContent = "";
  log(`Connecting to ${SERVICE_URL} ...\n`);

  const rpc = httpConnect(SERVICE_URL);
  const client = new VgiClient(rpc);

  try {
    // --- Catalogs ---
    log("=== Catalogs ===");
    const catalogs = await client.catalogs();
    for (const c of catalogs) log(`  ${c}`);

    // --- Attach ---
    const catalogName = catalogs[0] ?? "trains";
    log(`\n=== Attach '${catalogName}' ===`);
    const attach = await client.catalogAttach(catalogName);
    const attachOpaqueData = attach.attach_opaque_data;
    log(`  attachOpaqueData: ${attachOpaqueData.length} bytes`);
    log(`  defaultSchema: ${attach.default_schema}`);

    // --- Schemas ---
    log("\n=== Schemas ===");
    const schemas = await client.schemas(attachOpaqueData);
    for (const s of schemas) log(`  ${s.name}`);

    // --- Functions (table) ---
    const schemaName = schemas[0]?.name ?? "main";
    log(`\n=== Table functions in '${schemaName}' ===`);
    const funcs = await client.schemaContentsFunctions(
      attachOpaqueData,
      schemaName,
      "TABLE_FUNCTION",
    );
    for (const f of funcs) log(`  ${f.name}: ${f.description ?? ""}`);

    // --- Tables ---
    log(`\n=== Tables in '${schemaName}' ===`);
    const tables = await client.schemaContentsTables(attachOpaqueData, schemaName);
    for (const t of tables) {
      log(`  ${t.name}${t.comment ? ": " + t.comment : ""}`);
    }

    // --- SELECT * FROM train_stations ---
    log(`\n=== SELECT * FROM train_stations ===`);
    let stationCount = 0;
    for await (const rows of client.tableFunctionRows({
      functionName: "train_stations",
      arguments: new Arguments(),
    })) {
      for (const row of rows) {
        if (stationCount < 15) {
          log(`  ${row.code}  ${row.name}  (${row.country}, ${row.lat}, ${row.lng})`);
        }
        stationCount++;
      }
    }
    if (stationCount > 15) log(`  ... (${stationCount} rows total)`);
    log(`  Total stations: ${stationCount}`);

    // --- Call train_departures() ---
    log(`\n=== train_departures() ===`);
    let depCount = 0;
    for await (const rows of client.tableFunctionRows({
      functionName: "train_departures",
      arguments: new Arguments(),
    })) {
      for (const row of rows) {
        if (depCount < 10) {
          const time = row.planned_time?.slice(11, 16) ?? "?";
          const delay = row.delay_minutes > 0 ? ` (+${row.delay_minutes}m)` : "";
          log(`  ${time}${delay}  ${row.category} → ${row.destination}  [${row.status}]`);
        }
        depCount++;
      }
    }
    if (depCount > 10) log(`  ... (${depCount} departures total)`);
    log(`  Total departures: ${depCount}`);

    // --- Detach ---
    await client.catalogDetach(attachOpaqueData);
    log("\nDone!");
  } catch (err: any) {
    log(`\nERROR: ${err.message ?? err}`);
    throw err;
  } finally {
    client.close();
  }
}

main();
