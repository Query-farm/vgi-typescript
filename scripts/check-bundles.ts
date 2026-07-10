// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Post-build smoke check: every published `dist/` entry must actually import.
//
// This exists because a bundle can be emitted, be the right shape on disk, and
// still be invalid ESM. Under `sideEffects: false` the bundler tree-shook the
// pure-re-export entries down to an export list naming bindings that nothing
// defined or imported; `dist/client-entry.js` shipped that way. Bun consumers
// never noticed — the `bun` condition resolves those subpaths to `src/` — so
// only Node consumers hit it, and only at import time.
//
// Importing each entry is the cheapest check that catches it.

interface EntryCheck {
  /** Path to the built bundle. */
  file: string;
  /** Subpath consumers import it as. */
  subpath: string;
  /** Named exports that must be present and defined. */
  expect: string[];
}

const ENTRIES: EntryCheck[] = [
  { file: "./dist/index.js", subpath: "@query-farm/vgi", expect: ["Worker", "FunctionRegistry"] },
  { file: "./dist/client-entry.js", subpath: "@query-farm/vgi/client", expect: ["VgiClient"] },
  {
    file: "./dist/worker-cf-entry.js",
    subpath: "@query-farm/vgi/worker-cf",
    expect: ["createVgiFetch", "FunctionRegistry"],
  },
  {
    file: "./dist/serve-entry.js",
    subpath: "@query-farm/vgi/serve",
    expect: ["serveVgiWorker", "createVgiWorkerFetch", "parseSigningKeyHex"],
  },
];

let failed = 0;

for (const entry of ENTRIES) {
  // Resolve against the repo root, not this script's directory: a bare
  // "./dist/..." specifier resolves relative to the importing module.
  const resolved = new URL(entry.file, `file://${process.cwd()}/`).href;
  let mod: Record<string, unknown>;
  try {
    mod = (await import(resolved)) as Record<string, unknown>;
  } catch (err) {
    console.error(`FAIL ${entry.subpath} (${entry.file}) does not import:`);
    console.error(`     ${err instanceof Error ? err.message.split("\n")[0] : String(err)}`);
    failed++;
    continue;
  }

  const missing = entry.expect.filter((name) => mod[name] === undefined);
  if (missing.length > 0) {
    console.error(`FAIL ${entry.subpath} is missing exports: ${missing.join(", ")}`);
    failed++;
    continue;
  }
  console.log(`ok   ${entry.subpath} (${entry.expect.length} exports checked)`);
}

if (failed > 0) {
  console.error(`\n${failed} of ${ENTRIES.length} bundle entries are broken.`);
  process.exit(1);
}
console.log(`\nAll ${ENTRIES.length} bundle entries import cleanly.`);
