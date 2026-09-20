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

// Specifiers the browser client entry must NOT import at runtime.
//
// `@query-farm/vgi-rpc`'s root re-exports the entire framework — protocol,
// dispatch, access log, the server — and it is `external` in this build, so it
// lands in the CONSUMER's bundle, where nothing can tree-shake it. One value
// import of `RpcError` from the root cost every browser consumer ~160 kB and
// shipped them `RpcServer` to satisfy an `instanceof`. Client code takes the
// narrow `/connect` subpath instead.
//
// An `import type` is erased and so is invisible here; only a real runtime
// import trips this. The trap is `import { type X } from "..."`, which leaves
// `import {} from "..."` behind — a side-effect import of the whole graph,
// which this catches.
const CLIENT_ENTRY = "./dist/client-entry.js";
const FORBIDDEN_CLIENT_IMPORTS = ["@query-farm/vgi-rpc"];

let failed = 0;

{
  const source = await Bun.file(CLIENT_ENTRY).text();
  const imported = new Set(
    [...source.matchAll(/(?:^|\n)\s*(?:import|export)[^;\n]*?from\s*"([^"]+)"/g)].map((m) => m[1]),
  );
  const offenders = FORBIDDEN_CLIENT_IMPORTS.filter((specifier) => imported.has(specifier));
  if (offenders.length > 0) {
    console.error(
      `FAIL ${CLIENT_ENTRY} imports ${offenders.join(", ")} at runtime; ` +
        `use a narrow subpath (e.g. "@query-farm/vgi-rpc/connect") or "import type"`,
    );
    failed++;
  } else {
    console.log(`ok   ${CLIENT_ENTRY} pulls no server-side framework into browser bundles`);
  }
}

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
  console.error(`\n${failed} bundle check${failed === 1 ? "" : "s"} failed.`);
  process.exit(1);
}
console.log(`\nAll ${ENTRIES.length} bundle entries import cleanly, and the client entry stays client-only.`);
