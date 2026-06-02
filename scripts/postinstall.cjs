#!/usr/bin/env node
// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Postinstall: deduplicate @query-farm/apache-arrow across @query-farm/vgi-rpc
// to avoid dual-package instanceof issues.
//
// This is a LOCAL DEVELOPMENT-only fixup for the vgi-typescript monorepo, where
// @query-farm/vgi-rpc is a `file:` link with its own nested copy of apache-arrow.
// It must NOT run when this package is installed as a dependency of someone
// else's project — so it no-ops unless the sibling vgi-rpc-typescript checkout
// is present next to this repo. (Published consumers get a single hoisted
// apache-arrow from the registry, so no dedup is needed there.)
const fs = require("fs");
const path = require("path");

const PKG = "@query-farm/apache-arrow";

// Resolve everything relative to the repo root (this file lives in <repo>/scripts),
// not the process cwd, so behavior doesn't depend on where the installer runs us.
const repoRoot = path.resolve(__dirname, "..");

// Dev-monorepo signal: the sibling vgi-rpc source checkout. When this package is
// installed as a dependency, that path resolves inside the consumer's node_modules
// and won't exist — so we bail and leave their tree untouched.
const devSibling = path.resolve(repoRoot, "..", "vgi-rpc-typescript", "package.json");
if (!fs.existsSync(devSibling)) {
  process.exit(0);
}

// Deduplicate: replace vgi-rpc's nested copy with a symlink to ours.
//   Without this, two separate DataType classes exist at runtime and
//   instanceof checks fail across package boundaries.
//   Bun uses per-file symlinks for file: deps, so module resolution follows
//   the real path. We must target the REAL node_modules of vgi-rpc-typescript.
const ourArrow = path.join(repoRoot, "node_modules", PKG);
const rpcPkgLink = path.join(repoRoot, "node_modules", "@query-farm", "vgi-rpc", "package.json");
try {
  const realPkgPath = fs.realpathSync(rpcPkgLink);
  const rpcRoot = path.dirname(realPkgPath);
  const rpcArrow = path.join(rpcRoot, "node_modules", PKG);
  const st = fs.lstatSync(rpcArrow);
  if (!st.isSymbolicLink()) {
    fs.rmSync(rpcArrow, { recursive: true });
    fs.symlinkSync(ourArrow, rpcArrow);
  }
} catch (e) {
  if (e.code !== "ENOENT") throw e;
}
