#!/usr/bin/env node
// Postinstall: patch @query-farm/apache-arrow for bun (set main/types to TS source)
// and deduplicate across vgi-rpc to avoid dual-package instanceof issues.
const fs = require("fs");
const path = require("path");

const PKG = "@query-farm/apache-arrow";
const ENTRY = "src/Arrow.node.ts";

// 1. Patch our copy's package.json so bun/tsc find the TS entry point.
const ourArrow = path.resolve("node_modules", PKG);
const ourPkgPath = path.join(ourArrow, "package.json");
if (fs.existsSync(ourPkgPath)) {
  const pkg = JSON.parse(fs.readFileSync(ourPkgPath, "utf8"));
  pkg.main = ENTRY;
  pkg.types = ENTRY;
  fs.writeFileSync(ourPkgPath, JSON.stringify(pkg, null, 2) + "\n");
}

// 2. Deduplicate: replace vgi-rpc's nested copy with a symlink to ours.
//    Without this, two separate DataType classes exist at runtime and
//    instanceof checks fail across package boundaries.
//    Bun uses per-file symlinks for file: deps, so module resolution follows
//    the real path. We must target the REAL node_modules of vgi-rpc-typescript.
const rpcPkgLink = path.resolve("node_modules/vgi-rpc/package.json");
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
