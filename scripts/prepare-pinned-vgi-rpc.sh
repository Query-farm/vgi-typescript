#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# TODO(release-wave): delete this script when package.json switches back to a
# published @query-farm/vgi-rpc (see the "//devDependencies" note there).
#
# While the blob-vendor work is unreleased, the dev dependency on vgi-rpc is
# pinned to an exact vgi-rpc-typescript commit (a GitHub dependency). Bun runs
# such a dependency straight from its `src/` (the `bun` export condition), but
# a git checkout has no `dist/`, and tsc resolves the package's `types`
# condition to `dist/index.d.ts`. This builds those declarations in place.
# Idempotent; a no-op when they already exist (a registry install, or a `file:`
# link to a built sibling checkout).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RPC="$ROOT/node_modules/@query-farm/vgi-rpc"
[ -d "$RPC" ] || { echo "no $RPC: run 'bun install' first" >&2; exit 1; }
if [ -f "$RPC/dist/index.d.ts" ]; then
  echo "vgi-rpc declarations present ($RPC/dist)"
  exit 0
fi
echo "Building the pinned vgi-rpc's declarations in $RPC ..."
cd "$RPC"
# Its own dev dependencies (typescript, @types/bun) live in a nested
# node_modules for the build only, and are removed afterwards so the runtime
# keeps resolving the single hoisted copy of @query-farm/apache-arrow.
bun install --frozen-lockfile --ignore-scripts >/dev/null
bunx tsc -p tsconfig.build.json
rm -rf node_modules
[ -f dist/index.d.ts ] || { echo "vgi-rpc declarations were not produced" >&2; exit 1; }
echo "ok"
