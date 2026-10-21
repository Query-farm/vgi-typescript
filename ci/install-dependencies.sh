#!/usr/bin/env bash
# Build the required RPC revision until its 0.29.1 package is published.
# Keep this source pin exact; package.json and bun.lock retain the release floor.
set -euo pipefail

cd "$(dirname "$0")/.."
rpc_revision=0ded8a92ea08f3a63305dbee2f5b53b9bdec62ab
rpc_stage=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/vgi-rpc-ci.XXXXXX")
trap 'rm -rf "$rpc_stage"' EXIT

git init -q "$rpc_stage/source"
git -C "$rpc_stage/source" fetch -q --depth 1 https://github.com/Query-farm/vgi-rpc-typescript.git "$rpc_revision"
git -C "$rpc_stage/source" checkout -q --detach FETCH_HEAD
test "$(git -C "$rpc_stage/source" rev-parse HEAD)" = "$rpc_revision"
(
  cd "$rpc_stage/source"
  bun install --frozen-lockfile
  bun run build
  npm pack --ignore-scripts --pack-destination "$rpc_stage" >/dev/null
)

# A tarball install exercises the published package layout without modifying
# the SDK's declared version floor or its release lockfile.
npm install --ignore-scripts --no-package-lock --no-save --no-audit --no-fund \
  "$rpc_stage/query-farm-vgi-rpc-0.29.1.tgz"
