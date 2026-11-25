#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Workers ship as `bun build --compile` binaries (vgi-actions ts-release.yml,
# the fleet's Windows lane) and install none of the optional storage peers
# (@aws-sdk/client-s3, @aws-sdk/s3-request-presigner, @google-cloud/storage).
# This builds consumers of the PACKED tarball, without and then with those
# peers, and checks that:
#
#   - `bun build --compile` succeeds without the peers (0.52.2 + vgi-rpc
#     0.30.2 failed: `Could not resolve: "@aws-sdk/client-s3"`);
#   - the S3/GCS factory survives bundling, from the package entry and from
#     the serve entry, compiled and on Node (dist/): VGI_STORAGE_BACKEND=s3
#     reaches vgi-rpc's "needs the optional packages" error, never
#     "is not available here" (0.52.2's dist had the factory tree-shaken);
#   - with the peers installed, the compiled binary bundles them and gets
#     past loading the AWS SDK (here: a refused connection to 127.0.0.1:9).
#
# Usage: scripts/check-compile-worker.sh   (run after `bun run build`)
#   VGI_RPC_TARBALL=/path/to/query-farm-vgi-rpc-x.y.z.tgz  test against an
#   unreleased vgi-rpc instead of the peer range from npm.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/vgi-compile-worker.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

cd "$ROOT"
test -f dist/index.js || { echo "dist/ is missing: run 'bun run build' first" >&2; exit 1; }
TARBALL="$WORK/$(npm pack --ignore-scripts --silent --pack-destination "$WORK")"
ARROW_RANGE="$(node -p "require('./package.json').peerDependencies['@query-farm/apache-arrow']")"
RPC_RANGE="$(node -p "require('./package.json').peerDependencies['@query-farm/vgi-rpc']")"
S3_RANGE="$(node -p "require('./package.json').devDependencies['@aws-sdk/client-s3']")"
GCS_RANGE="$(node -p "require('./package.json').devDependencies['@google-cloud/storage']")"
RPC_SPEC="${VGI_RPC_TARBALL:-@query-farm/vgi-rpc@$RPC_RANGE}"

mkdir -p "$WORK/consumer"
cd "$WORK/consumer"
echo '{ "private": true, "type": "module" }' > package.json
bun add "$TARBALL" "@query-farm/apache-arrow@$ARROW_RANGE" "$RPC_SPEC" >/dev/null
for pkg in @aws-sdk/client-s3 @aws-sdk/s3-request-presigner @google-cloud/storage; do
  if [ -d "node_modules/$pkg" ]; then echo "FAIL: optional peer $pkg was installed" >&2; exit 1; fi
done

# The S3 environment every probe runs under: nothing listens on port 9.
export VGI_STORAGE_BACKEND=s3 VGI_STORAGE_S3_BUCKET=probe VGI_STORAGE_S3_ENDPOINT=http://127.0.0.1:9 \
  VGI_STORAGE_ALLOW_PLAIN_HTTP=1 VGI_STORAGE_S3_REGION=us-east-1 \
  VGI_STORAGE_S3_ACCESS_KEY_ID=probe VGI_STORAGE_S3_SECRET_ACCESS_KEY=probe

# Package entry only (what a stdio worker imports): the Worker class keeps the
# whole barrel in the graph; function storage reaches the S3 store.
cat > probe-index.ts <<'EOF'
import { functionBlobClientFromEnv, Worker } from "@query-farm/vgi";
void Worker;
try {
  await functionBlobClientFromEnv(process.env)!.putBytes(new Uint8Array([1]));
  console.log("index: ok");
} catch (error) {
  console.log(`index: ${(error as Error).message}`);
}
process.exit(0);
EOF
# Serve entry only (HTTP workers): the storage config HEADs the bucket.
cat > probe-serve.ts <<'EOF'
import { storageHttpConfigFromEnv } from "@query-farm/vgi/serve";
try {
  await storageHttpConfigFromEnv(process.env, () => {}).verify();
  console.log("serve: ok");
} catch (error) {
  console.log(`serve: ${(error as Error).message}`);
}
process.exit(0);
EOF
# Node runs dist/ through the `import` condition; Node cannot run .ts, so
# these are the same probes in plain JS.
sed -e 's/!\.putBytes/.putBytes/' -e 's/ as Error//' probe-index.ts > probe-index.mjs
sed -e 's/ as Error//' probe-serve.ts > probe-serve.mjs

compile() {
  for p in index serve; do
    rm -f "probe-$p" "probe-$p.exe"
    if ! bun build --compile "probe-$p.ts" --outfile "probe-$p" >build.log 2>&1; then
      cat build.log >&2
      echo "FAIL: bun build --compile probe-$p.ts ($1)" >&2
      exit 1
    fi
  done
}
run_compiled() { ./probe-index; ./probe-serve; }
run_node() { node probe-index.mjs; node probe-serve.mjs; }
check() { # $1 label, $2 log; every line must match $3 and none $4
  local label="$1" log="$2"
  if grep -q "is not available here" "$log"; then
    echo "FAIL ($label): the S3/GCS factory was not registered (tree-shaken?)" >&2; exit 1
  fi
  if [ "$3" = missing ]; then
    [ "$(grep -c "needs the optional packages" "$log")" = 2 ] || { echo "FAIL ($label): expected the missing-package error twice" >&2; exit 1; }
  else
    if grep -q "needs the optional packages\|Cannot find module\|Cannot find package" "$log"; then
      echo "FAIL ($label): the AWS SDK did not load" >&2; exit 1
    fi
  fi
}

echo "== no optional peers: compiled"
compile "no peers"
run_compiled | tee run.log
check "compiled, no peers" run.log missing
echo "== no optional peers: node (dist)"
run_node | tee run.log
check "node, no peers" run.log missing

echo "== optional peers installed: compiled"
bun add "@aws-sdk/client-s3@$S3_RANGE" "@aws-sdk/s3-request-presigner@$S3_RANGE" "@google-cloud/storage@$GCS_RANGE" >/dev/null
compile "peers"
mv node_modules node_modules.off   # the binary must not need them at runtime
run_compiled | tee run.log
mv node_modules.off node_modules
check "compiled, peers" run.log loaded
echo "== optional peers installed: node (dist)"
run_node | tee run.log
check "node, peers" run.log loaded

echo "OK: workers compile without the optional storage peers and load them when present"
