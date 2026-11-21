#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Run vgi's SDK-neutral Orchard hosted-mode harness
# (test/hosted/run_hosted_conformance.py, documented in test/hosted/README.md)
# against this SDK's hosted fixture worker, examples/orchard-hosted-worker.ts.
#
#   VGI_SRC=/path/to/vgi ci/run-orchard-hosted-conformance.sh [extra harness flags]
#
# Needs bun and uv (the harness is a uv script with pinned dependencies).
# VGI_BUN_CONDITIONS=flechette runs the worker on the flechette backend.
#
# VGI_DEBUG=1 is the worker's most verbose level: the harness reads its output
# for leaked credentials. Every non-HTTP transport Worker.run() offers is
# passed as a --refuse-cmd: stdio, --unix, --tcp and the Iroh raw upstream.
# No rule is skipped.
set -euo pipefail

: "${VGI_SRC:?path to a Query-farm/vgi checkout (needs test/hosted, vgi >= 2957674)}"
HARNESS="$VGI_SRC/test/hosted/run_hosted_conformance.py"
[ -f "$HARNESS" ] || { echo "::error::no $HARNESS (needs vgi >= 2957674)"; exit 1; }
REPO="$(cd "$(dirname "$0")/.." && pwd)"

W="bun ${VGI_BUN_CONDITIONS:+--conditions=$VGI_BUN_CONDITIONS} run examples/orchard-hosted-worker.ts"

exec uv run --script "$HARNESS" \
  --worker-cwd "$REPO" \
  --env VGI_DEBUG=1 \
  --worker-cmd "$W" \
  --refuse-cmd "$W --worker" \
  --refuse-cmd "$W --worker --unix /tmp/vgi-hosted-refuse-{port}.sock" \
  --refuse-cmd "$W --worker --tcp 127.0.0.1:{port}" \
  --refuse-cmd "$W --worker --iroh-raw-upstream 127.0.0.1:{port} --iroh-issuer iroh" \
  "$@"
