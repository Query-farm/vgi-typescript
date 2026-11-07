#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Type-check a tiny consumer project against the PACKED tarball, under the
# strictest compiler flags a consumer is likely to use.
#
# Why: the published declarations must stand on their own. A `.d.ts` that
# resolves (via package.json `imports`, e.g. `#arrow-impl`) to SDK *source*
# makes the consumer's tsc type-check that source under the consumer's flags —
# and `skipLibCheck` does not cover `.ts` files. That shipped in <=0.48.0:
# consumers with `noUncheckedIndexedAccess` got 46 errors inside the SDK.
#
# Usage: scripts/check-consumer-types.sh   (run after `bun run build`)
# Needs network access (npm install of the tarball's peer dependencies).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/vgi-consumer-types.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

cd "$ROOT"
if [ ! -f dist/index.d.ts ]; then
  echo "dist/ is missing — run 'bun run build' first" >&2
  exit 1
fi
TARBALL="$(npm pack --ignore-scripts --silent --pack-destination "$WORK")"
TARBALL="$WORK/$TARBALL"

ARROW_RANGE="$(node -p "require('./package.json').peerDependencies['@query-farm/apache-arrow']")"
RPC_RANGE="$(node -p "require('./package.json').peerDependencies['@query-farm/vgi-rpc']")"
# Consumers type-check with whatever tsc they have; the stable 5.x line is the
# common case (the SDK itself builds with tsgo/7.x).
TS_VERSION="${CONSUMER_TS_VERSION:-5.9}"

mkdir -p "$WORK/consumer/src"
cd "$WORK/consumer"
cat > package.json <<EOF
{ "private": true, "type": "module" }
EOF
npm install --silent --no-audit --no-fund \
  "$TARBALL" \
  "@query-farm/apache-arrow@$ARROW_RANGE" \
  "@query-farm/vgi-rpc@$RPC_RANGE" \
  "typescript@$TS_VERSION" \
  "@types/bun@latest" >/dev/null

# Touch every public entry point plus the types consumers actually write
# against, so their declaration graphs are all pulled into the program.
cat > src/consumer.ts <<'EOF'
import * as vgi from "@query-farm/vgi";
import * as client from "@query-farm/vgi/client";
import * as serve from "@query-farm/vgi/serve";
import * as cf from "@query-farm/vgi/worker-cf";
import type { TableCardinality } from "@query-farm/vgi";

export const entries = [vgi, client, serve, cf] as const;
export const schema = vgi.schema([vgi.field("n", vgi.int64())]);
export const card: TableCardinality = { estimate: 10n, max: null };
EOF

STRICT='"strict": true, "noUncheckedIndexedAccess": true, "exactOptionalPropertyTypes": true,
    "noImplicitOverride": true, "noPropertyAccessFromIndexSignature": true,
    "skipLibCheck": true, "noEmit": true, "target": "ES2022", "lib": ["ES2023", "DOM"], "types": ["bun"]'

status=0
for res in nodenext bundler; do
  if [ "$res" = nodenext ]; then mod=nodenext; else mod=esnext; fi
  cat > "tsconfig.$res.json" <<EOF
{ "compilerOptions": { $STRICT, "module": "$mod", "moduleResolution": "$res" },
  "include": ["src"] }
EOF
  echo "=== consumer type-check (typescript@$TS_VERSION, moduleResolution=$res) ==="
  if npx --no-install tsc -p "tsconfig.$res.json"; then
    echo "ok"
  else
    status=1
  fi
done

# Belt and braces: no SDK *source* file may be part of the consumer program.
if npx --no-install tsc -p tsconfig.nodenext.json --listFilesOnly \
    | grep -E 'node_modules/@query-farm/vgi/src/.*\.ts$'; then
  echo "::error::consumer program pulls in @query-farm/vgi source files (listed above)" >&2
  status=1
fi
exit $status
