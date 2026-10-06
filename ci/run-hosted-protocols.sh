#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Run vgi-rpc's hosted-protocols conformance group against the example fixture
# worker on stdio, AF_UNIX and HTTP (with vgi_rpc.Identity.v1), the way every
# VGI SDK does (MULTI_PROTOCOL_HOSTING.md §5/§7). The fixture hosts vgi.v2 and,
# through the `hostedProtocols` hook, conformance.Secondary.v1; over HTTP it
# opts into Identity under the IDENTITY_CONFORMANCE_FIXTURE.md policy.
#
# Needs `vgi-rpc-test-hosted` on PATH (vgi-rpc[http,conformance] + pytest +
# pytest-timeout). Override the command with VGI_RPC_TEST_HOSTED.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
HOSTED="${VGI_RPC_TEST_HOSTED:-vgi-rpc-test-hosted}"
EXPECT="vgi.v2,conformance.Secondary.v1"
WORKER="$REPO/bin/vgi-example-worker"
HTTP_WORKER="$REPO/bin/vgi-example-http-worker"
# Short and fixed-depth: AF_UNIX paths are capped near 108 bytes.
SOCK="$(mktemp -u /tmp/vgi-hosted-XXXXXX).sock"
WORK="$(mktemp -d)"
PIDS=()
cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
  rm -rf "$WORK" "$SOCK"
}
trap cleanup EXIT

echo "::group::stdio"
$HOSTED --cmd "$WORKER" --expect "$EXPECT"
echo "::endgroup::"

echo "::group::unix"
"$WORKER" --unix "$SOCK" --idle-timeout 0 >"$WORK/unix.out" 2>"$WORK/unix.err" &
PIDS+=($!)
for _ in $(seq 1 100); do [ -S "$SOCK" ] && break; sleep 0.1; done
[ -S "$SOCK" ] || { echo "unix worker never bound $SOCK"; cat "$WORK/unix.err"; exit 1; }
$HOSTED --unix "$SOCK" --expect "$EXPECT"
echo "::endgroup::"

echo "::group::http (--identity)"
"$HTTP_WORKER" >"$WORK/http.out" 2>"$WORK/http.err" &
PIDS+=($!)
PORT=""
for _ in $(seq 1 100); do
  PORT="$(grep -o 'PORT:[0-9]*' "$WORK/http.out" | head -1 | cut -d: -f2 || true)"
  [ -n "$PORT" ] && break
  sleep 0.1
done
[ -n "$PORT" ] || { echo "http worker reported no port"; cat "$WORK/http.err"; exit 1; }
$HOSTED --url "http://127.0.0.1:$PORT" --expect "$EXPECT" --identity
echo "::endgroup::"
