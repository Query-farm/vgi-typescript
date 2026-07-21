#!/usr/bin/env bash
# Copyright 2025, 2026 Query Farm LLC - https://query.farm
#
# Run the canonical Query-farm/vgi integration sqllogictest suite against the
# TypeScript (bun) example workers, using a prebuilt standalone
# `haybarn-unittest` and the signed community vgi extension — no C++ build from
# source. See ci/README.md.
#
# Ported from vgi-go / vgi-python, which share this harness verbatim apart from
# the worker wiring. The TypeScript differences:
#   * The "worker binaries" are the `bin/vgi-example-*` bash wrappers that exec
#     `bun run examples/<entry>.ts`. HTTP is a SEPARATE entry point per catalog
#     (`bin/vgi-example-*-http-worker`) rather than a `--http` flag, so
#     boot_http_worker takes the http binary directly and passes no arguments.
#   * VGI_BUN_CONDITIONS selects the Arrow backend (unset = arrow-js,
#     "flechette" = flechette). It is read by the bin/ wrappers, so this script
#     only has to let it through the environment — CI runs the whole lane twice,
#     once per backend.
#   * There is no simple_writable / bad-protocol / bad-enum fixture worker in
#     the TypeScript port, so those tests skip via their `require-env` gates
#     (accounted for explicitly below — see EXPECTED_SKIP_REASONS).
#
# ── Why this exists (the bug it replaces) ────────────────────────────────────
# The previous integration job set LOCAL_EXTENSION_REPO to the community repo
# *globally*. `vgi` is not autoloadable, so it installed fine from there; but
# `httpfs` IS autoloadable, so DuckDB's CheckRequire took the autoinstall branch
# and issued `INSTALL httpfs FROM '<community repo>'`, which 404s. A failed
# `require` is a silent SKIP_TEST, and the per-file runner counted a skip as a
# pass. 261 of 286 test cases were skipping while CI reported "passed: 280".
# The fix, inherited from the sibling repos: scope the community repo to `vgi`
# only (rewriting each `require` into an explicit signed INSTALL+LOAD, deps FROM
# core) and *account for every skip* — see summarize_run below.
#
# Required environment:
#   VGI_SRC           path to a Query-farm/vgi checkout (contains test/sql/integration)
#   HAYBARN_UNITTEST  path to the haybarn-unittest binary
# Optional:
#   BIN_DIR           dir holding the worker wrappers (default: $REPO/bin)
#   TRANSPORT         http | stdio | launch          (default: http)
#   VGI_BUN_CONDITIONS  Arrow backend for the bun workers (unset | flechette)
#   EXTRA_EXCLUDES    extra integration-relative paths to drop from staging
#                     (space-separated; used for tests that are version-skewed
#                      against the *prebuilt* community extension)
#   MIN_EXECUTED      floor on executed test cases (default: per-transport, below)
#   PREPROCESS        1 (default) rewrites every `require` gate into a signed
#                     INSTALL+LOAD for the standalone runner; 0 leaves the gates
#                     alone (still injecting LOAD httpfs on the http lane) — use
#                     with a from-source `unittest`, which links the extensions
#                     statically and has no published build to install from
#   STAGE             scratch dir for the preprocessed test tree (default: mktemp)
set -uo pipefail

: "${VGI_SRC:?path to a Query-farm/vgi checkout}"
: "${HAYBARN_UNITTEST:?path to the haybarn-unittest binary}"

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
BIN_DIR="${BIN_DIR:-$REPO/bin}"
STAGE="${STAGE:-$(mktemp -d)}"
TRANSPORT="${TRANSPORT:-http}"
PREPROCESS="${PREPROCESS:-1}"
INTEGRATION="$VGI_SRC/test/sql/integration"
[ -d "$INTEGRATION" ] || { echo "::error::no test/sql/integration under VGI_SRC=$VGI_SRC"; exit 1; }

# The example worker wrappers (they exec `bun run examples/<entry>.ts`).
WORKER="$BIN_DIR/vgi-example-worker"
HTTP_WORKER="$BIN_DIR/vgi-example-http-worker"
VERSIONED="$BIN_DIR/vgi-example-versioned-worker"
VERSIONED_HTTP="$BIN_DIR/vgi-example-versioned-http-worker"
VERSIONED_TABLES="$BIN_DIR/vgi-example-versioned-tables-worker"
VERSIONED_TABLES_HTTP="$BIN_DIR/vgi-example-versioned-tables-http-worker"
ATTACH_OPTIONS="$BIN_DIR/vgi-example-attach-options-worker"
ATTACH_OPTIONS_HTTP="$BIN_DIR/vgi-example-attach-options-http-worker"
for b in "$WORKER" "$HTTP_WORKER" "$VERSIONED" "$VERSIONED_HTTP" \
         "$VERSIONED_TABLES" "$VERSIONED_TABLES_HTTP" \
         "$ATTACH_OPTIONS" "$ATTACH_OPTIONS_HTTP"; do
  [ -x "$b" ] || { echo "::error::missing worker wrapper $b"; exit 1; }
done
command -v bun >/dev/null 2>&1 || { echo "::error::bun not on PATH (the workers exec it)"; exit 1; }

# ---------------------------------------------------------------------------
# Stage a preprocessed copy of the suite. preprocess-require.awk rewrites each
# `require <ext>` gate into a signed INSTALL+LOAD so the standalone runner
# (which links none of these extensions) can run them: `vgi` FROM community,
# everything else FROM core. On the http lane it also injects `LOAD httpfs`
# before each worker ATTACH, which the prebuilt binary needs before it can bind
# `ATTACH ... (TYPE vgi, LOCATION 'http://...')`.
# ---------------------------------------------------------------------------
#
# EXCLUDED holds integration-relative paths and is the single source of truth
# for "not in scope": a file that is not staged cannot run, and the runner is
# pointed at the staged tree with `--test-dir` (see below), so this list is the
# only place scope is decided.
# ---------------------------------------------------------------------------
# Out of scope on every lane (mirrors the Makefile's TEST_PATTERNS, same reasons):
#   writable/ — the opt-in generic writable catalog (VGI_WORKER_ENABLE_WRITABLE);
#     the TypeScript port has no fixture worker for it.
#   schema_reconcile.test — writable-style fixture, likewise not ported.
#   table/constant_columns_types.test — arrow-js has no TIMESTAMP_NS.
#   catalog/zero_count_bypass.test — broken upstream (its LIKE pattern matches
#     set_kind=table AND set_kind=table_function ambiguously); fails against the
#     Python worker too.
#   table_in_out/echo/nested_type_combinations.test — segfaults the prebuilt
#     standalone runner (a property of that C++ build, not the worker, which
#     passes it against a locally-built unittest). Same drop as vgi-go/-python.
EXCLUDED=(
  'writable/*'
  'schema_reconcile.test'
  'table/constant_columns_types.test'
  'catalog/zero_count_bypass.test'
  'table_in_out/echo/nested_type_combinations.test'
)
AWK_HTTP=0
if [ "$TRANSPORT" = "http" ]; then
  AWK_HTTP=1
  # Dropped on the http lane only. Each of these asserts something about the
  # *subprocess* worker pool that a single long-lived HTTP server cannot model;
  # they mirror the Makefile's HTTP_TEST_PATTERNS, with the reasons kept here so
  # the two lists stay legible side by side:
  #   * filter_echo_partitioned.test — asserts COUNT(DISTINCT worker_pid) > 1;
  #     an HTTP worker is one OS process, so worker_pid collapses to one value.
  #   * partitioned_sequence.test — same root cause, via distinct conn= ids in
  #     the batch_received logs under threads=4.
  #   * batch_index.test / order_preservation_modes.test — both read VGI
  #     batch_received log rows, which don't stream over HTTP (0 log rows).
  #   * cache/identity_isolation.test — asserts the WORKER-visible auth principal
  #     ("alice"/"bob"). The Python fixture server maps vgi-test-alice→alice; the
  #     TypeScript example HTTP worker has no token→principal resolver, so it
  #     reports "anonymous". A genuine port gap in the *fixture*, not the harness
  #     — the sibling test cache/partition_scope_identity.test still runs here and
  #     covers the isolation property itself (the C++ side folds the bearer-token
  #     fingerprint into the cache key regardless of the principal name).
  #   * dynamic_filter.test — the Top-N + dynamic-filter continuation terminates
  #     early over http, so the tightened pushdown never reaches the worker and
  #     COUNT(DISTINCT pushed_filters) stays at 1. Same drop, same reason, as
  #     vgi-go's and vgi-python's http lanes; verified still failing against a
  #     from-source vgi build, so it is not prebuilt-extension skew.
  EXCLUDED+=(
    'table/dynamic_filter.test'
    'table/filter_echo_partitioned.test'
    'table/partitioned_sequence.test'
    'table/batch_index.test'
    'table/order_preservation_modes.test'
    'cache/identity_isolation.test'
  )
fi
# Caller-supplied exclusions (integration-relative paths) — an escape hatch for a
# test that is skewed against a particular published extension build. CI sets it
# to nothing: every real exclusion belongs in EXCLUDED above, next to its reason.
for p in ${EXTRA_EXCLUDES:-}; do
  EXCLUDED+=("${p#./}")
done

FIND_SKIP=()
for p in "${EXCLUDED[@]}"; do FIND_SKIP+=(-not -path "./$p"); done

echo "Staging tests into $STAGE (transport=$TRANSPORT, preprocess=$PREPROCESS) ..."
mkdir -p "$STAGE/test/sql/integration"
( cd "$INTEGRATION"
  find . -name '*.test' "${FIND_SKIP[@]}" | while read -r f; do
    mkdir -p "$STAGE/test/sql/integration/$(dirname "$f")"
    awk -v http="$AWK_HTTP" -v install="$PREPROCESS" \
        -f "$HERE/preprocess-require.awk" "$f" > "$STAGE/test/sql/integration/$f"
  done )
STAGED=$(find "$STAGE/test/sql/integration" -name '*.test' | wc -l | tr -d ' ')
echo "Staged $STAGED test files."

# `--test-dir $STAGE` makes the runner discover *and* chdir to the staged tree.
# It is required, not cosmetic: a from-source `unittest` bakes DUCKDB_ROOT_DIRECTORY
# in at compile time and would otherwise silently run the build's own source tree,
# ignoring every exclusion and every rewrite made above.
UNITTEST_ARGS=(--test-dir "$STAGE")
# The http workers are booted with the same cwd, because copy_from/copy_to hand
# the worker a relative `__TEST_DIR__` path (duckdb_unittest_tempdir/<pid>/…)
# that only resolves if the worker shares DuckDB's working directory.
RUN_CWD="$STAGE"

# Force the C++ extension's init_global RPC to run synchronously so multi-conn
# parallel-init tests observe the worker's real max_workers (mirrors the Makefile).
export VGI_SYNC_INIT_GLOBAL=1

# Background workers (http servers) are tracked in a file and SIGTERMed on exit.
# A file (not a shell array) keeps the teardown robust regardless of how
# boot_http_worker is invoked.
BG_PIDS_FILE="$(mktemp)"
# shellcheck disable=SC2329  # invoked indirectly via `trap cleanup EXIT` below
cleanup() {
  [ -f "$BG_PIDS_FILE" ] || return 0
  while read -r p; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done < "$BG_PIDS_FILE"
}
trap cleanup EXIT

# boot_http_worker <http-binary> — start it and set BOOTED_PORT to the port it
# reports on stdout (`PORT:<n>`, the example workers' readiness contract; the
# Makefile's test-http target reads the same line). Sets a global rather than
# echoing because the caller must NOT wrap it in $(...): a command-substitution
# subshell reparents the backgrounded worker out of the main shell.
#
# The worker is spawned with cwd=$STAGE — the same directory the unittest binary
# runs from below. copy_from/copy_to tests have DuckDB write a source file under
# a relative `__TEST_DIR__` (duckdb_unittest_tempdir/...) that the worker then
# opens by the same relative path; it only resolves if the worker shares DuckDB's
# cwd. On the stdio lane the extension spawns the worker as a child that inherits
# that cwd for free; the `( cd … ; exec … )` subshell gives the http worker the
# same footing. Bun cold start is slow, hence the generous 60 s budget.
BOOTED_PORT=""
boot_http_worker() {
  local exe="$1" log pid port=""
  BOOTED_PORT=""
  log="$(mktemp)"
  ( cd "$RUN_CWD" && exec "$exe" ) >"$log" 2>&1 &
  pid=$!
  echo "$pid" >> "$BG_PIDS_FILE"
  for _ in $(seq 1 120); do
    kill -0 "$pid" 2>/dev/null || { echo "::error::http worker '$exe' exited" >&2; cat "$log" >&2; return 1; }
    port="$(sed -n 's/.*PORT:\([0-9]*\).*/\1/p' "$log" | head -1)"
    [ -n "$port" ] && break
    sleep 0.5
  done
  [ -n "$port" ] || { echo "::error::http worker '$exe' never reported a port" >&2; cat "$log" >&2; return 1; }
  BOOTED_PORT="$port"
  echo "  booted $(basename "$exe") on port $port"
}

# ---------------------------------------------------------------------------
# Expected skips.
#
# Every skip must be named here, with the reason it is legitimate. A skip whose
# reason is NOT listed fails the run — that is the whole point of this file: a
# whole-suite skip (`require httpfs: 227`) must never read as green again.
# The strings are the exact reasons Catch2 prints under "Skipped tests for the
# following reasons:".
# ---------------------------------------------------------------------------
EXPECTED_SKIP_REASONS=(
  # No such fixture worker in the TypeScript port (the gate is the whole point —
  # these are cross-language fixtures other SDKs provide).
  'require-env VGI_SIMPLE_WRITABLE_WORKER'   # generic write-path fixture, not ported
  'require-env VGI_BAD_PROTOCOL_WORKER'      # advertises an incompatible protocol_version
  'require-env VGI_BAD_ENUM_WORKER'          # advertises a malformed ENUM
  'require-env VGI_RULES_WORKER'             # vgi-rust multibatch repro worker
  'require-env VGI_WORKER_SUPPORTS_DYNAMIC_CODE'  # dynamic-code registration, not implemented
  # Infrastructure this repo's CI deliberately does not stand up.
  'require-env VGI_DOCKER_IMAGE'             # containerised worker lane
  'require-env VGI_DOCKER_TCP_IMAGE'         # containerised worker over TCP
  'require-env VGI_GITHUB_NETWORK_TESTS'     # hits github.com; opt-in only
  'require-env VGI_TEST_COMPANION_TARGET'    # companion-catalog fixture (Python-side)
  'require-env VGI_TEST_BRANCH_DIR'          # multi-branch Iceberg fixture tree
  'require-env VGI_TEST_BEARER_TOKEN'        # bearer-auth fixture server
  'require-env VGI_HTTP_DISABLE_ZSTD'        # gzip-fallback lane (separate run)
  'require spatial'                          # spatial is not published for every haybarn build
)
# Transport-specific additions.
case "$TRANSPORT" in
  http)
    # An HTTP worker is one shared process: SIGKILL-ing it would take down every
    # concurrent test, so the crash / pool-recovery tests gate themselves off.
    EXPECTED_SKIP_REASONS+=('require-env VGI_TEST_DEDICATED_WORKER')
    # The launcher lane's own tests (`launch:` LOCATION) don't apply over http.
    EXPECTED_SKIP_REASONS+=('require-env VGI_REQUIRE_LAUNCHER_TRANSPORT')
    ;;
  launch)
    # Bearer identity and the capability/codec probes ride HTTP headers only.
    EXPECTED_SKIP_REASONS+=('require-env VGI_HTTP_TRANSPORT')
    # Same reason as http: a `launch:` worker is shared by every unittest process
    # that hashes to the same tuple, so the SIGKILL-self tests stay gated off.
    EXPECTED_SKIP_REASONS+=('require-env VGI_TEST_DEDICATED_WORKER')
    ;;
  stdio)
    EXPECTED_SKIP_REASONS+=('require-env VGI_HTTP_TRANSPORT')
    ;;
esac

# Floor on executed test cases. The regression this harness exists to catch is
# "the suite silently stopped running", which shows up as a collapse in this
# number long before anyone notices a suspicious wall-clock. Deliberately a
# floor, not an equality: the upstream suite grows.
# Measured against Query-farm/vgi @ b9f3895: the http lane executes 253 of 282
# staged files. The floor leaves ~8 files of headroom for upstream churn and is
# far above the 25 that executed before this harness landed.
case "$TRANSPORT" in
  http)   MIN_EXECUTED="${MIN_EXECUTED:-245}" ;;
  stdio)  MIN_EXECUTED="${MIN_EXECUTED:-245}" ;;
  launch) MIN_EXECUTED="${MIN_EXECUTED:-240}" ;;
esac

case "$TRANSPORT" in
  stdio)
    # Subprocess transport: one bun worker per DuckDB process, pooled.
    export VGI_TEST_WORKER="$WORKER"
    export VGI_VERSIONED_WORKER="$VERSIONED"
    export VGI_VERSIONED_TABLES_WORKER="$VERSIONED_TABLES"
    export VGI_ATTACH_OPTIONS_WORKER="$ATTACH_OPTIONS"
    # A private worker per DuckDB process makes the SIGKILL-self crash tests safe.
    export VGI_TEST_DEDICATED_WORKER="$WORKER"
    # attach/versioned_tables_*_http and versioning_http attach an http:// worker
    # regardless of the main transport, so serve those two catalogs over http too.
    boot_http_worker "$VERSIONED_TABLES_HTTP" || exit 1
    export VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:${BOOTED_PORT}"
    boot_http_worker "$VERSIONED_HTTP" || exit 1
    export VGI_VERSIONED_HTTP_WORKER="http://localhost:${BOOTED_PORT}"
    ;;
  launch)
    # AF_UNIX launcher transport (the repo's default `make test`): the C++
    # launcher spawns one worker per (argv, cwd, VGI_RPC_*-env) tuple and every
    # unittest process that hashes to the same tuple reuses it — no per-test bun
    # cold start. src/worker.ts handles the launcher's --unix / --idle-timeout.
    export VGI_TEST_WORKER="launch:${WORKER}"
    export VGI_VERSIONED_WORKER="launch:${VERSIONED}"
    export VGI_VERSIONED_TABLES_WORKER="launch:${VERSIONED_TABLES}"
    export VGI_ATTACH_OPTIONS_WORKER="launch:${ATTACH_OPTIONS}"
    export VGI_REQUIRE_LAUNCHER_TRANSPORT=1
    export VGI_WORKER_IDLE_TIMEOUT="${VGI_WORKER_IDLE_TIMEOUT:-120}"
    boot_http_worker "$VERSIONED_TABLES_HTTP" || exit 1
    export VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:${BOOTED_PORT}"
    boot_http_worker "$VERSIONED_HTTP" || exit 1
    export VGI_VERSIONED_HTTP_WORKER="http://localhost:${BOOTED_PORT}"
    ;;
  http)
    # Whole-suite-over-HTTP (mirrors make test-http): every ATTACH goes over
    # http://, so staging injected `LOAD httpfs` and dropped the pool-semantics
    # files. All four catalogs get their own long-lived server.
    boot_http_worker "$HTTP_WORKER" || exit 1
    export VGI_TEST_WORKER="http://localhost:${BOOTED_PORT}"
    boot_http_worker "$VERSIONED_HTTP" || exit 1
    export VGI_VERSIONED_HTTP_WORKER="http://localhost:${BOOTED_PORT}"
    boot_http_worker "$ATTACH_OPTIONS_HTTP" || exit 1
    export VGI_ATTACH_OPTIONS_WORKER="http://localhost:${BOOTED_PORT}"
    boot_http_worker "$VERSIONED_TABLES_HTTP" || exit 1
    export VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:${BOOTED_PORT}"
    # attach/versioning.test and attach/versioned_tables*.test attach the
    # versioned catalogs over the SUBPROCESS transport (they assert bind-time
    # version resolution, which is transport-agnostic). Wire the stdio wrappers
    # too rather than let five files skip — the http lane is about the *main*
    # catalog's transport, not about starving the other catalogs.
    export VGI_VERSIONED_WORKER="$VERSIONED"
    export VGI_VERSIONED_TABLES_WORKER="$VERSIONED_TABLES"
    # Un-skips the HTTP-only tests (bearer identity, capability/codec probes)
    # exactly as the vgi repo's own test/run_http_integration.sh does.
    export VGI_HTTP_TRANSPORT=1
    ;;
  *)
    echo "::error::unknown TRANSPORT=$TRANSPORT (expected http|stdio|launch)"; exit 1 ;;
esac
SUITE_GLOB="test/sql/integration/*"

cd "$RUN_CWD"

if [ "$PREPROCESS" = "1" ]; then
  echo "Warming the extension cache (vgi from community, deps from core) ..."
  mkdir -p "$STAGE/test"
  # FORCE INSTALL vgi re-downloads the currently-published community build,
  # overriding any older cached copy, so the suite runs against what users can
  # install today. Everything else comes FROM core — never from the community
  # repo, which is the mistake that made 261 test cases skip.
  cat > "$STAGE/test/_warm.test" <<'EOF'
# name: test/_warm.test
# group: [warm]
statement ok
FORCE INSTALL vgi FROM community;

statement ok
INSTALL httpfs FROM core;

statement ok
INSTALL json FROM core;

statement ok
INSTALL parquet FROM core;
EOF
  "$HAYBARN_UNITTEST" "${UNITTEST_ARGS[@]}" "test/_warm.test" >/dev/null 2>&1 || echo "::warning::extension warm step did not fully succeed"
  rm -f "$STAGE/test/_warm.test"
fi

# ---------------------------------------------------------------------------
# summarize_run <log> — parse the unittest console report and enforce the skip
# contract. Catch2 reports skips, but only in aggregate and only as a warning;
# the exit code is 0 whether one test skipped or all of them did. So:
#
#   total    = the N in the last "[i/N] (..%):" progress line (test cases seen)
#   skipped  = the sum of the "Skipped tests for the following reasons:" block
#   executed = total - skipped        <- the number that must not collapse
#
# Fails if a skip reason is not in EXPECTED_SKIP_REASONS, or if executed drops
# below MIN_EXECUTED, or if the runner matched no tests at all.
# ---------------------------------------------------------------------------
TOTAL_EXECUTED=0
summarize_run() {
  local log="$1" total skipped executed rc=0 reason count
  if grep -q 'No test cases matched\|No tests ran' "$log"; then
    echo "::error::the runner matched no test cases — the glob or the staging is wrong"
    return 1
  fi
  total="$(sed -n 's/^\[[0-9]*\/\([0-9]*\)\].*/\1/p' "$log" | tail -1)"
  [ -n "$total" ] || { echo "::error::could not parse a test-case total out of the report"; return 1; }
  skipped="$(awk '
    /^Skipped tests for the following reasons:/ { in_block = 1; next }
    in_block && /^[[:space:]]*$/               { in_block = 0; next }
    in_block && match($0, /: [0-9]+[[:space:]]*$/) { n += substr($0, RSTART + 2) }
    END { print n + 0 }' "$log")"
  executed=$(( total - skipped ))
  echo ""
  echo "──────────────────────────────────────────────────────────"
  echo "  transport: $TRANSPORT   arrow-backend: ${VGI_BUN_CONDITIONS:-arrow-js}"
  echo "  executed:  $executed / $total test cases"
  echo "  skipped:   $skipped"
  if [ "$skipped" -gt 0 ]; then
    while IFS= read -r line; do
      reason="${line% : *}"; count="${line##* : }"
      if printf '%s\n' "${EXPECTED_SKIP_REASONS[@]}" | grep -qxF "$reason"; then
        echo "               - $reason: $count  (expected)"
      else
        echo "               - $reason: $count  ::UNEXPECTED::"
        echo "::error::unexpected skip reason '$reason' ($count test cases). Either the" \
             "gate genuinely regressed, or add it to EXPECTED_SKIP_REASONS in ci/run-integration.sh" \
             "with the reason it is legitimate."
        rc=1
      fi
    done < <(awk '
      /^Skipped tests for the following reasons:/ { in_block = 1; next }
      in_block && /^[[:space:]]*$/               { in_block = 0; next }
      in_block && match($0, /: [0-9]+[[:space:]]*$/) {
        printf "%s : %s\n", substr($0, 1, RSTART - 1), substr($0, RSTART + 2)
      }' "$log")
  fi
  if [ "$executed" -lt "$MIN_EXECUTED" ]; then
    echo "::error::only $executed test cases executed, floor is MIN_EXECUTED=$MIN_EXECUTED." \
         "This is the signature of a suite-wide silent skip (a failed 'require' is a" \
         "SKIP_TEST, not an error). Do NOT lower the floor to make this pass."
    rc=1
  fi
  echo "  floor:     $MIN_EXECUTED (MIN_EXECUTED)"
  echo "──────────────────────────────────────────────────────────"
  TOTAL_EXECUTED=$(( TOTAL_EXECUTED + executed ))
  return "$rc"
}

# run_unittest — invoke haybarn-unittest, streaming its output, and additionally
# fail on a fatal-signal report that the process's own exit code cannot express.
#
# Catch2 arms handlers for SIGTERM/SIGINT/SIGSEGV/... for the duration of a test
# case. Those handlers are inherited by any process the extension fork()s, and
# run in the child if a signal lands before it execs. The child then prints a
# full "FAILED: ... due to a fatal error condition: SIGTERM" block plus a run
# summary — the *parent's* accumulated counters, since it's an address-space
# copy — and dies. The parent never sees it, records no failure, and exits 0.
# The only trace is on stdout, so that is what we scan. (Inherited from
# vgi-python's harness, where it was first observed.)
run_unittest() {
  local log rc=0
  log="$(mktemp)"
  # Piped, not `|| true`: `|| true` runs before PIPESTATUS is read and overwrites
  # it with true's 0, silently swallowing every real test failure. (The script
  # runs without `set -e` for the same reason — the accounting below must run
  # even when the suite itself failed.)
  "$HAYBARN_UNITTEST" "${UNITTEST_ARGS[@]}" "$@" 2>&1 | tee "$log"
  rc="${PIPESTATUS[0]}"
  if grep -q 'due to a fatal error condition' "$log"; then
    echo "::error::a forked child ran the test harness's signal handler (see the" \
         "'fatal error condition' block above). The parent exited $rc and would" \
         "otherwise have passed. This is invisible to the exit code by construction."
    rc=1
  fi
  summarize_run "$log" || rc=1
  rm -f "$log"
  return "$rc"
}

echo "Running suite ($SUITE_GLOB, transport=$TRANSPORT) ..."
SECONDS=0
suite_rc=0
run_unittest "$SUITE_GLOB" || suite_rc=$?
echo "Suite wall clock: ${SECONDS}s for $TOTAL_EXECUTED executed test cases."

exit "$suite_rc"
