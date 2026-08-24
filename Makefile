# VGI TypeScript Makefile
# Build and test targets for vgi-typescript.
# Tests are independent targets — use `make -j8 test` for parallel execution.

# Recipes use bash features (`read -t`, functions, `${VAR:+...}`). On Ubuntu the
# default recipe shell is /bin/sh (dash), which lacks `read -t` — force bash.
SHELL := /bin/bash

.PHONY: build build\:types build\:js install clean test test-subprocess test-http test-all test-client

# --- Configuration (all overridable) ---

VGI_DIR      ?= /Users/rusty/Development/vgi
VGI_PYTHON_DIR ?= /Users/rusty/Development/vgi-python
TEST_TIMEOUT ?= 120
WORKER                ?= $(CURDIR)/bin/vgi-example-worker
HTTP_WORKER           := $(CURDIR)/bin/vgi-example-http-worker
VERSIONED_WORKER      := $(CURDIR)/bin/vgi-example-versioned-worker
VERSIONED_HTTP        := $(CURDIR)/bin/vgi-example-versioned-http-worker
VERSIONED_TABLES_WORKER := $(CURDIR)/bin/vgi-example-versioned-tables-worker
VERSIONED_TABLES_HTTP   := $(CURDIR)/bin/vgi-example-versioned-tables-http-worker
ATTACH_OPTIONS_WORKER := $(CURDIR)/bin/vgi-example-attach-options-worker
ATTACH_OPTIONS_HTTP   := $(CURDIR)/bin/vgi-example-attach-options-http-worker

TEST_DIR     := $(VGI_DIR)/test/sql
RELEASE_BIN  := $(VGI_DIR)/build/release/test/unittest

# --- Build targets ---

build:
	bun run build

build\:types:
	bun run build:types

build\:js:
	bun run build:js

install:
	bun install

clean:
	rm -rf dist/

# --- Test targets ---
#
# These are the LOCAL developer targets. They drive vgi/scripts/run_tests.py,
# which has no "skipped" category — a test that skips (a failed `require`, an
# unset `require-env`) exits 0 and is reported as a pass. That is fine while
# iterating, but it means a green `make test-http` is not evidence the suite ran.
# CI uses ci/run-integration.sh instead, which counts and allowlists every skip
# and enforces a floor on executed tests; run that when you need certainty.
#
# Use the unittest harness's own -j 8 parallelism (see ~/Development/vgi
# Makefile's test_subprocess target). One unittest invocation runs every
# matching test in parallel, captures output to a log file, and prints a
# pass/fail summary plus a list of failed tests at the end.
#
# Patterns:
#   "test/sql/*"                          — every test file
#   "~test/sql/integration/writable/*"    — exclude the writable fixture
#                                           tree (we don't port that worker)

TEST_LOG := /tmp/vgi-typescript-test.log

# Excluded patterns (use ~ prefix for unittest's filter exclusion syntax):
#   writable/                          — writable fixture worker not ported
#   schema_reconcile                   — writable-style fixture, also skipped
#   constant_columns_types             — arrow-js doesn't support TIMESTAMP_NS
#
# zero_count_bypass is NO LONGER excluded (removed 2026-08-21). The old reason
# ("broken upstream; its LIKE pattern matches set_kind=table AND
# set_kind=table_function ambiguously") was a correct diagnosis of a bug that has
# since been fixed centrally: the test now anchors on the field separator,
# LIKE '%set_kind=table,%'. Verified against THIS worker on all three lanes —
# subprocess, launch:, and http — 23 assertions pass on each.
#
# HTTP-only exclusions: NONE. This block listed three, and every reason was
# wrong — verified 2026-08-21 by running each against this worker over HTTP:
#   filter_echo_partitioned  "asserts COUNT(DISTINCT worker_pid) > 1" — stale;
#                            the test moved to the transport-neutral conn= form.
#   partitioned_sequence     inherited that misdiagnosis.
#   order_preservation_modes "batch_received logs don't stream over HTTP (0 log
#                            rows)" — measured 199 rows. The scan really was
#                            collapsing to ONE connection, because this SDK's
#                            HTTP turn loop packed a whole producer stream into
#                            a single response (fixed in vgi-rpc-typescript).
# The exclusions were hiding a real bug in this SDK for months. Before adding
# one here, measure the claim.
TEST_PATTERNS := "test/sql/*" \
	"~test/sql/integration/writable/*" \
	"~test/sql/integration/schema_reconcile.test" \
	"~test/sql/integration/table/constant_columns_types.test"

# Launcher transport excludes vgi_worker_pool.test, which asserts subprocess-pool
# semantics: `launch:` workers are pooled by the AF_UNIX socket, not by DuckDB's
# per-process subprocess pool, so vgi_worker_pool() legitimately returns no rows
# there (documented in vgi's CLAUDE.md). Mirrors vgi's own test_launcher target.
#
# Three more tests used to be excluded here and were removed 2026-08-21, having
# never been excluded on this repo's own CI `launch` lane (ci/run-integration.sh),
# which runs the same transport — the Makefile lane was the stale copy. Each was
# re-verified over launch: against this worker before removal:
#   filter_echo_partitioned  36 assertions pass. The stale reason was the old
#                            COUNT(DISTINCT worker_pid) > 1 form; the test now
#                            counts transport-neutral conn= ids.
#   versioned_tables_impl    35 assertions pass (with VGI_VERSIONED_TABLES_WORKER
#                            set, as this lane sets it below).
#   order_preservation_modes 16 assertions pass. The FIXED_ORDER → 1-distinct-conn
#                            claim is a real HTTP limitation, but it does not
#                            apply to launch:, which collapses to one connection
#                            like plain subprocess.
LAUNCHER_TEST_PATTERNS := $(TEST_PATTERNS) \
	"~test/sql/vgi_worker_pool.test"

# Idle-timeout for launcher-spawned workers. The C++ launcher passes
# --idle-timeout 300 by default; src/worker.ts honours
# VGI_WORKER_IDLE_TIMEOUT as an override.
#
# Under -j8 every parallel unittest process shares ONE warm Bun worker
# (the launcher pools by argv/cwd/env). With a 5 s idle timeout that shared
# worker would idle-exit and respawn mid-suite under bursty load; a query
# arriving during the respawn window stalled into the C++ side's 30 s
# catalog-RPC timeout and surfaced as flaky "VGI catalog operation timed
# out" failures (filter_echo, column_statistics, …). 120 s keeps the worker
# warm for the whole run; it still exits well before the next suite.
LAUNCHER_IDLE_TIMEOUT ?= 120

HTTP_TEST_PATTERNS := "test/sql/integration/*" \
	"~test/sql/integration/writable/*" \
	"~test/sql/integration/schema_reconcile.test" \
	"~test/sql/integration/table/constant_columns_types.test" \
	$(EXTRA_HTTP_EXCLUDES)

# Parallelism for the per-test runner. Default 8 locally; CI sets JOBS=1 to
# avoid an `INSTALL vgi` race when fresh unittest processes autoinstall the
# community extension into a cold cache simultaneously.
JOBS ?= 8
# Extra `~`-exclusions appended to HTTP_TEST_PATTERNS (set in CI to skip tests
# that are version-skewed against the prebuilt community extension).
EXTRA_HTTP_EXCLUDES ?=

# Default test target: launcher (`launch:`) transport.
#
# The vgi extension's C++ AF_UNIX launcher spawns each worker once per
# (argv, cwd, VGI_RPC_*-env) tuple and reuses it across every parallel
# unittest invocation that hashes to the same tuple. Running 8 jobs no
# longer means 8× Bun cold-starts — measured ~5× wall-clock improvement
# in the upstream extension's own suite.
#
# Worker support: src/worker.ts parses `--unix PATH` / `--idle-timeout SEC`
# (added by the launcher) and dispatches to vgi-rpc's serveUnix() instead
# of the stdio VgiRpcServer.
#
# Use `make test-subprocess` if you specifically need per-process subprocess
# semantics (e.g. debugging a worker startup issue).
# Coverage gates — see the note in vgi/Makefile (VGI_EXPECTED_SKIPS). A lane
# that stops running tests reports green, so a floor on executed tests and an
# allow-list of expected skip reasons are what keep a silently-shrinking lane
# from passing. TypeScript runs 293 today.
TS_MIN_EXECUTED ?= 290
COVERAGE_GATE := --min-executed $(TS_MIN_EXECUTED) \
	--allow-skip 'require spatial' \
	--allow-skip 'require-env VGI_DOCKER_IMAGE' \
	--allow-skip 'require-env VGI_DOCKER_TCP_IMAGE' \
	--allow-skip 'require-env VGI_GITHUB_NETWORK_TESTS' \
	--allow-skip 'require-env VGI_TEST_ICEBERG' \
	--allow-skip 'require-env VGI_TEST_COMPANION_TARGET' \
	--allow-skip 'require-env VGI_TEST_BEARER_TOKEN' \
	--allow-skip 'require-env VGI_TEST_DEDICATED_WORKER' \
	--allow-skip 'require-env VGI_HTTP_TRANSPORT' \
	--allow-skip 'require-env VGI_HTTP_DISABLE_ZSTD' \
	--allow-skip 'require-env VGI_HTTP_NO_COMPRESSION' \
	--allow-skip 'require-env VGI_VERSIONED_HTTP_WORKER' \
	--allow-skip 'require-env VGI_VERSIONED_TABLES_HTTP_WORKER' \
	--allow-skip 'require-env VGI_WORKER_SUPPORTS_DYNAMIC_CODE' \
	--allow-skip 'require-env VGI_SIMPLE_WRITABLE_WORKER' \
	--allow-skip 'require-env VGI_SCHEMA_RECONCILE_DB' \
	--allow-skip 'require-env VGI_RULES_WORKER' \
	--allow-skip 'require-env VGI_ATTACH_OPTIONS_REQUIRED_WORKER' \
	--allow-skip 'require-env VGI_BAD_ENUM_WORKER' \
	--allow-skip 'require-env VGI_BAD_PROTOCOL_WORKER'

test:
	@cd $(VGI_DIR) && \
	export VGI_TEST_WORKER="launch:$(WORKER)"; \
	export VGI_VERSIONED_WORKER="launch:$(VERSIONED_WORKER)"; \
	export VGI_VERSIONED_TABLES_WORKER="launch:$(VERSIONED_TABLES_WORKER)"; \
	export VGI_ATTACH_OPTIONS_WORKER="launch:$(ATTACH_OPTIONS_WORKER)"; \
	export VGI_REQUIRE_LAUNCHER_TRANSPORT=1; \
	export VGI_WORKER_IDLE_TIMEOUT=$(LAUNCHER_IDLE_TIMEOUT); \
	python3 scripts/run_tests.py -j $(JOBS) $(COVERAGE_GATE) $(LAUNCHER_TEST_PATTERNS) > $(TEST_LOG) 2>&1; \
	rc=$$?; \
	tail -n 20 $(TEST_LOG); \
	echo ""; \
	if [ $$rc -eq 0 ]; then \
		echo "All tests passed (launcher). Log: $(TEST_LOG)"; \
	else \
		echo "Some tests failed (rc=$$rc, launcher). Full log: $(TEST_LOG)"; \
	fi; \
	exit $$rc

# Plain subprocess transport — one worker per DuckDB process, pooled.
# Same suite as `test` plus vgi_worker_pool.test, the one file the launcher path
# can't satisfy (it asserts per-process subprocess-pool semantics; launch:
# workers are pooled by the AF_UNIX socket, so vgi_worker_pool() has no rows).
test-subprocess:
	@cd $(VGI_DIR) && \
	export VGI_TEST_WORKER="$(WORKER)"; \
	export VGI_VERSIONED_WORKER="$(VERSIONED_WORKER)"; \
	export VGI_VERSIONED_TABLES_WORKER="$(VERSIONED_TABLES_WORKER)"; \
	export VGI_ATTACH_OPTIONS_WORKER="$(ATTACH_OPTIONS_WORKER)"; \
	python3 scripts/run_tests.py -j $(JOBS) $(TEST_PATTERNS) > $(TEST_LOG) 2>&1; \
	rc=$$?; \
	tail -n 20 $(TEST_LOG); \
	echo ""; \
	if [ $$rc -eq 0 ]; then \
		echo "All tests passed (subprocess). Log: $(TEST_LOG)"; \
	else \
		echo "Some tests failed (rc=$$rc, subprocess). Full log: $(TEST_LOG)"; \
	fi; \
	exit $$rc

# HTTP transport: same pattern, but the workers need to be running at known
# URLs. The HTTP example workers each write a PORT line on stdout when they
# start; we read it through a FIFO and export the URLs before invoking unittest.
#
# Each FIFO is opened read-write on a dedicated fd (exec N<>fifo) *before* the
# worker is launched. A plain `read < fifo` would make the worker's open() for
# write block until the reader attaches; on macOS, launching the four heavy Bun
# workers this way deadlocks (the second worker never reaches its PORT write).
# Holding the read end open keeps the worker's write-open non-blocking, while
# preserving pipe semantics (a regular-file capture is block-buffered on Linux,
# so the PORT line never flushes there). Works on both macOS and Linux/CI.
test-http:
	@cd $(VGI_DIR) && \
	port_fifo=$$(mktemp -u); mkfifo "$$port_fifo"; exec 3<>"$$port_fifo"; \
	$(HTTP_WORKER) > "$$port_fifo" 2>/tmp/vgi-http-worker.err & http_pid=$$!; \
	vport_fifo=$$(mktemp -u); mkfifo "$$vport_fifo"; exec 4<>"$$vport_fifo"; \
	$(VERSIONED_HTTP) > "$$vport_fifo" 2>/dev/null & vhttp_pid=$$!; \
	aport_fifo=$$(mktemp -u); mkfifo "$$aport_fifo"; exec 5<>"$$aport_fifo"; \
	$(ATTACH_OPTIONS_HTTP) > "$$aport_fifo" 2>/dev/null & ahttp_pid=$$!; \
	tport_fifo=$$(mktemp -u); mkfifo "$$tport_fifo"; exec 6<>"$$tport_fifo"; \
	$(VERSIONED_TABLES_HTTP) > "$$tport_fifo" 2>/dev/null & thttp_pid=$$!; \
	cleanup() { \
		kill $$http_pid $$vhttp_pid $$ahttp_pid $$thttp_pid 2>/dev/null; \
		wait $$http_pid $$vhttp_pid $$ahttp_pid $$thttp_pid 2>/dev/null; \
		exec 3>&- 4>&- 5>&- 6>&- 2>/dev/null; \
		rm -f "$$port_fifo" "$$vport_fifo" "$$aport_fifo" "$$tport_fifo"; \
	}; \
	trap cleanup EXIT; \
	read -t 60 port_line <&3 || { echo "ERROR: HTTP worker timeout"; echo "--- worker stderr ---"; cat /tmp/vgi-http-worker.err 2>/dev/null; exit 1; }; \
	read -t 60 vport_line <&4 || { echo "ERROR: versioned HTTP worker timeout"; exit 1; }; \
	read -t 60 aport_line <&5 || { echo "ERROR: attach-options HTTP worker timeout"; exit 1; }; \
	read -t 60 tport_line <&6 || { echo "ERROR: versioned-tables HTTP worker timeout"; exit 1; }; \
	export VGI_TEST_WORKER="http://localhost:$${port_line#PORT:}"; \
	export VGI_VERSIONED_HTTP_WORKER="http://localhost:$${vport_line#PORT:}"; \
	export VGI_ATTACH_OPTIONS_WORKER="http://localhost:$${aport_line#PORT:}"; \
	export VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:$${tport_line#PORT:}"; \
	python3 scripts/run_tests.py -j $(JOBS) $(HTTP_TEST_PATTERNS) > $(TEST_LOG) 2>&1; \
	rc=$$?; \
	tail -n 20 $(TEST_LOG); \
	echo ""; \
	if [ $$rc -eq 0 ]; then \
		echo "All HTTP tests passed. Log: $(TEST_LOG)"; \
	else \
		echo "Some HTTP tests failed (rc=$$rc). Full log: $(TEST_LOG)"; \
	fi; \
	exit $$rc

test-all: test test-http

# Run the Arrow facade parity tests against both backends back-to-back.
# Both invocations must pass — same suite, different `#arrow-impl` resolution.
test-facade-parity:
	@echo "=== arrow-js (default condition) ==="
	bun test src/arrow/__tests__/parity.test.ts
	@echo ""
	@echo "=== flechette (--conditions=worker) ==="
	bun --conditions=worker test src/arrow/__tests__/parity.test.ts

# Per-test entry point — useful when iterating on a single failure.
# `make test/integration/filter_pushdown/integers` runs just that one test
# with the verbose -s flag so the failure detail prints inline. Defaults
# to the launcher transport; use `make test-subprocess/...` to force the
# subprocess path.
test/%:
	@test_file="$(TEST_DIR)/$*.test"; \
	if [ ! -f "$$test_file" ]; then \
		echo "ERROR: test file not found: $$test_file"; \
		exit 1; \
	fi; \
	export VGI_TEST_WORKER="launch:$(WORKER)"; \
	export VGI_VERSIONED_WORKER="launch:$(VERSIONED_WORKER)"; \
	export VGI_VERSIONED_TABLES_WORKER="launch:$(VERSIONED_TABLES_WORKER)"; \
	export VGI_ATTACH_OPTIONS_WORKER="launch:$(ATTACH_OPTIONS_WORKER)"; \
	export VGI_REQUIRE_LAUNCHER_TRANSPORT=1; \
	export VGI_WORKER_IDLE_TIMEOUT=$(LAUNCHER_IDLE_TIMEOUT); \
	cd $(VGI_DIR) && ./build/release/test/unittest -s "$$test_file"

# Subprocess single-test entry point — same shape as `test/%` but without
# the `launch:` prefix. Useful when isolating a hang at the worker spawn
# layer rather than the launcher cache layer.
test-subprocess/%:
	@test_file="$(TEST_DIR)/$*.test"; \
	if [ ! -f "$$test_file" ]; then \
		echo "ERROR: test file not found: $$test_file"; \
		exit 1; \
	fi; \
	export VGI_TEST_WORKER="$(WORKER)"; \
	export VGI_VERSIONED_WORKER="$(VERSIONED_WORKER)"; \
	export VGI_VERSIONED_TABLES_WORKER="$(VERSIONED_TABLES_WORKER)"; \
	export VGI_ATTACH_OPTIONS_WORKER="$(ATTACH_OPTIONS_WORKER)"; \
	cd $(VGI_DIR) && ./build/release/test/unittest -s "$$test_file"

# test-http/% — single-test HTTP entry point. Spawns the HTTP worker
# triplet, points VGI_TEST_WORKER at it, runs that one test verbosely.
test-http/%:
	@test_file="$(TEST_DIR)/$*.test"; \
	if [ ! -f "$$test_file" ]; then \
		echo "ERROR: test file not found: $$test_file"; \
		exit 1; \
	fi; \
	port_fifo=$$(mktemp -u); mkfifo "$$port_fifo"; exec 3<>"$$port_fifo"; \
	$(HTTP_WORKER) > "$$port_fifo" 2>/tmp/vgi-http-worker.err & http_pid=$$!; \
	vport_fifo=$$(mktemp -u); mkfifo "$$vport_fifo"; exec 4<>"$$vport_fifo"; \
	$(VERSIONED_HTTP) > "$$vport_fifo" 2>/dev/null & vhttp_pid=$$!; \
	aport_fifo=$$(mktemp -u); mkfifo "$$aport_fifo"; exec 5<>"$$aport_fifo"; \
	$(ATTACH_OPTIONS_HTTP) > "$$aport_fifo" 2>/dev/null & ahttp_pid=$$!; \
	tport_fifo=$$(mktemp -u); mkfifo "$$tport_fifo"; exec 6<>"$$tport_fifo"; \
	$(VERSIONED_TABLES_HTTP) > "$$tport_fifo" 2>/dev/null & thttp_pid=$$!; \
	cleanup() { \
		kill $$http_pid $$vhttp_pid $$ahttp_pid $$thttp_pid 2>/dev/null; \
		wait $$http_pid $$vhttp_pid $$ahttp_pid $$thttp_pid 2>/dev/null; \
		exec 3>&- 4>&- 5>&- 6>&- 2>/dev/null; \
		rm -f "$$port_fifo" "$$vport_fifo" "$$aport_fifo" "$$tport_fifo"; \
	}; \
	trap cleanup EXIT; \
	read -t 60 port_line <&3 || { echo "ERROR: HTTP worker timeout"; echo "--- worker stderr ---"; cat /tmp/vgi-http-worker.err 2>/dev/null; exit 1; }; \
	read -t 60 vport_line <&4 || { echo "ERROR: versioned HTTP worker timeout"; exit 1; }; \
	read -t 60 aport_line <&5 || { echo "ERROR: attach-options HTTP worker timeout"; exit 1; }; \
	read -t 60 tport_line <&6 || { echo "ERROR: versioned-tables HTTP worker timeout"; exit 1; }; \
	export VGI_TEST_WORKER="http://localhost:$${port_line#PORT:}"; \
	export VGI_VERSIONED_HTTP_WORKER="http://localhost:$${vport_line#PORT:}"; \
	export VGI_ATTACH_OPTIONS_WORKER="http://localhost:$${aport_line#PORT:}"; \
	export VGI_VERSIONED_TABLES_HTTP_WORKER="http://localhost:$${tport_line#PORT:}"; \
	cd $(VGI_DIR) && ./build/release/test/unittest -s "$$test_file"

# VgiClient end-to-end tests against vgi-python's HTTP workers.
# Spawns the normal + versioned HTTP workers, each with --port-file
# pointing at a temp file the worker writes atomically when listening.
# The Makefile polls those files (no FIFOs, no stdout parsing) and
# exports VGI_PYTHON_HTTP_WORKER + VGI_PYTHON_VERSIONED_HTTP_WORKER
# before running bun:test. Always cleans up on exit. Requires `uv` on
# PATH and vgi-python at $VGI_PYTHON_DIR.
test-client:
	@tmpdir=$$(mktemp -d); \
	normal_file="$$tmpdir/normal.port"; \
	vers_file="$$tmpdir/versioned.port"; \
	ao_file="$$tmpdir/attach-options.port"; \
	( cd "$(VGI_PYTHON_DIR)" && uv run vgi-fixture-http --port 0 --port-file "$$normal_file" ) >/dev/null 2>&1 & \
	py_pid=$$!; \
	( cd "$(VGI_PYTHON_DIR)" && uv run vgi-fixture-versioned-worker --http --port 0 --port-file "$$vers_file" ) >/dev/null 2>&1 & \
	vers_pid=$$!; \
	( cd "$(VGI_PYTHON_DIR)" && uv run vgi-fixture-attach-options-worker --http --port 0 --port-file "$$ao_file" ) >/dev/null 2>&1 & \
	ao_pid=$$!; \
	cleanup() { \
		kill $$py_pid $$vers_pid $$ao_pid 2>/dev/null; \
		wait $$py_pid $$vers_pid $$ao_pid 2>/dev/null; \
		rm -rf "$$tmpdir"; \
	}; \
	trap cleanup EXIT; \
	for i in $$(seq 1 300); do \
		[ -s "$$normal_file" ] && [ -s "$$vers_file" ] && [ -s "$$ao_file" ] && break; \
		sleep 0.1; \
	done; \
	if [ ! -s "$$normal_file" ] || [ ! -s "$$vers_file" ] || [ ! -s "$$ao_file" ]; then \
		echo "ERROR: Python workers did not publish ports within 30s"; \
		cleanup; exit 1; \
	fi; \
	export VGI_PYTHON_HTTP_WORKER="http://127.0.0.1:$$(cat $$normal_file)"; \
	export VGI_PYTHON_VERSIONED_HTTP_WORKER="http://127.0.0.1:$$(cat $$vers_file)"; \
	export VGI_PYTHON_ATTACH_OPTIONS_HTTP_WORKER="http://127.0.0.1:$$(cat $$ao_file)"; \
	echo "Python HTTP worker:             $$VGI_PYTHON_HTTP_WORKER"; \
	echo "Python versioned worker:        $$VGI_PYTHON_VERSIONED_HTTP_WORKER"; \
	echo "Python attach-options worker:   $$VGI_PYTHON_ATTACH_OPTIONS_HTTP_WORKER"; \
	bun test src/; \
	rc=$$?; \
	cleanup; \
	exit $$rc
