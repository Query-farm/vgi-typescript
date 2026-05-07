# VGI TypeScript Makefile
# Build and test targets for vgi-typescript.
# Tests are independent targets — use `make -j8 test` for parallel execution.

.PHONY: build build\:types build\:js install clean test test-http test-all test-client

# --- Configuration (all overridable) ---

VGI_DIR      ?= /Users/rusty/Development/vgi
VGI_PYTHON_DIR ?= /Users/rusty/Development/vgi-python
TEST_TIMEOUT ?= 120
WORKER                ?= $(CURDIR)/bin/vgi-example-worker
HTTP_WORKER           := $(CURDIR)/bin/vgi-example-http-worker
VERSIONED_WORKER      := $(CURDIR)/bin/vgi-example-versioned-worker
VERSIONED_HTTP        := $(CURDIR)/bin/vgi-example-versioned-http-worker
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
#   zero_count_bypass                  — broken upstream, fails against Python worker too;
#                                        the test's LIKE pattern matches set_kind=table
#                                        AND set_kind=table_function ambiguously
#
# HTTP-only exclusions (subprocess runs them fine):
#   filter_echo_partitioned            — asserts COUNT(DISTINCT worker_pid) > 1; an
#                                        HTTP worker is one OS process, so worker_pid
#                                        collapses to a single value (test's own
#                                        docstring spells this out). Python HTTP
#                                        auto-skips this via sqllogictest's default
#                                        ignore_error_messages={"HTTP", ...}; TS gets
#                                        farther so we exclude explicitly.
#   partitioned_sequence               — same root cause: asserts >=2 distinct conn=
#                                        in batch_received logs under threads=4. The
#                                        C++ HTTP transport's parallel-scan connection
#                                        accounting is what's tested, not the worker.
TEST_PATTERNS := "test/sql/*" \
	"~test/sql/integration/writable/*" \
	"~test/sql/integration/schema_reconcile.test" \
	"~test/sql/integration/table/constant_columns_types.test" \
	"~test/sql/integration/catalog/zero_count_bypass.test"

HTTP_TEST_PATTERNS := "test/sql/integration/*" \
	"~test/sql/integration/writable/*" \
	"~test/sql/integration/schema_reconcile.test" \
	"~test/sql/integration/table/constant_columns_types.test" \
	"~test/sql/integration/catalog/zero_count_bypass.test" \
	"~test/sql/integration/table/filter_echo_partitioned.test" \
	"~test/sql/integration/table/partitioned_sequence.test"

# Single-shot subprocess transport run. Pass `KEEP_LOG=1` to suppress the
# rm-on-exit so you can grep through the per-test verbose output.
test:
	@cd $(VGI_DIR) && \
	export VGI_TEST_WORKER="$(WORKER)"; \
	export VGI_VERSIONED_WORKER="$(VERSIONED_WORKER)"; \
	export VGI_ATTACH_OPTIONS_WORKER="$(ATTACH_OPTIONS_WORKER)"; \
	./build/release/test/unittest -j 8 $(TEST_PATTERNS) > $(TEST_LOG) 2>&1; \
	rc=$$?; \
	tail -n 20 $(TEST_LOG); \
	echo ""; \
	if [ $$rc -eq 0 ]; then \
		echo "All tests passed. Log: $(TEST_LOG)"; \
	else \
		echo "Some tests failed (rc=$$rc). Full log: $(TEST_LOG)"; \
	fi; \
	exit $$rc

# HTTP transport: same pattern, but the workers need to be running at known
# URLs. The HTTP example workers each write a PORT line on stdout when they
# start; we collect them through FIFOs and export the URLs before invoking
# unittest.
test-http:
	@cd $(VGI_DIR) && \
	port_fifo=$$(mktemp -u); mkfifo "$$port_fifo"; \
	$(HTTP_WORKER) > "$$port_fifo" 2>/dev/null & http_pid=$$!; \
	vport_fifo=$$(mktemp -u); mkfifo "$$vport_fifo"; \
	$(VERSIONED_HTTP) > "$$vport_fifo" 2>/dev/null & vhttp_pid=$$!; \
	aport_fifo=$$(mktemp -u); mkfifo "$$aport_fifo"; \
	$(ATTACH_OPTIONS_HTTP) > "$$aport_fifo" 2>/dev/null & ahttp_pid=$$!; \
	cleanup() { \
		kill $$http_pid $$vhttp_pid $$ahttp_pid 2>/dev/null; \
		wait $$http_pid $$vhttp_pid $$ahttp_pid 2>/dev/null; \
		rm -f "$$port_fifo" "$$vport_fifo" "$$aport_fifo"; \
	}; \
	trap cleanup EXIT; \
	read -t 10 port_line < "$$port_fifo" || { echo "ERROR: HTTP worker timeout"; exit 1; }; \
	read -t 10 vport_line < "$$vport_fifo" || { echo "ERROR: versioned HTTP worker timeout"; exit 1; }; \
	read -t 10 aport_line < "$$aport_fifo" || { echo "ERROR: attach-options HTTP worker timeout"; exit 1; }; \
	export VGI_TEST_WORKER="http://localhost:$${port_line#PORT:}/vgi"; \
	export VGI_VERSIONED_HTTP_WORKER="http://localhost:$${vport_line#PORT:}/vgi"; \
	export VGI_ATTACH_OPTIONS_WORKER="http://localhost:$${aport_line#PORT:}/vgi"; \
	./build/release/test/unittest -j 8 $(HTTP_TEST_PATTERNS) > $(TEST_LOG) 2>&1; \
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
# with the verbose -s flag so the failure detail prints inline.
test/%:
	@test_file="$(TEST_DIR)/$*.test"; \
	if [ ! -f "$$test_file" ]; then \
		echo "ERROR: test file not found: $$test_file"; \
		exit 1; \
	fi; \
	export VGI_TEST_WORKER="$(WORKER)"; \
	export VGI_VERSIONED_WORKER="$(VERSIONED_WORKER)"; \
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
	port_fifo=$$(mktemp -u); mkfifo "$$port_fifo"; \
	$(HTTP_WORKER) > "$$port_fifo" 2>/dev/null & http_pid=$$!; \
	vport_fifo=$$(mktemp -u); mkfifo "$$vport_fifo"; \
	$(VERSIONED_HTTP) > "$$vport_fifo" 2>/dev/null & vhttp_pid=$$!; \
	aport_fifo=$$(mktemp -u); mkfifo "$$aport_fifo"; \
	$(ATTACH_OPTIONS_HTTP) > "$$aport_fifo" 2>/dev/null & ahttp_pid=$$!; \
	cleanup() { \
		kill $$http_pid $$vhttp_pid $$ahttp_pid 2>/dev/null; \
		wait $$http_pid $$vhttp_pid $$ahttp_pid 2>/dev/null; \
		rm -f "$$port_fifo" "$$vport_fifo" "$$aport_fifo"; \
	}; \
	trap cleanup EXIT; \
	read -t 10 port_line < "$$port_fifo" || { echo "ERROR: HTTP worker timeout"; exit 1; }; \
	read -t 10 vport_line < "$$vport_fifo" || { echo "ERROR: versioned HTTP worker timeout"; exit 1; }; \
	read -t 10 aport_line < "$$aport_fifo" || { echo "ERROR: attach-options HTTP worker timeout"; exit 1; }; \
	export VGI_TEST_WORKER="http://localhost:$${port_line#PORT:}/vgi"; \
	export VGI_VERSIONED_HTTP_WORKER="http://localhost:$${vport_line#PORT:}/vgi"; \
	export VGI_ATTACH_OPTIONS_WORKER="http://localhost:$${aport_line#PORT:}/vgi"; \
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
	( cd "$(VGI_PYTHON_DIR)" && uv run vgi-example-http --port 0 --port-file "$$normal_file" ) >/dev/null 2>&1 & \
	py_pid=$$!; \
	( cd "$(VGI_PYTHON_DIR)" && uv run vgi-example-versioned-worker --http --port 0 --port-file "$$vers_file" ) >/dev/null 2>&1 & \
	vers_pid=$$!; \
	( cd "$(VGI_PYTHON_DIR)" && uv run vgi-example-attach-options-worker --http --port 0 --port-file "$$ao_file" ) >/dev/null 2>&1 & \
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
