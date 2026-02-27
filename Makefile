# VGI TypeScript Makefile
# Build and test targets for vgi-typescript.
# Tests are independent targets — use `make -j8 test` for parallel execution.

.PHONY: build build\:types build\:js install clean test

# --- Configuration (all overridable) ---

VGI_DIR      ?= /Users/rusty/Development/vgi
TEST_TIMEOUT ?= 60
WORKER       ?= $(CURDIR)/bin/vgi-example-worker

TEST_DIR     := $(VGI_DIR)/test/sql
RELEASE_BIN  := $(VGI_DIR)/build/release/test/unittest
DEBUG_BIN    := $(VGI_DIR)/build/debug/test/unittest

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

# Discover all .test files and derive target names: test/sql/foo/bar.test → test/foo/bar
TEST_FILES := $(shell find $(TEST_DIR) -name '*.test' 2>/dev/null)
TEST_TARGETS := $(patsubst $(TEST_DIR)/%.test,test/%,$(TEST_FILES))

# Tests expected to fail (arrow-js doesn't support TIMESTAMP_NS)
XFAIL_TESTS := integration/table/constant_columns_types

test: $(TEST_TARGETS)

# Pattern rule: each test target runs release first, debug on failure
test/%:
	@test_file="$(TEST_DIR)/$*.test"; \
	if [ ! -f "$$test_file" ]; then \
		echo "ERROR: test file not found: $$test_file"; \
		exit 1; \
	fi; \
	export VGI_TEST_WORKER="$(WORKER)"; \
	is_xfail=false; \
	for xf in $(XFAIL_TESTS); do \
		if [ "$$xf" = "$*" ]; then is_xfail=true; break; fi; \
	done; \
	if timeout $(TEST_TIMEOUT) $(RELEASE_BIN) --test-dir $(TEST_DIR) "$$test_file" > /dev/null 2>&1; then \
		if $$is_xfail; then \
			echo "XPASS $* (expected failure now passes — remove from XFAIL_TESTS)"; \
		else \
			echo "PASS  $*"; \
		fi; \
	else \
		rc=$$?; \
		if $$is_xfail; then \
			echo "XFAIL $* (expected failure)"; \
		else \
			echo "FAIL  $* (release, rc=$$rc) — rerunning with debug binary..."; \
			timeout $(TEST_TIMEOUT) $(DEBUG_BIN) --test-dir $(TEST_DIR) -s "$$test_file" 2>&1 || true; \
			exit 1; \
		fi; \
	fi
