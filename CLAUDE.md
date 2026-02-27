# VGI TypeScript

TypeScript port of vgi-python. DuckDB user-defined functions via Arrow IPC subprocess protocol.

## Makefile

The project has a Makefile that wraps build and test commands. Prefer `make` over raw commands.

```bash
make install              # bun install
make build                # Build types + JS bundle
make clean                # Remove dist/

make test                 # Run all integration tests (sequentially)
make -j8 test             # Run all tests in parallel (8 jobs)
make test/vgi_cardinality # Run a single test by name
make test/integration/table/sequence  # Subdirectory tests work too

# Override defaults:
make test TEST_TIMEOUT=30              # Custom timeout per test (default: 60s)
make test WORKER=/path/to/other-worker # Custom worker binary
make test VGI_DIR=/other/vgi           # Different VGI extension repo
```

Test target names mirror the test file paths under `vgi/test/sql/`, minus the `.test` extension:
- `vgi/test/sql/vgi_integration.test` → `make test/vgi_integration`
- `vgi/test/sql/integration/table/sequence.test` → `make test/integration/table/sequence`

Each test runs the **release** binary first. On failure, it reruns with the **debug** binary (`-s` flag) to show verbose diagnostic output.

## Build (raw commands)

```bash
bun run build           # Build types + JS bundle
bun run build:types     # TypeScript declarations only
bun run build:js        # JS bundle only
```

## Worker

```bash
# Run the example worker (for DuckDB integration):
bin/vgi-example-worker
# Or directly:
bun run examples/worker.ts
```

The worker communicates via Arrow IPC on stdin/stdout. Not interactive.

## Integration Tests

Prefer `make test` (see Makefile section above). The raw commands below are for
reference and manual debugging only.

Tests live in `/Users/rusty/Development/vgi/test/sql/` (DuckDB VGI extension repo).
Test format is [sqllogictest](https://duckdb.org/docs/stable/dev/sqllogictest/intro) —
each `.test` file contains `statement ok`, `query`, etc. blocks.
Reference the test format docs at https://duckdb.org/docs/stable/dev/sqllogictest/intro
and https://duckdb.org/docs/stable/dev/sqllogictest/writing_tests when reading or
debugging test files.

```bash
# Set the worker command:
export VGI_TEST_WORKER="/Users/rusty/Development/vgi-typescript/bin/vgi-example-worker"

# Run a specific test (use full path from -l output):
/Users/rusty/Development/vgi/build/debug/test/unittest \
  --test-dir /Users/rusty/Development/vgi/test/sql \
  "/Users/rusty/Development/vgi/test/sql/vgi_table_in_out.test"

# List all tests:
/Users/rusty/Development/vgi/build/debug/test/unittest \
  --test-dir /Users/rusty/Development/vgi/test/sql -l

# Run all tests:
/Users/rusty/Development/vgi/build/debug/test/unittest \
  --test-dir /Users/rusty/Development/vgi/test/sql
```

Always use `timeout 180` to avoid hangs blocking the session.

Don't use `tail` when running unittest — always capture full output.
Don't redirect stderr to stdout (no `2>&1`) when running unittest.

When diagnosing test failures after a unittest run shows failures, run the failing
tests individually in parallel — they are all independent. Just specify each test
file path separately.

### Useful unittest options

- `-s` — include successful tests in output (shows what passed)
- `-a` — abort at first failure
- `-x N` — abort after N failures
- `--output-sql true` — output SQL statements to stderr instead of running (useful for understanding what a test does)

### Decomposing tests for debugging

When a sqllogictest hangs, don't re-run the whole test file. Instead, extract
the SQL statements from the `.test` file and run them directly through the
DuckDB CLI one at a time to isolate the hanging statement:

```bash
# DuckDB CLI binary:
/Users/rusty/Development/vgi/build/debug/duckdb

# Run SQL directly (no need to cd — use full paths):
VGI_WORKER_STDERR_PASSTHROUGH=1 timeout 15 \
  /Users/rusty/Development/vgi/build/debug/duckdb -c "
LOAD vgi;
ATTACH 'example' AS vgi_test (TYPE vgi, LOCATION '/Users/rusty/Development/vgi-typescript/bin/vgi-example-worker');
CREATE TABLE test_data AS SELECT i AS a, i * 2 AS b FROM range(10) t(i);
SELECT * FROM vgi_test.echo((SELECT * FROM test_data)) ORDER BY a;
"
```

- `LOAD vgi;` loads the VGI extension
- `ATTACH ... (TYPE vgi, LOCATION '...')` launches the worker subprocess
- The test framework expands `${VGI_TEST_WORKER}` but the CLI does not — use literal paths
- Use short timeouts (15-30s) when isolating hangs
- stderr from the worker is visible with `VGI_WORKER_STDERR_PASSTHROUGH=1`
- Exit code 124 = timeout killed it (i.e., it hung)
- Don't capture the exit code to a file — just read it directly from the command
- Never redirect stderr to /dev/null — always keep stderr visible for debugging

### Debug flags

```bash
# Pass worker stderr to terminal:
VGI_WORKER_STDERR_PASSTHROUGH=1 /Users/rusty/Development/vgi/build/debug/duckdb -c "..."

# Full debug mode:
VGI_WORKER_DEBUG=1 /Users/rusty/Development/vgi/build/debug/duckdb -c "..."
```

## Dependencies

- `apache-arrow`: Query-farm fork (`github:Query-farm/arrow-js#feat_query_farm_1`)
- `vgi-rpc`: Local package (`../vgi-rpc-typescript`) - provides Protocol, Server, IPC transport
