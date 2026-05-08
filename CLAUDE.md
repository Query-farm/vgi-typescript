# VGI TypeScript

TypeScript port of vgi-python. DuckDB user-defined functions via Arrow IPC subprocess protocol.

## Makefile

The project has a Makefile that wraps build and test commands. Prefer `make` over raw commands.

```bash
make install              # bun install
make build                # Build types + JS bundle
make clean                # Remove dist/

make test                 # Run all tests via the launcher transport (preferred)
make -j8 test             # Same, with unittest -j 8 in parallel
make test-subprocess      # Run all tests via plain subprocess transport (legacy)
make test-http            # Run all HTTP transport tests
make -j8 test-http        # Run all HTTP tests in parallel
make -j8 test-all         # Run launcher + HTTP suites
make test/vgi_cardinality # Run a single test by name (launcher transport)
make test-subprocess/vgi_cardinality  # Same test, force subprocess transport
make test-http/vgi_cardinality        # Same test, HTTP transport
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

### Both transports must pass

Always run tests on **both** the local-IPC transport (launcher *or* subprocess)
and HTTP. `make -j8 test-all` runs `make -j8 test` (launcher) followed by
`make -j8 test-http`. A change is not complete until both transports pass.

### Launcher transport (default for `make test`)

The vgi C++ extension exposes a `launch:<argv>` LOCATION scheme that spawns
or reuses a long-running worker over AF_UNIX. The Makefile sets
`VGI_TEST_WORKER="launch:.../bin/vgi-example-worker"` so every parallel
unittest invocation hashing to the same `(argv, cwd, VGI_RPC_*-env)` tuple
shares a single warm Bun process — no per-test Bun cold-start, ~5× faster
than the per-process subprocess pool in the upstream extension's measured
runs.

How the worker handles it: `Worker.run()` (src/worker.ts) parses
`--unix PATH` / `--idle-timeout SEC` from argv (the C++ launcher appends
both) and dispatches to vgi-rpc's `serveUnix()` instead of the stdio
`VgiRpcServer`. With no `--unix`, it falls back to stdin/stdout — so the
same example-worker binary serves both the launcher and the legacy
subprocess paths.

Three tests are excluded from the launcher suite because they assert
subprocess-pool semantics (worker_pool, filter_echo_partitioned,
versioned_tables_impl) — these only run under `make test-subprocess`.

Use `make test-subprocess` when you need to debug worker spawn itself
or specifically exercise the per-process subprocess pool.

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

## Design Principles

### No in-memory state for HTTP transport

Never use single-process in-memory stores for state (e.g., in-memory maps keyed by
session ID). Always assume the HTTP transport will be used and requests will go to
different hosts via a load balancer. All state must be fully serializable and
self-contained in the state token that round-trips through the client. If something
can't be serialized, rearchitect the approach rather than falling back to in-memory
storage.

## Storage backends

`FunctionStorage` (`src/functions/storage.ts`) provides shared state across worker
processes — work queues for partitioned producers, per-worker state buffers for
table-in-out finalize. The interface is **async** (every method returns a Promise) so
HTTP-backed implementations can use `fetch` without sync hacks.

Backends:

- **`FunctionStorageSqlite`** — default. `bun:sqlite` with WAL. Honors
  `VGI_WORKER_SQLITE_PATH` (set to `:memory:` for ephemeral single-process fixtures).
- **`FunctionStorageCfDo`** — Cloudflare Durable Object over HTTPS. The DO is
  single-threaded SQLite, wire-compatible with `FunctionStorageSqlite`. Deploy the
  worker from `vgi-python/cloudflare/vgi-storage/` to Cloudflare. Configure the
  vgi-typescript worker via env vars. Aggregate-state, transaction-state, and
  window-partition methods are not implemented on the DO side — those operations
  throw on both the Python and TypeScript clients.

Backend selection is driven by env vars on first use of the default `storage`
singleton (matches Python's `_resolve_storage` pattern):

```bash
# Default — local SQLite under platform state dir
unset VGI_WORKER_SHARED_STORAGE

# In-memory SQLite for single-process fixtures
VGI_WORKER_SHARED_STORAGE=sqlite VGI_WORKER_SQLITE_PATH=:memory:

# Cloudflare Durable Object
VGI_WORKER_SHARED_STORAGE=cloudflare-do \
  VGI_CF_DO_URL=https://vgi-storage.<account>.workers.dev \
  VGI_CF_DO_TOKEN=<optional-bearer>
```

The `storage` export is a Proxy that lazy-resolves on first method call — importing
`vgi-typescript` no longer eagerly opens a SQLite connection. Construct
`FunctionStorageSqlite` / `FunctionStorageCfDo` directly to bypass env-driven
selection.

### Async lifecycle implications

Because `FunctionStorage` is async, lifecycle hooks that touch storage may also be
async. Both forms (sync return, `Promise` return) are accepted by:

- `defineTableFunction.onInit` — common: `queuePush(items)` to seed work
- `defineTableFunction.process` — `queuePop()` for partitioned producers
- `defineTableFunction.dynamicToString` — `storage.collect()` for EXPLAIN ANALYZE counters
- `defineTableInOutFunction.onInit` / `process` / `finalize`
- `VgiFunction.globalInit` (low-level)

Inside these hooks, always `await` storage method calls. The framework awaits the
hook returns, so a missing `await` will silently dispatch fire-and-forget writes.

## Arrow backend selection

The Arrow layer is a backend-agnostic facade (`src/arrow/`). Two implementations
ship in the same source tree and the bundler picks one at build time via
package.json `imports` conditional resolution:

| Backend         | Picked when         | Bundle (worker-cf)  | Used for                              |
| --------------- | ------------------- | ------------------- | ------------------------------------- |
| `impl-arrowjs`  | `default` (Node/Bun) | n/a (Node entry)   | Subprocess workers, HTTP under Bun, integration tests |
| `impl-flechette` | `workerd`/`worker`/`browser` | 252 KB min / 74 KB gzip | Cloudflare Workers, browsers          |

`#arrow-impl` (in this repo) and `#vgi-rpc-arrow` (in vgi-rpc-typescript) are
the resolution keys — see each `package.json`'s `imports` field. **Never import
`@query-farm/apache-arrow` or `@uwdata/flechette` directly from app code.**
Always go through `src/arrow/index.ts` so the same source compiles into either
bundle.

### Building each variant

```bash
# Node/Bun (default — uses arrow-js):
bun build ./src/index.ts --target=node --format=esm \
  --external @query-farm/apache-arrow --external vgi-rpc

# Cloudflare Workers (uses flechette):
bun build ./src/worker-cf-entry.ts --target=browser --format=esm \
  --conditions workerd --minify
```

The `worker-cf` entry is a separate exported subpath
(`vgi/worker-cf` → `src/worker-cf-entry.ts`); pulling it through the
`workerd`/`worker`/`browser` conditional export is what makes the bundler
resolve `#arrow-impl` to flechette.

### Adding to the facade

Both `impl-arrowjs/` and `impl-flechette/` must export the **same symbols**
with **identical semantics** — the parity test (`src/arrow/__tests__/parity.test.ts`)
runs a fixed corpus through both. When you add a new helper:

1. Add it to `src/arrow/index.ts` and `src/arrow/types.ts` (if a new type).
2. Implement in **both** `impl-arrowjs/` and `impl-flechette/` index.ts.
3. Extend the parity test to cover it.

Flechette has narrower coverage in a few places (no aggregate-statistics
SparseUnion path, no batch-level `data.children` for cast-rebuild). When a
backend can't implement something, throw `not implemented` on that backend
rather than silently returning a wrong shape — the parity test then encodes
the asymmetry as `expect(...).toThrow()`.

### Flechette fork

We depend on `github:Query-farm/flechette#fix/timestamp-bigint-encode`
(devDependency only — flechette is bundled into the `worker-cf` output, not
shipped as a runtime dep). The fork adds `tablesToIPC`/`concatTables` for
multi-batch IPC streams and fixes timestamp BigInt encoding. If a missing
flechette feature blocks a migration, prefer adding it to the fork over
keeping the arrow-js path.

## Dependencies

- `apache-arrow`: Query-farm fork (`github:Query-farm/arrow-js#feat_query_farm_1`)
- `@uwdata/flechette`: Query-farm fork (`github:Query-farm/flechette#fix/timestamp-bigint-encode`), devDep — bundled into worker-cf
- `vgi-rpc`: Local package (`../vgi-rpc-typescript`) - provides Protocol, Server, IPC transport. Has its own parallel facade under `src/arrow/` selected via `#vgi-rpc-arrow`.
