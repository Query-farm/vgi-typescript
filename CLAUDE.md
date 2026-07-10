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

## HTTP entry points

Three subpaths, one implementation (`src/http/fetch.ts` → `createVgiFetch`):

- **`@query-farm/vgi/serve`** (`src/serve-entry.ts`, Bun-only) — `serveVgiWorker({name, doc,
  version, registry, catalogInterface})` assembles protocol + signing key + CORS + landing
  surface and calls `Bun.serve`. This is what a worker repo's `scripts/serve.ts` should use;
  don't hand-roll `buildVgiProtocol` + `createHttpHandler` in a worker. Reads `PORT`,
  `VGI_SIGNING_KEY`, `VGI_TOKEN_TTL`, `CORS_ORIGINS`. `createVgiWorkerFetch` returns the bare
  handler if you own the server.
- **`@query-farm/vgi/worker-cf`** (`src/worker-cf-entry.ts`) — `createVgiFetch` for workerd.
- Worker repos pair this with a `src/parts.ts` exporting `makeWorkerParts()`, consumed by both
  `src/worker.ts` (stdio) and `scripts/serve.ts` (HTTP), so the registry is wired once.

Two traps, both fixed and both worth not reintroducing:

- `createHttpHandler`'s key option is **`tokenKey`**, not `signingKey`. Passing `signingKey`
  type-checks (it's an unknown property on a variable, not a literal) and is silently ignored —
  the handler then mints state tokens under a random key while `buildVgiProtocol` tries to
  recover them under yours. `createVgiFetch` feeds both seams from one key.
- **Never** set a bare `"sideEffects": false` in package.json. Bun tree-shakes any entry that is
  pure re-exports down to an export list with no imports and no definitions — invalid ESM that
  throws on first import. `dist/client-entry.js` shipped broken this way. The entry modules are
  now listed explicitly in `sideEffects`, and `bun run check:bundles` (part of `bun run build`)
  imports every `dist/` entry so it can't regress silently.

## Result cache (`vgi.cache.*`)

`src/cache-control.ts` is the cache-control vocabulary the C++ extension reads by
string. A table function advertises that its result is cacheable by attaching the
rendered keys to the **first** batch it emits:

```ts
import { cacheControlMetadata } from "@query-farm/vgi";

out.emit(firstBatch, cacheControlMetadata({ ttl: 300 }));
```

`cacheControlMetadata(cc, extra?)` merges `extra` first, so a function that already
emits per-batch metadata (`vgi_batch_index`, `vgi_partition_values#b64`) folds the
cache keys in without losing them. The cache keys win on collision.

Conditional revalidation: a worker that advertises `{ ttl: 0, etag, revalidatable: true }`
gets the client's stored validator back on its next call as
`params.ifNoneMatch` / `params.ifModifiedSince`, and answers a still-fresh result with
a 0-row `cacheControlMetadata({ notModified: true, ... })` batch instead of
re-streaming.

Those validators reach `process()` by different routes per transport, and both are
already wired: over subprocess they ride the first producer tick; over HTTP the first
producer turn folds into the `/init` POST, so vgi-rpc attaches that request's
`custom_metadata` to the first synthetic tick batch (`produceStreamResponse`, gated on
`firstTick`). `defineTableFunction`'s `onTick` reads them off the tick metadata. Requires
`@query-farm/vgi-rpc` >= 0.13.0 — under 0.12.0 the HTTP path silently never revalidates
and `test/sql/integration/cache/revalidate.test` fails on the HTTP lane only.

The example worker's fixtures live in `examples/cache.ts` (ported from vgi-python's
`vgi/_test_fixtures/table/cache.py`) and back the 35 tests under
`test/sql/integration/cache/`. `cache_multicol` is registered with the worker but
deliberately kept out of `catalog.functions` — it only backs the `data.cache_multicol`
table, and `integration/table/function_registration.test` pins the resulting count.

## Design Principles

### No in-memory state for HTTP transport

Never use single-process in-memory stores for state (e.g., in-memory maps keyed by
session ID). Always assume the HTTP transport will be used and requests will go to
different hosts via a load balancer. All state must be fully serializable and
self-contained in the state token that round-trips through the client. If something
can't be serialized, rearchitect the approach rather than falling back to in-memory
storage.

### TableBuffering is worker-only — not in the client

vgi-python's standalone client grew a `table_buffering_function()` driver (so a Python
caller can invoke a buffering function directly, without DuckDB). The TypeScript client
(`src/client/`) intentionally does **not** mirror this. TableBuffering is fully supported
on the worker/extension path (the integration tests exercise it end-to-end); the client
driver is out of scope. Do not add a TableBuffering method to `src/client/client.ts`.

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
`@query-farm/apache-arrow` or `@query-farm/flechette` directly from app code.**
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

### Flechette

We depend on the published Query-farm flechette fork (`@query-farm/flechette` on
npm). It adds `tablesToIPC`/`concatTables` for multi-batch IPC streams and fixes
timestamp BigInt encoding. If a missing flechette feature blocks a migration, prefer
adding it upstream over keeping the arrow-js path.

## Type representation codec architecture

`src/arrow/codec/` is the single source of truth for how each Arrow type maps to a JS
value. The pipeline is **JS value ⇄ canonical ⇄ backend column**, with one converter
per stage:

- **The canonical value is the single source of truth.** Canonical = the raw Arrow
  wire unit for the type (day-number for date32, ms-bigint for date64, raw-unit bigint
  for time64/timestamp/duration, unscaled bigint for decimals, see the header of
  `codec/registry.ts`). It is byte-for-byte identical across both backends — that's
  what keeps arrow-js and flechette in agreement.
- **`codecFor(type)` is the only conversion authority.** It returns a `Codec` with
  `richToCanonical` / `canonicalToRich` / `rawToCanonical` / `canonicalToRaw`. Nothing
  outside the codec module should hand-roll a `Date`↔day-number, decimal-byte, or
  bigint-unit conversion — route it through `codecFor`. The `rich` representation
  differs from canonical ONLY for date32/date64 (→ JS `Date`); `raw` is canonical with
  a branded type (`codec/branded.ts`, mapped at the type level in `codec/repr.ts` +
  `codec/type-descriptors.ts`).
- **Per-backend specifics live ONLY in `impl-{arrowjs,flechette}/canonical.ts`.**
  `writeCanonicalColumn(type, canonical[])` and `readCanonicalValue(type, col, i)` are
  the *only* places that know backend-native build/read details (manual Int32/BigInt64
  buffer building, decimal byte layout, list/map/struct offset handling). Everything
  else (`batchFromColumns`, `iterRows`, scalar I/O, statistics, filter pushdown,
  settings/secret reads) goes value → codec → canonical → `canonical.ts`.

### Adding a new Arrow type

1. Add a codec entry in `codec/registry.ts` (`codecFor` dispatch + the codec's four
   convert methods, validating/throwing on bad input).
2. Add canonical read/write handling in **both** `impl-arrowjs/canonical.ts` and
   `impl-flechette/canonical.ts` (same canonical unit on each side).
3. If it has a branded raw form, add the alias + `as…` constructor in
   `codec/branded.ts`, the descriptor in `codec/type-descriptors.ts`, and the
   `RichValue`/`RawValue` arms in `codec/repr.ts`.
4. Add a parity / round-trip test (`src/arrow/__tests__/parity.test.ts`,
   `codec.test.ts`, `raw-mode.test.ts`) covering both backends.

## Dependencies

- `@query-farm/apache-arrow` (`^21.1.1`): published Query-farm arrow-js fork. Default
  (Node/Bun) backend; kept external from the bundle.
- `@query-farm/flechette` (`^2.4.0`): published Query-farm fork, runtime dep — bundled
  into the `worker-cf` output (workerd/browser backend).
- `@query-farm/vgi-rpc`: provides Protocol, Server, IPC transport. Has its own
  parallel facade under `src/arrow/` selected via `#vgi-rpc-arrow`.
