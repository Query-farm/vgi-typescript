# CI: the vgi integration suite

The `integration` job in [`.github/workflows/ci.yml`](../.github/workflows/ci.yml)
runs the canonical [Query-farm/vgi](https://github.com/Query-farm/vgi)
integration sqllogictest suite against this repo's TypeScript example workers on
every push / PR. The same `.test` files run against the Python, Go, Java and Rust
ports, so a green run here is real wire-compatibility evidence — it exercises the
worker through the *published* DuckDB extension, not a mock, and not the
in-process `VgiClient` that the unit tests use.

This harness is a port of `vgi-go/ci/` and `vgi-python/ci/`, which are byte-identical
apart from worker wiring. [`preprocess-require.awk`](preprocess-require.awk) is the
sibling file plus one additive flag (`-v install=0`, below); with the flag unset its
output is byte-identical to theirs.

## Why it exists

The integration job used to call `make test-http` with `LOCAL_EXTENSION_REPO`
pointed at the community repo **globally**. Two independent bugs followed, each
sufficient on its own to make the whole suite a no-op:

1. **`require httpfs` could not resolve, so tests skipped silently.** `vgi` is not
   autoloadable, so `INSTALL vgi FROM '<community repo>'` worked. `httpfs` **is**
   autoloadable, so DuckDB's `CheckRequire` took the autoinstall branch and issued
   `INSTALL httpfs FROM '<community repo>'` — a 404. A failed `require` is a silent
   `SKIP_TEST`, and 255 of the 292 integration files carry `require httpfs`.
2. **`vgi/scripts/run_tests.py` counts a skip as a pass.** It has no skipped
   category at all (`passes = [r for r in results if r["exit"] == 0]`), and DuckDB
   exits 0 on a skip.

Measured on the same machine against the same prebuilt runner:

| | executed | skipped | assertions | wall clock |
|---|---:|---:|---:|---:|
| before (`make test-http`, community repo global) | **25** | 261 | 386 | 130 s |
| after (`ci/run-integration.sh`) | **253** | 29 | 11,280 | 150 s |

The public CI numbers matched the "before" row exactly: `passed: 280` in 84 s,
p50 0.27 s per file.

The fix is two-sided, and the second half matters more than the first:

- **Scope the community repo to `vgi`.** Nothing else may come from it.
  `preprocess-require.awk` rewrites each `require <ext>` gate into an explicit
  signed `INSTALL <ext> FROM {community,core}; LOAD <ext>;`, so no autoinstall
  branch is ever taken.
- **Account for every skip.** `run-integration.sh` parses the runner's report,
  prints a `skipped:` breakdown by reason, **fails on any reason not in its
  expected-skip allowlist**, and **fails if fewer than `MIN_EXECUTED` test cases
  actually executed**. A whole-suite skip can no longer read as green: fed the old
  configuration it exits 1 with `executed: 1 / 282` and
  `unexpected skip reason 'require httpfs' (269 test cases)`.

## How it works (no C++ build)

Rather than building the vgi DuckDB extension from source (which needs the
Haybarn vcpkg pipeline), CI drives a **prebuilt** standalone `haybarn-unittest`
and installs the **signed** vgi extension from the Haybarn community channel:

1. **Install deps** — `bun install`. The workers are the `bin/vgi-example-*`
   wrappers, which `exec bun run examples/<entry>.ts`; nothing is compiled.
2. **Checkout the test suite** — `Query-farm/vgi`; its
   `test/sql/integration/*.test` files are the suite.
3. **Download the runner** — `haybarn_unittest-linux-amd64.zip` from the latest
   Haybarn release (resolved at run time, so the unittest host stays ABI-compatible
   with the rebuilt community extension).
4. **Preprocess + stage** — `run-integration.sh` copies the suite through
   `preprocess-require.awk` into a scratch tree, dropping the out-of-scope files.
5. **Run** — it boots the four HTTP example workers, warms the extension cache
   (`FORCE INSTALL vgi FROM community` + the deps `FROM core`), and runs the whole
   lane in a single `haybarn-unittest` invocation, pointed at the staged tree with
   `--test-dir`.
6. **Account** — it then enforces the skip contract described above.

`--test-dir` is not cosmetic: a *from-source* `unittest` bakes
`DUCKDB_ROOT_DIRECTORY` in at compile time and would otherwise silently run the
build's own source tree, ignoring every exclusion and every rewrite.

## Transport lanes

`run-integration.sh` honours `TRANSPORT=http|stdio|launch`, mirroring the
Makefile's `test-http` / `test-subprocess` / `test` targets. CI runs three legs:
`http` × {arrow-js, flechette} and `launch` × arrow-js.

- **`http`** — the whole suite over the stateless HTTP transport, against the four
  long-lived `bin/vgi-example-*-http-worker` servers. Staging injects
  `LOAD httpfs` before the first worker ATTACH — 24 files in the suite attach an
  `http://` worker without a `require httpfs` line of their own, and the extension
  rejects that with a Binder Error whose text contains "HTTP", which
  sqllogictest's default `ignore_error_messages` turns into yet another silent
  skip. Measured: **253 executed / 282 staged, 11,280 assertions, ~150 s**.
- **`launch`** — the AF_UNIX launcher transport (the repo's default `make test`):
  one warm bun worker shared by every unittest process that hashes to the same
  `(argv, cwd, VGI_RPC_*-env)` tuple. It is both the *fastest* lane and the one
  that covers what http structurally cannot — the `launcher/*` tests and the four
  subprocess-pool-semantics files. Measured: **256 executed / 288 staged, 11,147
  assertions, ~47 s**.
- **`stdio`** — plain subprocess transport, one pooled bun worker per DuckDB
  process. Also sets `VGI_TEST_DEDICATED_WORKER`, which un-skips the SIGKILL-self
  crash / pool-recovery tests. Not run in CI: it pays a bun cold start per test
  and thrashes the 2-core runners. Use it locally when debugging worker spawn.

`VGI_BUN_CONDITIONS` selects the Arrow backend and is read by the `bin/` wrappers,
so the script only has to pass it through: unset = arrow-js, `flechette` =
flechette. flechette is HTTP-only (no stdio incremental streaming), hence no
launch leg for it.

## Excluded tests

Excluded on **every** lane — these are staged out, so they cannot run:

| test | why |
|---|---|
| `writable/*` | the opt-in *generic* writable catalog (`VGI_WORKER_ENABLE_WRITABLE`); no TypeScript fixture worker |
| `schema_reconcile.test` | writable-style fixture, likewise not ported |
| `table/constant_columns_types.test` | arrow-js has no `TIMESTAMP_NS` |
| `catalog/zero_count_bypass.test` | broken upstream — its `LIKE` pattern matches `set_kind=table` and `set_kind=table_function` ambiguously; fails against the Python worker too |
| `table_in_out/echo/nested_type_combinations.test` | segfaults the prebuilt standalone runner (a property of that C++ build, not the worker) |

Dropped on the **http** lane only:

| test | why |
|---|---|
| `table/filter_echo_partitioned.test` | asserts `COUNT(DISTINCT worker_pid) > 1`; an HTTP worker is one OS process |
| `table/partitioned_sequence.test` | same root cause, via distinct `conn=` ids under `threads=4` |
| `table/batch_index.test`, `table/order_preservation_modes.test` | both read VGI `batch_received` log rows, which don't stream over HTTP |
| `table/dynamic_filter.test` | Top-N + dynamic-filter continuation terminates early over http, so the tightened pushdown never reaches the worker. Same drop as vgi-go/vgi-python; verified still failing against a from-source vgi build, so it is not prebuilt-extension skew |
| `cache/identity_isolation.test` | asserts the *worker-visible* auth principal (`alice`/`bob`). The Python fixture server maps `vgi-test-alice`→`alice`; the TypeScript example HTTP worker has no token→principal resolver and reports `anonymous`. A gap in the example fixture, not the harness — `cache/partition_scope_identity.test` still runs and covers the isolation property itself, since the C++ side folds the bearer-token fingerprint into the cache key regardless of the principal name |

## Expected skips

Everything else that skips must match `EXPECTED_SKIP_REASONS` in
`run-integration.sh`, each entry carrying its reason. On the `http` lane that is
29 test cases (32 on `launch`): fixture workers this port does not ship
(`VGI_SIMPLE_WRITABLE_WORKER`, `VGI_BAD_PROTOCOL_WORKER`, `VGI_BAD_ENUM_WORKER`,
`VGI_RULES_WORKER`, `VGI_WORKER_SUPPORTS_DYNAMIC_CODE`), infrastructure CI
deliberately does not stand up (`VGI_DOCKER_IMAGE`, `VGI_DOCKER_TCP_IMAGE`,
`VGI_GITHUB_NETWORK_TESTS`, `VGI_TEST_COMPANION_TARGET`, `VGI_TEST_BRANCH_DIR`,
`VGI_TEST_BEARER_TOKEN`, `VGI_HTTP_DISABLE_ZSTD`), and two transport facts
(`VGI_TEST_DEDICATED_WORKER` — an HTTP worker is shared, so the SIGKILL-self tests
would take down every concurrent test; `VGI_REQUIRE_LAUNCHER_TRANSPORT` — the
launcher-only tests don't apply over http).

**Adding to that list is a decision, not a chore.** If a new reason appears, the
default assumption is that something regressed.

`skip on error_message matching 'HTTP'` is deliberately **not** allowlisted. It is
sqllogictest's runtime self-skip when a statement fails with an HTTP-shaped error,
and it is exactly the failure mode this harness exists to make visible.

## Running it locally

```sh
bun install
VGI_SRC=~/Development/vgi \
HAYBARN_UNITTEST=/path/to/haybarn-unittest \
TRANSPORT=http \
  ci/run-integration.sh
```

Download `haybarn-unittest` for your platform from the latest Haybarn release:

```sh
gh release download "$(gh release view --repo Query-farm-haybarn/haybarn --json tagName --jq .tagName)" \
  --repo Query-farm-haybarn/haybarn --pattern 'haybarn_unittest-osx-arm64.zip'
```

### Against a from-source vgi build

Set `PREPROCESS=0` and point `HAYBARN_UNITTEST` at the locally built binary:

```sh
VGI_SRC=~/Development/vgi \
HAYBARN_UNITTEST=~/Development/vgi/build/release/test/unittest \
TRANSPORT=http PREPROCESS=0 \
  ci/run-integration.sh
```

`PREPROCESS=0` suppresses only the `INSTALL`/`LOAD` rewrites — a from-source build
is compiled against a development DuckDB/haybarn version with no published
community `vgi` to install, and resolves `require vgi` from the statically linked
extension instead. The httpfs injection still happens (as a `require httpfs` line,
letting DuckDB's own autoload use the build's local repository). Everything else —
staging, exclusions, skip accounting, the executed-test floor — is identical, so
the two modes run the same set.

## Knobs

| var | meaning |
|---|---|
| `VGI_SRC` | path to a `Query-farm/vgi` checkout (required) |
| `HAYBARN_UNITTEST` | path to the runner binary (required) |
| `TRANSPORT` | `http` (default) \| `stdio` \| `launch` |
| `VGI_BUN_CONDITIONS` | Arrow backend: unset (arrow-js) \| `flechette` |
| `PREPROCESS` | `1` (default) rewrite `require` gates; `0` for a from-source runner |
| `MIN_EXECUTED` | floor on executed test cases (default 245 on http/stdio, 240 on launch) |
| `EXTRA_EXCLUDES` | extra integration-relative paths to drop from staging |
| `BIN_DIR` | dir holding the worker wrappers (default `bin/`) |
| `STAGE` | scratch dir for the staged tree (default `mktemp -d`) |

`MIN_EXECUTED` is a floor, not a target: the upstream suite grows, and 245 leaves
about eight files of headroom below the 253 measured against
`Query-farm/vgi@b9f3895`. **Do not lower it to make a run pass** — a drop is the
signature of the bug this harness was written to catch.

## Known-red: the flechette backend

The `flechette` matrix leg used to fail **67 of its 253 executed test cases**. Those
were genuine defects in the flechette Arrow path, not harness artifacts — they
reproduced against a from-source vgi build, and the arrow-js leg passed 253/253
through the identical harness. They had been invisible because the whole suite was
skipping.

Sixty-five of the sixty-seven are fixed (see `src/arrow/impl-flechette/compat.ts`,
`arrowjs-shape.ts` and the parity tests in `src/arrow/__tests__/parity.test.ts`):
unaligned IPC decode, the missing `Column#isValid` / `Table#slice`, `isValid`
returning false for an all-valid column, `isSigned` vs `signed`, `listSize` vs
`stride`, the dropped zero-field RecordBatch and per-batch metadata, MonthDayNano
interval shape, `metadata: null` on decoded fields, and `String(type)` as a type
identity.

**Two remain, and they are not fixable in this repo:**

- `filter_pushdown/enums.test`
- `table_in_out/echo/all_types.test`

Both fail with `HttpRpcError: Missing state token in exchange request`, and both
have an ENUM (Arrow Dictionary) in the stream's output schema. Root cause is in
`@query-farm/flechette`'s encoder:

`columnFromValues` emits no batch for a zero-length column (`if (row) next(b)`), so
a 0-row table has `column.data.length === 0`. `tableToIPC` then derives no record
batches, and its `batchMetadata` path synthesises an empty one via
`appendEmptyNodes` — but it does **not** synthesise the matching empty
`DictionaryBatch`, while `assembleSchema` still stamps a dictionary id onto the
schema. The resulting stream declares a dictionary id that no message defines;
flechette itself cannot read it back (`TypeError: undefined is not an object
(evaluating 'dictionary.cache')` in `setDictionary`), and neither can DuckDB.

`@query-farm/vgi-rpc` walks straight into it: every HTTP exchange `init` replies
with `buildEmptyBatch(outputSchema, {state-token})`
(`src/arrow/impl-flechette/index.ts` → `emptyBatchWithMetadata`), which is exactly
a 0-row batch carrying metadata. Over an ENUM schema the client never receives the
token, and the next `exchange` request arrives without one.

Verified: patching `tableToIPC` in `@query-farm/flechette` to synthesise empty
dictionary batches (and register their ids in `idMap`) alongside the empty record
batch makes both tests pass unchanged. The fix belongs in
`@query-farm/flechette@2.4.0` → a release → a `@query-farm/vgi-rpc` rebuild.

The leg is left **blocking** on purpose. Suppressing it would recreate exactly the
condition this harness was written to eliminate.
