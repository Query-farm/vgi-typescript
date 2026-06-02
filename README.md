# @query-farm/vgi

TypeScript framework for extending [DuckDB](https://duckdb.org) with user-defined
functions written in TypeScript, served to DuckDB over the **Vector Gateway
Interface (VGI)** — an Apache Arrow IPC protocol.

You write scalar, table, aggregate, and table-in-out functions (plus catalogs) in
TypeScript; a *worker* exposes them; DuckDB attaches to the worker and calls them as
if they were native. It is wire-compatible with [`vgi-python`](https://query.farm),
so a worker written in either language is interchangeable.

```sql
LOAD vgi;
ATTACH 'demo' AS demo (TYPE vgi, LOCATION '/path/to/your-worker');
SELECT demo.upper_case(name) FROM users;
```

## Install

```bash
npm install @query-farm/vgi
# or: bun add @query-farm/vgi
```

Requires **Node.js ≥ 22.15** or **Bun**.

Using VGI from DuckDB also requires the **`vgi` DuckDB extension**. The easiest way to
get it is via [**Haybarn**](https://haybarn.query.farm) — Query Farm's distribution of
DuckDB — which publishes the extension (and a DuckDB-Wasm engine) as prebuilt npm
packages, so there's no C++ toolchain or manual `INSTALL`/compile step:

```bash
# The vgi extension as prebuilt binaries, from npm:
npm install @haybarn/ext-vgi-h1-5-3      # built for Haybarn / DuckDB 1.5.3

# Haybarn's DuckDB-Wasm engine, for running VGI in the browser / Cloudflare Workers:
npm install @haybarn/haybarn-wasm
```

The `@haybarn/ext-*` packages are meta-packages: npm pulls only the prebuilt binary
matching your platform (native leaves for linux/darwin/win, plus
`wasm-mvp`/`wasm-eh`/`wasm-threads` variants for DuckDB-Wasm). The Wasm engine is what
lets VGI run in the browser and on Cloudflare Workers — see
[Runtimes & entry points](#runtimes--entry-points).

## Quick start

### 1. Define a scalar function and run a worker

Types are plain aliases (`str`, `int`, `float`, `bool`, `bytes`, …) and the input
arrives as a columnar Arrow batch:

```ts
// worker.ts
import { Worker, defineScalarFunction, str, int } from "@query-farm/vgi";

const upperCase = defineScalarFunction({
  name: "upper_case",                        // SQL name → demo.upper_case(...)
  description: "Convert a string to uppercase",

  // Declares the argument types *and order*. DuckDB calls scalar functions
  // positionally, so the keys are just parameter names (for docs/metadata) —
  // inside compute you read the input columns by position, not by this name.
  // Type aliases: str, int, int32, float, float32, bool, bytes — or any Arrow
  // DataType (e.g. `new Decimal(18, 2)`).
  params: { value: str },

  // Static output type. For a type that depends on the input, omit `returns`
  // and use `outputType: (bind) => <DataType>` (may be async).
  returns: str,

  // compute runs once per input batch. Read each argument as a column by
  // position (column 0 = first arg); return one output value per row (null ok).
  // Full signature: compute(batch, consts, info) where
  //   consts → values of any `constParams` (literals folded at bind time)
  //   info   → { settings, secrets, auth } — session settings, secrets, caller
  compute: (batch) => {
    const values = batch.getChildAt(0)!;     // first positional argument
    return Array.from(values, (v) =>
      v == null ? null : String(v).toUpperCase(),
    );
  },

  // Other optional fields: constParams, nullHandling, stability (volatility),
  // examples, categories, tags, requiredSettings, requiredSecrets, maxWorkers.
});

// A two-argument scalar — the args are positional, so column 0 is the first
// argument and column 1 is the second (the param keys are just names).
const multiply = defineScalarFunction({
  name: "multiply",
  description: "Multiply two numbers",
  params: { a: int, b: int },                // two positional arguments
  returns: int,                              // int = Int64 → values are bigint
  compute: (batch) => {
    const a = batch.getChildAt(0)!;          // first argument
    const b = batch.getChildAt(1)!;          // second argument
    return Array.from({ length: batch.numRows }, (_, i) => {
      const x = a.get(i);
      const y = b.get(i);
      return x == null || y == null ? null : x * y; // NULL in → NULL out
    });
  },
});

// Functions are served through a catalog — that's what DuckDB ATTACHes to.
// `name` is the catalog DuckDB sees (matches the ATTACH target below).
new Worker({
  catalog: { name: "demo", schemas: [{ name: "main", functions: [upperCase, multiply] }] },
}).run();
```

A worker speaks Arrow IPC over stdin/stdout (or AF_UNIX / HTTP — see
[Transports](#transports)). It is **not** interactive; DuckDB drives it.

### 2. Stream rows from a table function

Table functions are incremental producers: build a schema with `toSchema`, keep your
own `state`, and `emit` batches until you `finish`. DuckDB pulls lazily, so this
streams without materializing everything up front:

```ts
import { Worker, defineTableFunction, batchFromColumns, toSchema, int } from "@query-farm/vgi";

const schema = toSchema({ n: int });         // output columns → n BIGINT

const sequence = defineTableFunction({
  name: "sequence",
  description: "Emit integers 0..n-1, streamed in batches of 1000",

  // Positional args by name. Also available: argDefaults, varargs, argDocs.
  args: { n: int },

  // onBind runs once at plan time → declare the output schema. May be async,
  // and may also return opaqueData / secret + scope lookups for the call.
  onBind: () => ({ outputSchema: schema }),

  // Per-execution mutable state, built from the bound args.
  initialState: ({ args }) => ({ i: 0, n: Number(args.n) }),

  // process is the pull loop. DuckDB calls it repeatedly and consumes lazily:
  // emit zero or more batches per call, then finish() to signal end-of-stream.
  // You never have to materialize the whole result. May be async.
  process: (_params, state, out) => {
    if (state.i >= state.n) return out.finish();    // end-of-stream
    const end = Math.min(state.i + 1000, state.n);
    const ns: bigint[] = [];
    for (let k = state.i; k < end; k++) ns.push(BigInt(k));
    out.emit(batchFromColumns({ n: ns }, schema)); // hand a batch to DuckDB
    state.i = end;
  },

  // Optional optimizer/runtime hooks — all omittable:
  //   cardinality:    (bind) => ({ estimate, max })   row-count hints
  //   statistics:     (bind) => ColumnStatistics[]    min/max → filter folding
  //   projectionPushdown / filterPushdown / autoApplyFilters
  //   dynamicToString: (...) => Record<string,string> EXPLAIN ANALYZE counters
  //   onInit + shared storage for partitioned producers / work queues
  //   partitionKind, preservesOrder, lateMaterialization, samplingPushdown, …
});

// Serve every function from the same catalog.
new Worker({
  catalog: { name: "demo", schemas: [{ name: "main", functions: [upperCase, multiply, sequence] }] },
}).run();
```

### 3. Attach it from DuckDB

The `LOCATION` is a shell command DuckDB runs to spawn the worker. For Bun:

```sql
LOAD vgi;
ATTACH 'demo' AS demo (TYPE vgi, LOCATION 'bun run /abs/path/to/worker.ts');

SELECT demo.upper_case('hello');   -- HELLO
SELECT demo.multiply(6, 7);        -- 42
SELECT * FROM demo.sequence(5);    -- 0,1,2,3,4
```

## Function types

Five factories, each mapping to a DuckDB function shape with its own lifecycle:

- **`defineScalarFunction`** — row-in / row-out (`SELECT f(x)`). DuckDB hands you a
  columnar batch; you return one output value per input row. For transforms,
  parsing, formatting, crypto, encoding, per-row lookups.
- **`defineTableFunction`** — a set-returning producer (`SELECT * FROM f(...)`). You
  keep your own `state` and `emit` batches until `finish`, and DuckDB pulls lazily.
  The richest type: projection / filter / sampling pushdown, cardinality and
  per-column statistics for the optimizer, Hive-style partitioning, late
  materialization, and EXPLAIN ANALYZE counters.
- **`defineAggregate`** — aggregate / window function (`f(x) … GROUP BY`). Implement
  `initialState` / `update` / `combine` / `finalize`; `combine` lets DuckDB
  aggregate partitions in parallel and merge the results.
- **`defineTableInOutFunction`** — table-in / table-out
  (`SELECT * FROM f(TABLE t, …)`). Per-partition `state`, a streaming `process` per
  input batch, and a `finalize` that sees every worker's state — for windowing,
  reshaping, enrichment, sessionization.
- **`defineTableBufferingFunction`** — table-in / table-out that must observe **all**
  input before producing output (global sort, top-N, pivot). Sink (`process`) →
  `combine` → source (`finalize`), with explicit sink/source ordering controls.

### Capability matrix

| Capability | Scalar | Table | Aggregate | Table-in-out | Buffering |
|---|:--:|:--:|:--:|:--:|:--:|
| Columnar input rows | ✓ | — | ✓ | ✓ | ✓ |
| Streaming output (`emit` / `finish`) | — | ✓ | — | ✓ | ✓ |
| Per-group / per-partition state | — | — | ✓ | ✓ | ✓ |
| Parallel merge (`combine`) | — | — | ✓ | — | ✓ |
| Bind-time dynamic output type | ✓ | ✓ | ✓ | ✓ | ✓ |
| Named args · defaults · varargs | ✓¹ | ✓ | ✓ | ✓ | ✓ |
| Constant (bind-folded) params | ✓ | ✓² | ✓ | ✓² | ✓² |
| Cardinality hints | — | ✓ | — | — | ✓ |
| Column statistics (optimizer) | — | ✓ | — | — | — |
| Projection pushdown | — | ✓ | — | ✓ | ✓ |
| Filter pushdown (+ auto-apply) | — | ✓ | — | ✓ | ✓ |
| Sampling pushdown | — | ✓ | — | — | — |
| Late materialization | — | ✓ | — | — | — |
| Hive-style partitioning | — | ✓ | — | — | — |
| Batch-index threading | — | ✓ | — | — | ✓ |
| Order preservation / dependence | — | ✓ | — | — | ✓³ |
| EXPLAIN ANALYZE diagnostics | — | ✓ | — | — | — |
| Shared storage (work queues, cross-worker state) | — | ✓ | — | ✓ | ✓ |
| Settings & secrets access | ✓ | ✓ | — | ✓ | ✓ |
| Volatility / stability hint | ✓ | ✓ | — | ✓ | ✓ |
| NULL-handling control | ✓ | ✓ | ✓ | ✓ | ✓ |
| Async lifecycle | bind⁴ | ✓ | bind⁴ | ✓ | ✓ |

¹ via the ordered `parameters` form. &nbsp; ² supplied as bind-time `args`. &nbsp;
³ via `sinkOrderDependent` / `sourceOrderDependent`. &nbsp; ⁴ `onBind` / `outputType`
may be async; `compute` / `update` / `finalize` run synchronously.

Workers can also expose a **catalog** (schemas, tables, views, macros, secrets) via
`ReadOnlyCatalogInterface` / `CompositeCatalogInterface`, so an `ATTACH`ed worker
presents browsable database objects, not just functions.

## Transports

A worker serves the same functions over any of:

- **stdin/stdout** — the default; how DuckDB subprocess-spawns a worker.
- **AF_UNIX** — a long-lived warm worker (`--unix <path>`), reused across calls.
- **HTTP** — stateless; all state round-trips in a self-contained token, so requests
  can be load-balanced across hosts.

Transport handling lives in [`@query-farm/vgi-rpc`](https://www.npmjs.com/package/@query-farm/vgi-rpc).

## Runtimes & entry points

The package ships a backend-agnostic Arrow facade and selects an implementation at
build time per runtime, so the same source runs on servers, Cloudflare Workers, and
browsers:

| Runtime | Arrow backend |
| --- | --- |
| Node.js / Bun | [`@query-farm/apache-arrow`](https://www.npmjs.com/package/@query-farm/apache-arrow) (arrow-js) |
| Cloudflare Workers (`workerd`) / browser | [`@uwdata/flechette`](https://github.com/uwdata/flechette) |

Subpath exports:

- `@query-farm/vgi` — main API (define functions, `Worker`, `VgiClient`, catalogs).
- `@query-farm/vgi/client` — client-only entry (no server-side code).
- `@query-farm/vgi/worker-cf` — Cloudflare Workers entry (`workerd`/browser bundle).

## Relationship to other packages

- **[Haybarn](https://haybarn.query.farm)** — Query Farm's distribution of DuckDB,
  with the `vgi` extension (and others) as prebuilt npm packages
  ([`@haybarn/haybarn-wasm`](https://www.npmjs.com/package/@haybarn/haybarn-wasm),
  [`@haybarn/ext-vgi-h1-5-3`](https://www.npmjs.com/package/@haybarn/ext-vgi-h1-5-3)).
  The host side: it runs the DuckDB engine that attaches to your worker.
- **[`@query-farm/vgi-rpc`](https://www.npmjs.com/package/@query-farm/vgi-rpc)** —
  the RPC layer: protocol, server, transports, Arrow IPC framing. A runtime
  dependency of this package.
- **`vgi-python`** — the reference implementation. This package is a wire-compatible
  TypeScript port; workers from either side interoperate.

## Development

```bash
make install          # bun install
make build            # types + JS bundles
make test             # integration tests (launcher transport)
make -j8 test-all     # launcher + HTTP suites in parallel
```

See [`CLAUDE.md`](./CLAUDE.md) for the full build/test reference.

## License

Copyright © 2025, 2026 Query Farm LLC — https://query.farm

Distributed under the **Query Farm Source-Available License, Version 1.0**. Use,
modification, redistribution, and non-production use are permitted; some production
uses (notably competing offerings and commercial marketplaces) require a separate
commercial license. Each version converts to Apache-2.0 ten years after its release.
See [`LICENSE`](./LICENSE) for the full terms, or contact hello@query.farm for
commercial licensing.
