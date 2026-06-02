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

Requires **Node.js ≥ 22.15** or **Bun**. Using VGI from DuckDB also requires the
[`vgi` DuckDB extension](https://query.farm).

## Quick start

### 1. Define a scalar function and run a worker

Types are plain aliases (`str`, `int`, `float`, `bool`, `bytes`, …) and you read a
batch as ordinary row objects with `iterRows` — no manual Arrow vector wrangling:

```ts
// worker.ts
import { Worker, defineScalarFunction, iterRows, str } from "@query-farm/vgi";

const upperCase = defineScalarFunction({
  name: "upper_case",
  description: "Convert a string to uppercase",
  params: { value: str },
  returns: str,
  compute: (batch) =>
    Array.from(iterRows(batch), (row) =>
      row.value == null ? null : String(row.value).toUpperCase(),
    ),
});

// All WorkerConfig fields are optional — a functions-only worker is valid.
new Worker({ functions: [upperCase] }).run();
```

A worker speaks Arrow IPC over stdin/stdout (or AF_UNIX / HTTP — see
[Transports](#transports)). It is **not** interactive; DuckDB drives it.

### 2. Stream rows from a table function

Table functions are incremental producers: build a schema with `toSchema`, keep your
own `state`, and `emit` batches until you `finish`. DuckDB pulls lazily, so this
streams without materializing everything up front:

```ts
import { Worker, defineTableFunction, batchFromColumns, toSchema, int } from "@query-farm/vgi";

const schema = toSchema({ n: int });

const sequence = defineTableFunction({
  name: "sequence",
  description: "Emit integers 0..n-1, streamed in batches of 1000",
  args: { n: int },
  onBind: () => ({ outputSchema: schema }),
  initialState: ({ args }) => ({ i: 0, n: Number(args.n) }),
  process: (_params, state, out) => {
    if (state.i >= state.n) return out.finish();
    const end = Math.min(state.i + 1000, state.n);
    const ns: bigint[] = [];
    for (let k = state.i; k < end; k++) ns.push(BigInt(k));
    out.emit(batchFromColumns({ n: ns }, schema));
    state.i = end;
  },
});

new Worker({ functions: [upperCase, sequence] }).run();
```

### 3. Attach it from DuckDB

The `LOCATION` is a shell command DuckDB runs to spawn the worker. For Bun:

```sql
LOAD vgi;
ATTACH 'demo' AS demo (TYPE vgi, LOCATION 'bun run /abs/path/to/worker.ts');

SELECT demo.upper_case('hello');   -- HELLO
SELECT * FROM demo.sequence(5);    -- 0,1,2,3,4
```

## Function types

| Factory | DuckDB shape |
| --- | --- |
| `defineScalarFunction` | scalar UDF (row → row) |
| `defineTableFunction` | table-producing function |
| `defineAggregate` | aggregate / window function |
| `defineTableInOutFunction` | table in → table out (per-partition state, finalize) |
| `defineTableBufferingFunction` | buffering table function (full-input materialization) |

Workers can also expose a **catalog** (schemas, tables, views, macros) via
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
