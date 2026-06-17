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

> **Peer dependencies (since 0.3.0).** `@query-farm/apache-arrow` (`^21.1.1`) and
> `@query-farm/vgi-rpc` (`^0.7.5`) are **peerDependencies** — install them directly
> alongside `@query-farm/vgi`:
>
> ```bash
> npm install @query-farm/vgi @query-farm/apache-arrow @query-farm/vgi-rpc
> ```
>
> The SDK bundles both as external, so a single shared instance avoids
> duplicate-type errors — notably the `vgi-rpc` `Protocol` clash you hit when you
> also import `createHttpHandler` (or any `vgi-rpc` type) directly and two copies
> get installed. See [`MIGRATION.md`](./MIGRATION.md#020--030--peer-dependencies).

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

## Type representations

Every value DuckDB exchanges with a worker has an Arrow type, and each Arrow type
maps to exactly one JS value shape. A single **codec** layer is the only authority
for that mapping: it converts between the JS value you read/write and an internal
*canonical* pivot (the raw Arrow wire unit), identically across both Arrow backends.

There are two author-facing representations, selected per scalar function with
`repr`:

- **`rich`** (the default) — the ergonomic shape. Identical to the canonical wire
  unit for every type **except** `date32` / `date64`, which surface as a JS `Date`.
  Sub-second temporal types (`time`, `timestamp`, `duration`) stay numeric/`bigint`,
  because `Date` cannot hold microsecond/nanosecond precision losslessly.
- **`raw`** (opt-in) — the canonical wire unit with a **branded** TypeScript type that
  carries the unit (e.g. `TimestampMicros`, `Date32`, `UnscaledDecimal`). At runtime a
  branded value *is* the underlying `number`/`bigint`; the brand exists only at compile
  time so a wrong-unit mix-up is a type error.

### Per-type mapping

| Arrow type | `rich` JS value | `raw` branded type |
|---|---|---|
| `bool` | `boolean` | `boolean` |
| `int8` / `int16` / `int32` | `number` | `number` |
| `uint8` / `uint16` / `uint32` | `number` | `number` |
| `int64` | `bigint` | `Int64` |
| `uint64` | `bigint` | `Uint64` (exported as `Uint64Raw`) |
| `float16` / `float32` / `float64` | `number` | `number` |
| `utf8` / `largeUtf8` | `string` | `string` |
| `binary` / `largeBinary` / `fixedSizeBinary` | `Uint8Array` | `Uint8Array` |
| **`date32`** | **`Date`** | `Date32` (days since epoch, `number`) |
| **`date64`** | **`Date`** | `Date64Ms` (ms since epoch, `bigint`) |
| `time32[s]` / `time32[ms]` | `number` (raw unit) | `Time32S` / `Time32Ms` |
| `time64[us]` / `time64[ns]` | `bigint` (raw unit) | `Time64Us` / `Time64Ns` |
| `timestamp[s/ms/us/ns]` | `bigint` (raw unit) | `TimestampSeconds` / `TimestampMillis` / `TimestampMicros` / `TimestampNanos` |
| `duration[s/ms/us/ns]` | `bigint` (raw unit) | `DurationSeconds` / `DurationMillis` / `DurationMicros` / `DurationNanos` |
| `decimal128` / `decimal256` | `bigint` (UNSCALED integer) | `UnscaledDecimal` |
| `struct` | `{ field: richValue }` | `{ field: rawValue }` |
| `list` / `largeList` / `fixedSizeList` | `Array<richValue \| null>` | `Array<rawValue \| null>` |
| `map` | `Array<[richKey, richValue]>` | `Array<[rawKey, rawValue]>` |
| `dictionary` | the decoded value's `rich` | the decoded value's `raw` |

`date32` / `date64` are the **only** types where `rich` differs from the canonical
wire unit. Everywhere else, `rich` *is* the canonical value and `raw` is the same
value with a branded type. `null` / `undefined` pass through as `null` in every type.

Notes:

- **Discriminate Arrow types by `typeId` / predicates, never `constructor.name`.**
  The factories return Arrow *type instances*, not classes named after the factory.
  `dateDay()` returns an arrow-js `Date_` instance with `typeId === Type.Date` and
  `unit === DateUnit.DAY` — there is **no** class named `DateDay`, so a check like
  `type.constructor.name === "DateDay"` will always fail (and is brittle across the
  two Arrow backends and minified builds). Use the exported `isDate(type)` predicate,
  or compare `type.typeId` (to `Type.Date` / the backend-agnostic `TypeId.Date`) and
  `type.unit` (to `DateUnit.DAY` / `DateUnit.MILLISECOND`) to distinguish day vs.
  millisecond dates:

  ```ts
  import { dateDay, isDate, DateUnit, TypeId } from "@query-farm/vgi";

  const t = dateDay();
  isDate(t);                       // ✓ true   — backend-agnostic predicate
  t.typeId === TypeId.Date;        // ✓ true
  t.unit === DateUnit.DAY;         // ✓ true   — day-resolution date32
  t.constructor.name === "DateDay" // ✗ NEVER — no such class; this is always false
  ```

- **Decimals are unscaled.** A `decimal128(18, 2)` value of `123.45` is the bigint
  `12345n`; apply the scale yourself (`Number(v) / 100`). The declared
  precision/scale travel with the column type, not the value.
- **Temporal units are lossless `bigint`.** A `timestamp[us]` round-trips as the
  exact microsecond count — no `Date` narrowing, no precision loss.

### Symmetry, round-tripping, and validation

Reads and writes are symmetric: a value read from a column rebuilds into the same
column. `build(read(x))` round-trips, in either representation —
`iterRows(batch)` (and scalar inputs) return `rich` values, and a `rich` value fed
back through `batchFromColumns`/a scalar `compute` return rebuilds the original
column (pass `"raw"` / `repr: 'raw'` on both ends for the branded form).

The codec **validates and throws** on invalid or lossy input: a non-integer where an
integer is required, a `bigint` that overflows the declared width or the safe-integer
range when narrowing to `number`, an out-of-range `Date`, the wrong number of bytes
for a `fixedSizeBinary`, etc. You get a clear `codec[<type>]: …` `TypeError` at build
time rather than silently corrupt data on the wire.

### Typed author API

Declare `params` and `returns` (or `args`) with the typed factories and `compute` is
statically typed end to end — the input columns and the return value are checked
against the declared Arrow types and the chosen representation:

```ts
import { Worker, defineScalarFunction, timestampMicros, int64 } from "@query-farm/vgi";

// rich (default): timestamp values are plain bigint microsecond counts.
const addHour = defineScalarFunction({
  name: "add_hour",
  params: { ts: timestampMicros },          // input column: bigint (us)
  returns: timestampMicros,                  // output: bigint (us)
  compute: (batch) => {
    const ts = batch.getChildAt(0)!;
    return Array.from(ts, (v: bigint | null) =>
      v == null ? null : v + 3_600_000_000n, // +1h in microseconds
    );                                        // returning a Date here is a COMPILE error
  },
});
```

```ts
import {
  Worker, defineScalarFunction, timestampMicros,
  asTimestampMicros, type TimestampMicros,
} from "@query-farm/vgi";

// raw mode: outputs are branded units, constructed with `asTimestampMicros`.
const epoch = defineScalarFunction({
  name: "epoch_us",
  params: { ts: timestampMicros },
  returns: timestampMicros,
  repr: "raw",                               // opt in to branded raw units
  compute: (batch) => {
    const ts = batch.getChildAt(0)!;
    return Array.from(ts, (v: TimestampMicros | null) =>
      v == null ? null : asTimestampMicros(v + 1n), // branded in, branded out
    );
  },
});
```

For manual conversions outside a function, `codecFor(type)` returns the codec with
`richToCanonical` / `canonicalToRich` / `rawToCanonical` / `canonicalToRaw`.

### Factory name note

The typed Arrow type factories `int`, `int32`, `float32`, and `bool` are **not**
re-exported from the package root, because `@query-farm/vgi` already re-exports
vgi-rpc argument builders of the same names. Import those four from the arrow facade
(they ship as the typed factories there); the rest of the typed factory set
(`int8`/`int16`/`int64`, `uint*`, `float16`/`float64`, `decimal*`, `dateDay`,
`timestampMicros`, `struct`, `list`, `map`, …) is exported from the package root as
usual.

## Migration: the type-handling break

This is a pre-1.0 breaking change to how columnar values are represented in and out
of functions. The contract is now uniform across both Arrow backends and both
directions (read and write). For consumers upgrading:

- **`date32` / `date64` columns are now JS `Date` in *and* out by default.**
  Previously dates were inconsistent — a day-number went *in* but a `Date` came back
  *out*. Both directions are now `Date` under the default `rich` representation.
- **Reads return rich values.** `iterRows`, scalar inputs, and setting/secret reads
  all surface the `rich` value for their type.
- **Non-date temporal types are lossless `bigint` raw units.** `time64`, `timestamp`,
  and `duration` are the exact `bigint` count in their declared unit (us, ns, …) — no
  `Date`, no precision loss.
- **Decimals are unscaled `bigint`.** A `decimal(18,2)` of `123.45` is `12345n`.
- **Opt into `repr: 'raw'`** for branded, unit-tagged raw units everywhere (including
  `date32`/`date64` as plain day-number / ms-`bigint` rather than `Date`).

Before / after for the common date case:

```ts
// BEFORE (old, inconsistent): wrote a day-number, read back a Date.
returns: dateDay,
compute: () => [20000],                 // 20000 days since epoch

// AFTER (rich, default): write a Date, read a Date — symmetric.
returns: dateDay,
compute: () => [new Date("2024-10-19")],

// AFTER (raw): opt in to the branded day-number.
returns: dateDay,
repr: "raw",
compute: () => [asDate32(20000)],       // branded number, not a Date
```

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
| Cloudflare Workers (`workerd`) / browser | [`@query-farm/flechette`](https://www.npmjs.com/package/@query-farm/flechette) |

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
