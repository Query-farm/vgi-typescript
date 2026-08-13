# Migration guide

## 0.26.0 — the landing surface moved here from `@query-farm/vgi-rpc`

`GET {prefix}/` and `GET {prefix}/vgi-client.js` are now served by this package
rather than by `@query-farm/vgi-rpc`, and `LandingInfo` is exported from here.

### Why

vgi-rpc is generic RPC over Arrow. It depends on nothing in this package, yet it
vendored the shared `landing.html` — a page whose entire vocabulary is catalogs,
schemas, tables, functions and macros — together with a **compiled browser bundle
of this package's client**. That put a build artifact of the higher layer inside
the lower one, and cost every vgi-rpc consumer 911 KB of a 1269 KB bundle whether
or not they served a VGI page. vgi-rpc's bundle is 358 KB after the move.

vgi-python never had this problem: its landing surface is in `vgi/http/`, and
`vgi_rpc/http/` carries neither asset. The TypeScript port was the outlier.

### What to change

Nothing, if you use `serveVgiWorker` or `createVgiFetch` — they wire it up.

If you call `createHttpHandler` from `@query-farm/vgi-rpc` directly and passed
`landingInfo`, that option is gone. Contribute the surface instead:

```ts
import { createLandingRoutes } from "@query-farm/vgi";

const handler = createHttpHandler(protocol, {
  // ...
- landingInfo: { name: "my-worker", doc: "…", version: "1.0.0" },
+ extraRoutes: createLandingRoutes({ name: "my-worker", doc: "…", version: "1.0.0" }),
+ enableLandingPage: false,   // drop vgi-rpc's generic RPC-endpoint page
});
```

Import `LandingInfo` from `@query-farm/vgi` (or `@query-farm/vgi/worker-cf`)
rather than from `@query-farm/vgi-rpc`.

`extraRoutes` is generic and carries no VGI knowledge — it is a GET route hook
consulted after the OAuth browser-redirect branch and before the generic landing
page and the 404.

### Worth knowing: these routes are public without PKCE

vgi-rpc gates GET pages on `authenticate && pkceConfig`, and a non-browser caller
that fails auth falls through to page serving. So unless the worker configures the
OAuth PKCE flow, the landing page and the client bundle are served to anyone. This
is unchanged by the move — the surface sits exactly where it did — but it is easy
to assume otherwise. `vgi-rpc-python` is stricter and answers 401.

## 0.25.x → 0.26.0 — `createVgiFetch` requires `landingInfo`

`landingInfo` on `createVgiFetch` (`@query-farm/vgi/worker-cf`) is now
**required**, and omitting it throws at construction instead of type-checking.

### Why

It was optional, and the fallback was silent. A Cloudflare worker built without
it served vgi-rpc's generic "this is an RPC endpoint" placeholder at `GET /` —
about 2 KB, no catalog tree, no "Explore in Cupola" link, no `ATTACH` snippet —
and 404'd `GET /vgi-client.js`, the browser client that page imports to read the
catalog. The worker's RPC surface was completely fine, so nothing failed, no
test caught it, and the only symptom was a page that looked like a stub.
`vgi-open-meteo` shipped that way and nobody noticed until the page was opened
next to a vgi-python worker's.

`serveVgiWorker` was never exposed to this: it takes `name`/`doc`/`version` as
required options and builds `landingInfo` itself. The asymmetry between the two
entries was the whole bug, so the CF entry now demands the same information.

### What to change

```ts
const handler = createVgiFetch({
  protocol: { registry, catalogInterface },
  signingKey,
  prefix: "",
  serverId: "my-worker",
+ repositoryUrl: "https://github.com/me/my-worker",   // optional, linked from the page
+ landingInfo: {
+   name: "my-worker",
+   doc: "One line on what this worker serves.",
+   version: "1.0.0",
+ },
});
```

`name`, `doc`, and `version` are what the page's header and status document
show; `doc` should be a single line.

### Known issue this does not fix

On workerd the server stamps `X-VGI-Content-Encoding: gzip` (the Cloudflare edge
re-gzips a standard `Content-Encoding`, so the label has to move). The vendored
browser client bundle does not yet decode that header, so the landing page on a
Cloudflare-deployed worker renders but reports "Could not load worker metadata".
Fixing it means rebuilding the bundle in `vgi-web-frontend` and re-vendoring it
into `vgi-rpc-typescript` and `vgi-python`; it affects every CF-deployed VGI
worker regardless of language.

## 0.2.0 → 0.3.0 — peer dependencies

`@query-farm/apache-arrow` and `@query-farm/vgi-rpc` moved from regular
`dependencies` to **`peerDependencies`**. The SDK already bundles both as
`--external` (consumer-provided) in its build, so they were never meant to ship a
second, SDK-private copy.

### What changed for consumers

- **Install both peers directly**, at these ranges:

  ```bash
  npm install @query-farm/apache-arrow@^21.1.1 @query-farm/vgi-rpc@^0.7.5
  # or: bun add @query-farm/apache-arrow @query-farm/vgi-rpc
  ```

  (Plus `@query-farm/vgi` itself.) Most consumers already depend on these
  transitively; making them peers just makes the requirement explicit and
  guarantees a **single shared instance** of each.

- **Why this matters — the `vgi-rpc` `Protocol` clash.** When the SDK carried its
  own copy of `@query-farm/vgi-rpc` and a consumer *also* imported the package
  directly — e.g. calling `createHttpHandler` from their own HTTP entry — npm/bun
  could install two copies. That yields two separate `Protocol` type declarations,
  and TypeScript treats the two as incompatible, producing confusing "type X is not
  assignable to type X" compile errors at the boundary. A single peer-provided
  instance eliminates the duplicate-type error.

- **Action:** if you see duplicate-type errors around `Protocol`,
  `createHttpHandler`, or Arrow types after upgrading, dedupe — ensure exactly one
  copy of each peer is installed (`npm ls @query-farm/vgi-rpc @query-farm/apache-arrow`
  / `bun pm ls`).

This release has **no API or type-representation changes** — only the dependency
shape.

## 0.1.x → 0.2.0 — the type-handling break

A pre-1.0 breaking change standardizes how columnar values are represented as JS
values in and out of every function. The representation is now uniform across both
Arrow backends (arrow-js for Node/Bun, flechette for Workers/browser) and symmetric
across reads and writes: a value read from a column rebuilds into the same column.

See the **Type representations** section of the [README](./README.md) for the full
per-type table and the typed author API.

### What changed for consumers

- **`date32` / `date64` columns are now JS `Date` in *and* out by default.**
  Previously dates were inconsistent — a day-number went *in* but a `Date` came back
  *out*. Both directions are now `Date` under the default `rich` representation.
- **Reads return rich values.** `iterRows`, scalar `compute` inputs, and
  setting/secret reads all surface the `rich` value for their column type.
- **Non-date temporal types are lossless `bigint` raw units.** `time64`,
  `timestamp[s/ms/us/ns]`, and `duration[s/ms/us/ns]` are the exact `bigint` count in
  their declared unit — never a `Date`, never narrowed, no precision loss.
- **Decimals are unscaled `bigint`.** A `decimal(18, 2)` value of `123.45` is the
  bigint `12345n`. Apply the scale yourself; the precision/scale travel with the
  column type.
- **Codecs validate and throw.** Invalid or lossy input (non-integer where an integer
  is required, a `bigint` that overflows the declared width or the safe-integer range,
  an out-of-range `Date`, the wrong byte count for a `fixedSizeBinary`) raises a clear
  `codec[<type>]: …` `TypeError` at build time instead of corrupting the wire data.
- **Opt into `repr: 'raw'`** on `defineScalarFunction` for branded, unit-tagged raw
  units everywhere. In raw mode `date32`/`date64` are the plain day-number /
  ms-`bigint` (branded `Date32` / `Date64Ms`) rather than a `Date`.

### The common case: dates

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

### Checklist

1. Find every `date32` / `date64` column you write from a function and change
   day-numbers / ms-integers to `Date` (or set `repr: 'raw'` and wrap with
   `asDate32` / `asDate64Ms`).
2. Confirm `timestamp` / `time64` / `duration` producers emit `bigint` in the
   declared unit, and consumers read `bigint` (not `Date`).
3. Confirm decimal producers emit the **unscaled** integer as a `bigint`.
4. Run your tests — codec validation now throws on values it previously coerced.
