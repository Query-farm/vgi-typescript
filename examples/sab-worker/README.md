# TypeScript VGI worker over the browser `worker:` SAB transport

A vgi-typescript worker that serves the VGI DuckDB extension's in-browser `worker:`
transport — the SharedArrayBuffer duplex-ring channel — at parity with the Rust
`sabtable` worker. Proves a **TypeScript** worker (not just Rust) can be a serverless
in-browser VGI worker.

## Pieces

- **`sab.ts`** — the channel adapter. Opens the SAB channel (byte-exact to
  `vgi/src/include/vgi_sab_abi.hpp`) and presents each claimed slot's client→worker ring
  as a `ReadableStream<Uint8Array>` and the worker→client ring as a `ByteSink`
  (`serveSlotForever` awaits the ring via `Atomics.waitAsync`). Implements the same
  claim-id close **token** + reclaim **bail** invariants as the Rust/JS reference.
- **`boot.ts`** — builds the VGI protocol (`buildVgiProtocol` + a `FunctionRegistry` +
  a `ReadOnlyCatalogInterface` so `ATTACH` works) with fixtures (`ts_count`/`ts_double`,
  plus `ts_probe`/`ts_peek` for the parallelism proof) and drives `serveStream`. Imports
  from `index.core.js` (the browser-safe entry — the top-level `Worker` class pulls in
  Node-only `serveUnix`/`serveTcp`).
  **Truly parallel (thread-per-slot):** the bridge spawns this once (the BOOT role); it
  spawns ONE dedicated sub-Worker per channel slot (the SLOT role), sharing the SAB, so N
  slots are served on N real threads — matching the Rust worker's emscripten
  thread-per-slot (vs. multiplexing all slots on one event loop). Both roles run this same
  bundle, keyed by the init message type; `ts_probe` (a busy-loop under a shared
  concurrency counter) proves a peak of N simultaneous serves.

Requires vgi-rpc ≥ the `ByteSink` serve writable (`serveStream({ writable })` accepting a
`{ write(bytes) }` sink, not just a Node fd/Socket).

## Build

```bash
# ESM bundle (keeps import.meta, which arrow deps use)
bun build examples/sab-worker/boot.ts --outdir examples/sab-worker/dist --target browser --format esm
```

The bundle is loaded by a tiny **classic** worker shim (`ts-worker-boot.js` in the vgi
repo's `test/support/wasm-worker/`) via dynamic `import()`, because the extension bridge
spawns `new Worker(url)` (classic) and a classic worker can neither run an ESM bundle
directly nor use `import.meta`. The shim buffers the one-shot `vgi-init` message, imports
the ESM module, which drains the buffer and takes over.

## Test

Browser E2E lives in the vgi repo:
`vgi/test/support/wasm-worker/browser-e2e/test-ts-worker.mjs` — asserts a direct
`vgi_table_function('worker:ts-worker-boot.js','ts_count',[5])`, `ATTACH` + discovery +
`tcat.main.ts_count(3)`, and the `ts_double` scalar (exchange mode). Run it with
`VGI_ENTRY=test-ts-worker.mjs node serve.mjs` after copying this bundle to
`ts-worker-mod.js` there.
