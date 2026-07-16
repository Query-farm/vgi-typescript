# TypeScript VGI worker over the browser `worker:` SAB transport

A vgi-typescript worker that serves the VGI DuckDB extension's in-browser `worker:`
transport — the SharedArrayBuffer duplex-ring channel — at parity with the Rust
`sabtable` worker. Proves a **TypeScript** worker (not just Rust) can be a serverless
in-browser VGI worker.

## Pieces

- **`sab.ts`** — the channel adapter. Opens the SAB channel (byte-exact to
  `vgi/src/include/vgi_sab_abi.hpp`) and presents each claimed slot's client→worker ring
  as a `ReadableStream<Uint8Array>` and the worker→client ring as a `ByteSink`. One Web
  Worker multiplexes **all** slots on the event loop via `Atomics.waitAsync` (no
  blocking) — the JS analog of the Rust worker's thread-per-slot. Implements the same
  claim-id close **token** + reclaim **bail** invariants as the Rust/JS reference.
- **`boot.ts`** — builds the VGI protocol (`buildVgiProtocol` + a `FunctionRegistry` +
  a `ReadOnlyCatalogInterface` so `ATTACH` works) with two fixtures (`ts_count` table,
  `ts_double` scalar) and drives `serveStream` over each slot. Imports from
  `index.core.js` (the browser-safe entry — the top-level `Worker` class pulls in
  Node-only `serveUnix`/`serveTcp`).

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
