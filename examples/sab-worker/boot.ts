// TypeScript VGI worker served over the in-browser SAB (`worker:`) transport.
//
// The extension's bridge spawns this as `new Worker(url)` and postMessages
// `{type:'vgi-init', buffer, offset}` with DuckDB's shared linear memory (the SAB) +
// the channel offset. We build the VGI protocol (a couple of fixtures + a read-only
// catalog so ATTACH works) and drive `serveStream` over each SAB slot via
// `serveChannel`. Proves a TS worker reaches parity with the Rust `sabtable` worker.
//
// NB: import from `index.core.js` (the browser-safe entry) — the top-level `Worker`
// class pulls in Node-only serveUnix/serveTcp, which don't exist in a Web Worker.
import { Schema, Field, Int64 } from "@query-farm/apache-arrow";
import {
  defineTableFunction,
  defineScalarFunction,
  batchFromColumns,
  FunctionRegistry,
  ReadOnlyCatalogInterface,
  buildVgiProtocol,
  type TableBindParams,
  type TableProcessParams,
} from "../../src/index.core.js";
import { serveStream, type OutputCollector } from "@query-farm/vgi-rpc";
import { openChannel, serveChannel } from "./sab.js";

const N_SCHEMA = new Schema([new Field("value", new Int64(), true)]);

interface CountArgs { n: number }
interface CountState { i: number; n: number }

// ts_count(n) -> value 0..n-1 (producer; mirrors the Rust count_to).
const tsCount = defineTableFunction<CountArgs, CountState>({
  name: "ts_count",
  description: "Emit value 0..n-1 (TypeScript SAB worker)",
  args: { n: new Int64() },
  onBind: (_p: TableBindParams<CountArgs>) => ({ outputSchema: N_SCHEMA }),
  initialState: (p: TableProcessParams<CountArgs>) => ({ i: 0, n: Number(p.args.n) }),
  process: (p: TableProcessParams<CountArgs>, state: CountState, out: OutputCollector) => {
    if (state.i >= state.n) { out.finish(); return; }
    const values: bigint[] = [];
    for (let v = state.i; v < state.n; v++) values.push(BigInt(v));
    out.emit(batchFromColumns({ value: values }, p.outputSchema));
    state.i = state.n;
  },
});

// ts_double(x) -> x*2 (scalar; exchange 1:1, null passthrough).
const tsDouble = defineScalarFunction({
  name: "ts_double",
  description: "x * 2 (TypeScript SAB worker)",
  params: { x: new Int64() },
  returns: new Int64(),
  compute: (batch: { numRows: number; getChildAt: (i: number) => { get: (r: number) => bigint | null } }) => {
    const col = batch.getChildAt(0);
    const out: (bigint | null)[] = [];
    for (let i = 0; i < batch.numRows; i++) {
      const v = col.get(i);
      out.push(v == null ? null : (typeof v === "bigint" ? v : BigInt(v as number)) * 2n);
    }
    return out;
  },
});

// Build the protocol once: registry (dispatch) + a read-only catalog (so ATTACH +
// wcat.main.<fn> discovery/binding works, in addition to direct vgi_table_function()).
const registry = new FunctionRegistry();
registry.register(tsCount);
registry.register(tsDouble);
const catalog = { name: "main", schemas: [{ name: "main", functions: [tsCount, tsDouble] }] };
const catalogInterface = new ReadOnlyCatalogInterface(catalog, registry);
const protocol = buildVgiProtocol({ registry, catalogInterface, catalogName: "main" });

function handleInit(d: { type?: string; buffer?: SharedArrayBuffer; offset?: number }) {
  if (!d || d.type !== "vgi-init") return;
  try {
    const ch = openChannel(d.buffer as SharedArrayBuffer, d.offset as number);
    serveChannel(ch, (readable, writable) => serveStream(protocol, { readable, writable }));
    (self as unknown as { postMessage: (m: unknown) => void }).postMessage({ type: "vgi-ready" });
  } catch (err) {
    (self as unknown as { postMessage: (m: unknown) => void }).postMessage({ type: "vgi-error", error: String((err as Error)?.message ?? err) });
  }
}

// This module is loaded via a classic worker shim (ts-worker-boot.js) using dynamic
// import(), because a classic Worker can't `new Worker(url,{type:module})` here and an
// IIFE bundle can't use `import.meta` (arrow deps do). The shim buffers any messages
// (the one-shot `vgi-init`) that arrived before this module finished importing into
// globalThis.__vgiBuffered; drain them, then handle future ones.
const buffered = (globalThis as unknown as { __vgiBuffered?: unknown[] }).__vgiBuffered;
if (Array.isArray(buffered)) for (const d of buffered) handleInit(d as { type?: string });
self.addEventListener("message", (e: MessageEvent) => handleInit(e.data));
