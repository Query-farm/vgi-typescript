// TypeScript VGI worker over the in-browser SAB (`worker:`) transport — TRULY PARALLEL.
//
// The extension bridge spawns this once (the BOOT role). Instead of multiplexing all
// slots on one event loop, the boot worker spawns ONE dedicated sub-Worker per channel
// slot (the SLOT role) — real OS threads sharing the SAB — so N slots are served in
// genuine parallel, matching the Rust worker's emscripten thread-per-slot. Roles are
// distinguished by the init message type; both run this same bundle.
//
// NB: import from `index.core.js` (browser-safe) — the top-level `Worker` class pulls
// in Node-only serveUnix/serveTcp.
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
import { openChannel, serveSlotForever } from "./sab.js";

const N_SCHEMA = new Schema([new Field("value", new Int64(), true)]);

// Cross-sub-Worker concurrency counter (its own SharedArrayBuffer, handed to every SLOT
// worker): i32[0] = # of ts_probe serves active right now, i32[1] = peak ever seen.
let probe: Int32Array | undefined;

interface CountArgs { n: number }
interface CountState { i: number; n: number }

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

// ts_probe(busy_ms): declares maxWorkers=4 so a SINGLE scan fans out across DuckDB scan
// threads → N `worker:` connections → N slots → N SLOT sub-Workers. Each produce holds a
// shared concurrency guard while CPU-BUSY-LOOPING busy_ms. A busy loop (not setTimeout)
// is the load-bearing detail: on a single async event loop it blocks, so only one probe
// runs at a time (peak=1); N real sub-Worker threads run them at once (peak=N). ts_peek
// reads the peak. This distinguishes true parallelism from async multiplexing.
interface ProbeArgs { busy_ms: number }
const tsProbe = defineTableFunction<ProbeArgs, { done: boolean }>({
  name: "ts_probe",
  description: "Parallel-serve probe: busy-loop under a shared concurrency guard",
  args: { busy_ms: new Int64() },
  maxWorkers: 4,
  onBind: (_p: TableBindParams<ProbeArgs>) => ({ outputSchema: N_SCHEMA }),
  initialState: () => ({ done: false }),
  process: (p: TableProcessParams<ProbeArgs>, state: { done: boolean }, out: OutputCollector) => {
    if (state.done) { out.finish(); return; }
    state.done = true;
    let peak = 1;
    if (probe) {
      const cur = Atomics.add(probe, 0, 1) + 1; // enter
      let m = Atomics.load(probe, 1);
      while (cur > m) { const prev = Atomics.compareExchange(probe, 1, m, cur); if (prev === m) { m = cur; break; } m = prev; }
      const end = Date.now() + Math.max(0, Number(p.args.busy_ms));
      while (Date.now() < end) { /* CPU busy — blocks this thread's event loop */ }
      Atomics.sub(probe, 0, 1); // leave
      peak = Atomics.load(probe, 1);
    }
    out.emit(batchFromColumns({ value: [BigInt(peak)] }, p.outputSchema));
  },
});

const tsPeek = defineTableFunction<Record<string, never>, { done: boolean }>({
  name: "ts_peek",
  description: "Read back the peak simultaneous ts_probe serves",
  args: {},
  onBind: () => ({ outputSchema: N_SCHEMA }),
  initialState: () => ({ done: false }),
  process: (p: TableProcessParams<Record<string, never>>, state: { done: boolean }, out: OutputCollector) => {
    if (state.done) { out.finish(); return; }
    state.done = true;
    out.emit(batchFromColumns({ value: [BigInt(probe ? Atomics.load(probe, 1) : 0)] }, p.outputSchema));
  },
});

// Build the protocol once (each SLOT worker builds its own instance).
const registry = new FunctionRegistry();
for (const f of [tsCount, tsDouble, tsProbe, tsPeek]) registry.register(f);
const catalog = { name: "main", schemas: [{ name: "main", functions: [tsCount, tsDouble, tsProbe, tsPeek] }] };
const catalogInterface = new ReadOnlyCatalogInterface(catalog, registry);
const protocol = buildVgiProtocol({ registry, catalogInterface, catalogName: "main" });

const post = (m: unknown) => (self as unknown as { postMessage: (x: unknown) => void }).postMessage(m);

function handle(d: {
  type?: string;
  buffer?: SharedArrayBuffer;
  offset?: number;
  slot?: number;
  probeSab?: SharedArrayBuffer;
}) {
  if (!d) return;
  // BOOT role: the bridge sent the channel. Spawn one SLOT sub-Worker per slot (real
  // parallel threads sharing the SAB), then ack readiness to the bridge.
  if (d.type === "vgi-init") {
    try {
      const ch = openChannel(d.buffer as SharedArrayBuffer, d.offset as number);
      const probeSab = new SharedArrayBuffer(8); // [active, peak]
      for (let s = 0; s < ch.nSlots; s++) {
        const w = new Worker("ts-worker-boot.js");
        w.postMessage({ type: "vgi-slot-init", buffer: d.buffer, offset: d.offset, slot: s, probeSab });
      }
      post({ type: "vgi-ready" });
    } catch (err) {
      post({ type: "vgi-error", error: String((err as Error)?.message ?? err) });
    }
    return;
  }
  // SLOT role: serve exactly one slot forever (this whole Worker is dedicated to it).
  if (d.type === "vgi-slot-init") {
    try {
      probe = new Int32Array(d.probeSab as SharedArrayBuffer);
      const ch = openChannel(d.buffer as SharedArrayBuffer, d.offset as number);
      void serveSlotForever(ch, d.slot as number, (readable, writable) => serveStream(protocol, { readable, writable }));
    } catch (err) {
      post({ type: "vgi-error", error: String((err as Error)?.message ?? err) });
    }
  }
}

// The classic shim buffers messages that arrived before this module imported; drain, then
// handle future ones (see ts-worker-boot.js).
const buffered = (globalThis as unknown as { __vgiBuffered?: unknown[] }).__vgiBuffered;
if (Array.isArray(buffered)) for (const d of buffered) handle(d as { type?: string });
self.addEventListener("message", (e: MessageEvent) => handle(e.data));
