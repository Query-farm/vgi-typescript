// SAB (`worker:`) transport — worker side, in TypeScript.
//
// Bridges the VGI extension's SharedArrayBuffer duplex-ring channel (byte-exact to
// vgi/src/include/vgi_sab_abi.hpp) to vgi-rpc's `serveStream`: each claimed slot's
// client→worker ring (c2w) is presented as a `ReadableStream<Uint8Array>` of request
// bytes, and the worker→client ring (w2c) as a `ByteSink` of response bytes. Unlike
// the Rust worker (emscripten pthreads blocking on Atomics.wait), this runs in ONE Web
// Worker and multiplexes all slots on the event loop via `Atomics.waitAsync` — no
// blocking, real concurrency across slots.
import type { ByteSink } from "@query-farm/vgi-rpc";

// ---- ABI (i32 lane indices) -------------------------------------------------
const HDR_N_SLOTS = 2, HDR_RING_CAP = 3, HDR_SLOT_STRIDE = 4, HDR_SLOTS_OFF = 5;
const STATE = 0, C2W_W = 1, C2W_R = 2, C2W_CL = 3, W2C_W = 4, W2C_R = 5, W2C_CL = 6;
const CTRL_BYTES = 64;

export interface Channel {
  i32: Int32Array;
  u8: Uint8Array;
  base: number; // channel byte offset in the SAB
  nSlots: number;
  ringCap: number;
  slotStride: number;
  slotsOff: number;
}

export function openChannel(buffer: SharedArrayBuffer, offset: number): Channel {
  const i32 = new Int32Array(buffer);
  const h = offset >> 2;
  return {
    i32,
    u8: new Uint8Array(buffer),
    base: offset,
    nSlots: i32[h + HDR_N_SLOTS],
    ringCap: i32[h + HDR_RING_CAP],
    slotStride: i32[h + HDR_SLOT_STRIDE],
    slotsOff: i32[h + HDR_SLOTS_OFF],
  };
}

const slotBase = (ch: Channel, slot: number) => (ch.base + ch.slotsOff + slot * ch.slotStride) >> 2;

// Await a value change at lane `idx` (bounded), so the event loop stays live. Uses
// Atomics.waitAsync where available; falls back to a short poll otherwise.
function waitChange(i32: Int32Array, idx: number, expected: number, ms: number): Promise<void> {
  const anyAtomics = Atomics as unknown as {
    waitAsync?: (a: Int32Array, i: number, v: number, t?: number) => { async: boolean; value: Promise<string> | string };
  };
  if (anyAtomics.waitAsync) {
    const r = anyAtomics.waitAsync(i32, idx, expected, ms);
    return r.async ? (r.value as Promise<string>).then(() => undefined) : Promise.resolve();
  }
  return new Promise((res) => setTimeout(res, Math.min(ms, 8)));
}

// A ReadableStream of the c2w (client→worker) request bytes for one claim. Ends when
// the client closes c2w (C2W_CL==1, drained) OR the slot leaves `served` (reclaimed).
function c2wReadable(ch: Channel, slot: number, served: number): ReadableStream<Uint8Array> {
  const sb = slotBase(ch, slot);
  const dataByte = (sb << 2) + CTRL_BYTES;
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      for (;;) {
        if (Atomics.load(ch.i32, sb + STATE) !== served) { controller.close(); return; }
        const w = Atomics.load(ch.i32, sb + C2W_W);
        const r = Atomics.load(ch.i32, sb + C2W_R);
        const avail = w - r;
        if (avail === 0) {
          if (Atomics.load(ch.i32, sb + C2W_CL) === 1) { controller.close(); return; }
          await waitChange(ch.i32, sb + C2W_W, w, 250);
          continue;
        }
        const pos = r % ch.ringCap;
        const first = Math.min(avail, ch.ringCap - pos);
        const out = new Uint8Array(avail);
        out.set(ch.u8.subarray(dataByte + pos, dataByte + pos + first), 0);
        if (avail > first) out.set(ch.u8.subarray(dataByte, dataByte + avail - first), first);
        Atomics.store(ch.i32, sb + C2W_R, r + avail);
        Atomics.notify(ch.i32, sb + C2W_R);
        controller.enqueue(out);
        return;
      }
    },
  });
}

// A ByteSink writing response bytes into the w2c (worker→client) ring, honoring
// backpressure by awaiting the client's reads. Bails if the slot is reclaimed.
function w2cSink(ch: Channel, slot: number, served: number): ByteSink {
  const sb = slotBase(ch, slot);
  const dataByte = (sb << 2) + CTRL_BYTES + ch.ringCap;
  return {
    async write(bytes: Uint8Array) {
      let off = 0;
      while (off < bytes.length) {
        if (Atomics.load(ch.i32, sb + STATE) !== served) return; // reclaimed -> abort
        const w = Atomics.load(ch.i32, sb + W2C_W);
        const r = Atomics.load(ch.i32, sb + W2C_R);
        const free = ch.ringCap - (w - r);
        if (free === 0) { await waitChange(ch.i32, sb + W2C_R, r, 250); continue; }
        const k = Math.min(free, bytes.length - off);
        const pos = w % ch.ringCap;
        const first = Math.min(k, ch.ringCap - pos);
        ch.u8.set(bytes.subarray(off, off + first), dataByte + pos);
        if (k > first) ch.u8.set(bytes.subarray(off + first, off + k), dataByte);
        Atomics.store(ch.i32, sb + W2C_W, w + k);
        Atomics.notify(ch.i32, sb + W2C_W);
        off += k;
      }
    },
  };
}

export type ServeFn = (readable: ReadableStream<Uint8Array>, writable: ByteSink) => Promise<void>;

// One slot's dispatcher loop: wait for a claim, serve one connection over c2w/w2c,
// close w2c with the claim-id TOKEN (== our STATE, so a stale close after reclaim is
// ignored by the client), wait for release, repeat. Async — never blocks the worker.
async function serveSlotForever(ch: Channel, slot: number, serve: ServeFn): Promise<void> {
  const sb = slotBase(ch, slot);
  for (;;) {
    // await_slot: STATE 0 -> claim id
    let served = Atomics.load(ch.i32, sb + STATE);
    while (served === 0) {
      await waitChange(ch.i32, sb + STATE, 0, 500);
      served = Atomics.load(ch.i32, sb + STATE);
    }
    try {
      await serve(c2wReadable(ch, slot, served), w2cSink(ch, slot, served));
    } catch {
      /* error already delivered in-band as an error batch by the serve */
    }
    // Close w2c with the claim-id token + wake the client reading w2c.
    if (Atomics.load(ch.i32, sb + STATE) === served) {
      Atomics.store(ch.i32, sb + W2C_CL, served);
      Atomics.notify(ch.i32, sb + W2C_W);
    }
    // await_release: STATE leaves `served`
    while (Atomics.load(ch.i32, sb + STATE) === served) {
      await waitChange(ch.i32, sb + STATE, served, 500);
    }
  }
}

// Serve every slot concurrently on this worker's event loop.
export function serveChannel(ch: Channel, serve: ServeFn): void {
  for (let s = 0; s < ch.nSlots; s++) void serveSlotForever(ch, s, serve);
}
