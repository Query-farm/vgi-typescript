// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// The value of an HTTP stream's `init` StateRef (vgi docs/large-values-3.4.md §6).
//
// An HTTP stream has no memory between turns, so its cursor has to carry what
// the init turn knew: the init request as received (bind call, output schema,
// filters, join keys, split tokens, opaque data) and the worker's init response.
// Both are immutable for the life of the stream, so they ride as ONE vgi-rpc
// StateRef field, `init`, created with `ctx.stateRef()` in the init handler and
// copied unchanged by every later turn:
//
// - small inits (the common case) are inline: vgi-rpc compresses the payload
//   inside the ref, which is why this SDK no longer packs (zstd) the init
//   request itself;
// - a large init (a 1000-value IN list) is stored once at /init and costs a
//   ~130-byte sealed tag per turn after that.
//
// With argument references the init request holds the reference, never the
// argument, so it normally stays inline even for a very large argument.
//
// There is deliberately no per-process parse memo (spec decision 3): an inline
// init is decompressed and parsed on every turn, which measured as immaterial.
// The 128-entry LRU this SDK used to keep pinned parsed requests, and with them
// resolved arguments of any size.

/** Version byte of the value; a different build's frame is a clean refusal, not a misread. */
const STREAM_INIT_VERSION = 1;
const FLAG_OPAQUE_DATA = 1;

/** What an HTTP stream's `init` StateRef holds. */
export interface StreamInit {
  /** The init request's IPC bytes exactly as received (the `request` column of the init call). */
  initRequestIpc: Uint8Array;
  /** The worker's init response. */
  executionId: Uint8Array;
  maxWorkers: number;
  opaqueData: Uint8Array | null;
}

/**
 * Encode the `init` value: `u8 version | u8 flags | f64 max_workers |
 * bytes init_request | bytes execution_id | [bytes opaque_data]`, `bytes`
 * being `u32 length (LE), payload`.
 */
export function encodeStreamInit(init: StreamInit): Uint8Array {
  const chunks = [init.initRequestIpc, init.executionId];
  if (init.opaqueData != null) chunks.push(init.opaqueData);
  let size = 2 + 8;
  for (const chunk of chunks) size += 4 + chunk.byteLength;
  const out = new Uint8Array(size);
  const view = new DataView(out.buffer);
  out[0] = STREAM_INIT_VERSION;
  out[1] = init.opaqueData != null ? FLAG_OPAQUE_DATA : 0;
  view.setFloat64(2, Number(init.maxWorkers ?? 1), true);
  let offset = 10;
  for (const chunk of chunks) {
    view.setUint32(offset, chunk.byteLength, true);
    out.set(chunk, offset + 4);
    offset += 4 + chunk.byteLength;
  }
  return out;
}

/** Decode an `init` value written by {@link encodeStreamInit}. */
export function decodeStreamInit(bytes: Uint8Array): StreamInit {
  if (bytes.byteLength < 10 || bytes[0] !== STREAM_INIT_VERSION) {
    throw new Error("stream init state has an unknown format (minted by another build of this worker?)");
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const flags = bytes[1]!;
  const maxWorkers = view.getFloat64(2, true);
  let offset = 10;
  const take = (): Uint8Array => {
    if (offset + 4 > bytes.byteLength) throw new Error("stream init state is truncated");
    const length = view.getUint32(offset, true);
    offset += 4;
    if (offset + length > bytes.byteLength) throw new Error("stream init state is truncated");
    // Copied, not viewed: Arrow readers want a buffer of their own.
    const chunk = bytes.slice(offset, offset + length);
    offset += length;
    return chunk;
  };
  const initRequestIpc = take();
  const executionId = take();
  const opaqueData = flags & FLAG_OPAQUE_DATA ? take() : null;
  if (offset !== bytes.byteLength) throw new Error("stream init state has trailing bytes");
  return { initRequestIpc, executionId, maxWorkers, opaqueData };
}
