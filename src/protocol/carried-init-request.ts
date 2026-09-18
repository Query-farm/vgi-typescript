// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// The init request an HTTP stream carries in its cursor: packed once, parsed
// once per process.
//
// An HTTP stream has no memory between turns, so its cursor carries the whole
// init request (~9 KB of Arrow IPC for a DuckDB scan: the bind call with its
// arguments, settings and sealed attach, the output schema, the filter
// snapshot) and every turn used to re-parse it: the IPC read, the scalar-dict
// walk and the nested bind-call / schema / filter reads cost ~0.9 ms, the
// largest VGI-side share of the ~4 ms a 1000-row turn took. The cursor is also
// re-sealed and base64-encoded on every turn, so its size is paid twice per
// turn in AEAD and base64 work, and twice again on the wire.
//
// - `packInitRequest` zstd-compresses the request once, at /init (~9 KB ->
//   ~2.6 KB), behind a one-byte codec tag. A runtime without synchronous zstd
//   stores it raw.
// - `parseCarriedInitRequest` keeps a small per-process LRU from the packed
//   bytes to their parse. It is a pure memo of a pure function: the key is the
//   exact bytes (hash, then a full comparison), so a hit returns precisely
//   what parsing those bytes would -- and those bytes came out of a cursor the
//   worker sealed. Nothing principal-dependent is cached: the split tokens and
//   the sealed attach are still opened, under the caller, on every turn. A
//   miss (cold process, eviction, another replica's stream) parses as before,
//   so correctness never depends on a hit.

import { deserializeBatch } from "../arrow/index.js";
import { batchToScalarDict } from "../util/arrow/index.js";
import { zstdSync } from "../util/zstd-sync.js";
import { deserializeInitRequest } from "./serialize.js";
import type { InitRequest } from "./types.js";

const RAW = 0;
const ZSTD = 1;

/** Pack an init request's IPC bytes for the cursor: `u8 codec | payload`. */
export function packInitRequest(ipc: Uint8Array): Uint8Array {
  const codec = zstdSync();
  const compressed = codec ? codec.compress(ipc) : null;
  const useZstd = compressed !== null && compressed.byteLength < ipc.byteLength;
  const payload = useZstd ? compressed! : ipc;
  const packed = new Uint8Array(payload.byteLength + 1);
  packed[0] = useZstd ? ZSTD : RAW;
  packed.set(payload, 1);
  return packed;
}

function unpackInitRequest(packed: Uint8Array): Uint8Array {
  if (packed.byteLength === 0) throw new Error("state token carries no init request");
  switch (packed[0]) {
    case RAW:
      return packed.slice(1);
    case ZSTD: {
      const codec = zstdSync();
      if (!codec) {
        throw new Error(
          "state token carries a zstd-compressed init request, and this runtime has no synchronous zstd " +
            "(it was minted by a worker running under Bun or Node >= 22.15 with the same signing key)",
        );
      }
      return codec.decompress(packed.subarray(1));
    }
    default:
      throw new Error(`state token carries an init request with unknown codec ${packed[0]}`);
  }
}

/** What a continuation needs from the carried init request. */
export interface CarriedInitRequest {
  /**
   * The parsed request -- a fresh top-level object with a fresh `bind_call`
   * each call, so the per-turn assignments the dispatcher makes (split
   * payloads) never reach the shared parse. The nested values (Arrow
   * schemas and batches, arguments) are shared and must be treated as
   * read-only, as they always were.
   */
  request: InitRequest;
  /** The raw bind-call IPC bytes, which the split-token fingerprint hashes. */
  bindCallIpc: Uint8Array;
}

interface Parsed {
  packed: Uint8Array;
  request: InitRequest;
  bindCallIpc: Uint8Array;
}

/** Streams whose parse a process keeps. Past this, the least recently used is dropped. */
const MAX_ENTRIES = 128;
const parsed = new Map<number, Parsed>();
const stats = { parses: 0, hits: 0 };

/** FNV-1a over the bytes, folded with the length. */
function hashBytes(bytes: Uint8Array): number {
  let h = 0x811c9dc5 ^ bytes.byteLength;
  for (let i = 0; i < bytes.byteLength; i++) {
    h ^= bytes[i];
    h = Math.imul(h, 0x01000193);
  }
  return h >>> 0;
}

function sameBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.byteLength !== b.byteLength) return false;
  for (let i = 0; i < a.byteLength; i++) if (a[i] !== b[i]) return false;
  return true;
}

/**
 * Parse the init request a cursor carries (as packed by
 * {@link packInitRequest}), from the process memo when this stream's turns have
 * been here before.
 */
export function parseCarriedInitRequest(packed: Uint8Array): CarriedInitRequest {
  const key = hashBytes(packed);
  let entry = parsed.get(key);
  if (entry && sameBytes(entry.packed, packed)) {
    // Refresh its recency.
    parsed.delete(key);
    parsed.set(key, entry);
    stats.hits++;
  } else {
    const dict = batchToScalarDict(deserializeBatch(unpackInitRequest(packed)));
    stats.parses++;
    entry = { packed: packed.slice(), request: deserializeInitRequest(dict), bindCallIpc: dict.bind_call };
    // A hash collision simply replaces the other stream's entry.
    parsed.delete(key);
    if (parsed.size >= MAX_ENTRIES) parsed.delete(parsed.keys().next().value!);
    parsed.set(key, entry);
  }
  return {
    request: { ...entry.request, bind_call: { ...entry.request.bind_call } },
    bindCallIpc: entry.bindCallIpc,
  };
}

/** Cumulative parse / memo-hit counts for this process (observability and tests). */
export function carriedInitRequestStats(): { parses: number; hits: number; entries: number } {
  return { ...stats, entries: parsed.size };
}
