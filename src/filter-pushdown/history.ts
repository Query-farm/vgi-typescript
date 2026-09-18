// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Dynamic-filter state across HTTP turns: a compacted delta history.
//
// A table stream's filters can change mid-scan. DuckDB's Top-N operator (and
// friends) tightens a bound as rows arrive and ships the change as a v2 *delta*
// on the next tick's `vgi_pushdown_filters` metadata -- and only then: the
// extension sends each delta ONCE per stream, when a filter's value changes, not
// on every tick. A byte-stream transport keeps the parsed filters in memory
// between ticks. An HTTP stream cannot: every turn rebuilds its handlers from the
// state token, so whatever a delta changed has to ride that token or it is lost
// on the next turn, and the worker quietly falls back to the init snapshot.
//
// Carrying every delta ever received would be the opposite bug: the token grows
// by a delta per tick and each turn replays all of them, quadratic in the tick
// count (vgi-python 0283898). So the token carries a COMPACTED history -- for
// each (id, revision) of the live state, tombstones included, the first delta
// that carried it. Replaying just those reproduces the same predicates, values
// and revisions: an earlier update to an id is overwritten by its current
// revision, and a later one was stale when it arrived and is stale again on
// replay. The history is therefore bounded by the number of predicate ids, not
// the number of ticks. Replay cannot always reproduce predicate ORDER (an id
// removed and later re-added moves to the end), so the live order is recorded
// alongside and restored after replay.

import { type VgiBatch, deserializeBatch, readCanonicalValue } from "../arrow/index.js";
import { FilterV2Error } from "./deserialize.js";
import type { PushdownFilters } from "./evaluate.js";

/** Encoded-size limit on the base64 `vgi_pushdown_filters` value. */
const MAX_ENCODED_DELTA_BYTES = 24 << 20;
/** Decoded-size limit on one delta's Arrow IPC payload. */
const MAX_DELTA_IPC_BYTES = 17 << 20;
const BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

/**
 * What an HTTP stream carries between turns to rebuild its dynamic filters.
 *
 * Both halves are plain data so the state serializer can put them in the
 * cursor, and both are bounded by the number of predicate ids the scan has
 * seen (see {@link recordFilterDelta}).
 */
export interface FilterDeltaHistory {
  /** Arrow IPC bytes of each delta still needed, in arrival order. */
  readonly deltas: readonly Uint8Array[];
  /** The live predicate ids, in the order the stream had them. */
  readonly order: readonly string[];
}

/** A stream that has applied no delta. */
export const EMPTY_FILTER_HISTORY: FilterDeltaHistory = Object.freeze({
  deltas: Object.freeze([]) as readonly Uint8Array[],
  order: Object.freeze([]) as readonly string[],
});

/**
 * Decode the base64 `vgi_pushdown_filters` metadata value into delta IPC bytes.
 *
 * Validates the encoding strictly and enforces the encoded and decoded size
 * limits before anything is parsed.
 */
export function decodeDynamicFilterMetadata(encoded: string): Uint8Array {
  if (new TextEncoder().encode(encoded).byteLength > MAX_ENCODED_DELTA_BYTES) {
    throw new Error("base64 dynamic filter metadata exceeds the encoded-size limit");
  }
  if (encoded.length % 4 !== 0 || !BASE64.test(encoded)) {
    throw new Error("dynamic filter metadata is not valid base64");
  }
  const binary = (globalThis as any).atob
    ? (globalThis as any).atob(encoded)
    : Buffer.from(encoded, "base64").toString("binary");
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
  if (bytes.byteLength > MAX_DELTA_IPC_BYTES) {
    throw new Error("dynamic filter IPC payload exceeds the encoded-size limit");
  }
  return bytes;
}

/** Read one delta's IPC bytes into its single document batch. */
export function decodeFilterDelta(bytes: Uint8Array): VgiBatch {
  if (bytes.byteLength > MAX_DELTA_IPC_BYTES) {
    throw new Error("dynamic filter IPC payload exceeds the encoded-size limit");
  }
  const batch = bytes.byteLength > 0 ? deserializeBatch(bytes) : null;
  if (!batch || (batch.numRows === 0 && batch.schema.fields.length === 0)) {
    throw new Error("dynamic filter metadata must contain one RecordBatch");
  }
  return batch as VgiBatch;
}

/**
 * The `(id, revision)` of every update a delta document carries, as
 * `id\0revision` keys.
 *
 * A structural read only, for bookkeeping over deltas that have already been
 * applied -- and therefore validated -- by {@link PushdownFilters.applyDelta}.
 * It neither applies nor validates anything.
 */
function deltaRevisionKeys(batch: VgiBatch): string[] {
  const column = batch.getChildAt(0);
  if (!column) throw new FilterV2Error("delta document has no filter_spec column");
  const raw = readCanonicalValue(batch.schema.fields[0].type, column, 0);
  const document = JSON.parse(String(raw)) as { updates?: Array<{ id: string; revision: number }> };
  return (document.updates ?? []).map((update) => revisionKey(update.id, update.revision));
}

function revisionKey(id: string, revision: number): string {
  return `${id}\u0000${revision}`;
}

/**
 * Apply one tick's delta and return the new filters plus the history a cursor
 * must carry to rebuild them.
 *
 * The history is compacted to the deltas that installed some predicate's
 * CURRENT revision, tombstones included: for each `(id, revision)` of the live
 * state, the first delta (in arrival order) carrying it. That delta is the one
 * that was applied -- any later delta carrying the same revision was stale on
 * arrival -- and a delta none of whose updates is current can never matter
 * again, because revisions only move forward.
 *
 * @param current - The stream's filters before this delta.
 * @param history - The compacted history those filters were rebuilt from.
 * @param deltaBytes - The IPC bytes of the delta this tick carried.
 */
export function recordFilterDelta(
  current: PushdownFilters,
  history: FilterDeltaHistory,
  deltaBytes: Uint8Array,
): { filters: PushdownFilters; history: FilterDeltaHistory } {
  const incoming = decodeFilterDelta(deltaBytes);
  // Assignment happens only after complete validation, so a rejected delta
  // leaves the caller's prior state (and history) intact.
  const filters = current.applyDelta(incoming);
  const wanted = new Set<string>();
  for (const [id, revision] of filters.revisions) wanted.add(revisionKey(id, revision));
  const deltas: Uint8Array[] = [];
  const consider = (bytes: Uint8Array, batch: VgiBatch): void => {
    let carries = false;
    for (const key of deltaRevisionKeys(batch)) {
      if (wanted.delete(key)) carries = true;
    }
    if (carries) deltas.push(bytes);
  };
  for (const bytes of history.deltas) consider(bytes, decodeFilterDelta(bytes));
  consider(deltaBytes, incoming);
  return { filters, history: { deltas, order: filters.predicates.map((predicate) => predicate.id) } };
}

const HISTORY_FORMAT_VERSION = 1;
const UTF8_ENCODER = new TextEncoder();
const UTF8_DECODER = new TextDecoder("utf-8", { fatal: true });

/**
 * Encode a history for the state token, or `null` when there is nothing to
 * carry (no delta applied, which is every stream without dynamic filters).
 *
 * Layout (little-endian): `u8 version | u32 n | n x (u32 len, delta IPC) |
 * u32 m | m x (u32 len, utf8 id)`.
 */
export function encodeFilterHistory(history: FilterDeltaHistory | null | undefined): Uint8Array | null {
  if (!history || history.deltas.length === 0) return null;
  const ids = history.order.map((id) => UTF8_ENCODER.encode(id));
  let size = 1 + 4 + 4;
  for (const delta of history.deltas) size += 4 + delta.byteLength;
  for (const id of ids) size += 4 + id.byteLength;
  const out = new Uint8Array(size);
  const view = new DataView(out.buffer);
  let offset = 0;
  out[offset++] = HISTORY_FORMAT_VERSION;
  const put = (chunks: readonly Uint8Array[]): void => {
    view.setUint32(offset, chunks.length, true);
    offset += 4;
    for (const chunk of chunks) {
      view.setUint32(offset, chunk.byteLength, true);
      offset += 4;
      out.set(chunk, offset);
      offset += chunk.byteLength;
    }
  };
  put(history.deltas);
  put(ids);
  return out;
}

/** Decode {@link encodeFilterHistory}'s bytes; `null`/empty is the empty history. */
export function decodeFilterHistory(bytes: Uint8Array | null | undefined): FilterDeltaHistory {
  if (!bytes || bytes.byteLength === 0) return EMPTY_FILTER_HISTORY;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  let offset = 0;
  const need = (n: number): void => {
    if (offset + n > bytes.byteLength) throw new Error("dynamic filter history in the state token is truncated");
  };
  need(1);
  if (bytes[offset++] !== HISTORY_FORMAT_VERSION) {
    throw new Error("dynamic filter history in the state token has an unknown format");
  }
  const take = (): Uint8Array[] => {
    need(4);
    const count = view.getUint32(offset, true);
    offset += 4;
    const chunks: Uint8Array[] = [];
    for (let i = 0; i < count; i++) {
      need(4);
      const length = view.getUint32(offset, true);
      offset += 4;
      need(length);
      // A copy, not a view: Arrow readers want their own aligned buffer.
      chunks.push(bytes.slice(offset, offset + length));
      offset += length;
    }
    return chunks;
  };
  const deltas = take();
  const order = take().map((id) => UTF8_DECODER.decode(id));
  if (offset !== bytes.byteLength) throw new Error("dynamic filter history in the state token has trailing bytes");
  return { deltas, order };
}

/**
 * Rebuild a turn's filters from the init snapshot plus a carried history.
 *
 * Every delta here was validated when it first arrived and has ridden a
 * worker-sealed token since, but it is re-applied through the same strict
 * parser -- a TS delta parse is structural, with no bind to skip.
 *
 * @param filters - The filters parsed from the init request's snapshot.
 * @param history - The history {@link recordFilterDelta} produced.
 */
export function replayFilterHistory(
  filters: PushdownFilters | undefined,
  history: FilterDeltaHistory | null | undefined,
): PushdownFilters | undefined {
  if (!history || history.deltas.length === 0) return filters;
  if (!filters) throw new Error("dynamic filter history carried without an initial snapshot");
  let replayed = filters;
  for (const bytes of history.deltas) replayed = replayed.applyDelta(decodeFilterDelta(bytes));
  return replayed.withPredicateOrder(history.order);
}
