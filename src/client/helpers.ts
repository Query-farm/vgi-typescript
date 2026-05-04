// Internal helpers shared by VgiClient methods.

import { toUint8Array } from "../util/bytes.js";

/**
 * Deserialize a List<Binary> of serialized info batches into typed info objects.
 */
export function deserializeInfoList<T>(
  items: unknown,
  deserializeFn: (bytes: Uint8Array) => T,
): T[] {
  if (!items) return [];
  const arr: unknown[] = Array.isArray(items)
    ? items
    : [...(items as Iterable<unknown>)];
  return arr
    .filter((b) => b != null)
    .map((b) => deserializeFn(toUint8Array(b)));
}

/**
 * Deserialize Arrow Map entries into a plain Record<string, string>.
 */
export function deserializeTags(mapVal: unknown): Record<string, string> {
  const tags: Record<string, string> = {};
  if (!mapVal) return tags;
  const iterable = mapVal as { [Symbol.iterator]?: () => Iterator<unknown> };
  if (typeof iterable[Symbol.iterator] !== "function") return tags;
  for (const entry of iterable as Iterable<unknown>) {
    if (Array.isArray(entry)) {
      tags[String(entry[0])] = String(entry[1]);
    } else if (entry && typeof entry === "object") {
      const e = entry as Record<string, unknown> & { [0]?: unknown; [1]?: unknown };
      tags[String(e.key ?? e[0] ?? "")] = String(e.value ?? e[1] ?? "");
    }
  }
  return tags;
}

/**
 * Convert sync/async iterable to async iterator.
 */
export function toAsyncIterator<T>(
  input: Iterable<T> | AsyncIterable<T>,
): AsyncIterator<T> {
  if (Symbol.asyncIterator in (input as any)) {
    return (input as AsyncIterable<T>)[Symbol.asyncIterator]();
  }
  const syncIter = (input as Iterable<T>)[Symbol.iterator]();
  return {
    async next() {
      return syncIter.next();
    },
  };
}
