// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Catalog items that are built once and encoded once.
//
// A function listing from a descriptor-driven catalog depends only on static
// metadata, yet every catalog_schema_contents_functions request used to rebuild
// each FunctionInfo (argument and output schemas serialized to IPC, metadata
// resolved) and then Arrow-encode it again: ~1.5 ms per function, ~236 ms for
// the example worker's main schema, paid on every function-set load DuckDB
// makes. vgi-python fixed the same cost by caching the listing and storing each
// item's encoding on its (frozen dataclass) instance (6cc522a).
//
// JS objects are not frozen by default, and an encoding memo keyed by object
// identity is only sound for an object that cannot change after it was
// encoded. So the memo applies to FROZEN items only: a catalog opts in by
// returning items it froze with `freezeCatalogItem` (ReadOnlyCatalogInterface
// does), and every other item -- a catalog that builds fresh objects per call,
// or keeps and mutates its own -- is encoded on every call exactly as before.

import { encodeFunctionInfo, type FunctionInfo } from "../generated/vgi-client.js";

/** Encoded bytes per frozen item instance. Weak, so it never pins an item. */
const ENCODED = new WeakMap<object, Uint8Array>();

/**
 * Deep-freeze a catalog item (plain objects and arrays, recursively) so it can
 * be shared between requests and its encoding memoized.
 *
 * Binary fields (`Uint8Array` and other ArrayBuffer views) are left as they are
 * -- a typed array with elements cannot be frozen -- and must be treated as
 * read-only by convention, like the rest of a shared item.
 */
export function freezeCatalogItem<T extends object>(item: T): Readonly<T> {
  deepFreeze(item);
  return item;
}

function deepFreeze(value: unknown): void {
  if (value === null || typeof value !== "object" || Object.isFrozen(value) || ArrayBuffer.isView(value)) return;
  const proto = Object.getPrototypeOf(value);
  if (proto !== Object.prototype && proto !== Array.prototype && proto !== null) return;
  for (const child of Object.values(value)) deepFreeze(child);
  Object.freeze(value);
}

/**
 * Arrow-encode a FunctionInfo, once per frozen instance.
 *
 * The bytes returned for a frozen item are shared between calls; callers copy
 * them into a response and never write to them.
 */
export function encodeFunctionInfoOnce(info: FunctionInfo): Uint8Array {
  if (!Object.isFrozen(info)) return encodeFunctionInfo(info);
  let encoded = ENCODED.get(info);
  if (encoded === undefined) {
    encoded = encodeFunctionInfo(info);
    ENCODED.set(info, encoded);
  }
  return encoded;
}
