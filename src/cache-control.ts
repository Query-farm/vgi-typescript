// Copyright 2025, 2026 Query Farm LLC - https://query.farm

/**
 * Result-cache control metadata (`vgi.cache.*`).
 *
 * A table function can advertise that its result is cacheable by the client
 * (the DuckDB extension) by attaching `vgi.cache.*` metadata to the **first**
 * data batch it emits. The vocabulary mirrors HTTP caching (RFC 9111/9110): a
 * freshness lifetime (`ttl` / `expires`), a reuse `scope`, validators
 * (ETag / Last-Modified) for conditional revalidation, and stale-serving grace
 * windows.
 *
 * The key strings are the single source of truth shared with the C++ extension
 * (which reads them by string). {@link cacheControlMetadata} renders a
 * {@link CacheControl} to the `Map<string, string>` of `vgi.cache.*` keys that
 * rides on batch metadata:
 *
 * ```ts
 * out.emit(firstBatch, cacheControlMetadata({ ttl: 300 }));
 * ```
 *
 * Pass `extra` to merge in metadata a function already emits per batch (for
 * example `vgi_batch_index`); the rendered cache keys win on collision.
 *
 * Booleans render as `"1"` and are omitted when false; timestamps are RFC 3339
 * UTC strings; durations are whole seconds.
 *
 * Ports `vgi/cache_control.py` from vgi-python.
 */

// --- Response-side metadata keys (worker -> client) -------------------------
// Defined once here; the C++ extension reads these exact strings.

export const CACHE_TTL_KEY = "vgi.cache.ttl";
export const CACHE_EXPIRES_KEY = "vgi.cache.expires";
export const CACHE_NO_STORE_KEY = "vgi.cache.no_store";
export const CACHE_SCOPE_KEY = "vgi.cache.scope";
export const CACHE_ETAG_KEY = "vgi.cache.etag";
export const CACHE_LAST_MODIFIED_KEY = "vgi.cache.last_modified";
export const CACHE_REVALIDATABLE_KEY = "vgi.cache.revalidatable";
export const CACHE_STALE_WHILE_REVALIDATE_KEY = "vgi.cache.stale_while_revalidate";
export const CACHE_STALE_IF_ERROR_KEY = "vgi.cache.stale_if_error";
export const CACHE_NOT_MODIFIED_KEY = "vgi.cache.not_modified";
export const CACHE_PARTITION_SCOPE_KEY = "vgi.cache.partition_scope";
export const CACHE_PER_VALUE_KEY = "vgi.cache.per_value";

// --- Request-side metadata keys (client -> worker) --------------------------
// Ride on the first producer tick; surfaced as `TableProcessParams.ifNoneMatch`
// / `.ifModifiedSince`.

export const CACHE_IF_NONE_MATCH_KEY = "vgi.cache.if_none_match";
export const CACHE_IF_MODIFIED_SINCE_KEY = "vgi.cache.if_modified_since";

// --- Reuse-scope values -----------------------------------------------------

export const CACHE_SCOPE_CATALOG = "catalog";
export const CACHE_SCOPE_TRANSACTION = "transaction";

/** Reuse scope for a cacheable result. */
export type CacheScope = typeof CACHE_SCOPE_CATALOG | typeof CACHE_SCOPE_TRANSACTION;

/**
 * Cacheability advertised by a table function on its first result batch.
 *
 * Presence of `ttl` **or** `expires` is what makes a result cacheable;
 * `noStore` overrides any freshness key.
 */
export interface CacheControl {
  /** Freshness lifetime in whole seconds, relative to full-result receipt
   *  (skew-immune; wins over `expires`). */
  ttl?: number;
  /** Absolute RFC 3339 UTC deadline. Lifetime is `expires - now` at receipt. */
  expires?: string;
  /** Reuse scope — `"catalog"` (default; reusable across transactions within
   *  the calling catalog identity) or `"transaction"` (reused only within the
   *  same transaction). */
  scope?: CacheScope;
  /** Explicit "never cache"; overrides any freshness key. */
  noStore?: boolean;
  /** Strong validator (opaque quoted string) for conditional revalidation. */
  etag?: string;
  /** Weaker RFC 3339 UTC validator; fallback when no ETag. */
  lastModified?: string;
  /** The worker can check freshness cheaply without recomputing; gates whether
   *  the client ever sends a conditional request. */
  revalidatable?: boolean;
  /** Grace window (seconds) to serve stale immediately while revalidating in
   *  the background. */
  staleWhileRevalidate?: number;
  /** Grace window (seconds) to serve stale if a revalidation RPC fails. */
  staleIfError?: number;
  /** 304-equivalent — set on a 0-row batch in reply to a conditional request to
   *  assert the client's stored payload is still fresh (the client reuses it
   *  instead of re-streaming). */
  notModified?: boolean;
  /** Opt in to per-PARTITION caching: in addition to the whole-scan entry, the
   *  client stores the result split by distinct partition tuple, so a later
   *  `=`/`IN`-filtered scan on the partition column(s) can serve the requested
   *  partitions without calling the worker. Only meaningful for a
   *  `SINGLE_VALUE_PARTITIONS` table function; purely additive. */
  partitionScope?: boolean;
  /**
   * Opt in to per-VALUE memoization: in addition to the whole-result entry, the
   * client memoizes each distinct worker-input tuple's output keyed on that
   * tuple, so the same value is served without calling the worker on a later
   * chunk or a later query. Only meaningful for an exchange-mode MAP — a
   * scalar, or a blended table-in-out invoked through a correlated `LATERAL`.
   *
   * **Default off, and it should stay off unless one call is genuinely
   * expensive.** This is not a free win: serving a value from the memo costs a
   * key probe, a decode and a per-value assembly step, and that only pays back
   * when it is cheaper than asking you for the answer. For a cheap map —
   * arithmetic, a string tweak, a lookup in a table the worker already has in
   * memory — it is a large net loss (the engine measures a per-value serve at
   * roughly 50x the cost of just calling the worker for a simple arithmetic
   * map). Only the function author knows which side of that line a call falls
   * on, which is why the engine will not guess: turn it on for model
   * inference, geocoding, an external API call, or anything rate-limited or
   * billed per request.
   *
   * Independent of freshness: `ttl`/`expires` make a result cacheable at all,
   * while this decides whether the per-value tier is additionally populated.
   * Requires a deterministic, side-effect-free function, exactly like the rest
   * of the result cache.
   */
  perValue?: boolean;
}

const VALID_SCOPES: readonly string[] = [CACHE_SCOPE_CATALOG, CACHE_SCOPE_TRANSACTION];

/**
 * Render a {@link CacheControl} to the `vgi.cache.*` batch-metadata map.
 *
 * `scope` is always emitted so the client never has to infer the default.
 * Unset optional fields are omitted; false booleans are omitted. Entries from
 * `extra` are written first, so a rendered cache key wins on collision.
 *
 * Throws when `scope` is unrecognized or a duration is negative — a silently
 * ignored advertisement is far harder to debug than a stack trace at the
 * offending `emit()`.
 */
export function cacheControlMetadata(cc: CacheControl, extra?: Map<string, string>): Map<string, string> {
  const scope = cc.scope ?? CACHE_SCOPE_CATALOG;
  if (!VALID_SCOPES.includes(scope)) {
    throw new Error(`CacheControl.scope must be one of ${VALID_SCOPES.join(", ")}, got ${JSON.stringify(scope)}`);
  }
  for (const name of ["ttl", "staleWhileRevalidate", "staleIfError"] as const) {
    const value = cc[name];
    if (value !== undefined && value < 0) {
      throw new Error(`CacheControl.${name} must be >= 0, got ${value}`);
    }
  }

  const md = new Map<string, string>(extra ?? []);
  if (cc.ttl !== undefined) md.set(CACHE_TTL_KEY, String(cc.ttl));
  if (cc.expires !== undefined) md.set(CACHE_EXPIRES_KEY, cc.expires);
  if (cc.noStore) md.set(CACHE_NO_STORE_KEY, "1");
  md.set(CACHE_SCOPE_KEY, scope);
  if (cc.etag !== undefined) md.set(CACHE_ETAG_KEY, cc.etag);
  if (cc.lastModified !== undefined) md.set(CACHE_LAST_MODIFIED_KEY, cc.lastModified);
  if (cc.revalidatable) md.set(CACHE_REVALIDATABLE_KEY, "1");
  if (cc.staleWhileRevalidate !== undefined) md.set(CACHE_STALE_WHILE_REVALIDATE_KEY, String(cc.staleWhileRevalidate));
  if (cc.staleIfError !== undefined) md.set(CACHE_STALE_IF_ERROR_KEY, String(cc.staleIfError));
  if (cc.notModified) md.set(CACHE_NOT_MODIFIED_KEY, "1");
  if (cc.partitionScope) md.set(CACHE_PARTITION_SCOPE_KEY, "1");
  if (cc.perValue) md.set(CACHE_PER_VALUE_KEY, "1");
  return md;
}
