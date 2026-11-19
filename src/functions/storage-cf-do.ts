// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Cloudflare Durable Object storage for VGI function state.
//
// Port of vgi-python's vgi/function_storage_cf_do.py. Talks the Cloudflare
// Worker + Durable Object's unified state_* / queue_* protocol
// (vgi-cloudflare-durable-object-storage/src/index.ts). Every request carries
// the per-attach `shard_key` (BoundStorage calls forShard with the shard the
// framework derived from the request's attach; "" goes to `loc-anon`);
// destructive ops carry a fresh 32-hex `attempt_id`, reused across retries. The DO is single-threaded SQLite, so ops are atomic.
//
// Usage:
//   Set `VGI_WORKER_SHARED_STORAGE=cloudflare-do` plus `VGI_CF_DO_URL`.
//   Optionally set `VGI_CF_DO_TOKEN` for bearer auth.

import type { FunctionStorage, WorkerStateKey } from "./storage.js";
import { UnknownInvocationError, workerStateKey } from "./storage.js";

/** Minimal fetch signature satisfied by both the global `fetch` and a
 *  Cloudflare service-binding Fetcher (`env.<BINDING>`). */
export type FetchLike = (
  input: string,
  init?: { method?: string; headers?: Record<string, string>; body?: string; signal?: AbortSignal },
) => Promise<Response>;

/**
 * The storage service's per-value cap (`MAX_VALUE_BYTES` in its src/index.ts).
 * `resolveStorageFromEnv` / `setStorage` wrap this backend in
 * `ExternalizingStorage` with this cap, which envelopes every value and moves
 * large ones to the blob vendor.
 */
export const MAX_VALUE_BYTES = 1024 * 1024;

/** The DO server's shard_key rule (vgi-cloudflare-durable-object-storage). */
export const SHARD_KEY_RE = /^[A-Za-z0-9._-]{1,128}$/;

/**
 * Shard for a call not bound to an ATTACH (no attach uuid to shard on): one
 * shared DO per tenant, as vgi-python's client does. Every attach-bound call
 * carries its own `att-<uuid hex>` shard instead.
 */
export const ANONYMOUS_SHARD_KEY = "loc-anon";

/** Retry policy for the DO client. */
export interface CfDoRetryOptions {
  /** Per-attempt fetch timeout, ms (default 30000; 0 disables). */
  timeoutMs?: number;
  /** Total attempts including the first (default 4). */
  maxAttempts?: number;
  /** First backoff, ms (default 100); doubles per attempt, full jitter. */
  baseDelayMs?: number;
  /** Backoff cap, ms (default 2000). */
  maxDelayMs?: number;
}

/** A transient failure worth retrying: transport error, timeout, 5xx or 429. */
class RetryableError extends Error {
  constructor(message: string, readonly retryAfterMs: number | null = null) {
    super(message);
  }
}

// Namespaces under the unified state_* table for the legacy worker/queue
// families the DO no longer has dedicated endpoints for.
const NS_WORKER = new TextEncoder().encode("worker");

export class FunctionStorageCfDo implements FunctionStorage {
  /** Marks this backend for `setStorage`, which always envelopes it. */
  readonly storageKind = "cloudflare-do";
  /** The service's per-value cap ({@link MAX_VALUE_BYTES}). */
  readonly maxValueBytes = MAX_VALUE_BYTES;
  private readonly _baseUrl: string;
  private readonly _token: string | null;
  /** Per-attach routing key (att-<hex uuid>); "" until pinned via forShard. */
  private readonly _shardKey: string;
  /** Fetch implementation. Defaults to the global `fetch`; pass a Cloudflare
   *  service-binding Fetcher (env.<BINDING>) when the storage worker lives on
   *  the same account/zone — a same-zone public-URL fetch is rejected (CF
   *  error 1042), but a service binding routes worker-to-worker directly. */
  private readonly _fetch: FetchLike;
  private readonly _retry: Required<CfDoRetryOptions>;
  /** Test hook: how the client waits between attempts. */
  private readonly _sleep: (ms: number) => Promise<void>;

  constructor(opts: {
    url: string;
    token?: string | null;
    shardKey?: string;
    fetch?: FetchLike;
    retry?: CfDoRetryOptions;
    sleep?: (ms: number) => Promise<void>;
  }) {
    // Strip trailing slash so endpoint paths can be appended unconditionally.
    this._baseUrl = opts.url.replace(/\/+$/, "");
    this._token = opts.token ?? null;
    const shard = opts.shardKey ?? "";
    if (shard !== "" && !SHARD_KEY_RE.test(shard)) {
      throw new Error(`invalid CF DO shard_key ${JSON.stringify(shard)} (1-128 chars, [A-Za-z0-9._-])`);
    }
    this._shardKey = shard;
    this._fetch = opts.fetch ?? ((input, init) => fetch(input as any, init));
    this._retry = {
      timeoutMs: opts.retry?.timeoutMs ?? 30_000,
      maxAttempts: Math.max(1, opts.retry?.maxAttempts ?? 4),
      baseDelayMs: opts.retry?.baseDelayMs ?? 100,
      maxDelayMs: opts.retry?.maxDelayMs ?? 2_000,
    };
    this._sleep = opts.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
  }

  /** The shard this instance routes to ("" = the anonymous shard). */
  get shardKey(): string {
    return this._shardKey;
  }

  /** Build an instance from environment variables. */
  static fromEnv(): FunctionStorageCfDo {
    const url = process.env.VGI_CF_DO_URL;
    if (!url) {
      throw new Error(
        "VGI_CF_DO_URL environment variable is required when " +
          "VGI_WORKER_SHARED_STORAGE=cloudflare-do",
      );
    }
    return new FunctionStorageCfDo({
      url,
      token: process.env.VGI_CF_DO_TOKEN || null,
    });
  }

  /**
   * Return a view of this backend pinned to one shard key, so callers can route
   * per logical ATTACH. Shares the URL/token; only the shard key differs.
   */
  forShard(shardKey: string): FunctionStorageCfDo {
    if (shardKey === this._shardKey) return this;
    return new FunctionStorageCfDo({
      url: this._baseUrl,
      token: this._token,
      shardKey,
      fetch: this._fetch,
      retry: this._retry,
      sleep: this._sleep,
    });
  }

  // --- Worker state → ns=worker, key = substream_id bytes, or int64(worker_id) ---

  async workerPut(executionId: Uint8Array, worker: WorkerStateKey, state: Uint8Array): Promise<void> {
    await this._statePutMany(executionId, NS_WORKER, [[workerStateKey(worker), state]]);
  }

  async workerCollect(executionId: Uint8Array): Promise<Uint8Array[]> {
    const rows = await this._statePaged("state_drain", executionId, NS_WORKER, newAttemptId());
    return rows.map(([, v]) => v);
  }

  async workerScan(executionId: Uint8Array): Promise<Array<[number, Uint8Array]>> {
    const rows = await this._statePaged("state_scan", executionId, NS_WORKER, null);
    return rows.map(([k, v]) => [int64FromKey(k), v]);
  }

  // --- Work queue ---

  async queuePush(executionId: Uint8Array, items: Uint8Array[]): Promise<number> {
    const data = await this._post<{ count: number }>("queue_push", {
      execution_id: bytesToB64(executionId),
      items: items.map(bytesToB64),
      attempt_id: newAttemptId(),
    });
    return Number(data.count);
  }

  async queuePop(executionId: Uint8Array): Promise<Uint8Array | null> {
    const data = await this._post<{ item: string | null }>("queue_pop", {
      execution_id: bytesToB64(executionId),
      attempt_id: newAttemptId(),
    });
    return data.item ? b64ToBytes(data.item) : null;
  }

  async queueClear(executionId: Uint8Array): Promise<number> {
    const data = await this._post<{ cleared: number }>("queue_clear", {
      execution_id: bytesToB64(executionId),
      attempt_id: newAttemptId(),
    });
    return Number(data.cleared);
  }

  // --- Namespaced key/value state ---

  async stateGet(scopeId: Uint8Array, ns: Uint8Array, key: Uint8Array): Promise<Uint8Array | null> {
    return (await this.stateGetMany(scopeId, ns, [key]))[0] ?? null;
  }

  async statePut(scopeId: Uint8Array, ns: Uint8Array, key: Uint8Array, value: Uint8Array): Promise<void> {
    await this._statePutMany(scopeId, ns, [[key, value]]);
  }

  async stateGetMany(scopeId: Uint8Array, ns: Uint8Array, keys: Uint8Array[]): Promise<Array<Uint8Array | null>> {
    if (keys.length === 0) return [];
    const data = await this._post<{ rows: Array<{ value: string } | null> }>("state_get_many", {
      scope_id: bytesToB64(scopeId),
      ns: bytesToB64(ns),
      keys: keys.map(bytesToB64),
    });
    const rows = data.rows ?? [];
    return keys.map((_, i) => {
      const row = rows[i];
      return row ? b64ToBytes(row.value) : null;
    });
  }

  async statePutMany(scopeId: Uint8Array, ns: Uint8Array, items: Array<[Uint8Array, Uint8Array]>): Promise<void> {
    if (items.length === 0) return;
    await this._statePutMany(scopeId, ns, items);
  }

  async stateDelete(scopeId: Uint8Array, ns: Uint8Array, keys: Uint8Array[]): Promise<number> {
    if (keys.length === 0) return 0;
    // Naturally idempotent; the attempt_id is for audit only.
    const data = await this._post<{ deleted: number }>("state_delete", {
      scope_id: bytesToB64(scopeId),
      ns: bytesToB64(ns),
      keys: keys.map(bytesToB64),
      attempt_id: newAttemptId(),
    });
    return Number(data.deleted ?? 0);
  }

  // --- Append-only log ---

  async stateAppend(scopeId: Uint8Array, ns: Uint8Array, key: Uint8Array, item: Uint8Array): Promise<number> {
    const data = await this._post<{ ordinal: number }>("state_append", {
      scope_id: bytesToB64(scopeId),
      ns: bytesToB64(ns),
      key: bytesToB64(key),
      item: bytesToB64(item),
      attempt_id: newAttemptId(),
    });
    return Number(data.ordinal);
  }

  async stateLogScan(
    scopeId: Uint8Array,
    ns: Uint8Array,
    key: Uint8Array,
    afterId: number = -1,
    limit: number | null = null,
  ): Promise<Array<[number, Uint8Array]>> {
    const body: Record<string, unknown> = {
      scope_id: bytesToB64(scopeId),
      ns: bytesToB64(ns),
      key: bytesToB64(key),
      after_id: afterId,
    };
    if (limit != null && limit > 0) body.limit = limit;
    const data = await this._post<{ rows: Array<{ id: number; value: string }> }>("state_log_scan", body);
    return (data.rows ?? []).map((r) => [Number(r.id), b64ToBytes(r.value)]);
  }

  async executionClear(scopeId: Uint8Array): Promise<number> {
    const data = await this._post<{ deleted: number }>("execution_clear", {
      scope_id: bytesToB64(scopeId),
      attempt_id: newAttemptId(),
    });
    return Number(data.deleted);
  }

  // --- unified state_* helpers ---

  private async _statePutMany(
    scopeId: Uint8Array,
    ns: Uint8Array,
    items: Array<[Uint8Array, Uint8Array]>,
  ): Promise<void> {
    await this._post("state_put_many", {
      scope_id: bytesToB64(scopeId),
      ns: bytesToB64(ns),
      items: items.map(([k, v]) => ({ key: bytesToB64(k), value: bytesToB64(v) })),
      attempt_id: newAttemptId(),
    });
  }

  /** Drive state_scan / state_drain across pages. attemptId is null for scan. */
  private async _statePaged(
    endpoint: string,
    scopeId: Uint8Array,
    ns: Uint8Array,
    attemptId: string | null,
  ): Promise<Array<[Uint8Array, Uint8Array]>> {
    const out: Array<[Uint8Array, Uint8Array]> = [];
    let afterKey: string | undefined;
    for (;;) {
      const body: Record<string, unknown> = { scope_id: bytesToB64(scopeId), ns: bytesToB64(ns) };
      if (afterKey != null) body.after_key = afterKey;
      if (attemptId != null) body.attempt_id = attemptId;
      const data = await this._post<{ rows: Array<{ key: string; value: string }>; next_after?: string }>(
        endpoint,
        body,
      );
      for (const r of data.rows ?? []) out.push([b64ToBytes(r.key), b64ToBytes(r.value)]);
      if (!data.next_after) break;
      afterKey = data.next_after;
    }
    return out;
  }

  // --- HTTP plumbing ---

  /**
   * POST one call. The body (and so its `attempt_id`, for destructive ops) is
   * built once and resent unchanged on every retry, so the DO's replay
   * detection makes a retried put/drain/pop a no-op. Retries transport errors,
   * timeouts, 5xx and 429 with exponential backoff and full jitter, bounded by
   * `maxAttempts`. 4xx other than 429 is the caller's error and is not retried.
   * The bearer token never appears in an error message.
   */
  private async _post<T = Record<string, unknown>>(
    endpoint: string,
    body: Record<string, unknown>,
  ): Promise<T> {
    const url = `${this._baseUrl}/${endpoint}`;
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (this._token) headers["Authorization"] = `Bearer ${this._token}`;
    // The Worker routes on shard_key (idFromName) and rejects requests without
    // one. An attach-bound call carries att-<uuid hex>; one bound to no ATTACH
    // goes to the shared anonymous shard, as in vgi-python.
    const payload = JSON.stringify({ ...body, shard_key: this._shardKey || ANONYMOUS_SHARD_KEY });

    const { maxAttempts, baseDelayMs, maxDelayMs } = this._retry;
    let lastErr: Error | undefined;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      if (attempt > 0) {
        const cap = Math.min(maxDelayMs, baseDelayMs * 2 ** (attempt - 1));
        const jittered = Math.random() * cap;
        const hinted = lastErr instanceof RetryableError && lastErr.retryAfterMs != null
          ? Math.min(maxDelayMs, lastErr.retryAfterMs)
          : 0;
        await this._sleep(Math.max(jittered, hinted));
      }
      try {
        return await this._once<T>(url, endpoint, headers, payload);
      } catch (e) {
        if (!(e instanceof RetryableError)) throw e;
        lastErr = e;
      }
    }
    throw new Error(
      `CF DO storage ${endpoint} failed after ${maxAttempts} attempts: ${lastErr?.message ?? "unknown error"}`,
    );
  }

  private async _once<T>(
    url: string,
    endpoint: string,
    headers: Record<string, string>,
    payload: string,
  ): Promise<T> {
    const { timeoutMs } = this._retry;
    const controller = timeoutMs > 0 ? new AbortController() : null;
    const timer = controller ? setTimeout(() => controller.abort(), timeoutMs) : null;
    try {
      let resp: Response;
      try {
        resp = await this._fetch(url, {
          method: "POST",
          headers,
          body: payload,
          ...(controller ? { signal: controller.signal } : {}),
        });
      } catch (err: any) {
        if (controller?.signal.aborted) {
          throw new RetryableError(`timed out after ${timeoutMs} ms on ${endpoint}`);
        }
        throw new RetryableError(`transport error on ${endpoint}: ${err?.message ?? String(err)}`);
      }

      if (resp.status === 429 || resp.status >= 500) {
        const text = await resp.text().catch(() => "");
        throw new RetryableError(
          `CF DO storage error ${resp.status} on ${endpoint}: ${text.slice(0, 200) || "<empty body>"}`,
          parseRetryAfter(resp.headers?.get?.("Retry-After") ?? null),
        );
      }

      let data: any;
      try {
        data = await resp.json();
      } catch (err: any) {
        if (controller?.signal.aborted) {
          throw new RetryableError(`timed out after ${timeoutMs} ms reading ${endpoint}`);
        }
        if (!resp.ok) {
          throw new Error(`CF DO storage error ${resp.status} on ${endpoint}: <non-json body>`);
        }
        return {} as T;
      }

      if (resp.status === 404 && data?.error === "unknown_invocation") {
        throw new UnknownInvocationError(
          data.message ??
            "Invocation is not registered. Call queuePush first to register the invocation.",
        );
      }
      if (resp.status === 401) {
        throw new Error(`Authentication failed: ${data?.error ?? "unauthorized"}`);
      }
      if (!resp.ok) {
        throw new Error(`CF DO storage error ${resp.status} on ${endpoint}: ${JSON.stringify(data)}`);
      }
      return data as T;
    } finally {
      if (timer) clearTimeout(timer);
    }
  }
}

/** `Retry-After` in ms (delta-seconds form only), or null. */
function parseRetryAfter(v: string | null): number | null {
  if (!v) return null;
  const n = Number(v.trim());
  return Number.isFinite(n) && n >= 0 ? n * 1000 : null;
}

/** Fresh 32-char lowercase-hex idempotency token (the DO's attempt_id shape). */
function newAttemptId(): string {
  const b = new Uint8Array(16);
  crypto.getRandomValues(b);
  let s = "";
  for (let i = 0; i < 16; i++) s += b[i]!.toString(16).padStart(2, "0");
  return s;
}

/** Decode an 8-byte big-endian state key to its int64 worker id; any other
 *  key (a substream id) has none, and decodes to 0. */
function int64FromKey(b: Uint8Array): number {
  if (b.length !== 8) return 0;
  return Number(new DataView(b.buffer, b.byteOffset, 8).getBigInt64(0, false));
}

function bytesToB64(buf: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < buf.length; i++) binary += String.fromCharCode(buf[i]!);
  return btoa(binary);
}

function b64ToBytes(b64: string): Uint8Array {
  const binary = atob(b64);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}
