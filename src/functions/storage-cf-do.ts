// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Cloudflare Durable Object storage for VGI function state.
//
// Port of vgi-python's vgi/function_storage_cf_do.py. Talks the Cloudflare
// Worker + Durable Object's unified state_* / queue_* protocol
// (vgi-cloudflare-durable-object-storage/src/index.ts). Every request carries
// the per-attach `shard_key` (set via forShard); destructive ops carry a fresh
// 32-hex `attempt_id`. The DO is single-threaded SQLite, so ops are atomic.
//
// Usage:
//   Set `VGI_WORKER_SHARED_STORAGE=cloudflare-do` plus `VGI_CF_DO_URL`.
//   Optionally set `VGI_CF_DO_TOKEN` for bearer auth.

import type { FunctionStorage } from "./storage.js";
import { UnknownInvocationError } from "./storage.js";

/** Minimal fetch signature satisfied by both the global `fetch` and a
 *  Cloudflare service-binding Fetcher (`env.<BINDING>`). */
export type FetchLike = (
  input: string,
  init?: { method?: string; headers?: Record<string, string>; body?: string },
) => Promise<Response>;

// Namespaces under the unified state_* table for the legacy worker/queue
// families the DO no longer has dedicated endpoints for.
const NS_WORKER = new TextEncoder().encode("worker");

export class FunctionStorageCfDo implements FunctionStorage {
  private readonly _baseUrl: string;
  private readonly _token: string | null;
  /** Per-attach routing key (att-<hex uuid>); "" until pinned via forShard. */
  private readonly _shardKey: string;
  /** Fetch implementation. Defaults to the global `fetch`; pass a Cloudflare
   *  service-binding Fetcher (env.<BINDING>) when the storage worker lives on
   *  the same account/zone — a same-zone public-URL fetch is rejected (CF
   *  error 1042), but a service binding routes worker-to-worker directly. */
  private readonly _fetch: FetchLike;

  constructor(opts: {
    url: string;
    token?: string | null;
    shardKey?: string;
    fetch?: FetchLike;
  }) {
    // Strip trailing slash so endpoint paths can be appended unconditionally.
    this._baseUrl = opts.url.replace(/\/+$/, "");
    this._token = opts.token ?? null;
    this._shardKey = opts.shardKey ?? "";
    this._fetch = opts.fetch ?? ((input, init) => fetch(input as any, init));
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
    return new FunctionStorageCfDo({
      url: this._baseUrl,
      token: this._token,
      shardKey,
      fetch: this._fetch,
    });
  }

  // --- Worker state → ns=worker, key = int64(worker_id) ---

  async workerPut(executionId: Uint8Array, workerId: number, state: Uint8Array): Promise<void> {
    await this._statePutMany(executionId, NS_WORKER, [[int64Key(workerId), state]]);
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
    const data = await this._post<{ rows: Array<{ value: string } | null> }>("state_get_many", {
      scope_id: bytesToB64(scopeId),
      ns: bytesToB64(ns),
      keys: [bytesToB64(key)],
    });
    const row = (data.rows ?? [])[0];
    return row ? b64ToBytes(row.value) : null;
  }

  async statePut(scopeId: Uint8Array, ns: Uint8Array, key: Uint8Array, value: Uint8Array): Promise<void> {
    await this._statePutMany(scopeId, ns, [[key, value]]);
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

  private async _post<T = Record<string, unknown>>(
    endpoint: string,
    body: Record<string, unknown>,
  ): Promise<T> {
    const url = `${this._baseUrl}/${endpoint}`;
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (this._token) headers["Authorization"] = `Bearer ${this._token}`;
    // The Worker routes on shard_key (idFromName) and rejects requests without
    // one — always splice it in.
    const payload = JSON.stringify({ ...body, shard_key: this._shardKey });

    let lastErr: unknown;
    // One retry on transport-level failures, matching the python client.
    for (let attempt = 0; attempt < 2; attempt++) {
      let resp: Response;
      try {
        resp = await this._fetch(url, { method: "POST", headers, body: payload });
      } catch (err) {
        lastErr = err;
        continue;
      }

      let data: any;
      try {
        data = await resp.json();
      } catch {
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
    }
    throw lastErr instanceof Error
      ? lastErr
      : new Error(`CF DO storage transport error on ${endpoint}: ${String(lastErr)}`);
  }
}

/** Fresh 32-char lowercase-hex idempotency token (the DO's attempt_id shape). */
function newAttemptId(): string {
  const b = new Uint8Array(16);
  crypto.getRandomValues(b);
  let s = "";
  for (let i = 0; i < 16; i++) s += b[i].toString(16).padStart(2, "0");
  return s;
}

/** Encode an int64 worker/group id as an 8-byte big-endian state key. */
function int64Key(v: number): Uint8Array {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigInt64(0, BigInt(v), false);
  return b;
}

function int64FromKey(b: Uint8Array): number {
  if (b.length !== 8) return 0;
  return Number(new DataView(b.buffer, b.byteOffset, 8).getBigInt64(0, false));
}

function bytesToB64(buf: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < buf.length; i++) binary += String.fromCharCode(buf[i]);
  return btoa(binary);
}

function b64ToBytes(b64: string): Uint8Array {
  const binary = atob(b64);
  const out = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
  return out;
}
