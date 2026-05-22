// Cloudflare Durable Object storage for VGI function state.
//
// Port of vgi-python's vgi/function_storage_cf_do.py. Communicates with the
// Cloudflare Worker + Durable Object in
// vgi-python/cloudflare/vgi-storage/src/index.ts. The DO is single-threaded
// and runs SQLite internally, so all operations are inherently atomic and
// the wire shape matches FunctionStorageSqlite.
//
// Usage:
//   Set `VGI_WORKER_SHARED_STORAGE=cloudflare-do` plus `VGI_CF_DO_URL`.
//   Optionally set `VGI_CF_DO_TOKEN` for bearer auth.

import type { FunctionStorage } from "./storage.js";
import { UnknownInvocationError } from "./storage.js";

export class FunctionStorageCfDo implements FunctionStorage {
  private readonly _baseUrl: string;
  private readonly _token: string | null;

  constructor(opts: { url: string; token?: string | null }) {
    // Strip trailing slash so endpoint paths can be appended unconditionally.
    this._baseUrl = opts.url.replace(/\/+$/, "");
    this._token = opts.token ?? null;
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

  // --- Worker State ---

  async workerPut(
    executionId: Uint8Array,
    workerId: number,
    state: Uint8Array,
  ): Promise<void> {
    await this._post("worker_put", {
      execution_id: bytesToB64(executionId),
      worker_id: workerId,
      state: bytesToB64(state),
    });
  }

  async workerCollect(executionId: Uint8Array): Promise<Uint8Array[]> {
    const data = await this._post<{ states: string[] }>("worker_collect", {
      execution_id: bytesToB64(executionId),
    });
    return (data.states ?? []).map(b64ToBytes);
  }

  async workerScan(
    executionId: Uint8Array,
  ): Promise<Array<[number, Uint8Array]>> {
    const data = await this._post<{ rows: Array<{ worker_id: number; state: string }> }>(
      "worker_scan",
      { execution_id: bytesToB64(executionId) },
    );
    return (data.rows ?? []).map((r) => [Number(r.worker_id), b64ToBytes(r.state)]);
  }

  // --- Work Queue ---

  async queuePush(
    executionId: Uint8Array,
    items: Uint8Array[],
  ): Promise<number> {
    const data = await this._post<{ count: number }>("queue_push", {
      execution_id: bytesToB64(executionId),
      items: items.map(bytesToB64),
    });
    return Number(data.count);
  }

  async queuePop(executionId: Uint8Array): Promise<Uint8Array | null> {
    const data = await this._post<{ item: string | null }>("queue_pop", {
      execution_id: bytesToB64(executionId),
    });
    return data.item ? b64ToBytes(data.item) : null;
  }

  async queueClear(executionId: Uint8Array): Promise<number> {
    const data = await this._post<{ cleared: number }>("queue_clear", {
      execution_id: bytesToB64(executionId),
    });
    return Number(data.cleared);
  }

  // Namespaced state + append-log are not implemented on the CF DO side
  // (table_buffering currently targets SQLite-backed transports). Throw
  // rather than silently returning wrong shapes — matches the CLAUDE.md
  // contract for unimplemented backend operations.
  async stateGet(): Promise<Uint8Array | null> {
    throw new Error("FunctionStorageCfDo.stateGet: not implemented");
  }
  async statePut(): Promise<void> {
    throw new Error("FunctionStorageCfDo.statePut: not implemented");
  }
  async stateAppend(): Promise<number> {
    throw new Error("FunctionStorageCfDo.stateAppend: not implemented");
  }
  async stateLogScan(): Promise<Array<[number, Uint8Array]>> {
    throw new Error("FunctionStorageCfDo.stateLogScan: not implemented");
  }
  async executionClear(): Promise<number> {
    throw new Error("FunctionStorageCfDo.executionClear: not implemented");
  }

  // --- HTTP plumbing ---

  private async _post<T = Record<string, unknown>>(
    endpoint: string,
    body: Record<string, unknown>,
  ): Promise<T> {
    const url = `${this._baseUrl}/${endpoint}`;
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (this._token) headers["Authorization"] = `Bearer ${this._token}`;

    let lastErr: unknown;
    // One retry on transport-level failures, matching the python client.
    // Application errors (4xx/5xx) are not retried here — they bubble up
    // immediately.
    for (let attempt = 0; attempt < 2; attempt++) {
      let resp: Response;
      try {
        resp = await fetch(url, {
          method: "POST",
          headers,
          body: JSON.stringify(body),
        });
      } catch (err) {
        lastErr = err;
        continue;
      }

      // Cloudflare Workers always return JSON for these endpoints; parse and
      // dispatch to a typed error if needed.
      let data: any;
      try {
        data = await resp.json();
      } catch {
        if (!resp.ok) {
          throw new Error(
            `CF DO storage error ${resp.status} on ${endpoint}: <non-json body>`,
          );
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
        throw new Error(
          `Authentication failed: ${data?.error ?? "unauthorized"}`,
        );
      }
      if (!resp.ok) {
        throw new Error(
          `CF DO storage error ${resp.status} on ${endpoint}: ${JSON.stringify(data)}`,
        );
      }
      return data as T;
    }
    throw lastErr instanceof Error
      ? lastErr
      : new Error(`CF DO storage transport error on ${endpoint}: ${String(lastErr)}`);
  }
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
