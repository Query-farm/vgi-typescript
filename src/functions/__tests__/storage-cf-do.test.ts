// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Tests for the Cloudflare Durable Object storage client: it must speak the
// DO's unified state_* / queue_* protocol, carry a valid shard_key on every
// request and a 32-hex attempt_id on every destructive op, and round-trip data
// against an in-memory mock of the Worker.

import { afterEach, describe, expect, test } from "bun:test";
import { FunctionStorageCfDo } from "../storage-cf-do.js";
import { deriveShardKey } from "../../protocol/handlers/catalog/shared.js";

const SHARD_RE = /^[A-Za-z0-9._-]{1,128}$/;
const ATTEMPT_RE = /^[0-9a-f]{32}$/;
const enc = new TextEncoder();

const realFetch = globalThis.fetch;
afterEach(() => {
  globalThis.fetch = realFetch;
});

// ---------------------------------------------------------------------------
// In-memory mock Worker, installed as global fetch. Validates shard_key on
// every request and attempt_id on destructive ops; routes state by
// (shard_key, scope_id, ns, key).
// ---------------------------------------------------------------------------

interface Captured {
  shardKeys: Set<string>;
  attemptIds: string[];
  missingShard: number;
  missingAttempt: number;
}

function installMock(): Captured {
  const cap: Captured = { shardKeys: new Set(), attemptIds: [], missingShard: 0, missingAttempt: 0 };
  const kv = new Map<string, string>(); // "shard\x1fscope\x1fns\x1fkey" -> value(b64)
  const log = new Map<string, Array<{ id: number; value: string }>>();
  const queues = new Map<string, string[]>();
  const regd = new Set<string>();
  let logSeq = 0;

  const json = (status: number, body: unknown): Response =>
    new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });

  const skOf = (b: any, needsAttempt: boolean): string | null => {
    const sk = typeof b.shard_key === "string" ? b.shard_key : "";
    if (!SHARD_RE.test(sk)) {
      cap.missingShard++;
      return null;
    }
    cap.shardKeys.add(sk);
    if (needsAttempt) {
      const aid = typeof b.attempt_id === "string" ? b.attempt_id : "";
      if (!ATTEMPT_RE.test(aid)) {
        cap.missingAttempt++;
        return null;
      }
      cap.attemptIds.push(aid);
    }
    return sk;
  };
  const ck = (sk: string, b: any, key: string) => `${sk}\x1f${b.scope_id}\x1f${b.ns}\x1f${key}`;
  const pfx = (sk: string, b: any) => `${sk}\x1f${b.scope_id}\x1f${b.ns}\x1f`;

  globalThis.fetch = (async (_url: string, init?: RequestInit): Promise<Response> => {
    const endpoint = String(_url).split("/").pop();
    const b = JSON.parse(String(init?.body));
    const destructive = ["state_put_many", "state_drain", "state_delete", "state_append", "execution_clear", "queue_push", "queue_pop", "queue_clear"].includes(endpoint!);
    const sk = skOf(b, destructive);
    if (sk == null) return json(400, { error: "bad_request" });

    switch (endpoint) {
      case "state_put_many":
        for (const it of b.items ?? []) kv.set(ck(sk, b, it.key), it.value);
        return json(200, { written: (b.items ?? []).length });
      case "state_get_many":
        return json(200, { rows: (b.keys ?? []).map((k: string) => (kv.has(ck(sk, b, k)) ? { value: kv.get(ck(sk, b, k)) } : null)) });
      case "state_scan":
      case "state_drain": {
        const prefix = pfx(sk, b);
        const rows = [...kv.entries()].filter(([k]) => k.startsWith(prefix)).map(([k, v]) => ({ key: k.slice(prefix.length), value: v }));
        rows.sort((x, y) => (x.key < y.key ? -1 : 1));
        if (endpoint === "state_drain") for (const k of [...kv.keys()]) if (k.startsWith(prefix)) kv.delete(k);
        return json(200, { rows });
      }
      case "state_append": {
        logSeq++;
        const key = ck(sk, b, b.key);
        if (!log.has(key)) log.set(key, []);
        log.get(key)!.push({ id: logSeq, value: b.item });
        return json(200, { ordinal: logSeq });
      }
      case "state_log_scan": {
        const after = typeof b.after_id === "number" ? b.after_id : -1;
        const rows = (log.get(ck(sk, b, b.key)) ?? []).filter((e) => e.id > after);
        return json(200, { rows: b.limit ? rows.slice(0, b.limit) : rows });
      }
      case "execution_clear": {
        const prefix = `${sk}\x1f${b.scope_id}\x1f`;
        for (const k of [...kv.keys()]) if (k.startsWith(prefix)) kv.delete(k);
        for (const k of [...log.keys()]) if (k.startsWith(prefix)) log.delete(k);
        return json(200, { deleted: 0 });
      }
      case "queue_push": {
        const qk = `${sk}\x1f${b.execution_id}`;
        regd.add(qk);
        queues.set(qk, [...(queues.get(qk) ?? []), ...(b.items ?? [])]);
        return json(200, { count: (b.items ?? []).length });
      }
      case "queue_pop": {
        const qk = `${sk}\x1f${b.execution_id}`;
        if (!regd.has(qk)) return json(404, { error: "unknown_invocation" });
        const q = queues.get(qk) ?? [];
        if (q.length === 0) return json(200, { item: null });
        const item = q.shift()!;
        return json(200, { item });
      }
      case "queue_clear": {
        const qk = `${sk}\x1f${b.execution_id}`;
        const n = (queues.get(qk) ?? []).length;
        queues.delete(qk);
        regd.delete(qk);
        return json(200, { cleared: n });
      }
      default:
        return json(404, { error: "not found" });
    }
  }) as typeof fetch;
  return cap;
}

function newStorage(): FunctionStorageCfDo {
  return new FunctionStorageCfDo({ url: "https://mock" }).forShard(
    "att-0123456789abcdef0123456789abcdef",
  );
}

describe("deriveShardKey", () => {
  test("att- + hex of the 16-byte uuid", () => {
    const uuid = new Uint8Array(16).fill(0xab);
    expect(deriveShardKey(uuid)).toBe("att-" + "ab".repeat(16));
    expect(deriveShardKey(uuid)).toMatch(/^att-[0-9a-f]{32}$/);
  });
  test("throws on non-16-byte uuid", () => {
    expect(() => deriveShardKey(new Uint8Array(8))).toThrow();
    expect(() => deriveShardKey(new Uint8Array(0))).toThrow();
  });
});

describe("FunctionStorageCfDo unified protocol", () => {
  test("state put/get round-trips and carries shard_key + attempt_id", async () => {
    const cap = installMock();
    const s = newStorage();
    const scope = enc.encode("exec1");
    const ns = enc.encode("agg");
    await s.statePut(scope, ns, enc.encode("k"), enc.encode("v"));
    const got = await s.stateGet(scope, ns, enc.encode("k"));
    expect(got && new TextDecoder().decode(got)).toBe("v");
    expect(await s.stateGet(scope, ns, enc.encode("missing"))).toBeNull();
    expect(cap.missingShard).toBe(0);
    expect(cap.missingAttempt).toBe(0);
    expect([...cap.shardKeys]).toEqual(["att-0123456789abcdef0123456789abcdef"]);
    expect(cap.attemptIds.length).toBeGreaterThan(0);
  });

  test("worker put/collect drains in key order", async () => {
    installMock();
    const s = newStorage();
    const exec = enc.encode("exec-w");
    await s.workerPut(exec, 2, enc.encode("two"));
    await s.workerPut(exec, 1, enc.encode("one"));
    const states = (await s.workerCollect(exec)).map((b) => new TextDecoder().decode(b));
    expect(states).toEqual(["one", "two"]); // int64 BE keys sort by worker id
    expect(await s.workerCollect(exec)).toEqual([]); // drained
  });

  test("append log + scan, and queue round-trip", async () => {
    installMock();
    const s = newStorage();
    const exec = enc.encode("exec-q");
    const ns = enc.encode("log");
    const o1 = await s.stateAppend(exec, ns, enc.encode("k"), enc.encode("a"));
    const o2 = await s.stateAppend(exec, ns, enc.encode("k"), enc.encode("b"));
    expect(o1).toBeLessThan(o2);
    const rows = (await s.stateLogScan(exec, ns, enc.encode("k"))).map(([, v]) => new TextDecoder().decode(v));
    expect(rows).toEqual(["a", "b"]);

    await s.queuePush(exec, [enc.encode("item1")]);
    const popped = await s.queuePop(exec);
    expect(popped && new TextDecoder().decode(popped)).toBe("item1");
    expect(await s.queuePop(exec)).toBeNull();
  });

  test("empty shard_key (no forShard) is rejected by the server contract", async () => {
    const cap = installMock();
    const s = new FunctionStorageCfDo({ url: "https://mock" }); // no forShard -> shard_key ""
    await expect(s.statePut(enc.encode("e"), enc.encode("n"), enc.encode("k"), enc.encode("v"))).rejects.toThrow();
    expect(cap.missingShard).toBeGreaterThan(0);
  });
});
