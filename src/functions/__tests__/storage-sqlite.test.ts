// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Conformance for the default in-process backend, FunctionStorageSqlite — the
// state mechanism every non-cloudflare worker uses. Exercises worker state,
// the work queue (incl. unknown-invocation registration), namespaced key/value
// state, the append-only log, and execution_clear.

import { afterEach, describe, expect, test } from "bun:test";
import { FunctionStorageSqlite } from "../storage.js";

const enc = new TextEncoder();
const dec = new TextDecoder();
const b = (s: string) => enc.encode(s);
const str = (u: Uint8Array | null) => (u == null ? null : dec.decode(u));

let store: FunctionStorageSqlite;
function fresh(): FunctionStorageSqlite {
  store?.close?.();
  store = new FunctionStorageSqlite(":memory:");
  return store;
}
afterEach(() => store?.close?.());

describe("FunctionStorageSqlite — worker state", () => {
  test("put → scan (non-destructive) → collect (drains, ordered)", async () => {
    const s = fresh();
    const exec = b("exec-w");
    await s.workerPut(exec, 2, b("two"));
    await s.workerPut(exec, 1, b("one"));
    const scanned = (await s.workerScan(exec)).map(([id, v]) => [id, str(v)]);
    expect(scanned.sort()).toEqual([[1, "one"], [2, "two"]]);
    // scan didn't consume.
    expect((await s.workerScan(exec)).length).toBe(2);
    const collected = (await s.workerCollect(exec)).map(str).sort();
    expect(collected).toEqual(["one", "two"]); // order across slots is unspecified
    expect(await s.workerCollect(exec)).toEqual([]); // drained
  });

  test("put replaces an existing worker slot", async () => {
    const s = fresh();
    const exec = b("exec-r");
    await s.workerPut(exec, 1, b("v1"));
    await s.workerPut(exec, 1, b("v2"));
    expect((await s.workerScan(exec)).map(([, v]) => str(v))).toEqual(["v2"]);
  });
});

describe("FunctionStorageSqlite — work queue", () => {
  test("FIFO push/pop; empty queue returns null", async () => {
    const s = fresh();
    const exec = b("exec-q");
    await s.queuePush(exec, [b("i1"), b("i2")]);
    expect(str(await s.queuePop(exec))).toBe("i1");
    expect(str(await s.queuePop(exec))).toBe("i2");
    expect(await s.queuePop(exec)).toBeNull(); // drained
  });

  test("pop on a never-pushed execution returns null (no registration)", async () => {
    const s = fresh();
    expect(await s.queuePop(b("never-pushed"))).toBeNull();
  });

  test("clear drops items; pop then returns null", async () => {
    const s = fresh();
    const exec = b("exec-c");
    await s.queuePush(exec, [b("x")]);
    expect(await s.queueClear(exec)).toBe(1);
    expect(await s.queuePop(exec)).toBeNull();
  });
});

describe("FunctionStorageSqlite — namespaced key/value state", () => {
  test("put/get round-trips; missing key is null", async () => {
    const s = fresh();
    const scope = b("exec1");
    const ns = b("agg");
    await s.statePut(scope, ns, b("k"), b("v"));
    expect(str(await s.stateGet(scope, ns, b("k")))).toBe("v");
    expect(await s.stateGet(scope, ns, b("missing"))).toBeNull();
  });

  test("isolated by scope and namespace", async () => {
    const s = fresh();
    await s.statePut(b("scopeA"), b("ns"), b("k"), b("a"));
    await s.statePut(b("scopeB"), b("ns"), b("k"), b("b"));
    await s.statePut(b("scopeA"), b("ns2"), b("k"), b("c"));
    expect(str(await s.stateGet(b("scopeA"), b("ns"), b("k")))).toBe("a");
    expect(str(await s.stateGet(b("scopeB"), b("ns"), b("k")))).toBe("b");
    expect(str(await s.stateGet(b("scopeA"), b("ns2"), b("k")))).toBe("c");
  });
});

describe("FunctionStorageSqlite — append log", () => {
  test("monotonic ordinals; scan from start / after_id / limit; key isolation", async () => {
    const s = fresh();
    const exec = b("exec-log");
    const ns = b("buf");
    const id1 = await s.stateAppend(exec, ns, b("k"), b("a"));
    const id2 = await s.stateAppend(exec, ns, b("k"), b("b"));
    const id3 = await s.stateAppend(exec, ns, b("k"), b("c"));
    expect(id1 < id2 && id2 < id3).toBe(true);

    const all = (await s.stateLogScan(exec, ns, b("k"))).map(([id, v]) => [id, str(v)]);
    expect(all).toEqual([[id1, "a"], [id2, "b"], [id3, "c"]]);
    expect((await s.stateLogScan(exec, ns, b("k"), id1)).map(([, v]) => str(v))).toEqual(["b", "c"]);
    expect((await s.stateLogScan(exec, ns, b("k"), -1, 2)).length).toBe(2);
    expect(await s.stateLogScan(exec, ns, b("other"))).toEqual([]);
  });
});

describe("FunctionStorageSqlite — execution_clear", () => {
  test("wipes state + log for a scope across namespaces", async () => {
    const s = fresh();
    const exec = b("exec-clear");
    await s.statePut(exec, b("agg"), b("k"), b("v"));
    await s.stateAppend(exec, b("buf"), b("lk"), b("a"));
    await s.executionClear(exec);
    expect(await s.stateGet(exec, b("agg"), b("k"))).toBeNull();
    expect(await s.stateLogScan(exec, b("buf"), b("lk"))).toEqual([]);
  });
});
