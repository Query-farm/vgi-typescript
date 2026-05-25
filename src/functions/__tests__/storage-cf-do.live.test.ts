// © Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// End-to-end test of FunctionStorageCfDo against a REAL Cloudflare Worker +
// Durable Object over HTTP (a local `wrangler dev` or a deployed instance) —
// proves the client speaks the actual protocol, not just the in-process mock.
// Skipped unless VGI_CF_DO_INTEGRATION_URL is set; VGI_CF_DO_TOKEN supplies the
// bearer key.

import { describe, expect, test } from "bun:test";
import { FunctionStorageCfDo } from "../storage-cf-do.js";

const URL = process.env.VGI_CF_DO_INTEGRATION_URL;
const enc = new TextEncoder();
const dec = new TextDecoder();

function liveStorage(): FunctionStorageCfDo {
  // Fresh random shard keeps this run isolated from prior runs on the DO.
  const u = new Uint8Array(16);
  crypto.getRandomValues(u);
  let hex = "";
  for (const b of u) hex += b.toString(16).padStart(2, "0");
  return new FunctionStorageCfDo({
    url: URL!,
    token: process.env.VGI_CF_DO_TOKEN || null,
  }).forShard("att-" + hex);
}

describe.skipIf(!URL)("FunctionStorageCfDo over the wire", () => {
  test("namespaced state put/get round-trips", async () => {
    const s = liveStorage();
    const scope = enc.encode("exec-live");
    const ns = enc.encode("agg");
    await s.statePut(scope, ns, enc.encode("k"), enc.encode("hello"));
    const got = await s.stateGet(scope, ns, enc.encode("k"));
    expect(got && dec.decode(got)).toBe("hello");
    expect(await s.stateGet(scope, ns, enc.encode("nope"))).toBeNull();
  });

  test("append log + scan round-trips", async () => {
    const s = liveStorage();
    const exec = enc.encode("exec-log");
    const ns = enc.encode("buf");
    const o1 = await s.stateAppend(exec, ns, enc.encode("k"), enc.encode("a"));
    const o2 = await s.stateAppend(exec, ns, enc.encode("k"), enc.encode("b"));
    expect(o1).toBeLessThan(o2);
    const rows = (await s.stateLogScan(exec, ns, enc.encode("k"))).map(([, v]) => dec.decode(v));
    expect(rows).toEqual(["a", "b"]);
    expect((await s.stateLogScan(exec, ns, enc.encode("k"), o1, 1)).length).toBe(1);
  });

  test("worker put/collect drains in key order", async () => {
    const s = liveStorage();
    const exec = enc.encode("exec-w");
    await s.workerPut(exec, 2, enc.encode("two"));
    await s.workerPut(exec, 1, enc.encode("one"));
    const states = (await s.workerCollect(exec)).map((b) => dec.decode(b));
    expect(states).toEqual(["one", "two"]);
    expect(await s.workerCollect(exec)).toEqual([]);
  });

  test("queue push/pop FIFO; empty pop returns null", async () => {
    const s = liveStorage();
    const exec = enc.encode("exec-q");
    await s.queuePush(exec, [enc.encode("i1"), enc.encode("i2")]);
    expect(dec.decode((await s.queuePop(exec))!)).toBe("i1");
    expect(dec.decode((await s.queuePop(exec))!)).toBe("i2");
    expect(await s.queuePop(exec)).toBeNull();
  });

  test("executionClear wipes state", async () => {
    const s = liveStorage();
    const exec = enc.encode("exec-clear");
    const ns = enc.encode("agg");
    await s.statePut(exec, ns, enc.encode("k"), enc.encode("v"));
    await s.executionClear(exec);
    expect(await s.stateGet(exec, ns, enc.encode("k"))).toBeNull();
  });
});
