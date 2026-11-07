// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// VgiClient.tableFunctionRows({ int64 }) — int64 output columns surface either
// as a magnitude-dependent number/bigint mix ("number-if-safe", what the RPC
// layer does by default) or uniformly as bigint ("bigint"). Float columns
// holding integral values must NOT be touched.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { httpConnect } from "@query-farm/vgi-rpc";
import { VgiClient } from "../client.js";
import { field, float64, int32, int64, schema, utf8 } from "../../arrow/index.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { FunctionStorageSqlite, resolveStorageFromEnv, setStorage } from "../../functions/storage.js";
import { defineTableFunction } from "../../functions/table.js";
import { SIGNING_KEY_BYTES, serveVgiWorker, type VgiHttpServer } from "../../serve-entry.js";
import { batchFromColumns } from "../../util/arrow/index.js";
import type { Int64Mode } from "../types.js";

const BIG = 2n ** 60n;
const OUT = schema([
  field("i", int64(), true),
  field("f", float64(), true),
  field("s", int32(), true),
  field("label", utf8(), true),
]);

const ints = defineTableFunction({
  name: "ints",
  onBind: () => ({ outputSchema: OUT }),
  process: (_p, _s, out) => {
    out.emit(batchFromColumns({ i: [1n, BIG, null], f: [2, 2.5, null], s: [3, 4, null], label: ["a", "b", "c"] }, OUT));
    out.finish();
  },
});

let dir: string;
let server: VgiHttpServer;

beforeAll(() => {
  dir = mkdtempSync(join(tmpdir(), "vgi-int64-rows-"));
  setStorage(new FunctionStorageSqlite(join(dir, "state.db")));
  const registry = new FunctionRegistry();
  registry.register(ints);
  server = serveVgiWorker({
    name: "demo",
    doc: "int64 rows test worker.",
    version: "0.0.1",
    registry,
    catalogInterface: new ReadOnlyCatalogInterface({ name: "demo", schemas: [{ name: "main", functions: [ints] }] }, registry),
    port: 0,
    signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(7),
    quiet: true,
    env: {},
  });
});

afterAll(() => {
  server?.stop(true);
  setStorage(resolveStorageFromEnv());
  rmSync(dir, { recursive: true, force: true });
});

async function rows(int64?: Int64Mode, rpcInt64?: Int64Mode) {
  const client = new VgiClient(httpConnect(`http://localhost:${server.port}`, { prefix: "", int64: rpcInt64 }));
  const out: Record<string, any>[] = [];
  for await (const batch of client.tableFunctionRows({ functionName: "ints", int64 })) out.push(...batch);
  return out;
}

describe("tableFunctionRows int64 option", () => {
  test("omitted: unchanged default (safe values narrowed to number)", async () => {
    const r = await rows();
    expect(r.map((x) => x.i)).toEqual([1, BIG, null]);
  });

  test('"bigint": every int64 value is a bigint; other columns untouched', async () => {
    const r = await rows("bigint");
    expect(r.map((x) => x.i)).toEqual([1n, BIG, null]);
    expect(r.map((x) => x.f)).toEqual([2, 2.5, null]);
    expect(r.map((x) => x.s)).toEqual([3, 4, null]);
    expect(r.map((x) => x.label)).toEqual(["a", "b", "c"]);
  });

  test('"number-if-safe" explicit', async () => {
    expect((await rows("number-if-safe")).map((x) => x.i)).toEqual([1, BIG, null]);
  });

  test("RPC client created with int64: \"bigint\" is honoured, and the per-call option overrides it", async () => {
    expect((await rows(undefined, "bigint")).map((x) => x.i)).toEqual([1n, BIG, null]);
    expect((await rows("number-if-safe", "bigint")).map((x) => x.i)).toEqual([1, BIG, null]);
  });
});
