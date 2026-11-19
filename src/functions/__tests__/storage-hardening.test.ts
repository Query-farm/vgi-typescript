// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Storage security + performance contract:
//   - sealed framework payload v1 (round trip, tamper, moved slot, wrong key,
//     version/length, and the cross-SDK vector shared with vgi-python);
//   - every storage call of a buffered and an aggregate execution is routed to
//     the request's attach shard (att-<uuid hex>), including against a mock of
//     the Cloudflare DO that enforces the server's shard_key rule;
//   - no secret reaches the SQLite file in plaintext;
//   - the buffered init is cached (process = the function's own calls only)
//     and evicted by the destructor; aggregates are 2 calls per batch;
//   - the DO client retries 5xx / 429 / transport errors with backoff, reusing
//     the attempt_id, times out a hung fetch, and never leaks the token.

import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  schema,
  field,
  int64,
  utf8,
  binary,
  struct,
  batchFromColumns,
  serializeBatch,
  serializeSchema,
  deserializeBatch,
  batchToScalarDict,
  type VgiBatch,
} from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { FunctionType, TableInOutPhase } from "../../types.js";
import { FunctionRegistry } from "../registry.js";
import { defineAggregate } from "../aggregate.js";
import { defineTableBufferingFunction, decodeBufferingInit, bufferingInitCache } from "../table-buffering.js";
import {
  BoundStorage,
  FrameworkNS,
  FunctionStorageSqlite,
  packIntKey,
  setStorage,
  type FunctionStorage,
  type WorkerStateKey,
} from "../storage.js";
import { FunctionStorageCfDo, SHARD_KEY_RE, type FetchLike } from "../storage-cf-do.js";
import {
  StoragePayloadError,
  deriveStoragePayloadKey,
  openPayload,
  openWith,
  sealPayload,
  sealWith,
  setStorageSealHostLocalKey,
  storagePayloadAad,
  type StorageSlot,
} from "../storage-seal.js";
import { buildVgiService } from "../../protocol/dispatch.js";
import { resolveStorageSealKey, resolveSigningKey, parseSigningKeyHex } from "../../serve-entry.js";
import { sealAttach } from "../../protocol/handlers/catalog/shared.js";
import { serializeArguments } from "../../protocol/serializers/arguments.js";
import { serializeInitRequest } from "../../protocol/serialize.js";
import {
  AggregateBindRequestSchema,
  AggregateCombineRequestSchema,
  AggregateDestructorRequestSchema,
  AggregateFinalizeRequestSchema,
  AggregateUpdateRequestSchema,
  TableBufferingCombineRequestSchema,
  TableBufferingDestructorRequestSchema,
  TableBufferingProcessRequestSchema,
} from "../../generated/vgi-protocol-schemas.js";
import type { BindRequest, InitRequest } from "../../protocol/types.js";

const enc = new TextEncoder();
const dec = new TextDecoder();
const hex = (b: Uint8Array) => Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
const unhex = (s: string) => new Uint8Array(s.match(/../g)!.map((h) => parseInt(h, 16)));
const ATT_SHARD = /^att-[0-9a-f]{32}$/;
const SECRET = "SUPERSECRET-token-8c1f";
const SIGNING_KEY = new Uint8Array(32).fill(7);

// The host-local key (no signing key) is a file under the user's runtime dir;
// keep the tests off it.
beforeAll(() => setStorageSealHostLocalKey(async () => new Uint8Array(32).fill(9)));
afterAll(() => setStorageSealHostLocalKey(null));

// ---------------------------------------------------------------------------
// Sealed framework payload v1
// ---------------------------------------------------------------------------

const SLOT: StorageSlot = {
  shardKey: "att-0123",
  scopeId: new Uint8Array(16).fill(1),
  ns: enc.encode("_vgi/buffering_init"),
  key: enc.encode("k"),
};

describe("sealed framework payload v1", () => {
  for (const [label, key] of [["signing key", SIGNING_KEY], ["host-local", undefined]] as const) {
    test(`round trip (${label})`, async () => {
      const sealed = await sealPayload(enc.encode("secret context"), key, SLOT);
      expect(dec.decode(sealed)).not.toContain("secret context");
      expect(sealed[0]).toBe(0x01);
      expect(sealed.length).toBe(1 + 12 + "secret context".length + 16);
      expect(dec.decode(await openPayload(sealed, key, SLOT))).toBe("secret context");
    });
  }

  test("tampered byte is refused", async () => {
    const sealed = await sealPayload(enc.encode("payload"), SIGNING_KEY, SLOT);
    for (const i of [1, 13, sealed.length - 1]) {
      const t = sealed.slice();
      t[i]! ^= 1;
      await expect(openPayload(t, SIGNING_KEY, SLOT)).rejects.toThrow(/failed authentication/);
    }
  });

  const moved: Array<[string, Partial<StorageSlot>]> = [
    ["scope", { scopeId: new Uint8Array(16).fill(2) }],
    ["key", { key: enc.encode("other") }],
    ["ns", { ns: enc.encode("_vgi/aggregate_state") }],
    ["shard", { shardKey: "att-9999" }],
  ];
  for (const [what, change] of moved) {
    test(`moved to another ${what} is refused`, async () => {
      const sealed = await sealPayload(enc.encode("payload"), SIGNING_KEY, SLOT);
      await expect(openPayload(sealed, SIGNING_KEY, { ...SLOT, ...change })).rejects.toBeInstanceOf(StoragePayloadError);
    });
  }

  test("wrong key, and host-local vs signing key, are refused", async () => {
    const sealed = await sealPayload(enc.encode("payload"), SIGNING_KEY, SLOT);
    await expect(openPayload(sealed, new Uint8Array(32).fill(8), SLOT)).rejects.toBeInstanceOf(StoragePayloadError);
    await expect(openPayload(sealed, undefined, SLOT)).rejects.toBeInstanceOf(StoragePayloadError);
  });

  test("unknown version, truncation and plaintext are refused, never passed through", async () => {
    const sealed = await sealPayload(enc.encode("payload"), SIGNING_KEY, SLOT);
    const v2 = sealed.slice();
    v2[0] = 0x02;
    await expect(openPayload(v2, SIGNING_KEY, SLOT)).rejects.toThrow(/unknown seal version 0x02/);
    await expect(openPayload(sealed.subarray(0, 20), SIGNING_KEY, SLOT)).rejects.toThrow(/too short/);
    await expect(openPayload(enc.encode("plain old unsealed bytes!!!!!!!!"), SIGNING_KEY, SLOT)).rejects.toBeInstanceOf(
      StoragePayloadError,
    );
  });

  // Cross-SDK vector: the same bytes as vgi-python tests/test_storage_seal.py
  // (and the Go/Java/Rust ports). AES-256-GCM is deterministic for a fixed key
  // and nonce.
  test("cross-SDK test vector", async () => {
    const ikm = enc.encode("vgi-test-signing-key-0123456789abcdef");
    const slot: StorageSlot = {
      shardKey: "att-00112233445566778899aabbccddeeff",
      scopeId: Uint8Array.from({ length: 16 }, (_, i) => i),
      ns: enc.encode("_vgi/buffering_init"),
      key: BoundStorage.packIntKey(-1),
    };
    const nonce = Uint8Array.from({ length: 12 }, (_, i) => i);
    const plaintext = enc.encode("vgi sealed framework payload v1");
    expect(hex(await deriveStoragePayloadKey(ikm))).toBe("f5f759ad2009e91cf4b963a20b524a800224d2ac4e01472396dc1a2dab78bea9");
    expect(hex(storagePayloadAad(slot))).toBe(
      "7667692e73746f726167652e7061796c6f61642e7631000000246174742d30303131323233333434353536363737383839396161" +
        "6262636364646565666600000010000102030405060708090a0b0c0d0e0f000000135f7667692f627566666572696e675f696e6974" +
        "00000008ffffffffffffffff",
    );
    const wireHex =
      "01000102030405060708090a0bc79698647483a87eb533cf7332d1c527275cc7470dc6a3bdf1f72f624a97e246416d4de55281bd0eee" +
      "6d6491796a40";
    expect(hex(await sealWith(plaintext, ikm, slot, nonce))).toBe(wireHex);
    expect(dec.decode(await openWith(unhex(wireHex), ikm, slot))).toBe("vgi sealed framework payload v1");
  });
});

// ---------------------------------------------------------------------------
// Storage-seal ikm = the UTF-8 bytes of VGI_SIGNING_KEY as configured
// ---------------------------------------------------------------------------

describe("storage-seal ikm resolution", () => {
  const HEX64 = "0123456789abcdef".repeat(4);
  const noWarn = () => {};

  test("VGI_SIGNING_KEY (64 hex chars) seals with ikm = those 64 ASCII bytes, not the decoded 32", () => {
    const env = { VGI_SIGNING_KEY: HEX64 };
    const tokenKey = resolveSigningKey(undefined, env, noWarn);
    expect(tokenKey.length).toBe(32); // state tokens / envelopes: unchanged
    const ikm = resolveStorageSealKey(undefined, env, tokenKey);
    expect(ikm.length).toBe(64);
    expect(dec.decode(ikm)).toBe(HEX64);
  });

  test("explicit string -> its UTF-8; explicit bytes -> unchanged; generated -> its bytes", () => {
    const decoded = parseSigningKeyHex(HEX64);
    expect(dec.decode(resolveStorageSealKey(HEX64, {}, decoded))).toBe(HEX64);
    const bytes = new Uint8Array(32).fill(3);
    expect(resolveStorageSealKey(bytes, { VGI_SIGNING_KEY: HEX64 }, bytes)).toBe(bytes);
    const minted = new Uint8Array(32).fill(4);
    expect(resolveStorageSealKey(undefined, {}, minted)).toBe(minted);
  });

  // Cross-checked against vgi-python 0.53.3:
  //   vgi._storage_seal.seal_with(b"aggregate bind context", b"0123456789abcdef"*4,
  //     shard_key="att-"+"ab"*16, scope_id=bytes(range(16)),
  //     ns=b"_vgi/aggregate_state", key=BoundStorage.pack_int_key(-2),
  //     nonce=bytes(range(12, 24)))
  test("a payload sealed under a 64-hex VGI_SIGNING_KEY matches vgi-python byte for byte", async () => {
    const ikm = resolveStorageSealKey(undefined, { VGI_SIGNING_KEY: HEX64 }, parseSigningKeyHex(HEX64));
    expect(hex(await deriveStoragePayloadKey(ikm))).toBe("bcc6b0475564abef4aacbbd08c02a68829f72b91dd31733801a81ed3a8de0006");
    const slot: StorageSlot = {
      shardKey: "att-" + "ab".repeat(16),
      scopeId: Uint8Array.from({ length: 16 }, (_, i) => i),
      ns: enc.encode("_vgi/aggregate_state"),
      key: packIntKey(-2),
    };
    expect(hex(slot.key)).toBe("feffffffffffffff");
    const wire = await sealWith(enc.encode("aggregate bind context"), ikm, slot, Uint8Array.from({ length: 12 }, (_, i) => i + 12));
    expect(hex(wire)).toBe(
      "010c0d0e0f1011121314151617fe93c8ad50bab6fed0c3f2f00bbd5fe0b1f1b5bd2a0fb56d53a2bb93b5036d03d0be3f10ca11",
    );
  });

  test("buildVgiService seals with storageSealKey, not the decoded signing key", async () => {
    const calls: Call[] = [];
    const sqlite = new FunctionStorageSqlite(":memory:");
    setStorage(new CountingStorage(sqlite, calls));
    const tokenKey = parseSigningKeyHex(HEX64);
    const sealKey = enc.encode(HEX64);
    const service: any = buildVgiService({ registry, signingKey: tokenKey, storageSealKey: sealKey });
    const a = await sealAttach(enc.encode("catalog-bytes"), undefined, tokenKey);
    const bound = unwrap(
      await service.aggregateBind(
        req(AggregateBindRequestSchema, {
          function_name: "hardening_sum",
          arguments: serializeArguments(new Arguments()),
          input_schema: serializeSchema(INPUT),
          attach_opaque_data: a,
        }),
        {},
      ),
    );
    const exec = bound.execution_id as Uint8Array;
    const raw = (await sqlite.stateGet(exec, FrameworkNS.AGGREGATE_STATE, packIntKey(-2)))!;
    const slot = new BoundStorage(sqlite, exec, null, calls[0]!.shard);
    await expect(slot.openSealed(FrameworkNS.AGGREGATE_STATE, packIntKey(-2), raw, sealKey)).resolves.toBeInstanceOf(Uint8Array);
    await expect(slot.openSealed(FrameworkNS.AGGREGATE_STATE, packIntKey(-2), raw, tokenKey)).rejects.toBeInstanceOf(StoragePayloadError);
  });
});

// ---------------------------------------------------------------------------
// Counting / shard-recording FunctionStorage
// ---------------------------------------------------------------------------

interface Call {
  op: string;
  shard: string;
}

class CountingStorage implements FunctionStorage {
  constructor(
    readonly inner: FunctionStorage,
    readonly calls: Call[] = [],
    readonly shard: string = "<unrouted>",
  ) {}
  forShard(shardKey: string): FunctionStorage {
    const inner = typeof this.inner.forShard === "function" ? this.inner.forShard(shardKey) : this.inner;
    return new CountingStorage(inner, this.calls, shardKey);
  }
  private rec(op: string) {
    this.calls.push({ op, shard: this.shard });
  }
  workerPut(e: Uint8Array, w: WorkerStateKey, s: Uint8Array) { this.rec("workerPut"); return this.inner.workerPut(e, w, s); }
  workerCollect(e: Uint8Array) { this.rec("workerCollect"); return this.inner.workerCollect(e); }
  workerScan(e: Uint8Array) { this.rec("workerScan"); return this.inner.workerScan(e); }
  queuePush(e: Uint8Array, i: Uint8Array[]) { this.rec("queuePush"); return this.inner.queuePush(e, i); }
  queuePop(e: Uint8Array) { this.rec("queuePop"); return this.inner.queuePop(e); }
  queueClear(e: Uint8Array) { this.rec("queueClear"); return this.inner.queueClear(e); }
  stateGet(s: Uint8Array, n: Uint8Array, k: Uint8Array) { this.rec("stateGet"); return this.inner.stateGet(s, n, k); }
  statePut(s: Uint8Array, n: Uint8Array, k: Uint8Array, v: Uint8Array) { this.rec("statePut"); return this.inner.statePut(s, n, k, v); }
  stateGetMany(s: Uint8Array, n: Uint8Array, k: Uint8Array[]) { this.rec("stateGetMany"); return this.inner.stateGetMany(s, n, k); }
  statePutMany(s: Uint8Array, n: Uint8Array, i: Array<[Uint8Array, Uint8Array]>) { this.rec("statePutMany"); return this.inner.statePutMany(s, n, i); }
  stateAppend(s: Uint8Array, n: Uint8Array, k: Uint8Array, i: Uint8Array) { this.rec("stateAppend"); return this.inner.stateAppend(s, n, k, i); }
  stateLogScan(s: Uint8Array, n: Uint8Array, k: Uint8Array, a?: number, l?: number | null) { this.rec("stateLogScan"); return this.inner.stateLogScan(s, n, k, a, l); }
  executionClear(s: Uint8Array) { this.rec("executionClear"); return this.inner.executionClear(s); }
}

// ---------------------------------------------------------------------------
// Minimal in-memory Cloudflare DO mock: enforces the server's shard_key rule
// (400 otherwise) and routes state by (shard, scope, ns, key).
// ---------------------------------------------------------------------------

function doMock(): { fetch: FetchLike; shards: Set<string>; rejected: number } {
  const out = { shards: new Set<string>(), rejected: 0 } as { fetch: FetchLike; shards: Set<string>; rejected: number };
  const kv = new Map<string, string>();
  const log = new Map<string, Array<{ id: number; value: string }>>();
  let seq = 0;
  const json = (status: number, body: unknown) =>
    new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
  out.fetch = async (url, init) => {
    const endpoint = url.split("/").pop()!;
    const b = JSON.parse(String(init?.body));
    if (typeof b.shard_key !== "string" || !SHARD_KEY_RE.test(b.shard_key)) {
      out.rejected++;
      return json(400, { error: "bad_request", message: "shard_key is required" });
    }
    const sk = b.shard_key;
    out.shards.add(sk);
    const ck = (k: string) => `${sk}|${b.scope_id}|${b.ns}|${k}`;
    switch (endpoint) {
      case "state_put_many":
        for (const it of b.items) kv.set(ck(it.key), it.value);
        return json(200, { written: b.items.length });
      case "state_get_many":
        return json(200, { rows: b.keys.map((k: string) => (kv.has(ck(k)) ? { value: kv.get(ck(k)) } : null)) });
      case "state_append": {
        const k = ck(b.key);
        log.set(k, [...(log.get(k) ?? []), { id: ++seq, value: b.item }]);
        return json(200, { ordinal: seq });
      }
      case "state_log_scan": {
        const rows = (log.get(ck(b.key)) ?? []).filter((r) => r.id > (b.after_id ?? -1));
        return json(200, { rows: b.limit ? rows.slice(0, b.limit) : rows });
      }
      case "execution_clear": {
        const p = `${sk}|${b.scope_id}|`;
        let n = 0;
        for (const k of [...kv.keys()]) if (k.startsWith(p)) { kv.delete(k); n++; }
        for (const k of [...log.keys()]) if (k.startsWith(p)) { log.delete(k); n++; }
        return json(200, { deleted: n });
      }
      default:
        return json(404, { error: "not_found" });
    }
  };
  return out;
}

// ---------------------------------------------------------------------------
// Fixtures: a buffered sink and an aggregate, driven through the real vgi.v3
// handlers (buildVgiService) with a sealed attach, as an HTTP worker sees them.
// ---------------------------------------------------------------------------

const BUF_NS = enc.encode("rows");
const sink = defineTableBufferingFunction({
  name: "hardening_sink",
  process: async (batch: VgiBatch, p: any) => {
    // The function's own storage traffic: one append per batch.
    await p.storage.stateAppend(BUF_NS, new Uint8Array(0), serializeBatch(batch));
    // The secret really reaches process() (from the sealed init).
    if (!JSON.stringify(p.secrets).includes(SECRET)) throw new Error("secret missing in process()");
    return new Uint8Array([1]);
  },
  combine: async (ids: Uint8Array[]) => ids.slice(0, 1),
  finalize: async () => {},
} as any);

const sumAgg = defineAggregate<Record<string, never>, { total: bigint }>({
  name: "hardening_sum",
  args: { value: int64() },
  outputType: int64(),
  initialState: () => ({ total: 0n }),
  update: ({ groupIds, columns, ensureState }) => {
    for (let i = 0; i < groupIds.length; i++) {
      const v = columns[0]?.get(i);
      if (v == null) continue;
      ensureState(groupIds[i]!).total += BigInt(v);
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total }),
  finalize: ({ groupIds, states, outputSchema }) =>
    batchFromColumns({ result: groupIds.map((g) => states.get(g)?.total ?? null) }, outputSchema),
});

const registry = new FunctionRegistry();
registry.register(sink as any);
registry.register(sumAgg as any);

const SECRET_SCHEMA = schema([
  field("my_secret", struct([field("type", utf8(), true), field("name", utf8(), true), field("token", utf8(), true)]), true),
]);
const secretsBatch = () =>
  batchFromColumns({ my_secret: [{ type: "vgi_example", name: "my_secret", token: SECRET }] }, SECRET_SCHEMA);

const req = (sch: any, row: Record<string, unknown>) => ({
  request: serializeBatch(batchFromColumns(Object.fromEntries(sch.fields.map((f: any) => [f.name, [row[f.name] ?? null]])), sch)),
});
const unwrap = (r: { result: Uint8Array }) => batchToScalarDict(deserializeBatch(r.result));

const INPUT = schema([field("value", int64(), true)]);

async function runBuffered(service: any, attach: Uint8Array, batches: number) {
  const bind: BindRequest = {
    function_name: "hardening_sink",
    arguments: new Arguments(),
    function_type: FunctionType.TABLE_BUFFERING,
    input_schema: INPUT,
    settings: null,
    secrets: secretsBatch(),
    attach_opaque_data: attach,
    transaction_opaque_data: null,
    resolved_secrets_provided: true,
  };
  const init: InitRequest = {
    bind_call: bind,
    output_schema: INPUT,
    bind_opaque_data: null,
    projection_ids: null,
    pushdown_filters: null,
    join_keys: [],
    phase: TableInOutPhase.TABLE_BUFFERING,
    finalize_state_id: null,
    split_tokens: null,
    row_limit: null,
    execution_id: null,
    init_opaque_data: null,
    substream_id: null,
  } as unknown as InitRequest;
  const state = await service.init.init({ request: serializeBatch(serializeInitRequest(init)) });
  const executionId: Uint8Array = state.executionId;
  const marks: number[] = [];
  return { executionId, marks, async process(calls: Call[]) {
    for (let i = 0; i < batches; i++) {
      const before = calls.length;
      await service.tableBufferingProcess(
        req(TableBufferingProcessRequestSchema, {
          function_name: "hardening_sink",
          execution_id: executionId,
          input_batch: serializeBatch(batchFromColumns({ value: [BigInt(i), 2n] }, INPUT)),
          attach_opaque_data: attach,
        }),
        {},
      );
      marks.push(calls.length - before);
    }
    await service.tableBufferingCombine(
      req(TableBufferingCombineRequestSchema, {
        function_name: "hardening_sink",
        execution_id: executionId,
        state_ids: [new Uint8Array([1])],
        attach_opaque_data: attach,
      }),
      {},
    );
  }, async destroy() {
    await service.tableBufferingDestructor(
      req(TableBufferingDestructorRequestSchema, {
        function_name: "hardening_sink",
        execution_id: executionId,
        attach_opaque_data: attach,
      }),
      {},
    );
  } };
}

async function runAggregate(service: any, attach: Uint8Array, calls: Call[]) {
  const counts: Record<string, number> = {};
  const step = async (name: string, f: () => Promise<any>) => {
    const before = calls.length;
    const r = await f();
    counts[name] = (counts[name] ?? 0) + calls.length - before;
    return r;
  };
  const bound = unwrap(
    await step("bind", () =>
      service.aggregateBind(
        req(AggregateBindRequestSchema, {
          function_name: "hardening_sum",
          arguments: serializeArguments(new Arguments()),
          input_schema: serializeSchema(INPUT),
          secrets: serializeBatch(secretsBatch()),
          attach_opaque_data: attach,
        }),
        {},
      ),
    ),
  );
  const executionId = bound.execution_id as Uint8Array;
  const UPD = schema([field("__vgi_group_id", int64(), false), field("value", int64(), true)]);
  for (let i = 0; i < 3; i++) {
    await step("update", () =>
      service.aggregateUpdate(
        req(AggregateUpdateRequestSchema, {
          function_name: "hardening_sum",
          execution_id: executionId,
          input_batch: serializeBatch(batchFromColumns({ __vgi_group_id: [0n, 1n, 2n, 0n], value: [1n, 2n, 3n, 4n] }, UPD)),
          attach_opaque_data: attach,
        }),
        {},
      ),
    );
  }
  const MERGE = schema([field("source_group_id", int64(), false), field("target_group_id", int64(), false)]);
  await step("combine", () =>
    service.aggregateCombine(
      req(AggregateCombineRequestSchema, {
        function_name: "hardening_sum",
        execution_id: executionId,
        merge_batch: serializeBatch(batchFromColumns({ source_group_id: [2n], target_group_id: [1n] }, MERGE)),
        attach_opaque_data: attach,
      }),
      {},
    ),
  );
  const OUT = schema([field("result", int64(), true)]);
  const fin = unwrap(
    await step("finalize", () =>
      service.aggregateFinalize(
        req(AggregateFinalizeRequestSchema, {
          function_name: "hardening_sum",
          execution_id: executionId,
          group_ids_batch: serializeBatch(batchFromColumns({ group_id: [0n, 1n, 3n] }, schema([field("group_id", int64(), false)]))),
          output_schema: serializeSchema(OUT),
          attach_opaque_data: attach,
        }),
        {},
      ),
    ),
  );
  const result = deserializeBatch(fin.result_batch as Uint8Array);
  const values = Array.from({ length: result.numRows }, (_, i) => result.getChild("result")!.get(i));
  await step("destructor", () =>
    service.aggregateDestructor(
      req(AggregateDestructorRequestSchema, {
        function_name: "hardening_sum",
        execution_id: executionId,
        group_ids_batch: serializeBatch(batchFromColumns({ group_id: [0n, 1n, 2n] }, schema([field("group_id", int64(), false)]))),
        attach_opaque_data: attach,
      }),
      {},
    ),
  );
  return { counts, values, executionId };
}

// ---------------------------------------------------------------------------
// Shard routing, secrets on disk, call counts
// ---------------------------------------------------------------------------

describe.each([
  ["HTTP (signing key)", SIGNING_KEY],
  ["stdio (no signing key)", undefined],
] as const)("FunctionStorage over %s", (_label, signingKey) => {
  let dir: string;
  let sqlite: FunctionStorageSqlite;
  let calls: Call[];
  let service: any;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "vgi-hardening-"));
    sqlite = new FunctionStorageSqlite(join(dir, "state.db"));
    calls = [];
    setStorage(new CountingStorage(sqlite, calls));
    service = buildVgiService({ registry, signingKey });
    bufferingInitCache.clear();
  });

  const attach = () => sealAttach(enc.encode("catalog-bytes"), undefined, signingKey);

  test("buffered: every call on the attach shard, init sealed, 1 call per process batch, destructor clears", async () => {
    const a = await attach();
    const run = await runBuffered(service, a, 4);
    // The init turn's only storage call: the sealed init.
    expect(calls.map((c) => c.op)).toEqual(["statePut"]);
    await run.process(calls);
    // Cached init: each process batch is the function's own append only.
    expect(run.marks).toEqual([1, 1, 1, 1]);
    const shard = calls[0]!.shard;
    expect(shard).toMatch(ATT_SHARD);
    for (const c of calls) expect(c.shard).toBe(shard);

    // A cold process (another worker, or an evicted entry): one sealed read,
    // then cached again.
    const processOnce = (attachValue: Uint8Array) =>
      service.tableBufferingProcess(
        req(TableBufferingProcessRequestSchema, {
          function_name: "hardening_sink",
          execution_id: run.executionId,
          input_batch: serializeBatch(batchFromColumns({ value: [9n] }, INPUT)),
          attach_opaque_data: attachValue,
        }),
        {},
      );
    bufferingInitCache.clear();
    let before = calls.length;
    await processOnce(a);
    expect(calls.slice(before).map((c) => c.op)).toEqual(["stateGet", "stateAppend"]);
    before = calls.length;
    await processOnce(a);
    expect(calls.slice(before).map((c) => c.op)).toEqual(["stateAppend"]);

    // Another attach is another shard. SQLite does not partition by shard, so
    // the row is found, but the shard is in the seal's AAD: it does not open.
    // (On the DO the row is simply not in that shard's Durable Object.)
    before = calls.length;
    await expect(processOnce(await attach())).rejects.toThrow(/failed authentication/);
    expect(calls[before]!.shard).not.toBe(shard);

    // The stored init is sealed: no secret, no attach, opens only for its slot.
    const raw = await sqlite.stateGet(run.executionId, FrameworkNS.BUFFERING_INIT, packIntKey(-1));
    expect(raw![0]).toBe(0x01);
    expect(dec.decode(raw!)).not.toContain(SECRET);
    const bound = new BoundStorage(sqlite, run.executionId, null, shard);
    const opened = await bound.openSealed(FrameworkNS.BUFFERING_INIT, packIntKey(-1), raw!, signingKey);
    const { request } = decodeBufferingInit(opened);
    expect(request.bind_call.attach_opaque_data ?? null).toBeNull();
    expect(request.bind_call.transaction_opaque_data ?? null).toBeNull();
    await expect(
      new BoundStorage(sqlite, run.executionId, null, "att-" + "0".repeat(32)).openSealed(
        FrameworkNS.BUFFERING_INIT, packIntKey(-1), raw!, signingKey),
    ).rejects.toBeInstanceOf(StoragePayloadError);

    // Destructor: clears the execution in its shard and evicts the cache.
    await run.destroy();
    expect(calls.at(-1)).toEqual({ op: "executionClear", shard });
    expect(bufferingInitCache.size).toBe(0);
    expect(await sqlite.stateGet(run.executionId, FrameworkNS.BUFFERING_INIT, packIntKey(-1))).toBeNull();
  });

  test("aggregate: correct result, every call on the attach shard, 2 calls per batch, context sealed", async () => {
    const { counts, values, executionId } = await runAggregate(service, await attach(), calls);
    // group 0: 3*(1+4)=15; group 1: 3*2 + combined group 2 (3*3) = 15; group 3 never updated.
    expect(values).toEqual([15n, 15n, null]);
    expect(counts).toEqual({ bind: 1, update: 6, combine: 2, finalize: 1, destructor: 1 });
    for (const c of calls) expect(c.shard).toMatch(ATT_SHARD);
    expect(new Set(calls.map((c) => c.shard)).size).toBe(1);
    // Destructor cleared everything, so re-bind and check the stored context
    // while it is live.
    expect(await sqlite.stateGet(executionId, FrameworkNS.AGGREGATE_STATE, packIntKey(-2))).toBeNull();
  });

  test("aggregate: a cold process reads the sealed context once, then caches it", async () => {
    const { aggregateContextCache } = await import("../aggregate.js");
    const a = await attach();
    const bound = unwrap(
      await service.aggregateBind(
        req(AggregateBindRequestSchema, {
          function_name: "hardening_sum",
          arguments: serializeArguments(new Arguments()),
          input_schema: serializeSchema(INPUT),
          secrets: serializeBatch(secretsBatch()),
          attach_opaque_data: a,
        }),
        {},
      ),
    );
    const executionId = bound.execution_id as Uint8Array;
    const shard = calls[0]!.shard;
    const ctx = await sqlite.stateGet(executionId, FrameworkNS.AGGREGATE_STATE, packIntKey(-2));
    expect(ctx![0]).toBe(0x01);
    expect(dec.decode(ctx!)).not.toContain(SECRET);
    // The context is sealed to its slot.
    await expect(
      new BoundStorage(sqlite, executionId, null, "att-" + "f".repeat(32)).openSealed(
        FrameworkNS.AGGREGATE_STATE, packIntKey(-2), ctx!, signingKey),
    ).rejects.toBeInstanceOf(StoragePayloadError);
    aggregateContextCache.clear();
    const UPD = schema([field("__vgi_group_id", int64(), false), field("value", int64(), true)]);
    const update = () =>
      service.aggregateUpdate(
        req(AggregateUpdateRequestSchema, {
          function_name: "hardening_sum",
          execution_id: executionId,
          input_batch: serializeBatch(batchFromColumns({ __vgi_group_id: [0n], value: [1n] }, UPD)),
          attach_opaque_data: a,
        }),
        {},
      );
    let before = calls.length;
    await update();
    expect(calls.slice(before).map((c) => c.op)).toEqual(["stateGet", "stateGetMany", "statePutMany"]);
    before = calls.length;
    await update();
    expect(calls.slice(before).map((c) => c.op)).toEqual(["stateGetMany", "statePutMany"]);
    expect(calls.every((c) => c.shard === shard)).toBe(true);
  });

  test("no secret reaches the SQLite file in plaintext", async () => {
    const a = await attach();
    const run = await runBuffered(service, a, 2);
    await run.process(calls);
    // Aggregate bind with the same secret, left live (no destructor).
    await service.aggregateBind(
      req(AggregateBindRequestSchema, {
        function_name: "hardening_sum",
        arguments: serializeArguments(new Arguments()),
        input_schema: serializeSchema(INPUT),
        secrets: serializeBatch(secretsBatch()),
        attach_opaque_data: a,
      }),
      {},
    );
    sqlite.close();
    let scanned = 0;
    for (const f of ["state.db", "state.db-wal", "state.db-shm"]) {
      const p = join(dir, f);
      if (!existsSync(p)) continue;
      scanned++;
      expect(readFileSync(p).includes(Buffer.from(SECRET))).toBe(false);
    }
    expect(scanned).toBeGreaterThan(0);
    rmSync(dir, { recursive: true, force: true });
  });
});

describe("cloudflare-do backend end to end (mock enforcing the server's shard_key rule)", () => {
  test("buffered + aggregate executions route every call to att-<uuid>; none rejected", async () => {
    const mock = doMock();
    const calls: Call[] = [];
    setStorage(new CountingStorage(new FunctionStorageCfDo({ url: "https://do.mock", token: "t", fetch: mock.fetch }), calls));
    bufferingInitCache.clear();
    const service = buildVgiService({ registry, signingKey: SIGNING_KEY });
    const a = await sealAttach(enc.encode("catalog-bytes"), undefined, SIGNING_KEY);
    const run = await runBuffered(service, a, 3);
    await run.process(calls);
    await run.destroy();
    const { values } = await runAggregate(service, a, calls);
    expect(values).toEqual([15n, 15n, null]);
    expect(mock.rejected).toBe(0);
    expect([...mock.shards]).toHaveLength(1);
    expect([...mock.shards][0]).toMatch(ATT_SHARD);
  });

  test("a call bound to no ATTACH goes to the anonymous shard, not an empty one", async () => {
    const mock = doMock();
    const s = new FunctionStorageCfDo({ url: "https://do.mock", fetch: mock.fetch });
    await s.statePut(enc.encode("e"), enc.encode("n"), enc.encode("k"), enc.encode("v"));
    expect(mock.rejected).toBe(0);
    expect([...mock.shards]).toEqual(["loc-anon"]);
  });

  test("forShard refuses a shard the server would reject", () => {
    const s = new FunctionStorageCfDo({ url: "https://do.mock" });
    expect(() => s.forShard("bad shard!")).toThrow(/invalid CF DO shard_key/);
    expect(() => s.forShard("x".repeat(129))).toThrow(/invalid CF DO shard_key/);
  });

  test("stateGetMany / statePutMany are one round trip each", async () => {
    let posts = 0;
    const mock = doMock();
    const s = new FunctionStorageCfDo({
      url: "https://do.mock",
      fetch: (u, i) => { posts++; return mock.fetch(u, i); },
    }).forShard("att-" + "a".repeat(32));
    const scope = enc.encode("scope"), ns = enc.encode("ns");
    await s.statePutMany(scope, ns, [[enc.encode("a"), enc.encode("1")], [enc.encode("b"), enc.encode("2")]]);
    const got = await s.stateGetMany(scope, ns, [enc.encode("b"), enc.encode("zz"), enc.encode("a")]);
    expect(got.map((v) => (v ? dec.decode(v) : null))).toEqual(["2", null, "1"]);
    expect(posts).toBe(2);
    await s.statePutMany(scope, ns, []);
    expect(await s.stateGetMany(scope, ns, [])).toEqual([]);
    expect(posts).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// DO client: retries, backoff, timeout, token hygiene
// ---------------------------------------------------------------------------

describe("FunctionStorageCfDo retry and timeout", () => {
  const TOKEN = "tok-DO-NOT-LEAK-123";
  function client(fetchImpl: FetchLike, retry: Record<string, number> = {}) {
    const sleeps: number[] = [];
    const s = new FunctionStorageCfDo({
      url: "https://do.mock",
      token: TOKEN,
      fetch: fetchImpl,
      retry: { maxAttempts: 4, baseDelayMs: 100, maxDelayMs: 1000, ...retry },
      sleep: async (ms) => { sleeps.push(ms); },
    }).forShard("att-" + "b".repeat(32));
    return { s, sleeps };
  }
  const ok = (body: unknown) => new Response(JSON.stringify(body), { status: 200 });

  test("retries 503 / 429 / transport errors, reusing the attempt_id, then succeeds", async () => {
    const bodies: any[] = [];
    const script = [
      () => new Response("busy", { status: 503 }),
      () => new Response(JSON.stringify({ error: "rate" }), { status: 429, headers: { "Retry-After": "0" } }),
      () => { throw new TypeError("connection reset"); },
      () => ok({ written: 1 }),
    ];
    let n = 0;
    const { s, sleeps } = client(async (_u, init) => {
      bodies.push(JSON.parse(String(init?.body)));
      return script[n++]!();
    });
    await s.statePut(enc.encode("e"), enc.encode("n"), enc.encode("k"), enc.encode("v"));
    expect(bodies).toHaveLength(4);
    expect(new Set(bodies.map((b) => b.attempt_id)).size).toBe(1);
    expect(bodies[0].attempt_id).toMatch(/^[0-9a-f]{32}$/);
    expect(sleeps).toHaveLength(3);
    // Exponential cap with full jitter: attempt k waits in [0, base*2^(k-1)].
    expect(sleeps[0]!).toBeLessThanOrEqual(100);
    expect(sleeps[1]!).toBeLessThanOrEqual(200);
    expect(sleeps[2]!).toBeLessThanOrEqual(400);
  });

  test("gives up after maxAttempts with a message that does not contain the token", async () => {
    let n = 0;
    const { s } = client(async () => { n++; return new Response("down", { status: 502 }); }, { maxAttempts: 3 });
    const err = (await s.stateGet(enc.encode("e"), enc.encode("n"), enc.encode("k")).then(
      () => new Error("expected a failure"),
      (e: unknown) => e as Error,
    ));
    expect(n).toBe(3);
    expect(String(err.message)).toMatch(/failed after 3 attempts.*502/);
    expect(String(err.message)).not.toContain(TOKEN);
    expect(String(err.stack ?? "")).not.toContain(TOKEN);
  });

  test("4xx other than 429 is not retried", async () => {
    let n = 0;
    const { s } = client(async () => { n++; return new Response(JSON.stringify({ error: "bad_request" }), { status: 400 }); });
    await expect(s.stateGet(enc.encode("e"), enc.encode("n"), enc.encode("k"))).rejects.toThrow(/400/);
    expect(n).toBe(1);
  });

  test("a hung fetch is aborted by the timeout and retried", async () => {
    let n = 0;
    const { s } = client(
      (_u, init) => {
        n++;
        if (n === 1) {
          return new Promise<Response>((_, reject) => {
            init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
          });
        }
        return Promise.resolve(ok({ rows: [null] }));
      },
      { timeoutMs: 20 },
    );
    const t0 = Date.now();
    expect(await s.stateGet(enc.encode("e"), enc.encode("n"), enc.encode("k"))).toBeNull();
    expect(n).toBe(2);
    expect(Date.now() - t0).toBeLessThan(5000);
  });

  test("sends the bearer token in the header only", async () => {
    let seen: any;
    const { s } = client(async (_u, init) => { seen = init; return ok({ rows: [null] }); });
    await s.stateGet(enc.encode("e"), enc.encode("n"), enc.encode("k"));
    expect(seen.headers.Authorization).toBe(`Bearer ${TOKEN}`);
    expect(String(seen.body)).not.toContain(TOKEN);
  });
});

// ---------------------------------------------------------------------------
// Buffered init framing is bounds-checked
// ---------------------------------------------------------------------------

describe("buffered init framing", () => {
  test("truncated / overlong / trailing payloads are refused", () => {
    const u32 = (n: number) => { const b = new Uint8Array(4); new DataView(b.buffer).setUint32(0, n, true); return b; };
    const cat = (...p: Uint8Array[]) => { const o = new Uint8Array(p.reduce((a, x) => a + x.length, 0)); let i = 0; for (const x of p) { o.set(x, i); i += x.length; } return o; };
    expect(() => decodeBufferingInit(new Uint8Array(2))).toThrow(StoragePayloadError);
    expect(() => decodeBufferingInit(cat(u32(1000), new Uint8Array(3)))).toThrow(/truncated init payload/);
    const resp = cat(u32(0), u32(0), u32(8), new Uint8Array(8));
    expect(() => decodeBufferingInit(cat(u32(0), u32(resp.length), resp, new Uint8Array([1])))).toThrow(/trailing bytes/);
  });
});

// Keep unused-import linting quiet for helpers only some backends exercise.
void binary;
