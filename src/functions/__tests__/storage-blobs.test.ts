// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// ExternalizingStorage (./storage-blobs.ts): the value envelope, byte for byte
// against the cross-SDK vectors (vgi test/vectors/storage_envelope_v1.json),
// and blob-vendor externalization against the reference fake vendor.
//
// The vendor groups use VGI_BLOB_VENDOR_URL when it points at a fake vendor
// started with --test-routes (the CI vendor lane runs this file inside its
// lane), else start one with the reference Python (VGI_RPC_PYTHON_BIN, or
// python3) when vgi_rpc.conformance.fake_vendor is importable, else skip,
// loudly. VGI_VENDOR_TESTS_REQUIRED=1 turns the skip into a failure.

import { afterAll, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { VendorBlobClient } from "@query-farm/vgi-rpc";
import {
  blobExecutionId,
  blobShard,
  decodePointer,
  encodeInline,
  encodePointer,
  envelopeTag,
  ExternalizingStorage,
  StorageBlobIntegrityError,
  StorageBlobMissingError,
  StorageEnvelopeError,
  StorageValueTooLargeError,
  wrapStorageFromEnv,
} from "../storage-blobs.js";
import { BoundStorage, FunctionStorageSqlite, type FunctionStorage } from "../storage.js";
import { FunctionStorageCfDo, MAX_VALUE_BYTES, type FetchLike } from "../storage-cf-do.js";

const enc = new TextEncoder();
const hex = (b: Uint8Array) => Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
const unhex = (s: string) => new Uint8Array((s.match(/../g) ?? []).map((h) => Number.parseInt(h, 16)));

// ---------------------------------------------------------------------------
// Envelope vectors
// ---------------------------------------------------------------------------

const VECTORS_PATH = join(import.meta.dir, "fixtures", "storage_envelope_v1.json");
const VECTORS = JSON.parse(readFileSync(VECTORS_PATH, "utf8"));

describe("storage envelope v1 vectors", () => {
  test("the vendored copy matches the vgi checkout's, when it is a sibling", () => {
    const sibling = join(import.meta.dir, "..", "..", "..", "..", "vgi", "test", "vectors", "storage_envelope_v1.json");
    if (!existsSync(sibling)) return;
    expect(readFileSync(VECTORS_PATH, "utf8")).toBe(readFileSync(sibling, "utf8"));
  });

  test("constants", () => {
    expect(VECTORS.format).toBe("vgi.storage_envelope.v1");
    expect(VECTORS.tag_inline).toBe(0);
    expect(VECTORS.tag_pointer).toBe(1);
    expect(VECTORS.pointer_fixed_bytes).toBe(41);
    expect(VECTORS.default_externalize_bytes).toBe(512 * 1024);
  });

  for (const c of VECTORS.inline) {
    test(`inline: ${c.name}`, () => {
      const env = encodeInline(unhex(c.value_hex));
      expect(hex(env)).toBe(c.envelope_hex);
      expect(envelopeTag(env)).toBe(0);
    });
  }

  for (const c of VECTORS.pointers) {
    test(`pointer: ${c.name}`, () => {
      const env = encodePointer({ objectId: c.object_id, size: c.size, sha256: c.sha256 });
      expect(hex(env)).toBe(c.envelope_hex);
      expect(envelopeTag(env)).toBe(1);
      expect(decodePointer(unhex(c.envelope_hex))).toEqual({ objectId: c.object_id, size: c.size, sha256: c.sha256 });
    });
  }

  for (const c of VECTORS.malformed) {
    test(`malformed (${c.error}): ${c.name}`, () => {
      const env = unhex(c.envelope_hex);
      const decode = () => (envelopeTag(env) === 1 ? decodePointer(env) : env);
      expect(decode).toThrow(StorageEnvelopeError);
      const message = (() => {
        try {
          decode();
        } catch (e) {
          return (e as Error).message;
        }
        return "";
      })();
      if (c.error === "empty") expect(message).toMatch(/empty stored value/);
      if (c.error === "unknown_tag") expect(message).toMatch(/unknown envelope tag/);
      if (c.error === "malformed_pointer") expect(message).toMatch(/malformed blob pointer/);
    });
  }

  for (const c of VECTORS.scope_execution_ids.cases) {
    test(`execution id ${c.kind}/${c.ident_hex.length / 2} bytes`, async () => {
      expect(await blobExecutionId(c.kind, unhex(c.ident_hex))).toBe(c.execution_id);
    });
  }

  for (const c of VECTORS.scope_shards.cases) {
    test(`shard ${JSON.stringify(c.shard_key).slice(0, 40)}`, async () => {
      expect(await blobShard(c.shard_key)).toBe(c.shard);
    });
  }

  test("encodePointer refuses an object id outside the charset", () => {
    expect(() => encodePointer({ objectId: "a/b", size: 1, sha256: "00".repeat(32) })).toThrow(/unusable object id/);
  });
});

// ---------------------------------------------------------------------------
// Without a vendor
// ---------------------------------------------------------------------------

const SCOPE = enc.encode("scope-1");
const NS = enc.encode("ns");

describe("ExternalizingStorage without a vendor", () => {
  test("every value is enveloped inline in the backend", async () => {
    const base = new FunctionStorageSqlite(":memory:");
    const s = new ExternalizingStorage(base, { blobClient: null });
    await s.statePut(SCOPE, NS, enc.encode("k"), enc.encode("hello"));
    expect(hex((await base.stateGet(SCOPE, NS, enc.encode("k")))!)).toBe("00" + hex(enc.encode("hello")));
    expect(await s.stateGet(SCOPE, NS, enc.encode("k"))).toEqual(enc.encode("hello"));
    // A user value that starts with the pointer tag is safe.
    await s.statePut(SCOPE, NS, enc.encode("t"), new Uint8Array([1, 2, 3]));
    expect(await s.stateGet(SCOPE, NS, enc.encode("t"))).toEqual(new Uint8Array([1, 2, 3]));
  });

  test("a raw (unenveloped) value is refused", async () => {
    const base = new FunctionStorageSqlite(":memory:");
    await base.statePut(SCOPE, NS, enc.encode("raw"), enc.encode("\x05abc"));
    const s = new ExternalizingStorage(base, { blobClient: null });
    await expect(s.stateGet(SCOPE, NS, enc.encode("raw"))).rejects.toThrow(StorageEnvelopeError);
  });

  test("a pointer read with no vendor configured is an envelope error", async () => {
    const base = new FunctionStorageSqlite(":memory:");
    await base.statePut(SCOPE, NS, enc.encode("p"), encodePointer({ objectId: "ab", size: 3, sha256: "00".repeat(32) }));
    const s = new ExternalizingStorage(base, { blobClient: null });
    await expect(s.stateGet(SCOPE, NS, enc.encode("p"))).rejects.toThrow(/VGI_BLOB_VENDOR_URL is not configured/);
  });

  test("a backend cap refuses an over-cap value, naming the vendor", async () => {
    const s = new ExternalizingStorage(new FunctionStorageSqlite(":memory:"), {
      blobClient: null,
      threshold: 8,
      maxValueBytes: 8,
    });
    await s.statePut(SCOPE, NS, enc.encode("ok"), new Uint8Array(7));
    await expect(s.statePut(SCOPE, NS, enc.encode("big"), new Uint8Array(8))).rejects.toThrow(StorageValueTooLargeError);
    await expect(s.statePut(SCOPE, NS, enc.encode("big"), new Uint8Array(8))).rejects.toThrow(/VGI_BLOB_VENDOR_URL/);
  });

  test("threshold must fit the cap", () => {
    expect(() => new ExternalizingStorage(new FunctionStorageSqlite(":memory:"), { blobClient: null, threshold: 0 })).toThrow();
    expect(
      () =>
        new ExternalizingStorage(new FunctionStorageSqlite(":memory:"), { blobClient: null, threshold: 9, maxValueBytes: 8 }),
    ).toThrow(/between 1 and 8/);
  });

  test("worker state, queue and log values round-trip enveloped", async () => {
    const s = new ExternalizingStorage(new FunctionStorageSqlite(":memory:"), { blobClient: null });
    const exec = enc.encode("exec");
    await s.workerPut(exec, 1, enc.encode("w1"));
    expect((await s.workerScan(exec)).map(([, v]) => new TextDecoder().decode(v))).toEqual(["w1"]);
    expect((await s.workerCollect(exec)).map((v) => new TextDecoder().decode(v))).toEqual(["w1"]);
    await s.queuePush(exec, [enc.encode("a"), new Uint8Array()]);
    expect(await s.queuePop(exec)).toEqual(enc.encode("a"));
    expect(await s.queuePop(exec)).toEqual(new Uint8Array());
    expect(await s.queuePop(exec)).toBeNull();
    await s.stateAppend(SCOPE, NS, enc.encode("log"), enc.encode("x"));
    expect((await s.stateLogScan(SCOPE, NS, enc.encode("log"))).map(([, v]) => v)).toEqual([enc.encode("x")]);
    await s.statePutMany(SCOPE, NS, [[enc.encode("d"), enc.encode("v")]]);
    expect(await s.stateDelete(SCOPE, NS, [enc.encode("d")])).toBe(1);
    expect(await s.stateGet(SCOPE, NS, enc.encode("d"))).toBeNull();
  });

  test("wrapStorageFromEnv", () => {
    const base = new FunctionStorageSqlite(":memory:");
    expect(wrapStorageFromEnv(base, { env: {} })).toBe(base);
    expect(() => wrapStorageFromEnv(base, { env: { VGI_STORAGE_EXTERNALIZE: "1" } })).toThrow(/needs the blob vendor/);
    const capped = wrapStorageFromEnv(base, { env: {}, maxValueBytes: MAX_VALUE_BYTES });
    expect(capped).toBeInstanceOf(ExternalizingStorage);
    expect((capped as ExternalizingStorage).threshold).toBe(512 * 1024);
    expect((capped as ExternalizingStorage).blobClient).toBeNull();
    const wrapped = wrapStorageFromEnv(base, {
      env: {
        VGI_STORAGE_EXTERNALIZE: "1",
        VGI_BLOB_VENDOR_URL: "http://127.0.0.1:1",
        VGI_BLOB_VENDOR_TOKEN: "t",
        VGI_STORAGE_EXTERNALIZE_BYTES: "4096",
      },
    }) as ExternalizingStorage;
    expect(wrapped.threshold).toBe(4096);
    expect(wrapped.blobClient).toBeInstanceOf(VendorBlobClient);
    expect(wrapStorageFromEnv(wrapped, { env: {} })).toBe(wrapped);
    expect(() =>
      wrapStorageFromEnv(base, { env: { VGI_STORAGE_EXTERNALIZE: "1", VGI_BLOB_VENDOR_URL: "http://127.0.0.1:1" } }),
    ).toThrow(/VGI_BLOB_VENDOR_TOKEN/);
    expect(() =>
      wrapStorageFromEnv(base, { env: { VGI_CF_DO_EXTERNALIZE_BYTES: "0" }, maxValueBytes: MAX_VALUE_BYTES }),
    ).toThrow(/positive/);
  });
});

// ---------------------------------------------------------------------------
// A minimal in-memory Durable Object (FetchLike), for the cloudflare-do path.
// ---------------------------------------------------------------------------

function memoryDo(): { fetch: FetchLike; shards: Set<string>; values: () => Uint8Array[] } {
  const kv = new Map<string, string>();
  const shards = new Set<string>();
  const json = (body: unknown) => new Response(JSON.stringify(body), { headers: { "Content-Type": "application/json" } });
  const b64 = (s: string) => Uint8Array.from(atob(s), (c) => c.charCodeAt(0));
  const fetch: FetchLike = async (url, init) => {
    const endpoint = url.split("/").pop();
    const b = JSON.parse(String(init?.body));
    shards.add(b.shard_key);
    const key = (k: string) => `${b.shard_key}\x1f${b.scope_id}\x1f${b.ns}\x1f${k}`;
    switch (endpoint) {
      case "state_put_many":
        for (const it of b.items) kv.set(key(it.key), it.value);
        return json({ written: b.items.length });
      case "state_get_many":
        return json({ rows: b.keys.map((k: string) => (kv.has(key(k)) ? { value: kv.get(key(k)) } : null)) });
      case "state_delete": {
        let deleted = 0;
        for (const k of b.keys) if (kv.delete(key(k))) deleted++;
        return json({ deleted });
      }
      default:
        return new Response(JSON.stringify({ error: "not found" }), { status: 404 });
    }
  };
  return { fetch, shards, values: () => [...kv.values()].map(b64) };
}

describe("cloudflare-do", () => {
  test("setStorage-style wrapping: a bare FunctionStorageCfDo is always enveloped", async () => {
    const mock = memoryDo();
    const cf = new FunctionStorageCfDo({ url: "https://do.example", fetch: mock.fetch });
    expect(cf.storageKind).toBe("cloudflare-do");
    const wrapped = wrapStorageFromEnv(cf, { env: {}, maxValueBytes: cf.maxValueBytes }) as ExternalizingStorage;
    const bound = new BoundStorage(wrapped, enc.encode("exec"), null, "att-" + "ab".repeat(16));
    await bound.statePut(NS, enc.encode("k"), enc.encode("v"));
    expect(await bound.stateGet(NS, enc.encode("k"))).toEqual(enc.encode("v"));
    expect(mock.values().map(hex)).toEqual(["00" + hex(enc.encode("v"))]);
    expect([...mock.shards]).toEqual(["att-" + "ab".repeat(16)]);
    expect(await bound.stateDelete(NS, [enc.encode("k")])).toBe(1);
    expect(await bound.stateGet(NS, enc.encode("k"))).toBeNull();
    // Without a vendor a value over the DO's 1 MiB cap is refused client-side.
    await expect(bound.statePut(NS, enc.encode("big"), new Uint8Array(MAX_VALUE_BYTES))).rejects.toThrow(
      StorageValueTooLargeError,
    );
  });
});

// ---------------------------------------------------------------------------
// Against the reference fake vendor
// ---------------------------------------------------------------------------

const PYTHON = process.env.VGI_RPC_PYTHON_BIN ?? "python3";
const REQUIRED = process.env.VGI_VENDOR_TESTS_REQUIRED === "1";
const TOKEN = process.env.VGI_BLOB_VENDOR_TOKEN || "test-token";

async function laneVendorUrl(): Promise<string | null> {
  const url = process.env.VGI_BLOB_VENDOR_URL?.trim();
  if (!url) return null;
  try {
    const r = await fetch(`${url}/__stats__`);
    await r.text();
    return r.ok ? url : null;
  } catch {
    return null;
  }
}

let vendorUrl = await laneVendorUrl();
let canSpawn = false;
if (!vendorUrl) {
  try {
    canSpawn = Bun.spawnSync([PYTHON, "-c", "import vgi_rpc.conformance.fake_vendor"]).exitCode === 0;
  } catch {}
}
const HAVE_VENDOR = vendorUrl !== null || canSpawn;
if (!HAVE_VENDOR) {
  const why = `no fake vendor: VGI_BLOB_VENDOR_URL unset and vgi_rpc.conformance.fake_vendor not importable with ${PYTHON}`;
  if (REQUIRED) throw new Error(`VGI_VENDOR_TESTS_REQUIRED=1 but ${why}`);
  console.warn(`SKIP ExternalizingStorage vendor tests: ${why}`);
}

let proc: ReturnType<typeof Bun.spawn> | undefined;

async function stats(): Promise<Record<string, number>> {
  return JSON.parse(await (await fetch(`${vendorUrl}/__stats__`)).text());
}

async function admin(path: string): Promise<void> {
  const r = await fetch(`${vendorUrl}/__admin__/${path}`);
  expect(r.status).toBe(200);
  await r.text();
}

function wrapped(base: FunctionStorage = new FunctionStorageSqlite(":memory:"), threshold = 4096): ExternalizingStorage {
  return new ExternalizingStorage(base, { blobClient: new VendorBlobClient(vendorUrl!, TOKEN), threshold });
}

function fill(n: number, byte: number): Uint8Array {
  return new Uint8Array(n).fill(byte);
}

describe.skipIf(!HAVE_VENDOR)("ExternalizingStorage with the fake vendor", () => {
  beforeAll(async () => {
    if (vendorUrl) return;
    proc = Bun.spawn(
      [PYTHON, "-c", "from vgi_rpc.conformance.fake_vendor import main; main()", "--test-routes", "--token", `${TOKEN}=tenant-a`],
      { stdout: "pipe", stderr: "inherit" },
    );
    const reader = (proc.stdout as ReadableStream<Uint8Array>).getReader();
    let text = "";
    while (!/URL:\S+/.test(text)) {
      const { done, value } = await reader.read();
      if (done) throw new Error(`fake vendor exited: ${text}`);
      text += new TextDecoder().decode(value);
    }
    reader.releaseLock();
    vendorUrl = /URL:(\S+)/.exec(text)![1]!;
  });

  afterAll(() => {
    proc?.kill();
  });

  beforeEach(async () => {
    await admin("reset");
  });

  test("threshold boundary: one byte below stays inline, at and above become blobs", async () => {
    const base = new FunctionStorageSqlite(":memory:");
    const s = wrapped(base);
    const before = await stats();
    const items: Array<[Uint8Array, Uint8Array]> = [
      [enc.encode("below"), fill(4095, 0x61)],
      [enc.encode("at"), fill(4096, 0x62)],
      [enc.encode("above"), fill(4097, 0x63)],
    ];
    await s.statePutMany(SCOPE, NS, items);
    const mid = await stats();
    expect(mid.puts! - before.puts!).toBe(2);
    expect(mid.bytes_uploaded! - before.bytes_uploaded!).toBe(4096 + 4097);
    const raw = await base.stateGetMany(SCOPE, NS, items.map(([k]) => k));
    expect(raw.map((r) => r![0])).toEqual([0, 1, 1]);
    expect(raw[1]!.length).toBe(41 + 32);
    const back = await s.stateGetMany(SCOPE, NS, items.map(([k]) => k));
    expect(back).toEqual(items.map(([, v]) => v));
    const after = await stats();
    expect(after.gets! - mid.gets!).toBe(2);
    expect(after.bytes_downloaded! - mid.bytes_downloaded!).toBe(4096 + 4097);
  });

  test("the threshold counts UTF-8 bytes", async () => {
    const base = new FunctionStorageSqlite(":memory:");
    const s = wrapped(base);
    const value = enc.encode("é".repeat(2048)); // 2048 characters, 4096 bytes
    await s.statePut(SCOPE, NS, enc.encode("utf8"), value);
    expect((await base.stateGet(SCOPE, NS, enc.encode("utf8")))![0]).toBe(1);
    expect(await s.stateGet(SCOPE, NS, enc.encode("utf8"))).toEqual(value);
  });

  test("a corrupted blob fails the integrity contract; a missing one the missing contract", async () => {
    const s = wrapped();
    await s.statePut(SCOPE, NS, enc.encode("c"), fill(5000, 7));
    await admin("fault?purpose=fs&action=corrupt&count=1");
    const corrupt = await s.stateGet(SCOPE, NS, enc.encode("c")).catch((e) => e);
    expect(corrupt).toBeInstanceOf(StorageBlobIntegrityError);
    expect(corrupt.message).toMatch(/storage blob.*(sha256|size) mismatch/s);
    await admin("reset");
    await s.statePut(SCOPE, NS, enc.encode("m"), fill(5000, 8));
    await admin("fault?purpose=fs&action=missing&count=1");
    const missing = await s.stateGet(SCOPE, NS, enc.encode("m")).catch((e) => e);
    expect(missing).toBeInstanceOf(StorageBlobMissingError);
    expect(missing.message).toMatch(/storage blob.*(missing|not found)/s);
    await admin("reset");
    expect(await s.stateGet(SCOPE, NS, enc.encode("c"))).toEqual(fill(5000, 7));
  });

  test("a deleted blob reads as missing", async () => {
    const base = new FunctionStorageSqlite(":memory:");
    const s = wrapped(base);
    await s.statePut(SCOPE, NS, enc.encode("gone"), fill(4096, 1));
    const ref = decodePointer((await base.stateGet(SCOPE, NS, enc.encode("gone")))!);
    await s.blobClient!.delete([ref]);
    await expect(s.stateGet(SCOPE, NS, enc.encode("gone"))).rejects.toThrow(/storage blob .* is missing/);
  });

  test("queue items, log items and worker state externalize; clean-up deletes them", async () => {
    const s = wrapped();
    const exec = enc.encode("exec-q");
    const before = await stats();
    await s.queuePush(exec, [fill(5000, 1), fill(10, 2)]);
    expect(await s.queuePop(exec)).toEqual(fill(5000, 1));
    expect(await s.queuePop(exec)).toEqual(fill(10, 2));
    await s.stateAppend(exec, NS, enc.encode("log"), fill(6000, 3));
    expect((await s.stateLogScan(exec, NS, enc.encode("log")))[0]![1]).toEqual(fill(6000, 3));
    await s.workerPut(exec, enc.encode("substream"), fill(7000, 4));
    expect(await s.workerCollect(exec)).toEqual([fill(7000, 4)]);
    await s.executionClear(exec);
    await s.queueClear(exec);
    await s.flushBlobCleanup();
    const after = await stats();
    expect(after.puts! - before.puts!).toBe(3);
    // queuePop and workerCollect delete what they claimed; the two clears delete their scopes.
    expect(after.deletes! - before.deletes!).toBe(2);
    expect(after.delete_scopes! - before.delete_scopes!).toBe(2);
  });

  test("many values in one call round-trip, uploaded in parallel", async () => {
    const s = wrapped(new FunctionStorageSqlite(":memory:"), 100);
    const items: Array<[Uint8Array, Uint8Array]> = Array.from({ length: 40 }, (_, i) => [
      enc.encode(`k${i}`),
      fill(i % 2 === 0 ? 50 : 150 + i, i),
    ]);
    await s.statePutMany(SCOPE, NS, items);
    expect(await s.stateGetMany(SCOPE, NS, items.map(([k]) => k))).toEqual(items.map(([, v]) => v));
  });

  test("the cloudflare-do backend externalizes through the vendor too", async () => {
    const mock = memoryDo();
    const cf = new FunctionStorageCfDo({ url: "https://do.example", fetch: mock.fetch });
    const s = new ExternalizingStorage(cf, {
      blobClient: new VendorBlobClient(vendorUrl!, TOKEN),
      threshold: 4096,
      maxValueBytes: MAX_VALUE_BYTES,
    });
    const bound = new BoundStorage(s, enc.encode("exec"), null, "att-" + "cd".repeat(16));
    const big = fill(MAX_VALUE_BYTES + 10, 9);
    await bound.statePut(NS, enc.encode("big"), big);
    expect(mock.values()[0]![0]).toBe(1);
    expect(await bound.stateGet(NS, enc.encode("big"))).toEqual(big);
    expect([...mock.shards]).toEqual(["att-" + "cd".repeat(16)]);
  });
});
