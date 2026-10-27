// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import { test, expect } from "bun:test";
import { CatalogSnapshotManager, MemoryCatalogPublicationRepository, S3CatalogPublicationRepository, type CatalogManifestObjectStore } from "../publication.js";
import { buildCatalogContentsResponse } from "../../generated/vgi-protocol-types.js";
import { encodeCatalogStreams } from "../contents-codec.js";
import { deserializeBatches as rpcBatches, serializeBatches as rpcSerialize, withBatchMetadata } from "@query-farm/vgi-rpc/arrow";

const identity = { catalogNamespace: "demo", sourceRevision: "deploy-1", visibilityScope: "tenant-a", catalogVersion: 1 };
function fixture() {
  const objects = new Map<string, Uint8Array>();
  let uploads = 0, gets = 0;
  const storage = { async upload(data: Uint8Array) { uploads++; const url = `https://storage.test/${uploads}`; objects.set(url, data.slice()); return url; } };
  const resolver = { storage, fetch: (async (input: RequestInfo | URL) => {
    gets++; const value = objects.get(String(input));
    return value ? new Response(value, { headers: { "Content-Type": "application/vnd.apache.arrow.stream" } }) : new Response(null, { status: 404 });
  }) as typeof fetch };
  const repository = new MemoryCatalogPublicationRepository("test-identity", storage, resolver, async () => ({ stable: true }));
  const policy = { delivery: "published" as const, repository, identity: () => identity };
  let builds = 0;
  const build = async () => {
    builds++;
    return buildCatalogContentsResponse({ catalog_version: 1, ...encodeCatalogStreams([{ schema: { path: ["main"], comment: null, tags: {} }, tables: [], views: [], scalar_functions: [], aggregate_functions: [], table_functions: [], scalar_macros: [], table_macros: [], indexes: [] }]) });
  };
  return { objects, storage, resolver, repository, policy, build, counters: () => ({ builds, uploads, gets }) };
}

test("published catalogs upload/read back once, recover after restart, and isolate visibility", async () => {
  const f = fixture();
  const manager = new CatalogSnapshotManager(f.policy);
  const [a,b] = await Promise.all([manager.warm(identity, f.build), manager.warm(identity, f.build)]);
  expect(a).toEqual(b);
  expect(a.protocolName).toBe("vgi.v3");
  expect(a.protocolVersion).toBe("3.0.0");
  expect(f.counters()).toEqual({ builds: 1, uploads: 1, gets: 1 });
  await manager.warm(identity, f.build);
  expect(f.counters()).toEqual({ builds: 1, uploads: 1, gets: 1 });
  const restarted = new CatalogSnapshotManager(f.policy);
  expect(await restarted.warm(identity, f.build)).toEqual(a);
  expect(f.counters()).toEqual({ builds: 1, uploads: 1, gets: 2 });
  await restarted.warm({ ...identity, visibilityScope: "tenant-b" }, f.build);
  expect(f.counters()).toEqual({ builds: 2, uploads: 2, gets: 3 });
  await manager.invalidate(a.key);
  await manager.warm(identity, f.build);
  expect(f.counters()).toEqual({ builds: 3, uploads: 3, gets: 4 });
});

test("publication import refuses manifests from the published v2 protocol", async () => {
  const f = fixture();
  const manager = new CatalogSnapshotManager(f.policy);
  const original = await manager.warm(identity, f.build);
  await expect(manager.import({ ...original, protocolName: "vgi.v2", protocolVersion: "2.2.0" }, identity)).rejects.toThrow();
  expect(f.counters()).toEqual({ builds: 1, uploads: 1, gets: 1 });
});

test("publication caps apply before upload and damaged recovery rebuilds", async () => {
  const f = fixture();
  await expect(new CatalogSnapshotManager({ ...f.policy, maxRawBytes: 10 }).warm(identity, f.build)).rejects.toThrow("byte limit");
  expect(f.counters().uploads).toBe(0);
  const first = await new CatalogSnapshotManager(f.policy).warm(identity, f.build);
  f.objects.set(first.url, new Uint8Array([1,2,3]));
  const next = await new CatalogSnapshotManager(f.policy).warm(identity, f.build);
  expect(next.url).not.toBe(first.url);
  expect(f.counters().uploads).toBe(2);
});

test("import rejects pointer/log envelope metadata and additional response batches", async () => {
  const f = fixture();
  const manager = new CatalogSnapshotManager(f.policy);
  const original = await manager.warm(identity, f.build);
  const record = rpcBatches(f.objects.get(original.url)!)[0];
  const variants = [
    [withBatchMetadata(record, new Map([["vgi_rpc.location", "https://other.test/object"]]))],
    [withBatchMetadata(record, new Map([["vgi_rpc.log_level", "INFO"]]))],
    [record, record],
  ];
  for (const batches of variants) {
    const bytes = rpcSerialize(record.schema, batches);
    f.objects.set(original.url, bytes);
    const sha256 = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", bytes as BufferSource)), (v) => v.toString(16).padStart(2, "0")).join("");
    await expect(manager.import({ ...original, manifestId: crypto.randomUUID(), sha256, rawSize: bytes.length, storedSize: bytes.length }, identity)).rejects.toThrow("published RPC envelope");
  }
});

test("durable conditional object repository survives a new repository instance", async () => {
  const f = fixture();
  const values = new Map<string, { body: Uint8Array; etag: string }>();
  const objects: CatalogManifestObjectStore = {
    async get(key) { return values.get(key) ?? null; },
    async put(key, body) { if (values.has(key)) return false; values.set(key, { body, etag: String(values.size) }); return true; },
    async delete(key, condition) { if (values.get(key)?.etag !== condition.ifMatch) return false; return values.delete(key); },
  };
  const repo = () => new S3CatalogPublicationRepository("test-identity", f.storage, f.resolver, objects, async () => ({ stable: true }));
  const first = await new CatalogSnapshotManager({ ...f.policy, repository: repo() }).warm(identity, f.build);
  const second = await new CatalogSnapshotManager({ ...f.policy, repository: repo() }).warm(identity, f.build);
  expect(first).toEqual(second);
  expect(f.counters()).toEqual({ builds: 1, uploads: 1, gets: 2 });
});

test("gzip and zstd publications resolve through HTTP and preserve raw-size limits", async () => {
  for (const compression of ["gzip", "zstd"] as const) {
    let object: Uint8Array | null = null, encoding = "", uploads = 0;
    const server = Bun.serve({ port: 0, fetch() { return new Response(object, { headers: { "Content-Encoding": encoding } }); } });
    try {
      const storage = { async upload(data: Uint8Array, contentEncoding: string) { uploads++; object = data; encoding = contentEncoding; return `${server.url}catalog`; } };
      const repository = new MemoryCatalogPublicationRepository("http-test", storage, { storage, urlValidator() {} }, async () => ({ stable: true }));
      const manager = new CatalogSnapshotManager({ delivery: "published", repository, identity: () => identity, compression });
      const f = fixture();
      const m = await manager.warm(identity, f.build);
      expect(uploads).toBe(1);
      expect(encoding).toBe(compression);
      expect(m.storedSize).toBeLessThan(m.rawSize);
      await manager.warm(identity, f.build);
      expect(uploads).toBe(1);
    } finally { server.stop(true); }
  }
});

test("S3 HTTP adapter persists and renews a manifest without rebuilding or uploading its object", async () => {
  const { S3CatalogObjectStorage } = await import("../s3-publication.js");
  const objects = new Map<string, { body: Uint8Array; etag: string; encoding: string }>();
  let revision = 0, objectUploads = 0, objectGets = 0;
  const server = Bun.serve({ port: 0, async fetch(request) {
    const key = new URL(request.url).pathname.slice(1), previous = objects.get(key);
    if (request.method === "GET") {
      if (!previous) return new Response(null, { status: 404 });
      if (key.endsWith(".arrow")) objectGets++;
      const headers = new Headers({ ETag: previous.etag });
      if (previous.encoding) headers.set("Content-Encoding", previous.encoding);
      return new Response(previous.body, { headers });
    }
    if (request.headers.has("If-None-Match") && previous) return new Response(null, { status: 412 });
    if (request.headers.has("If-Match") && request.headers.get("If-Match") !== previous?.etag) return new Response(null, { status: 412 });
    if (request.method === "DELETE") { objects.delete(key); return new Response(null, { status: 204 }); }
    if (key.endsWith(".arrow")) objectUploads++;
    objects.set(key, { body: new Uint8Array(await request.arrayBuffer()), etag: `"${++revision}"`, encoding: request.headers.get("Content-Encoding") ?? "" });
    return new Response(null, { status: 200 });
  } });
  try {
    const storage = new S3CatalogObjectStorage(async ({ key }) => ({ url: `${server.url}${key}?revision=${revision}`, expiresAt: Date.now() + 120_000 }));
    const repository = storage.repository("s3-test", { urlValidator() {} });
    const f = fixture();
    const policy = { ...f.policy, repository };
    const first = await new CatalogSnapshotManager(policy).warm(identity, f.build);
    expect(objectUploads).toBe(1); expect(objectGets).toBe(1);
    const key = `vgi/catalog-manifests/${first.key}.json`;
    const original = objects.get(key)!;
    objects.set(key, { ...original, body: new TextEncoder().encode(JSON.stringify({ ...first, expiresAt: Date.now() - 1 })), etag: `"${++revision}"` });
    const second = await new CatalogSnapshotManager(policy).warm(identity, f.build);
    expect(second.url).not.toBe(first.url);
    expect(second.sha256).toBe(first.sha256);
    expect(second.objectId).toBe(first.objectId);
    expect(objectUploads).toBe(1); expect(objectGets).toBe(2);
    expect(f.counters().builds).toBe(1);
  } finally { server.stop(true); }
});

test("real catalog RPC publishes its full result, revalidates inline, and isolates default attachment scope", async () => {
  const { httpConnect } = await import("@query-farm/vgi-rpc");
  const { VgiClient } = await import("../../client/client.js");
  const { ReadOnlyCatalogInterface } = await import("../read-only.js");
  const { FunctionRegistry } = await import("../../functions/registry.js");
  const { serveVgiWorker, SIGNING_KEY_BYTES } = await import("../../serve-entry.js");
  const f = fixture();
  const registry = new FunctionRegistry();
  const catalog = new ReadOnlyCatalogInterface({ name: "publication", schemas: [{ name: "main" }] }, registry);
  let builds = 0;
  const original = catalog.catalogContents.bind(catalog);
  catalog.catalogContents = async (...args) => { builds++; return original(...args); };
  let revision = "one";
  catalog.catalogSnapshotPolicy = { delivery: "published", repository: f.repository,
    identity: ({ catalogVersion }) => ({ catalogNamespace: "publication", sourceRevision: revision, catalogVersion }) };
  const server = serveVgiWorker({ name: "publication", doc: "publication test", version: "0", registry, catalogInterface: catalog,
    prefix: "", port: 0, signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(7), quiet: true, env: {} });
  try {
    const client = new VgiClient(httpConnect(`http://localhost:${server.port}`, { prefix: "", externalLocation: f.resolver, fetch: ((input: RequestInfo | URL, init?: RequestInit) => String(input).startsWith("https://storage.test/") ? f.resolver.fetch(input, init) : fetch(input, init)) as typeof fetch }));
    const attached = await client.catalogAttach("publication");
    const first = await client.catalogContents(attached.attach_opaque_data);
    expect(first.schemas.map((s) => s.schema.path)).toEqual([["main"]]);
    expect(builds).toBe(1);
    expect(f.counters()).toEqual({ builds: 0, uploads: 1, gets: 2 });
    await client.catalogContents(attached.attach_opaque_data);
    expect(builds).toBe(1);
    expect(f.counters().gets).toBe(3);
    const unchanged = await client.catalogContents(attached.attach_opaque_data, first.etag);
    expect(unchanged.not_modified).toBe(true);
    expect(f.counters().gets).toBe(3);
    const other = await client.catalogAttach("publication");
    await client.catalogContents(other.attach_opaque_data);
    expect(builds).toBe(2); expect(f.counters().uploads).toBe(2);
    const damaged = attached.attach_opaque_data.slice(); damaged[damaged.length - 1] ^= 1;
    await expect(client.catalogContents(damaged)).rejects.toThrow();
    expect(builds).toBe(2); expect(f.counters().uploads).toBe(2);
    // Publication may legitimately commit an old immutable object, but a
    // source revision changing inside CAS must prevent serving its reference.
    revision = "two";
    const commit = f.repository.commitIfAbsent.bind(f.repository);
    f.repository.commitIfAbsent = async (manifest) => { const winner = await commit(manifest); revision = "three"; return winner; };
    await expect(client.catalogContents(attached.attach_opaque_data)).rejects.toThrow("catalog changed");
    f.repository.commitIfAbsent = commit;
    expect((await client.catalogContents(attached.attach_opaque_data)).schemas.length).toBe(1);
  } finally { server.stop(true); }
});
