// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Immutable, validated catalog publications. The caller authenticates before lookup.
import { ExternalRef, publishExternalResult, resolveExternalLocation, type ExternalStorage, type ExternalLocationConfig, type CallContext } from "@query-farm/vgi-rpc";
import { schema, field, binary, typeSignature } from "../arrow/index.js";
import { decodeASDCollection, encodeASD } from "../codec/asd.js";
import { CatalogContentsResultSchema, VGI_CATALOG_CONTRACT_HASH } from "../generated/vgi-protocol-schemas.js";
import type { CatalogContentsResponse } from "../generated/vgi-client.js";
import { VGI_PROTOCOL_NAME, VGI_PROTOCOL_VERSION } from "../generated/vgi-service.js";
import { decodeCatalogStreams, CATALOG_STREAM_KINDS } from "./contents-codec.js";
import { catalogContentsDigest } from "./contents-digest.js";
import { validateCatalogPublicationEnvelope } from "./publication-envelope.js";
import type { AttachOpaqueData, CatalogInterface } from "./interface.js";

export interface SnapshotIdentity {
  catalogNamespace: string;
  sourceRevision: string;
  /** Explicit equivalence class for sharing; omitted by a handler defaults to the sealed attachment hash. */
  visibilityScope?: string;
  catalogVersion: number;
}
export interface SnapshotRequest {
  catalog: CatalogInterface;
  attachOpaqueData: AttachOpaqueData;
  callContext?: CallContext;
  catalogVersion: number;
}
export interface CatalogPublicationManifest {
  formatVersion: 1;
  /** Unique repository generation for conditional invalidation and URL renewal. */
  manifestId: string;
  key: string;
  protocolName: string;
  protocolVersion: string;
  contractHash: string;
  codecRevision: 1;
  catalogVersion: number;
  etag: string;
  sha256: string;
  rawSize: number;
  storedSize: number;
  url: string;
  objectId?: string;
  expiresAt?: number;
  retainUntil?: number;
  stable: boolean;
  validated: true;
}
export interface CatalogPublicationRepository {
  /** Change when storage credentials, namespace or delivery profile change. */
  readonly profile: string;
  readonly storage: ExternalStorage;
  /** Supply a bounded resolver (URL policy, fetch implementation, caps). */
  readonly resolver: ExternalLocationConfig;
  lookup(key: string): Promise<CatalogPublicationManifest | null>;
  /** Atomic create-if-absent; return the committed manifest, including another publisher's winner. */
  commitIfAbsent(manifest: CatalogPublicationManifest): Promise<CatalogPublicationManifest>;
  invalidate(key: string, expected: CatalogPublicationManifest): Promise<void>;
  /** Renew the URL without changing the object's identity, content or retention. */
  renew?(manifest: CatalogPublicationManifest): Promise<CatalogPublicationManifest | null>;
  /** Require either explicit stability or a URL expiration time. */
  describe(url: string): Promise<Pick<CatalogPublicationManifest, "objectId" | "expiresAt" | "retainUntil" | "stable">>;
}
export interface CatalogSnapshotPolicy {
  delivery: "ordinary" | "published";
  repository: CatalogPublicationRepository;
  identity(request: SnapshotRequest): SnapshotIdentity | Promise<SnapshotIdentity>;
  onPublicationFailure?: "error" | "ordinary";
  compression?: "identity" | "gzip" | "zstd";
  maxRawBytes?: number;
  maxStoredBytes?: number;
  maxCachedManifests?: number;
  referenceGraceMs?: number;
  /** Optional cheap validator; avoids constructing even a first publication for a conditional hit. */
  etag?(request: SnapshotRequest): string | null | Promise<string | null>;
}

const OUTER_SCHEMA = schema([field("result", binary(), false)]);
const DEFAULT_MAX = 64 * 1024 * 1024;
const utf8 = new TextEncoder();
const equalBytes = (a: Uint8Array, b: Uint8Array) => a.length === b.length && a.every((v, i) => v === b[i]);
async function digest(value: string): Promise<string> {
  const bytes = new Uint8Array(await crypto.subtle.digest("SHA-256", utf8.encode(value)));
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}
function assertRenewedPublication(before: CatalogPublicationManifest, after: CatalogPublicationManifest) {
  const unchanged = (m: CatalogPublicationManifest) => JSON.stringify([m.key, m.formatVersion, m.protocolName, m.protocolVersion, m.contractHash, m.codecRevision, m.catalogVersion, m.etag, m.sha256, m.rawSize, m.storedSize, m.objectId, m.retainUntil, m.stable, m.validated]);
  if (unchanged(before) !== unchanged(after)) throw new Error("renewal changed immutable catalog identity");
}
const manifestSame = (a: CatalogPublicationManifest, b: CatalogPublicationManifest) => a.manifestId === b.manifestId;

/** Memory repository is useful for tests and process-local reuse; storage stays caller-owned. */
export class MemoryCatalogPublicationRepository implements CatalogPublicationRepository {
  private manifests = new Map<string, CatalogPublicationManifest>();
  constructor(readonly profile: string, readonly storage: ExternalStorage, readonly resolver: ExternalLocationConfig, readonly describe: CatalogPublicationRepository["describe"]) {}
  async lookup(key: string) { return this.manifests.get(key) ?? null; }
  async commitIfAbsent(manifest: CatalogPublicationManifest) {
    const existing = this.manifests.get(manifest.key);
    if (existing) return existing;
    const committed = Object.freeze({ ...manifest });
    this.manifests.set(manifest.key, committed);
    return committed;
  }
  async invalidate(key: string, expected: CatalogPublicationManifest) {
    const current = this.manifests.get(key);
    if (current && manifestSame(current, expected)) this.manifests.delete(key);
  }
}

/** A small S3-compatible client surface; adapters map these conditions to If-None-Match / If-Match. */
export interface CatalogManifestObjectStore {
  get(key: string): Promise<{ body: Uint8Array; etag: string } | null>;
  put(key: string, body: Uint8Array, condition: { ifNoneMatch: "*" } | { ifMatch: string }): Promise<boolean>;
  delete(key: string, condition: { ifMatch: string }): Promise<boolean>;
}
/** Durable manifest repository, usable with S3/R2 conditional object operations and any RPC uploader. */
export class S3CatalogPublicationRepository implements CatalogPublicationRepository {
  renewReference?: (manifest: CatalogPublicationManifest) => Promise<CatalogPublicationManifest | null>;
  constructor(readonly profile: string, readonly storage: ExternalStorage, readonly resolver: ExternalLocationConfig,
    private readonly objects: CatalogManifestObjectStore, readonly describe: CatalogPublicationRepository["describe"], private readonly prefix = "vgi/catalog-manifests/") {}
  private objectKey(key: string) { if (!/^[0-9a-f]{64}$/.test(key)) throw new Error("invalid publication key"); return `${this.prefix}${key}.json`; }
  async lookup(key: string) {
    const value = await this.objects.get(this.objectKey(key));
    if (!value) return null;
    if (value.body.byteLength > 64 * 1024) throw new Error("catalog manifest exceeds 64 KiB");
    return JSON.parse(new TextDecoder().decode(value.body)) as CatalogPublicationManifest;
  }
  async commitIfAbsent(manifest: CatalogPublicationManifest) {
    const stored = await this.objects.put(this.objectKey(manifest.key), utf8.encode(JSON.stringify(manifest)), { ifNoneMatch: "*" });
    if (stored) return manifest;
    const current = await this.lookup(manifest.key);
    if (!current) throw new Error("catalog manifest changed during commit");
    return current;
  }
  async renew(manifest: CatalogPublicationManifest) {
    if (!this.renewReference) return null;
    const current = await this.objects.get(this.objectKey(manifest.key));
    if (!current) return null;
    const existing = JSON.parse(new TextDecoder().decode(current.body)) as CatalogPublicationManifest;
    if (!manifestSame(existing, manifest)) return existing;
    const renewed = await this.renewReference(manifest);
    if (!renewed) return null;
    assertRenewedPublication(manifest, renewed);
    if (await this.objects.put(this.objectKey(manifest.key), utf8.encode(JSON.stringify(renewed)), { ifMatch: current.etag })) return renewed;
    return this.lookup(manifest.key);
  }
  async invalidate(key: string, expected: CatalogPublicationManifest) {
    const current = await this.objects.get(this.objectKey(key));
    if (current && manifestSame(JSON.parse(new TextDecoder().decode(current.body)), expected)) {
      await this.objects.delete(this.objectKey(key), { ifMatch: current.etag });
    }
  }
}

export class CatalogSnapshotManager {
  private cached = new Map<string, CatalogPublicationManifest>();
  private pending = new Map<string, Promise<CatalogPublicationManifest>>();
  constructor(readonly policy: CatalogSnapshotPolicy) {
    for (const value of [policy.maxRawBytes, policy.maxStoredBytes, policy.maxCachedManifests]) {
      if (value != null && (!Number.isSafeInteger(value) || value <= 0)) throw new Error("catalog publication limits must be positive safe integers");
    }
    if (policy.referenceGraceMs != null && (!Number.isSafeInteger(policy.referenceGraceMs) || policy.referenceGraceMs < 0)) throw new Error("catalog publication reference grace must be a nonnegative safe integer");
  }
  private usable(m: CatalogPublicationManifest) {
    const deadline = Date.now() + (this.policy.referenceGraceMs ?? 60_000);
    return (m.stable || m.expiresAt != null) && (m.expiresAt == null || m.expiresAt > deadline) && (m.retainUntil == null || m.retainUntil > deadline);
  }
  async key(identity: SnapshotIdentity): Promise<string> {
    for (const value of [identity.catalogNamespace, identity.sourceRevision, identity.visibilityScope]) if (!value) throw new Error("published catalogs require explicit namespace, revision and visibility scope");
    if (!Number.isSafeInteger(identity.catalogVersion)) throw new Error("catalog version is not a safe integer");
    return digest(JSON.stringify([VGI_PROTOCOL_NAME, VGI_PROTOCOL_VERSION, VGI_CATALOG_CONTRACT_HASH, 1,
      identity.catalogNamespace, identity.sourceRevision, identity.visibilityScope, identity.catalogVersion, this.policy.repository.profile, this.policy.compression ?? "identity"]));
  }
  private remember(m: CatalogPublicationManifest) {
    this.cached.delete(m.key); this.cached.set(m.key, Object.freeze({ ...m }));
    while (this.cached.size > (this.policy.maxCachedManifests ?? 128)) this.cached.delete(this.cached.keys().next().value!);
  }
  async invalidate(key: string) {
    await this.pending.get(key)?.catch(() => undefined);
    const m = this.cached.get(key) ?? await this.policy.repository.lookup(key);
    this.cached.delete(key);
    if (m) await this.policy.repository.invalidate(key, m);
  }
  private async validate(m: CatalogPublicationManifest, key: string, identity: SnapshotIdentity, expected?: CatalogContentsResponse) {
    if (m.formatVersion !== 1 || typeof m.manifestId !== "string" || !m.manifestId || m.key !== key || m.protocolName !== VGI_PROTOCOL_NAME || m.protocolVersion !== VGI_PROTOCOL_VERSION
      || m.contractHash !== VGI_CATALOG_CONTRACT_HASH || m.codecRevision !== 1 || m.catalogVersion !== identity.catalogVersion || m.validated !== true || typeof m.stable !== "boolean"
      || typeof m.etag !== "string" || !/^[0-9a-f]{64}$/.test(m.sha256) || !Number.isSafeInteger(m.rawSize) || !Number.isSafeInteger(m.storedSize) || m.rawSize <= 0 || m.storedSize <= 0
      || m.rawSize > (this.policy.maxRawBytes ?? DEFAULT_MAX) || m.storedSize > (this.policy.maxStoredBytes ?? DEFAULT_MAX)
      || !this.usable(m)) throw new Error("invalid or expired catalog publication manifest");
    const ref = new ExternalRef(m.url, m.sha256);
    const configuredFetch = this.policy.repository.resolver.fetch ?? fetch;
    const resolved = await resolveExternalLocation(ref.pointerBatch(OUTER_SCHEMA), {
      ...this.policy.repository.resolver,
      fetch: ((input: RequestInfo | URL, init?: RequestInit) => configuredFetch(input, { ...init, signal: AbortSignal.any([...(init?.signal ? [init.signal] : []), AbortSignal.timeout(30_000)]) })) as typeof fetch,
      maxFetchBytes: Math.min(this.policy.maxStoredBytes ?? DEFAULT_MAX, this.policy.repository.resolver.maxFetchBytes ?? Infinity),
      maxDecompressedBytes: Math.min(this.policy.maxRawBytes ?? DEFAULT_MAX, this.policy.repository.resolver.maxDecompressedBytes ?? Infinity),
      validateFetchedIpc: (bytes) => {
        this.policy.repository.resolver.validateFetchedIpc?.(bytes);
        if (bytes.length !== m.rawSize) throw new Error("catalog publication raw size mismatch");
        validateCatalogPublicationEnvelope(bytes);
      },
    });
    if (resolved.numRows !== 1 || resolved.schema.fields.length !== 1 || resolved.schema.fields[0].name !== "result"
      || typeSignature(resolved.schema.fields[0].type) !== typeSignature(binary()) || resolved.schema.fields[0].nullable) throw new Error("invalid published RPC envelope");
    const bytes = resolved.getChild("result")?.get(0);
    if (!(bytes instanceof Uint8Array)) throw new Error("published catalog has no result bytes");
    const decoded = decodeASDCollection<CatalogContentsResponse>(CatalogContentsResultSchema, bytes);
    if (decoded.length !== 1) throw new Error("published catalog must contain exactly one response row");
    const response = decoded[0];
    if (response.not_modified || Number(response.catalog_version) !== identity.catalogVersion || response.etag !== m.etag) throw new Error("published catalog version or ETag mismatch");
    decodeCatalogStreams(response);
    if (expected) for (const kind of CATALOG_STREAM_KINDS) {
      const a = response[kind], b = expected[kind];
      if ((a == null) !== (b == null) || (a != null && b != null && !equalBytes(a, b))) throw new Error("publication readback changed catalog metadata");
    }
  }
  async import(manifest: CatalogPublicationManifest, identity: SnapshotIdentity) {
    const key = await this.key(identity);
    await this.validate(manifest, key, identity);
    const winner = await this.policy.repository.commitIfAbsent(manifest);
    if (!manifestSame(manifest, winner)) await this.validate(winner, key, identity);
    this.remember(winner);
    return winner;
  }
  async warm(identity: SnapshotIdentity, build: () => Promise<CatalogContentsResponse>): Promise<CatalogPublicationManifest> {
    const key = await this.key(identity);
    const cached = this.cached.get(key);
    if (cached && this.usable(cached)) return cached;
    const active = this.pending.get(key); if (active) return active;
    const pending = this.recoverOrPublish(key, identity, build);
    this.pending.set(key, pending);
    try { const m = await pending; this.remember(m); return m; }
    finally { if (this.pending.get(key) === pending) this.pending.delete(key); }
  }
  private async recoverOrPublish(key: string, identity: SnapshotIdentity, build: () => Promise<CatalogContentsResponse>) {
    const repository = this.policy.repository;
    let existing = await repository.lookup(key);
    if (existing) {
      try {
        if (!this.usable(existing) && repository.renew) { const renewed = await repository.renew(existing); if (renewed) { assertRenewedPublication(existing, renewed); existing = renewed; } }
        await this.validate(existing, key, identity); return existing; }
      catch { await repository.invalidate(key, existing); }
    }
    const response = await build();
    if (response.not_modified || Number(response.catalog_version) !== identity.catalogVersion) throw new Error("publication requires a full snapshot of the requested version");
    decodeCatalogStreams(response);
    response.etag ??= await catalogContentsDigest(response, identity.catalogVersion);
    const payload = encodeASD(CatalogContentsResultSchema, response);
    let rawSize = 0, storedSize = 0;
    // Identity encoding keeps the counted bytes identical to the publisher's
    // raw envelope: cap before upload without serializing the envelope twice.
    const bounded: ExternalStorage = { upload: async (data, encoding) => {
      if (encoding) throw new Error("catalog publication uploader requires identity encoding");
      rawSize = data.byteLength;
      if (rawSize > (this.policy.maxRawBytes ?? DEFAULT_MAX)) throw new Error("catalog publication exceeds byte limit");
      let body = data;
      const compression = this.policy.compression ?? "identity";
      if (compression !== "identity" && compression !== "gzip" && compression !== "zstd") throw new Error("catalog publication compression must be identity, gzip or zstd");
      if (compression === "gzip") {
        body = new Uint8Array(await new Response(new Blob([data as BlobPart]).stream().pipeThrough(new CompressionStream("gzip"))).arrayBuffer());
      } else if (compression === "zstd") {
        const runtime = (globalThis as any).Bun;
        if (typeof runtime?.zstdCompress === "function") {
          body = new Uint8Array(await runtime.zstdCompress(data));
        } else if (typeof process !== "undefined" && process.versions?.node) {
          // The indirect specifier keeps a Node-only module out of browser/worker bundles.
          const nodeZlib = "node:zlib";
          const zlib = await import(nodeZlib);
          if (typeof zlib.zstdCompressSync !== "function") throw new Error("zstd catalog publication requires Node >=22.15, Bun, or an encoder-capable runtime");
          body = new Uint8Array(zlib.zstdCompressSync(data));
        } else {
          throw new Error("zstd catalog publication requires an encoder-capable runtime; use gzip or identity here");
        }
      }
      storedSize = body.byteLength;
      if (storedSize > (this.policy.maxStoredBytes ?? DEFAULT_MAX)) throw new Error("catalog publication exceeds byte limit");
      return repository.storage.upload(body, compression === "identity" ? "" : compression);
    } };
    const ref = await publishExternalResult(OUTER_SCHEMA, { result: payload }, bounded, { includeSha256: true });
    const m: CatalogPublicationManifest = {
      formatVersion: 1, manifestId: crypto.randomUUID(), key, protocolName: VGI_PROTOCOL_NAME, protocolVersion: VGI_PROTOCOL_VERSION,
      contractHash: VGI_CATALOG_CONTRACT_HASH, codecRevision: 1, catalogVersion: identity.catalogVersion,
      etag: response.etag, sha256: ref.sha256!, rawSize, storedSize, url: ref.url, validated: true,
      ...await repository.describe(ref.url),
    };
    await this.validate(m, key, identity, response);
    const winner = await repository.commitIfAbsent(m);
    if (!manifestSame(m, winner)) await this.validate(winner, key, identity);
    return winner;
  }
}
