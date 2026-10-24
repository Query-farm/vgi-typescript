// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import type { ExternalStorage, ExternalLocationConfig } from "@query-farm/vgi-rpc";
import { S3CatalogPublicationRepository, type CatalogManifestObjectStore, type CatalogPublicationManifest } from "./publication.js";

/** Sign an S3/R2 request using the application's existing credentials/SDK.
 * Include the supplied headers when signing; they are sent exactly as given. */
export type CatalogS3Signer = (request: {
  key: string; method: "GET" | "PUT" | "DELETE"; headers: Record<string, string>;
}) => Promise<{ url: string; expiresAt?: number; stable?: boolean }>;

/** Concrete S3 HTTP adapter with conditional manifest operations and immutable objects.
 * A signer keeps AWS/R2 credentials out of the framework and stored manifests. */
export class S3CatalogObjectStorage implements ExternalStorage, CatalogManifestObjectStore {
  private locations = new Map<string, Pick<CatalogPublicationManifest, "objectId" | "expiresAt" | "retainUntil" | "stable">>();
  constructor(readonly sign: CatalogS3Signer, private readonly request: typeof fetch = fetch,
    private readonly objectPrefix = "vgi/catalog-results/", private readonly retentionMs?: number) {}
  async upload(data: Uint8Array, contentEncoding: string): Promise<string> {
    const key = `${this.objectPrefix}${crypto.randomUUID()}.arrow`;
    const headers: Record<string, string> = { "Content-Type": "application/vnd.apache.arrow.stream", "If-None-Match": "*" };
    if (contentEncoding) headers["Content-Encoding"] = contentEncoding;
    const put = await this.sign({ key, method: "PUT", headers });
    const response = await this.request(put.url, { method: "PUT", headers, body: data as BodyInit, redirect: "error", signal: AbortSignal.timeout(30_000) });
    if (!response.ok) throw new Error(`catalog object upload failed (${response.status})`);
    const get = await this.sign({ key, method: "GET", headers: {} });
    if (!get.stable && get.expiresAt == null) throw new Error("S3 catalog signer must declare GET URL expiration or stability");
    this.locations.set(get.url, { objectId: key, expiresAt: get.expiresAt, stable: get.stable ?? false,
      retainUntil: this.retentionMs == null ? undefined : Date.now() + this.retentionMs });
    return get.url;
  }
  async describe(url: string) {
    const value = this.locations.get(url);
    if (!value) throw new Error("unknown catalog object location");
    this.locations.delete(url);
    return value;
  }
  async get(key: string) {
    const signed = await this.sign({ key, method: "GET", headers: {} });
    const response = await this.request(signed.url, { redirect: "error", signal: AbortSignal.timeout(30_000) });
    if (response.status === 404) return null;
    if (!response.ok) throw new Error(`catalog manifest GET failed (${response.status})`);
    const etag = response.headers.get("etag");
    if (!etag) throw new Error("S3 catalog manifest lacks conditional-operation ETag");
    const reader = response.body?.getReader();
    if (!reader) throw new Error("empty catalog manifest body");
    let size = 0; const parts: Uint8Array[] = [];
    while (true) {
      const { done, value } = await reader.read(); if (done) break;
      size += value.byteLength;
      if (size > 64 * 1024) { await reader.cancel(); throw new Error("catalog manifest exceeds 64 KiB"); }
      parts.push(value);
    }
    const body = new Uint8Array(size); let offset = 0;
    for (const part of parts) { body.set(part, offset); offset += part.byteLength; }
    return { body, etag };
  }
  async put(key: string, body: Uint8Array, condition: { ifNoneMatch: "*" } | { ifMatch: string }) {
    const headers = { "Content-Type": "application/json", ...("ifNoneMatch" in condition ? { "If-None-Match": condition.ifNoneMatch } : { "If-Match": condition.ifMatch }) };
    const signed = await this.sign({ key, method: "PUT", headers });
    const response = await this.request(signed.url, { method: "PUT", headers, body: body as BodyInit, redirect: "error", signal: AbortSignal.timeout(30_000) });
    if (response.status === 412 || response.status === 409) return false;
    if (!response.ok) throw new Error(`catalog manifest PUT failed (${response.status})`);
    return true;
  }
  async delete(key: string, condition: { ifMatch: string }) {
    const headers = { "If-Match": condition.ifMatch };
    const signed = await this.sign({ key, method: "DELETE", headers });
    const response = await this.request(signed.url, { method: "DELETE", headers, redirect: "error", signal: AbortSignal.timeout(30_000) });
    if (response.status === 412 || response.status === 404) return false;
    if (!response.ok) throw new Error(`catalog manifest DELETE failed (${response.status})`);
    return true;
  }
  repository(profile: string, resolver: Omit<ExternalLocationConfig, "storage"> = {}, manifestPrefix = "vgi/catalog-manifests/") {
    const repository = new S3CatalogPublicationRepository(profile, this, { ...resolver, storage: this }, this, (url) => this.describe(url), manifestPrefix);
    repository.renewReference = async (manifest) => {
      if (!manifest.objectId) return null;
      const signed = await this.sign({ key: manifest.objectId, method: "GET", headers: {} });
      return { ...manifest, manifestId: crypto.randomUUID(), url: signed.url, expiresAt: signed.expiresAt, stable: signed.stable ?? false };
    };
    return repository;
  }
}
