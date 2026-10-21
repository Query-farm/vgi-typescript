// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// catalog_contents: the worker side of the wire 2.2 bulk enumeration,
// with etag revalidation and the frozen-catalog response cache. Mirrors
// vgi-python's Worker.catalog_contents / _catalog_contents_response.

import {
  buildCatalogContentsResponse,
  type CatalogContentsResponse,
} from "../../../generated/vgi-protocol-types.js";
import { ExternalRef, type CallContext } from "@query-farm/vgi-rpc";
import { CatalogSnapshotManager, type CatalogSnapshotPolicy } from "../../../catalog/publication.js";
import { CatalogContentsResultSchema } from "../../../generated/vgi-protocol-schemas.js";
import { encodeCatalogStreams, decodeCatalogStreams } from "../../../catalog/contents-codec.js";
import type { AttachOpaqueData, CatalogInterface } from "../../../catalog/interface.js";
import { catalogContentsDigest } from "../../../catalog/contents-digest.js";
import { wrapResult } from "../shared.js";

type Wire = { result: Uint8Array };
const schemaPathKey = (path: readonly string[]) => JSON.stringify(path.map((part) => part.toLowerCase()));

/** One cached answer per catalog instance, valid for one catalog version. */
interface ContentsSnapshot {
  version: number;
  etag: string | null;
  wire: Wire;
}

/**
 * Cached catalog_contents responses, keyed by the owning catalog instance
 * (weak, so a dropped catalog takes its snapshot with it). Holds the build
 * promise, so concurrent first calls build once.
 */
const PUBLICATIONS = new WeakMap<CatalogSnapshotPolicy, CatalogSnapshotManager>();

const CACHE = new WeakMap<CatalogInterface, Promise<ContentsSnapshot>>();

/**
 * Ask the catalog for its contents and shape the typed wire response.
 *
 * Enforces the revalidation rules: `not_modified` needs an etag equal to
 * `ifNoneMatch` and no schemas; a catalog with no etag never yields
 * `not_modified` (and so ignores `ifNoneMatch`). Validates the schema paths
 * (unique, parent present) and orders them parent-first. The six collection
 * streams retain the fields used by the existing per-schema RPC metadata.
 */
export async function buildCatalogContents(
  cat: CatalogInterface,
  owner: CatalogInterface,
  attach: AttachOpaqueData,
  version: number,
  ifNoneMatch: string | null,
): Promise<CatalogContentsResponse> {
  const result = await cat.catalogContents(attach, ifNoneMatch);
  const etag0 = result.etag ?? null;
  if (result.not_modified) {
    if (etag0 === null || ifNoneMatch === null || etag0 !== ifNoneMatch) {
      throw new Error(
        "catalog_contents returned not_modified, but only a catalog whose etag equals " +
          "if_none_match may (and it must return that etag)",
      );
    }
    if (result.schemas && result.schemas.length > 0) {
      throw new Error("catalog_contents returned not_modified with schemas; it must return none");
    }
    return buildCatalogContentsResponse({ catalog_version: version, etag: etag0, not_modified: true });
  }

  const infos = [...(result.schemas ?? [])];
  const keys = infos.map((c) => schemaPathKey(c.schema.path));
  const keySet = new Set(keys);
  if (keySet.size !== keys.length) throw new Error("catalog_contents returned duplicate schema paths");
  for (const c of infos) {
    if (c.schema.path.length > 1 && !keySet.has(schemaPathKey(c.schema.path.slice(0, -1)))) {
      throw new Error(`catalog_contents returned schema path ${JSON.stringify(c.schema.path)} without its parent`);
    }
  }

  const streams = encodeCatalogStreams(infos.sort((a, b) => a.schema.path.length - b.schema.path.length));
  let etag = etag0;
  if (etag === null && owner.catalogContentsEtag === "content-hash") {
    etag = await catalogContentsDigest(streams, version);
  }
  if (etag !== null && ifNoneMatch !== null && etag === ifNoneMatch) {
    return buildCatalogContentsResponse({ catalog_version: version, etag, not_modified: true });
  }
  const response = buildCatalogContentsResponse({ catalog_version: version, etag, ...streams });
  decodeCatalogStreams(response);
  return response;
}

/**
 * Serve one catalog_contents call (the attach value is already opened).
 *
 * Caching: a catalog whose `catalogContentsCacheable()` is true (by default:
 * version frozen and contents attach-independent; ReadOnlyCatalogInterface) is
 * built once per (catalog instance, version); every later call reuses the same
 * serialized response bytes. A conditional call matching the cached etag is
 * answered `not_modified` without touching the snapshot.
 */
export async function serveCatalogContents(
  cat: CatalogInterface,
  attach: AttachOpaqueData,
  ifNoneMatch: string | null,
  callContext?: CallContext,
  sealedAttach: Uint8Array = attach,
): Promise<Wire | ExternalRef> {
  const owner = cat.catalogContentsOwner(attach);
  const version = Number(await cat.version(attach));
  const policy = owner.catalogSnapshotPolicy;
  if (policy?.delivery === "published") {
    const request = { catalog: cat, attachOpaqueData: attach, callContext, catalogVersion: version };
    const inline = (etag: string) => wrapResult(buildCatalogContentsResponse({ catalog_version: version, etag, not_modified: true }), CatalogContentsResultSchema);
    const cheapEtag = await policy.etag?.(request);
    if (cheapEtag != null && cheapEtag === ifNoneMatch) return inline(cheapEtag);
    let manager = PUBLICATIONS.get(policy);
    if (!manager) { manager = new CatalogSnapshotManager(policy); PUBLICATIONS.set(policy, manager); }
    const scopeBytes = new Uint8Array(await crypto.subtle.digest("SHA-256", sealedAttach as BufferSource));
    const attachmentScope = "attachment:" + Array.from(scopeBytes, (b) => b.toString(16).padStart(2, "0")).join("");
    const identify = async (input: typeof request) => { const id = await policy.identity(input); return { ...id, visibilityScope: id.visibilityScope ?? attachmentScope }; };
    const identity = await identify(request);
    if (identity.catalogVersion !== version) throw new Error("snapshot identity version differs from the attached catalog");
    const assertCurrentIdentity = async () => {
      const currentVersion = Number(await cat.version(attach));
      const current = await identify({ ...request, catalogVersion: currentVersion });
      if (currentVersion !== version || await manager!.key(identity) !== await manager!.key(current)) throw new Error("catalog changed while resolving its publication");
    };
    const buildSnapshot = async (validator: string | null) => {
      const response = await buildCatalogContents(cat, owner, attach, version, validator);
      await assertCurrentIdentity();
      return response;
    };
    try {
      const manifest = await manager.warm(identity, () => buildSnapshot(null));
      await assertCurrentIdentity();
      if (manifest.etag === ifNoneMatch) return inline(manifest.etag);
      return new ExternalRef(manifest.url, manifest.sha256);
    } catch (error) {
      if (policy.onPublicationFailure !== "ordinary") throw error;
      return wrapResult(await buildSnapshot(ifNoneMatch), CatalogContentsResultSchema);
    }
  }
  if (owner.catalogContentsCacheable()) {
    let pending = CACHE.get(owner);
    let snapshot = pending ? await pending.catch(() => null) : null;
    if (!snapshot || snapshot.version !== version) {
      pending = (async () => {
        const built = await buildCatalogContents(cat, owner, attach, version, null);
        return {
          version,
          etag: built.etag ?? null,
          wire: wrapResult(built, CatalogContentsResultSchema),
        };
      })();
      CACHE.set(owner, pending);
      try {
        snapshot = await pending;
      } catch (err) {
        // A failed build is not cached: the next call tries again.
        if (CACHE.get(owner) === pending) CACHE.delete(owner);
        throw err;
      }
    }
    if (ifNoneMatch !== null && snapshot.etag !== null && ifNoneMatch === snapshot.etag) {
      return wrapResult(
        buildCatalogContentsResponse({ catalog_version: version, etag: snapshot.etag, not_modified: true }),
        CatalogContentsResultSchema,
      );
    }
    return snapshot.wire;
  }
  return wrapResult(
    await buildCatalogContents(cat, owner, attach, version, ifNoneMatch),
    CatalogContentsResultSchema,
  );
}
