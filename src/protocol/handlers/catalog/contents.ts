// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// catalog_contents: the worker side of the protocol 2.1.0 bulk enumeration,
// with etag revalidation and the frozen-catalog response cache. Mirrors
// vgi-python's Worker.catalog_contents / _catalog_contents_response.

import {
  encodeSchemaInfo,
  encodeTableInfo,
  encodeViewInfo,
  encodeMacroInfo,
  encodeIndexInfo,
} from "../../../generated/vgi-client.js";
import {
  buildCatalogContentsResponse,
  buildSchemaContents,
  type CatalogContentsResponse,
  type SchemaContents,
} from "../../../generated/vgi-protocol-types.js";
import { CatalogContentsResultSchema } from "../../../generated/vgi-protocol-schemas.js";
import { encodeFunctionInfoOnce } from "../../../catalog/item-encoding.js";
import type { AttachOpaqueData, CatalogInterface } from "../../../catalog/interface.js";
import { catalogContentsDigest } from "../../../catalog/contents-digest.js";
import { schemaPathKey } from "../../../schema-path.js";
import { wrapResult } from "../shared.js";

type Wire = { result: Uint8Array };

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
const CACHE = new WeakMap<CatalogInterface, Promise<ContentsSnapshot>>();

/**
 * Ask the catalog for its contents and shape the typed wire response.
 *
 * Enforces the revalidation rules: `not_modified` needs an etag equal to
 * `ifNoneMatch` and no schemas; a catalog with no etag never yields
 * `not_modified` (and so ignores `ifNoneMatch`). Validates the schema paths
 * (unique, parent present) and orders them parent-first. Every item is
 * encoded with the SAME encoder its per-schema RPC uses, so items are
 * byte-identical to catalog_schemas / catalog_schema_contents_* items.
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

  // Same parent-before-child order catalog_schemas guarantees (stable sort).
  const schemas: SchemaContents[] = infos
    .sort((a, b) => a.schema.path.length - b.schema.path.length)
    .map((c) =>
      buildSchemaContents({
        path: [...c.schema.path],
        schema: encodeSchemaInfo(c.schema),
        tables: c.tables.map((t) => encodeTableInfo(t)),
        views: c.views.map((v) => encodeViewInfo(v)),
        scalar_functions: c.scalar_functions.map((f) => encodeFunctionInfoOnce(f)),
        aggregate_functions: c.aggregate_functions.map((f) => encodeFunctionInfoOnce(f)),
        table_functions: c.table_functions.map((f) => encodeFunctionInfoOnce(f)),
        scalar_macros: c.scalar_macros.map((m) => encodeMacroInfo(m)),
        table_macros: c.table_macros.map((m) => encodeMacroInfo(m)),
        indexes: c.indexes.map((i) => encodeIndexInfo(i)),
      }),
    );
  let etag = etag0;
  if (etag === null && owner.catalogContentsEtag === "content-hash") {
    etag = await catalogContentsDigest(schemas);
  }
  if (etag !== null && ifNoneMatch !== null && etag === ifNoneMatch) {
    return buildCatalogContentsResponse({ catalog_version: version, etag, not_modified: true });
  }
  return buildCatalogContentsResponse({ catalog_version: version, etag, schemas });
}

/**
 * Serve one catalog_contents call (the attach value is already opened).
 *
 * Caching: a catalog whose version is frozen (`catalogVersionFrozen`) and whose
 * contents are attach-independent (`catalogContentsAttachIndependent`) is built
 * once per (catalog instance, version); every later call reuses the same
 * serialized response bytes. A conditional call matching the cached etag is
 * answered `not_modified` without touching the snapshot.
 */
export async function serveCatalogContents(
  cat: CatalogInterface,
  attach: AttachOpaqueData,
  ifNoneMatch: string | null,
): Promise<Wire> {
  const owner = cat.catalogContentsOwner(attach);
  const version = Number(await cat.version(attach));
  if (owner.catalogContentsAttachIndependent && owner.catalogVersionFrozen) {
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
