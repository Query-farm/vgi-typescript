# Catalog collections and external publication

SDK 0.45.0 requires a client using VGI wire major/minor `2.2`, including the
DuckDB extension and browser/WASM clients. Wire `2.1` peers are incompatible;
upgrade both ends together. The protocol routing name remains `vgi.v2`.

Wire `vgi.v2` revision `2.2.0` carries six nullable Arrow streams in
`catalog_contents`: schemas, tables, views, functions, macros and indexes. Each
stream spans all schemas and may contain multiple record batches. The client
reads every row and batch; function and macro subtype fields remain intact.
The author-facing `catalogContents` grouped inventory API is unchanged. A matching
ETag returns an inline `not_modified` response with six null streams.

`FunctionInfo.arguments`, `output_schema` and `parameter_default_values` use
`dictionary<int32, binary>` on the wire, including singleton RPCs. Equal bytes
share dictionary entries; author and client APIs still expose `Uint8Array`.
The first two fields remain required, and absent defaults remain null.

`SchemaInfo` no longer contains `attach_opaque_data`. The attachment handle from
`catalog_attach` is still sent to subsequent catalog and execution methods.

## Enable publication

Assign a policy to the catalog that owns the inventory:

```ts
import { S3CatalogObjectStorage } from "@query-farm/vgi";

const objects = new S3CatalogObjectStorage(async ({ key, method, headers }) => {
  // Use your existing AWS SDK/R2 signer. Include every supplied header in the
  // signature, especially If-Match, If-None-Match and Content-Encoding.
  return signCatalogRequest({ key, method, headers });
  // Return { url, expiresAt: epochMilliseconds } for expiring GET URLs,
  // or { url, stable: true } for deliberately durable non-expiring URLs.
});
const repository = objects.repository("catalog-bucket-v1", {
  maxFetchBytes: 16 * 1024 * 1024,
  maxDecompressedBytes: 64 * 1024 * 1024,
});
catalog.catalogSnapshotPolicy = {
  delivery: "published",
  repository,
  compression: "gzip",
  identity: ({ catalogVersion }) => ({
    catalogNamespace: "weather",
    sourceRevision: deploymentRevision,
    catalogVersion,
    // Omit visibilityScope for attachment-local reuse. Supply a stable scope
    // only when every principal/option in that scope sees identical metadata.
  }),
};
```

Ordinary delivery remains the default. Published delivery requires an explicit
namespace and source revision. Authentication and attachment validation run
before repository/cache access. The default scope hashes the original sealed
attachment. A shared scope must include every credential, tenant, authorization
and attach option that changes visible metadata; never place secret values in it.
The identity also includes catalog version, the canonical wire contract hash,
codec revision, repository profile and compression. Change the repository profile
when its storage namespace or delivery policy changes.

The framework publishes the complete RPC result envelope with SHA-256 over the
raw Arrow bytes, then retrieves and validates that exact object before committing
its manifest. Validation checks the envelope, all six metadata schemas, parent
paths, subtypes, version, ETag and original category bytes. Concurrent requests in
one manager share one publication. Process-local warm hits reuse a small manifest;
a fresh process recovers the durable manifest and validates its object once.

`MemoryCatalogPublicationRepository` offers the same API for process-local use.
`S3CatalogPublicationRepository` and `S3CatalogObjectStorage` use conditional
object operations for create, renewal and invalidation. The signer retains
credentials; manifests contain only object identifiers and serving URLs. Signed
URLs can be renewed without uploading or serializing the immutable object again.
The storage lifecycle must retain objects until all issued references have expired.

## Limits and maintenance

Defaults cap raw/stored bytes at 64 MiB each, cache 128 manifests, require 60
seconds of URL/retention validity, and limit object requests to 30 seconds.
Stricter resolver caps remain in force. Caps are checked before upload. Identity
changes during collection fail the attempt. Failures return an error unless
`onPublicationFailure: "ordinary"` explicitly enables a fresh validated inline
response. Identity, gzip and zstd profiles are separate cache entries; zstd output
requires Bun or Node with a native encoder. Worker runtimes can use gzip/identity.

`CatalogSnapshotManager` exposes `key(identity)`, `warm(identity, build)`,
`import(manifest, identity)` and `invalidate(key)` for prewarming and maintenance.
Direct manager callers provide an explicit visibility scope and an authorized,
consistent builder; attachment-local scoping is supplied by the catalog handler.
Import validates object readback before durable CAS commit. Invalidation waits
for this manager's pending publication and conditionally removes its manifest;
immutable objects remain under the configured retention policy.

TypeScript requires the published `@query-farm/vgi-rpc ^0.29.1` package for gzip
object resolution in runtimes that return the compressed body directly.
