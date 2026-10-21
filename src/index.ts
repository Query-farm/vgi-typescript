// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// VGI TypeScript - Public API (node/bun barrel)
//
// The runtime-agnostic core plus the node-only `Worker` (stdio + AF_UNIX
// launcher transport). Cloudflare Worker / browser code imports from
// `./index.core.js` (via worker-cf-entry.ts) instead, which omits `Worker` so
// the launcher's `serveUnix` (node:net) never enters the bundle.
export * from "./index.core.js";

// Worker (node/bun only — stdio + AF_UNIX launcher transport)
export { Worker, type WorkerConfig } from "./worker.js";

export { CatalogSnapshotManager, MemoryCatalogPublicationRepository, S3CatalogPublicationRepository } from "./catalog/publication.js";
export type { CatalogSnapshotPolicy, SnapshotIdentity, SnapshotRequest, CatalogPublicationManifest, CatalogPublicationRepository, CatalogManifestObjectStore } from "./catalog/publication.js";
export { S3CatalogObjectStorage } from "./catalog/s3-publication.js";
export type { CatalogS3Signer } from "./catalog/s3-publication.js";
