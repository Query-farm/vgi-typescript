// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Loads the direct S3/GCS object stores (vgi-rpc's s3/gcs entries; their AWS
// and Google SDKs are optional peers that vgi-rpc imports on first use, inside
// a try, so `bun build --compile` works without them) and registers them with
// storage-select.ts.
//
// Registration is an explicit call, not an import side effect: this module is
// not in package.json `sideEffects`, so a bare `import "./storage-direct.js"`
// is tree-shaken by Bun (and any bundler honouring `sideEffects`). 0.52.2
// shipped that way: dist/ had no S3/GCS factory, so VGI_STORAGE_BACKEND=s3|gcs
// failed with "not available here" on Node and in compiled workers.
import { createGCSStorage, gcsStorageConfigFromEnv } from "@query-farm/vgi-rpc/gcs";
import { createS3Storage, s3StorageConfigFromEnv } from "@query-farm/vgi-rpc/s3";
import { registerDirectObjectStoreFactory } from "./storage-select.js";

/** Register the S3/GCS object stores with storage-select.ts (idempotent). */
export function registerDirectStorage(): void {
  registerDirectObjectStoreFactory((backend, env, role) =>
    backend === "s3"
      ? createS3Storage(s3StorageConfigFromEnv(env, { role }))
      : createGCSStorage(gcsStorageConfigFromEnv(env, { role })),
  );
}
