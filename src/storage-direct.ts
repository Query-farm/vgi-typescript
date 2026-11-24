// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Loads the direct S3/GCS object stores (vgi-rpc's s3/gcs entries; their AWS
// and Google SDKs are optional peers, imported on first use) and registers
// them with storage-select.ts. Imported by the package entry and the serve
// entry, so any worker process can select VGI_STORAGE_BACKEND=s3|gcs.
import { createGCSStorage, gcsStorageConfigFromEnv } from "@query-farm/vgi-rpc/gcs";
import { createS3Storage, s3StorageConfigFromEnv } from "@query-farm/vgi-rpc/s3";
import { registerDirectObjectStoreFactory } from "./storage-select.js";

registerDirectObjectStoreFactory((backend, env) =>
  backend === "s3" ? createS3Storage(s3StorageConfigFromEnv(env)) : createGCSStorage(gcsStorageConfigFromEnv(env)),
);
