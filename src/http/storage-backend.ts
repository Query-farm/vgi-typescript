// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Storage backend selection (direct storage contract v1): the blob vendor,
// the operator's own S3 or GCS bucket, or none.
//
//   VGI_STORAGE_BACKEND = vendor | s3 | gcs | none
//
// Unset: `vendor` if VGI_BLOB_VENDOR_URL is set, else `s3` if
// VGI_STORAGE_S3_BUCKET is, else `gcs` if VGI_STORAGE_GCS_BUCKET is, else
// `none` (everything inline). More than one of those set without an explicit
// VGI_STORAGE_BACKEND refuses to start. The S3/GCS settings are read by
// vgi-rpc (s3StorageConfigFromEnv / gcsStorageConfigFromEnv).
//
// With VGI_STORAGE_S3_CHECKSUM=none the S3 backend issues no upload grants:
// no uploadUrlProvider, so neither VGI-Upload-URL-Support nor
// VGI-Arguments-Ref-Bytes is advertised (argument refs and large requests stay
// inline); results and stored objects are still externalized.

import {
  maxFetchBytesFromEnv,
  maxUploadBytesFromEnv,
  type ExternalLocationConfig,
  type ObjectStore,
  type UploadUrlProvider,
} from "@query-farm/vgi-rpc";
import "../storage-direct.js";
import {
  directObjectStore,
  ENV_STORAGE_BACKEND,
  selectStorageBackend,
  type StorageBackend,
} from "../storage-select.js";
import { blobVendorHttpConfigFromEnv, type BlobVendorHttpConfig } from "./blob-vendor.js";

export { ENV_STORAGE_BACKEND, selectStorageBackend, type StorageBackend } from "../storage-select.js";

type Env = Readonly<Record<string, string | undefined>>;

/** `1` HEADs the S3 bucket at startup instead of on first use. */
export const ENV_STORAGE_S3_VERIFY = "VGI_STORAGE_S3_VERIFY";


/** {@link BlobVendorHttpConfig} for any backend, plus what it was built from. */
export interface StorageHttpConfig extends BlobVendorHttpConfig {
  /** The selected backend. */
  backend: StorageBackend;
  /** The S3/GCS object store (StateRef objects, argument refs, function-storage blobs). */
  objectStore?: ObjectStore | undefined;
  /** HEAD the bucket (S3); resolves at once for other backends. */
  verify: () => Promise<void>;
  /** `VGI_STORAGE_S3_VERIFY=1`: run {@link verify} at startup. */
  verifyAtStartup: boolean;
}

function positiveInt(env: Env, name: string): number | undefined {
  const raw = (env[name] ?? "").trim();
  if (!raw) return undefined;
  if (!/^\d+$/.test(raw) || Number(raw) < 1) throw new Error(`${name} must be a positive integer, got ${JSON.stringify(raw)}`);
  return Number(raw);
}

function compressionFromEnv(env: Env): ExternalLocationConfig["compression"] | undefined {
  const raw = (env.VGI_RPC_EXTERNAL_COMPRESSION ?? "").trim().toLowerCase();
  if (raw === "" || raw === "none" || raw === "off" || raw === "identity") return undefined;
  const [name, level] = raw.split(":", 2) as [string, string | undefined];
  if (name !== "zstd" || (level !== undefined && !/^-?\d+$/.test(level))) {
    throw new Error(`VGI_RPC_EXTERNAL_COMPRESSION must be none or zstd[:level], got ${JSON.stringify(raw)}`);
  }
  return level === undefined ? { algorithm: "zstd" } : { algorithm: "zstd", level: Number(level) };
}

/**
 * HTTP server storage settings for whichever backend the environment selects.
 * For `vendor` this is {@link blobVendorHttpConfigFromEnv}. For `s3` / `gcs`:
 * results externalize to the bucket (`VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES`,
 * default 1 MiB; `VGI_RPC_EXTERNAL_COMPRESSION`), inbound pointers are pinned
 * to the bucket's URL host, and upload grants are vended (except with
 * `VGI_STORAGE_S3_CHECKSUM=none`). `VGI_RPC_MAX_REQUEST_BYTES` is read either way.
 *
 * @param log Startup notices (default `console.warn`).
 * @throws Error on a backend conflict or a malformed variable.
 */
export function storageHttpConfigFromEnv(env: Env, log: (message: string) => void = console.warn): StorageHttpConfig {
  const backend = selectStorageBackend(env);
  const hosted = ["1", "true", "yes", "on"].includes((env.VGI_ORCHARD_HOSTED ?? "").trim().toLowerCase());
  const verifyAtStartup = ["1", "true", "yes", "on"].includes((env[ENV_STORAGE_S3_VERIFY] ?? "").trim().toLowerCase());
  if (backend === "vendor" || backend === "none") {
    const base = blobVendorHttpConfigFromEnv(backend === "vendor" ? env : { ...env, VGI_BLOB_VENDOR_URL: "" });
    return { ...base, backend, verify: async () => {}, verifyAtStartup: false };
  }
  if (hosted) log(`storage: Orchard hosted mode with ${ENV_STORAGE_BACKEND}=${backend} (the operator's own bucket, not the vendor)`);
  const s = directObjectStore(backend, env);
  const config: StorageHttpConfig = {
    backend,
    objectStore: s,
    verify: async () => {
      await s.verify?.();
    },
    verifyAtStartup,
    externalLocation: {
      storage: s as never,
      externalizeThresholdBytes: positiveInt(env, "VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES") ?? 1_048_576,
      urlValidator: s.urlValidator,
      maxFetchBytes: maxFetchBytesFromEnv(env),
    },
  };
  const compression = compressionFromEnv(env);
  if (compression) config.externalLocation!.compression = compression;
  const maxRequestBytes = positiveInt(env, "VGI_RPC_MAX_REQUEST_BYTES");
  if (maxRequestBytes !== undefined) config.maxRequestBytes = maxRequestBytes;
  if (typeof s.generateUploadUrl === "function") {
    config.uploadUrlProvider = s as UploadUrlProvider;
    const maxUpload = maxUploadBytesFromEnv(env);
    if (maxUpload !== null) config.maxUploadBytes = maxUpload;
  } else {
    log(
      "storage: VGI_STORAGE_S3_CHECKSUM=none: no upload grants (argument refs and large requests stay inline); " +
        "results and stored objects are still externalized",
    );
  }
  return config;
}
