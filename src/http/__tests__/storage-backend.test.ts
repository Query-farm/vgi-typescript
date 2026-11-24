// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// VGI_STORAGE_BACKEND selection and the HTTP settings it yields (direct
// storage contract v1, sections 1, 2 and the CHECKSUM=none decision).
import { describe, expect, test } from "bun:test";
import { functionBlobClientFromEnv, ObjectStoreBlobClient } from "../../functions/storage-blobs.js";
import { selectStorageBackend, storageHttpConfigFromEnv } from "../storage-backend.js";

const S3 = { VGI_STORAGE_S3_BUCKET: "b", VGI_STORAGE_S3_ENDPOINT: "http://127.0.0.1:9", VGI_STORAGE_S3_ACCESS_KEY_ID: "a", VGI_STORAGE_S3_SECRET_ACCESS_KEY: "s" };

describe("selectStorageBackend", () => {
  test("explicit, inferred, none", () => {
    expect(selectStorageBackend({})).toBe("none");
    expect(selectStorageBackend({ VGI_BLOB_VENDOR_URL: "https://v" })).toBe("vendor");
    expect(selectStorageBackend({ VGI_STORAGE_S3_BUCKET: "b" })).toBe("s3");
    expect(selectStorageBackend({ VGI_STORAGE_GCS_BUCKET: "b" })).toBe("gcs");
    expect(selectStorageBackend({ VGI_STORAGE_BACKEND: "NONE", VGI_STORAGE_S3_BUCKET: "b" })).toBe("none");
    expect(selectStorageBackend({ VGI_STORAGE_BACKEND: "s3", VGI_BLOB_VENDOR_URL: "https://v", VGI_STORAGE_S3_BUCKET: "b" })).toBe("s3");
  });

  test("two backends without VGI_STORAGE_BACKEND refuse to start, naming both", () => {
    expect(() => selectStorageBackend({ VGI_BLOB_VENDOR_URL: "https://v", VGI_STORAGE_S3_BUCKET: "b" })).toThrow(
      /VGI_BLOB_VENDOR_URL, VGI_STORAGE_S3_BUCKET.*VGI_STORAGE_BACKEND/,
    );
    expect(() => selectStorageBackend({ VGI_STORAGE_BACKEND: "azure" })).toThrow("VGI_STORAGE_BACKEND");
  });
});

describe("storageHttpConfigFromEnv", () => {
  test("none: only the request cap", () => {
    const c = storageHttpConfigFromEnv({ VGI_RPC_MAX_REQUEST_BYTES: "32768" });
    expect(c.backend).toBe("none");
    expect(c.externalLocation).toBeUndefined();
    expect(c.uploadUrlProvider).toBeUndefined();
    expect(c.maxRequestBytes).toBe(32768);
  });

  test("s3: results, upload grants and the upload cap", () => {
    const c = storageHttpConfigFromEnv({ ...S3, VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES: "4096", VGI_RPC_MAX_UPLOAD_BYTES: "1000000" });
    expect(c.backend).toBe("s3");
    expect(c.externalLocation?.externalizeThresholdBytes).toBe(4096);
    expect(typeof c.externalLocation?.urlValidator).toBe("function");
    expect(c.uploadUrlProvider).toBe(c.objectStore as never);
    expect(c.maxUploadBytes).toBe(1000000);
    expect(c.verifyAtStartup).toBe(false);
  });

  test("s3 with CHECKSUM=none: no upload grants (so no upload or argument-ref capability), results still externalized", () => {
    const logs: string[] = [];
    const c = storageHttpConfigFromEnv(
      { ...S3, VGI_STORAGE_S3_CHECKSUM: "none", VGI_RPC_MAX_UPLOAD_BYTES: "1000000", VGI_STORAGE_S3_VERIFY: "1" },
      (m) => logs.push(m),
    );
    expect(c.externalLocation?.storage).toBeDefined();
    expect(c.uploadUrlProvider).toBeUndefined();
    expect(c.maxUploadBytes).toBeUndefined();
    expect(c.verifyAtStartup).toBe(true);
    expect(logs.join()).toContain("CHECKSUM=none");
  });

  test("hosted mode with s3 is allowed and logged", () => {
    const logs: string[] = [];
    storageHttpConfigFromEnv({ ...S3, VGI_ORCHARD_HOSTED: "1" }, (m) => logs.push(m));
    expect(logs.join()).toContain("Orchard hosted mode");
  });

  test("function storage blobs live on the bucket", () => {
    expect(functionBlobClientFromEnv({})).toBeNull();
    expect(functionBlobClientFromEnv({ ...S3 })).toBeInstanceOf(ObjectStoreBlobClient);
  });
});
