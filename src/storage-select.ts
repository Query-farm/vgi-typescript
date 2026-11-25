// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Storage backend selection (direct storage contract v1), runtime-agnostic:
// no S3/GCS code here, so the Cloudflare entry does not bundle it. The
// S3/GCS object stores come from storage-direct.ts, whose
// registerDirectStorage() the package entry and http/storage-backend.ts call.

import type { ObjectStore, UploadUrlProvider } from "@query-farm/vgi-rpc";

type Env = Readonly<Record<string, string | undefined>>;

/** Environment variable selecting the storage backend. */
export const ENV_STORAGE_BACKEND = "VGI_STORAGE_BACKEND";

/** Where large values go. */
export type StorageBackend = "vendor" | "s3" | "gcs" | "none";

/** An S3/GCS object store as built from the environment. */
export type DirectObjectStore = ObjectStore &
  Partial<UploadUrlProvider> & {
    verify?: () => Promise<void>;
    urlValidator: (url: string) => Promise<void>;
    deleteObject?: (objectId: string) => Promise<void>;
  };

/** The half of the bucket a store writes: `rpc/` or `fs/` (function storage). */
export type StorageRole = "rpc" | "fs";

function isSet(env: Env, name: string): boolean {
  return (env[name] ?? "").trim() !== "";
}

/**
 * The storage backend the environment selects: `VGI_STORAGE_BACKEND`, else
 * `vendor` if `VGI_BLOB_VENDOR_URL` is set, `s3` if `VGI_STORAGE_S3_BUCKET` is,
 * `gcs` if `VGI_STORAGE_GCS_BUCKET` is, else `none`.
 *
 * @throws Error for an unknown `VGI_STORAGE_BACKEND`, or more than one backend
 *   configured without one.
 */
export function selectStorageBackend(env: Env): StorageBackend {
  const explicit = (env[ENV_STORAGE_BACKEND] ?? "").trim().toLowerCase();
  if (explicit) {
    if (explicit === "vendor" || explicit === "s3" || explicit === "gcs" || explicit === "none") return explicit;
    throw new Error(`${ENV_STORAGE_BACKEND} must be vendor, s3, gcs or none, got ${JSON.stringify(explicit)}`);
  }
  const configured = (
    [
      ["VGI_BLOB_VENDOR_URL", "vendor"],
      ["VGI_STORAGE_S3_BUCKET", "s3"],
      ["VGI_STORAGE_GCS_BUCKET", "gcs"],
    ] as const
  ).filter(([name]) => isSet(env, name));
  if (configured.length > 1) {
    throw new Error(
      `more than one storage backend is configured (${configured.map(([n]) => n).join(", ")}); ` +
        `set ${ENV_STORAGE_BACKEND} to choose one`,
    );
  }
  return configured[0]?.[1] ?? "none";
}

type Factory = (backend: "s3" | "gcs", env: Env, role: StorageRole) => DirectObjectStore;
let factory: Factory | null = null;
const memo = new WeakMap<object, Map<StorageRole, DirectObjectStore>>();

/** @internal Called by storage-direct.ts registerDirectStorage(). */
export function registerDirectObjectStoreFactory(f: Factory): void {
  factory = f;
}

/**
 * The S3/GCS object store for `env` and `role` (one per `env` object and role).
 *
 * @throws Error when this runtime has no S3/GCS support loaded.
 */
export function directObjectStore(backend: "s3" | "gcs", env: Env, role: StorageRole = "rpc"): DirectObjectStore {
  let byRole = memo.get(env);
  if (!byRole) {
    byRole = new Map();
    memo.set(env, byRole);
  }
  const cached = byRole.get(role);
  if (cached) return cached;
  if (!factory) {
    throw new Error(
      `${ENV_STORAGE_BACKEND}=${backend} is not available here: import "@query-farm/vgi/serve" (Bun/Node) to load the S3/GCS backends`,
    );
  }
  const store = factory(backend, env, role);
  byRole.set(role, store);
  return store;
}
