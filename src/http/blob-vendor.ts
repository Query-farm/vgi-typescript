// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// HTTP server settings derived from the blob-vendor environment.
// Port of vgi-python's `vgi.serve.resolve_blob_vendor_http_config`.
//
// `VGI_BLOB_VENDOR_URL` (with `VGI_BLOB_VENDOR_TOKEN`, falling back to
// `VGI_CF_DO_TOKEN`) turns on result externalization
// (`VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES`, default 1 MiB;
// `VGI_RPC_EXTERNAL_COMPRESSION`) and client upload URLs
// (`VGI_RPC_MAX_UPLOAD_BYTES`). `VGI_RPC_MAX_REQUEST_BYTES` is read either way:
// it is an explicit operator setting, advertised and enforced, and clients
// (the DuckDB extension) only take the upload-URL path for a request larger
// than it, so set it with the vendor.
//
// Runtime-agnostic: serveVgiWorker reads it from `process.env`; a Cloudflare
// Worker passes its own `env` and spreads the result into createVgiFetch:
//
//   createVgiFetch({ ...blobVendorHttpConfigFromEnv(env), protocol, signingKey, landingInfo });

import {
  externalConfigFromEnv,
  maxUploadBytesFromEnv,
  type ExternalLocationConfig,
  type UploadUrlProvider,
} from "@query-farm/vgi-rpc";

/** The variables {@link blobVendorHttpConfigFromEnv} reads. */
export interface BlobVendorEnv {
  VGI_BLOB_VENDOR_URL?: string | undefined;
  VGI_BLOB_VENDOR_TOKEN?: string | undefined;
  VGI_CF_DO_TOKEN?: string | undefined;
  VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES?: string | undefined;
  VGI_RPC_EXTERNAL_COMPRESSION?: string | undefined;
  VGI_RPC_MAX_REQUEST_BYTES?: string | undefined;
  VGI_RPC_MAX_UPLOAD_BYTES?: string | undefined;
}

/** HTTP handler options from the blob-vendor environment. Every field is
 *  absent when nothing is configured. */
export interface BlobVendorHttpConfig {
  /** Large results are uploaded to the vendor and replaced by a pointer
   *  carrying `vgi_rpc.location.sha256`; inbound pointers are fetched only from
   *  the vendor's storage hosts. */
  externalLocation?: ExternalLocationConfig | undefined;
  /** The vendor, vending pre-signed upload URLs (`__upload_url__/init`) so
   *  clients can send requests over {@link maxRequestBytes}. */
  uploadUrlProvider?: UploadUrlProvider | undefined;
  /** `VGI_RPC_MAX_UPLOAD_BYTES`, advertised as `VGI-Max-Upload-Bytes`. */
  maxUploadBytes?: number | undefined;
  /** `VGI_RPC_MAX_REQUEST_BYTES`, advertised as `VGI-Max-Request-Bytes` and enforced. */
  maxRequestBytes?: number | undefined;
}

function positiveInt(env: BlobVendorEnv, name: keyof BlobVendorEnv): number | undefined {
  const raw = (env[name] ?? "").trim();
  if (!raw) return undefined;
  if (!/^[+-]?\d+$/.test(raw)) throw new Error(`${name} must be an integer, got ${JSON.stringify(raw)}`);
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be positive, got ${value}`);
  return value;
}

/**
 * Read the blob-vendor HTTP settings from `env`.
 *
 * @throws Error if a variable is set but malformed, or the vendor URL is set
 *   without a token.
 */
export function blobVendorHttpConfigFromEnv(env: BlobVendorEnv): BlobVendorHttpConfig {
  const config: BlobVendorHttpConfig = {};
  const maxRequestBytes = positiveInt(env, "VGI_RPC_MAX_REQUEST_BYTES");
  if (maxRequestBytes !== undefined) config.maxRequestBytes = maxRequestBytes;
  const vendorEnv = env as Readonly<Record<string, string | undefined>>;
  const external = externalConfigFromEnv(vendorEnv);
  if (external === null) return config;
  config.externalLocation = external;
  config.uploadUrlProvider = external.storage;
  const maxUploadBytes = maxUploadBytesFromEnv(vendorEnv);
  if (maxUploadBytes !== null) config.maxUploadBytes = maxUploadBytes;
  return config;
}
