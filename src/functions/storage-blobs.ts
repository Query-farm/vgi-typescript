// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Value envelope and blob-vendor externalization for any FunctionStorage.
// Port of vgi-python's vgi/function_storage_blobs.py; the stored bytes, the
// vendor scope names and the error messages are the same, so workers of one
// deployment written in either language read each other's values.
//
// `ExternalizingStorage` wraps a backend and stores values too large for it in
// the *blob vendor* (`VGI_BLOB_VENDOR_URL`), keeping only a small pointer in
// the backend. The FunctionStorage API does not change.
//
// Envelope (every value the wrapper writes: worker state, work-queue items,
// state_* values, log items):
//
//   inline:  0x00 || raw
//   pointer: 0x01 || u64be(size) || sha256(raw) (32 bytes) || object_id (ASCII)
//
// With a vendor client, a value of `threshold` bytes or more is uploaded
// *before* the backend call; reads fetch it back and verify size and SHA-256
// against the pointer. Without one every value is inline, and when the backend
// has a per-value cap (the Cloudflare DO's 1 MiB) a value whose envelope
// exceeds it is refused client-side with StorageValueTooLargeError, naming
// VGI_BLOB_VENDOR_URL. Decoding is strict: an empty value, an unknown tag, a
// truncated pointer or an object id outside the charset is a
// StorageEnvelopeError, as is a pointer read with no vendor configured.
//
// Where it applies (see wrapStorageFromEnv / resolveStorageFromEnv):
//   * cloudflare-do: always wrapped, vendor or not, so the stored format never
//     depends on a per-process environment variable.
//   * any other backend: only with VGI_STORAGE_EXTERNALIZE=1 (which then
//     requires the vendor). SQLite's on-disk format is unchanged by default.
//
// Clean-up: blobs are uploaded under the vendor scope (shard, "q.<hex
// execution id>") for queue items and (shard, "s.<hex scope id>") for state,
// worker state and log values. queueClear and executionClear delete the
// matching scope, workerCollect deletes the blobs it drained and queuePop the
// blob it claimed -- best effort, in the background, never on the query's
// critical path. stateDelete leaves blobs until the scope is cleared. The
// vendor's idle sweep and the bucket lifecycle rules are the backstop.
//
// Errors follow the SDK-neutral contract in the vgi extension's
// docs/testing/blob-test-catalog.md:
//   StorageBlobIntegrityError  "storage blob <id> failed its integrity check: sha256 mismatch" / "... size mismatch ..."
//   StorageBlobMissingError    "storage blob <id> is missing: not found in the blob vendor (expired or deleted?)"
//   StorageBlobError           "storage blob <id> could not be read: <detail>"
//
// Runtime-agnostic (fetch + Web Crypto through vgi-rpc's VendorBlobClient), so
// it wraps the Durable Object client on Cloudflare Workers as well as SQLite on
// Bun/Node.

import { VendorBlobClient, VendorError, storageStallTimeoutMsFromEnv, type BlobRef, type BlobScope } from "@query-farm/vgi-rpc";
import type { FunctionStorage, WorkerStateKey } from "./storage.js";

/** Values of at least this many bytes go to the blob vendor (default). */
export const DEFAULT_EXTERNALIZE_BYTES = 512 * 1024;
/** Largest blob a read will fetch (the vendor's per-object cap). */
export const MAX_BLOB_BYTES = 4 * 1024 ** 3;

export const ENV_VENDOR_URL = "VGI_BLOB_VENDOR_URL";
export const ENV_VENDOR_TOKEN = "VGI_BLOB_VENDOR_TOKEN";
export const ENV_EXTERNALIZE = "VGI_STORAGE_EXTERNALIZE";
export const ENV_EXTERNALIZE_BYTES = "VGI_STORAGE_EXTERNALIZE_BYTES";
export const ENV_EXTERNALIZE_BYTES_ALIAS = "VGI_CF_DO_EXTERNALIZE_BYTES";

export const TAG_INLINE = 0x00;
export const TAG_POINTER = 0x01;
/** Tag + u64 size + 32-byte SHA-256: the pointer's fixed part. */
export const POINTER_FIXED_BYTES = 1 + 8 + 32;
const OBJECT_ID_RE = /^[A-Za-z0-9._:-]{1,128}$/;
// The vendor's scope.shard charset; shard keys outside it are hashed.
const SHARD_RE = /^[A-Za-z0-9._-]{1,128}$/;
/** Concurrent vendor transfers per storage call. */
const TRANSFER_PARALLELISM = 8;
const TRUE_VALUES = new Set(["1", "true", "yes", "on"]);

type Env = Readonly<Record<string, string | undefined>>;

function processEnv(): Env {
  return (globalThis as { process?: { env?: Env } }).process?.env ?? {};
}

// ============================================================================
// Errors
// ============================================================================

/** A value read from storage is not a valid envelope (empty, unknown tag,
 *  malformed pointer), or is a pointer read with no blob vendor configured. */
export class StorageEnvelopeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StorageEnvelopeError";
  }
}

/** An externalized storage value's blob could not be read. The message starts
 *  `storage blob <object id>`. */
export class StorageBlobError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StorageBlobError";
  }
}

/** The downloaded blob does not match the pointer's size or SHA-256. */
export class StorageBlobIntegrityError extends StorageBlobError {
  constructor(message: string) {
    super(message);
    this.name = "StorageBlobIntegrityError";
  }
}

/** The blob is gone from the vendor or its storage (expired, deleted, never uploaded). */
export class StorageBlobMissingError extends StorageBlobError {
  constructor(message: string) {
    super(message);
    this.name = "StorageBlobMissingError";
  }
}

/** A value is too large for the backend and no blob vendor is configured. */
export class StorageValueTooLargeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StorageValueTooLargeError";
  }
}

const MISSING_CODES = new Set(["not_found", "not_uploaded", "NoSuchKey"]);

/** Map a vendor client failure onto the storage-blob error contract. */
function blobReadError(ref: BlobRef, error: unknown): StorageBlobError {
  const code = error instanceof VendorError ? error.code : null;
  const status = error instanceof VendorError ? error.status : null;
  if (code === "sha256_mismatch") {
    return new StorageBlobIntegrityError(`storage blob ${ref.objectId} failed its integrity check: sha256 mismatch`);
  }
  if (code === "size_mismatch") {
    return new StorageBlobIntegrityError(
      `storage blob ${ref.objectId} failed its integrity check: size mismatch (expected ${ref.size} bytes)`,
    );
  }
  if (status === 404 || (code !== null && MISSING_CODES.has(code))) {
    return new StorageBlobMissingError(
      `storage blob ${ref.objectId} is missing: not found in the blob vendor (expired or deleted?)`,
    );
  }
  return new StorageBlobError(
    `storage blob ${ref.objectId} could not be read: ${(error as Error)?.message ?? String(error)}`,
  );
}

// ============================================================================
// Envelope
// ============================================================================

function hexToBytes(hex: string): Uint8Array {
  if (!/^[0-9a-fA-F]*$/.test(hex) || hex.length % 2 !== 0) throw new Error(`not hex: ${hex}`);
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return out;
}

function bytesToHex(bytes: Uint8Array): string {
  let s = "";
  for (let i = 0; i < bytes.length; i++) s += bytes[i]!.toString(16).padStart(2, "0");
  return s;
}

async function sha256Hex(data: Uint8Array): Promise<string> {
  const copy = new Uint8Array(data.byteLength);
  copy.set(data);
  return bytesToHex(new Uint8Array(await crypto.subtle.digest("SHA-256", copy.buffer)));
}

/** The inline envelope of `value`: `0x00 || value`. */
export function encodeInline(value: Uint8Array): Uint8Array {
  const out = new Uint8Array(value.byteLength + 1);
  out[0] = TAG_INLINE;
  out.set(value, 1);
  return out;
}

/**
 * The pointer envelope for a vendored blob:
 * `0x01 || u64be(size) || sha256 || object_id`.
 * @throws Error if the object id is outside `[A-Za-z0-9._:-]{1,128}`.
 */
export function encodePointer(ref: BlobRef): Uint8Array {
  if (!OBJECT_ID_RE.test(ref.objectId)) {
    throw new Error(`blob vendor returned an unusable object id ${JSON.stringify(ref.objectId)}`);
  }
  const digest = hexToBytes(ref.sha256);
  if (digest.length !== 32) throw new Error("blob sha256 must be 32 bytes of hex");
  const id = new TextEncoder().encode(ref.objectId);
  const out = new Uint8Array(POINTER_FIXED_BYTES + id.length);
  out[0] = TAG_POINTER;
  new DataView(out.buffer).setBigUint64(1, BigInt(ref.size), false);
  out.set(digest, 9);
  out.set(id, POINTER_FIXED_BYTES);
  return out;
}

/** Parse a pointer envelope (tag 0x01).
 *  @throws StorageEnvelopeError if it is truncated or its object id is malformed. */
export function decodePointer(envelope: Uint8Array): BlobRef {
  const idBytes = envelope.subarray(POINTER_FIXED_BYTES);
  let objectId = "";
  for (let i = 0; i < idBytes.length; i++) objectId += String.fromCharCode(idBytes[i]!);
  if (envelope.length <= POINTER_FIXED_BYTES || !OBJECT_ID_RE.test(objectId)) {
    throw new StorageEnvelopeError(`malformed blob pointer in stored value (${envelope.length} bytes)`);
  }
  const view = new DataView(envelope.buffer, envelope.byteOffset, envelope.byteLength);
  return {
    objectId,
    size: Number(view.getBigUint64(1, false)),
    sha256: bytesToHex(envelope.subarray(9, POINTER_FIXED_BYTES)),
  };
}

/** The envelope's tag.
 *  @throws StorageEnvelopeError for an empty value or an unknown tag. */
export function envelopeTag(envelope: Uint8Array): number {
  if (envelope.length === 0) {
    throw new StorageEnvelopeError("empty stored value: not a value envelope (written without the envelope?)");
  }
  const tag = envelope[0]!;
  if (tag !== TAG_INLINE && tag !== TAG_POINTER) {
    throw new StorageEnvelopeError(
      `stored value has unknown envelope tag 0x${tag.toString(16).padStart(2, "0")} (written by an older client or ` +
        "without the envelope? mixing client versions within one query is not supported)",
    );
  }
  return tag;
}

/** Vendor `scope.execution_id` for one queue (`q`) or state scope (`s`):
 *  `<kind>.<hex ident>`, or `<kind>.h<hex sha256(ident)>` past 128 characters. */
export async function blobExecutionId(kind: "q" | "s", ident: Uint8Array): Promise<string> {
  const text = `${kind}.${bytesToHex(ident)}`;
  if (text.length <= 128) return text;
  return `${kind}.h${await sha256Hex(ident)}`;
}

/** Vendor `scope.shard` for a storage shard key: empty becomes `loc-anon`; a key
 *  outside `[A-Za-z0-9._-]{1,128}` becomes `h-<hex sha256(utf8 key)>`. */
export async function blobShard(shardKey: string): Promise<string> {
  const shard = shardKey || "loc-anon";
  if (SHARD_RE.test(shard)) return shard;
  return `h-${await sha256Hex(new TextEncoder().encode(shard))}`;
}

async function parallelMap<T, R>(items: readonly T[], fn: (item: T) => Promise<R>): Promise<R[]> {
  if (items.length <= 1) return Promise.all(items.map(fn));
  const out = new Array<R>(items.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(TRANSFER_PARALLELISM, items.length) }, async () => {
    for (;;) {
      const i = next++;
      if (i >= items.length) return;
      out[i] = await fn(items[i]!);
    }
  });
  await Promise.all(workers);
  return out;
}

function newCallId(): string {
  const b = new Uint8Array(16);
  crypto.getRandomValues(b);
  return bytesToHex(b);
}

// ============================================================================
// Environment
// ============================================================================

/** Read `VGI_STORAGE_EXTERNALIZE_BYTES` (alias `VGI_CF_DO_EXTERNALIZE_BYTES`);
 *  {@link DEFAULT_EXTERNALIZE_BYTES} when unset.
 *  @throws Error if the value is not a positive integer. */
export function externalizeBytesFromEnv(env: Env = processEnv()): number {
  for (const name of [ENV_EXTERNALIZE_BYTES, ENV_EXTERNALIZE_BYTES_ALIAS]) {
    const raw = (env[name] ?? "").trim();
    if (!raw) continue;
    if (!/^[+-]?\d+$/.test(raw)) throw new Error(`${name} must be an integer, got ${JSON.stringify(raw)}`);
    const value = Number(raw);
    if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be positive, got ${value}`);
    return value;
  }
  return DEFAULT_EXTERNALIZE_BYTES;
}

/** The vendor client from `VGI_BLOB_VENDOR_URL` and its token (falling back to
 *  `VGI_CF_DO_TOKEN`), or `null` when the URL is unset. Storage transfers use
 *  a stall timeout (`VGI_RPC_STORAGE_STALL_TIMEOUT_SECONDS`, default 30 s) read
 *  from the same `env`, never a whole-transfer deadline.
 *  @throws Error if the URL is set but no token is, or the stall timeout will not parse. */
export function blobClientFromEnv(env: Env = processEnv()): VendorBlobClient | null {
  const url = (env[ENV_VENDOR_URL] ?? "").trim();
  if (!url) return null;
  const token = (env[ENV_VENDOR_TOKEN] ?? "").trim() || (env.VGI_CF_DO_TOKEN ?? "").trim();
  if (!token) throw new Error(`${ENV_VENDOR_URL} is set but neither ${ENV_VENDOR_TOKEN} nor VGI_CF_DO_TOKEN is`);
  return new VendorBlobClient(url, token, {
    purpose: "fs",
    maxFetchBytes: MAX_BLOB_BYTES,
    stallTimeoutMs: storageStallTimeoutMsFromEnv(env),
  });
}

/** Options for {@link wrapStorageFromEnv}. */
export interface WrapStorageOptions {
  /** The backend's per-value cap, or undefined for none. A backend with a cap
   *  (the Cloudflare DO) is always wrapped. */
  maxValueBytes?: number | undefined;
  /** Environment source (default `process.env`; on Cloudflare Workers pass the
   *  Worker's `env`). */
  env?: Env | undefined;
}

/**
 * Wrap `base` in {@link ExternalizingStorage} when the environment asks for it.
 *
 * A backend with a per-value cap (`maxValueBytes`, i.e. the Cloudflare DO) is
 * always wrapped, vendor or not, so its stored format is the same in every
 * process. Any other backend is wrapped only with `VGI_STORAGE_EXTERNALIZE=1`,
 * which then requires `VGI_BLOB_VENDOR_URL`. An already-wrapped backend is
 * returned unchanged.
 *
 * @throws Error for `VGI_STORAGE_EXTERNALIZE=1` without a vendor, or a malformed variable.
 */
export function wrapStorageFromEnv(base: FunctionStorage, options: WrapStorageOptions = {}): FunctionStorage {
  if (base instanceof ExternalizingStorage) return base;
  const env = options.env ?? processEnv();
  const enabled = TRUE_VALUES.has((env[ENV_EXTERNALIZE] ?? "").trim().toLowerCase());
  const maxValueBytes = options.maxValueBytes;
  if (maxValueBytes === undefined && !enabled) return base;
  const blobs = blobClientFromEnv(env);
  if (enabled && blobs === null) {
    throw new Error(`${ENV_EXTERNALIZE}=1 needs the blob vendor: set ${ENV_VENDOR_URL}`);
  }
  let threshold = externalizeBytesFromEnv(env);
  if (maxValueBytes !== undefined) threshold = Math.min(threshold, maxValueBytes);
  return new ExternalizingStorage(base, { blobClient: blobs, threshold, maxValueBytes });
}

// ============================================================================
// The wrapper
// ============================================================================

/** Options for {@link ExternalizingStorage}. */
export interface ExternalizingStorageOptions {
  /** The vendor client, or null to keep every value inline. */
  blobClient: VendorBlobClient | null;
  /** With a vendor, values of at least this many bytes are stored as blobs
   *  (default {@link DEFAULT_EXTERNALIZE_BYTES}). */
  threshold?: number | undefined;
  /** The backend's per-value cap. The envelope of an inline value must fit, so
   *  `threshold` may not exceed it; with no vendor, an over-cap value raises
   *  {@link StorageValueTooLargeError}. */
  maxValueBytes?: number | undefined;
}

/** State one wrapper shares with its per-shard views. */
interface SharedState {
  readonly blobs: VendorBlobClient | null;
  readonly threshold: number;
  readonly maxValueBytes: number | undefined;
  readonly pending: Set<Promise<void>>;
}

/**
 * A FunctionStorage that envelopes values and externalizes large ones to the
 * blob vendor. Delegates every operation to the wrapped backend; see the module
 * header for the envelope, scoping and clean-up.
 */
export class ExternalizingStorage implements FunctionStorage {
  private readonly _base: FunctionStorage;
  private readonly _shared: SharedState;
  private readonly _shardKey: string;

  /** @throws Error if `threshold` is not positive or exceeds `maxValueBytes`. */
  constructor(base: FunctionStorage, options: ExternalizingStorageOptions);
  /** @internal A per-shard view sharing the vendor client and clean-up. */
  constructor(base: FunctionStorage, options: ExternalizingStorageOptions, shared: SharedState, shardKey: string);
  constructor(base: FunctionStorage, options: ExternalizingStorageOptions, shared?: SharedState, shardKey = "") {
    this._base = base;
    this._shardKey = shardKey;
    if (shared) {
      this._shared = shared;
      return;
    }
    const threshold = options.threshold ?? DEFAULT_EXTERNALIZE_BYTES;
    const max = options.maxValueBytes;
    if (!Number.isSafeInteger(threshold) || threshold < 1 || (max !== undefined && threshold > max)) {
      throw new Error(
        `externalize threshold (${ENV_EXTERNALIZE_BYTES}) must be between 1 and ` +
          `${max !== undefined ? max : "unbounded"}, got ${threshold}`,
      );
    }
    this._shared = { blobs: options.blobClient, threshold, maxValueBytes: max, pending: new Set() };
  }

  /** The wrapped backend. */
  get base(): FunctionStorage {
    return this._base;
  }

  /** The vendor client, if any. */
  get blobClient(): VendorBlobClient | null {
    return this._shared.blobs;
  }

  /** Values of at least this many bytes are externalized (with a vendor). */
  get threshold(): number {
    return this._shared.threshold;
  }

  /** The shard this view routes to ("" = not bound to an ATTACH). */
  get shardKey(): string {
    return this._shardKey;
  }

  /** A view routed to one attach shard (the wrapped backend's own view when it
   *  shards remotely), sharing the vendor client and clean-up. */
  forShard(shardKey: string): ExternalizingStorage {
    if (shardKey === this._shardKey) return this;
    const base = typeof this._base.forShard === "function" ? this._base.forShard(shardKey) : this._base;
    return new ExternalizingStorage(base, { blobClient: null }, this._shared, shardKey);
  }

  /** Wait for pending background blob clean-up, then close the backend. */
  async close(): Promise<void> {
    await this.flushBlobCleanup();
    await this._base.close?.();
  }

  // --- encode / decode -----------------------------------------------------

  /**
   * Envelope each value, uploading large ones first. Each upload carries the
   * idempotency key `<call id>.<index>`, so a vendor-level retry of the same
   * logical write reuses the object.
   */
  async encodeValues(values: readonly Uint8Array[], blobExecId: string): Promise<Uint8Array[]> {
    const { blobs, threshold, maxValueBytes } = this._shared;
    const large: number[] = [];
    const envelopes: Array<Uint8Array | null> = values.map((v, i) => {
      if (blobs !== null && v.byteLength >= threshold) {
        large.push(i);
        return null;
      }
      return encodeInline(v);
    });
    if (blobs === null && maxValueBytes !== undefined) {
      for (const env of envelopes) {
        if (env !== null && env.byteLength > maxValueBytes) {
          throw new StorageValueTooLargeError(
            `storage value of ${env.byteLength - 1} bytes exceeds the backend's ${maxValueBytes}-byte limit; ` +
              `set ${ENV_VENDOR_URL} (the blob vendor) so large values are stored as blobs`,
          );
        }
      }
    }
    if (large.length > 0 && blobs !== null) {
      const callId = newCallId();
      const scope: BlobScope = { shard: await blobShard(this._shardKey), executionId: blobExecId };
      const pointers = await parallelMap(large, async (i) =>
        encodePointer(await blobs.putBytes(values[i]!, { scope, idempotencyKey: `${callId}.${i}` })),
      );
      large.forEach((i, j) => {
        envelopes[i] = pointers[j]!;
      });
    }
    return envelopes as Uint8Array[];
  }

  /**
   * Decode envelopes, fetching pointers in parallel (null passes through).
   * @param refsOut When given, receives the blob references that were read.
   * @throws StorageEnvelopeError for an invalid envelope or a pointer read with
   *   no vendor configured; StorageBlobError (or a subclass) when a pointer's
   *   blob is missing, fails its size or SHA-256 check, or cannot be fetched.
   */
  async decodeValues(envelopes: ReadonlyArray<Uint8Array | null>, refsOut?: BlobRef[]): Promise<Array<Uint8Array | null>> {
    const pointers: number[] = [];
    const out: Array<Uint8Array | null> = envelopes.map((env, i) => {
      if (env === null) return null;
      if (envelopeTag(env) === TAG_POINTER) {
        pointers.push(i);
        return null;
      }
      return env.slice(1);
    });
    if (pointers.length === 0) return out;
    const blobs = this._shared.blobs;
    if (blobs === null) {
      throw new StorageEnvelopeError(
        `stored value is a blob pointer but ${ENV_VENDOR_URL} is not configured in this process`,
      );
    }
    const refs = pointers.map((i) => decodePointer(envelopes[i]!));
    const data = await parallelMap(refs, async (ref) => {
      try {
        return await blobs.getBytes(ref);
      } catch (error) {
        throw blobReadError(ref, error);
      }
    });
    pointers.forEach((i, j) => {
      out[i] = data[j]!;
    });
    refsOut?.push(...refs);
    return out;
  }

  private async decodeAll(envelopes: readonly Uint8Array[], refsOut?: BlobRef[]): Promise<Uint8Array[]> {
    return (await this.decodeValues(envelopes, refsOut)) as Uint8Array[];
  }

  // --- background clean-up -------------------------------------------------

  private submitCleanup(fn: () => Promise<unknown>, what: string): void {
    const pending = this._shared.pending;
    const task: Promise<void> = fn()
      .then(() => undefined)
      .catch((error) => {
        console.warn(`blob vendor clean-up after ${what} failed: ${(error as Error)?.message ?? error}`);
      })
      .finally(() => {
        pending.delete(task);
      });
    pending.add(task);
  }

  /** Wait for pending background blob clean-up (for tests and shutdown). */
  async flushBlobCleanup(): Promise<void> {
    while (this._shared.pending.size > 0) {
      await Promise.all([...this._shared.pending]);
    }
  }

  private deleteBlobs(refs: BlobRef[], what: string): void {
    const blobs = this._shared.blobs;
    if (blobs === null || refs.length === 0) return;
    this.submitCleanup(() => blobs.delete(refs), what);
  }

  private deleteBlobScope(kind: "q" | "s", ident: Uint8Array, what: string): void {
    const blobs = this._shared.blobs;
    if (blobs === null) return;
    const shardKey = this._shardKey;
    this.submitCleanup(
      async () => blobs.deleteScope(await blobShard(shardKey), await blobExecutionId(kind, ident)),
      what,
    );
  }

  // --- FunctionStorage -----------------------------------------------------

  async workerPut(executionId: Uint8Array, worker: WorkerStateKey, state: Uint8Array): Promise<void> {
    const [encoded] = await this.encodeValues([state], await blobExecutionId("s", executionId));
    await this._base.workerPut(executionId, worker, encoded!);
  }

  /** Drain and decode; the drained blobs are deleted in the background. */
  async workerCollect(executionId: Uint8Array): Promise<Uint8Array[]> {
    const drained: BlobRef[] = [];
    const values = await this.decodeAll(await this._base.workerCollect(executionId), drained);
    this.deleteBlobs(drained, "workerCollect");
    return values;
  }

  async workerScan(executionId: Uint8Array): Promise<Array<[number, Uint8Array]>> {
    const rows = await this._base.workerScan(executionId);
    const values = await this.decodeAll(rows.map(([, v]) => v));
    return rows.map(([id], i) => [id, values[i]!]);
  }

  async queuePush(executionId: Uint8Array, items: Uint8Array[]): Promise<number> {
    const encoded = await this.encodeValues(items, await blobExecutionId("q", executionId));
    return this._base.queuePush(executionId, encoded);
  }

  /** Pop one item; its blob, if any, is deleted once fetched. */
  async queuePop(executionId: Uint8Array): Promise<Uint8Array | null> {
    const env = await this._base.queuePop(executionId);
    if (env === null) return null;
    const claimed: BlobRef[] = [];
    const [value] = await this.decodeValues([env], claimed);
    this.deleteBlobs(claimed, "queuePop");
    return value ?? null;
  }

  /** Clear the queue and delete its blob scope. */
  async queueClear(executionId: Uint8Array): Promise<number> {
    const cleared = await this._base.queueClear(executionId);
    this.deleteBlobScope("q", executionId, "queueClear");
    return cleared;
  }

  async stateGet(scopeId: Uint8Array, ns: Uint8Array, key: Uint8Array): Promise<Uint8Array | null> {
    const env = await this._base.stateGet(scopeId, ns, key);
    return env === null ? null : ((await this.decodeValues([env]))[0] ?? null);
  }

  async statePut(scopeId: Uint8Array, ns: Uint8Array, key: Uint8Array, value: Uint8Array): Promise<void> {
    const [encoded] = await this.encodeValues([value], await blobExecutionId("s", scopeId));
    await this._base.statePut(scopeId, ns, key, encoded!);
  }

  async stateGetMany(scopeId: Uint8Array, ns: Uint8Array, keys: Uint8Array[]): Promise<Array<Uint8Array | null>> {
    return this.decodeValues(await this._base.stateGetMany(scopeId, ns, keys));
  }

  async statePutMany(scopeId: Uint8Array, ns: Uint8Array, items: Array<[Uint8Array, Uint8Array]>): Promise<void> {
    if (items.length === 0) {
      await this._base.statePutMany(scopeId, ns, items);
      return;
    }
    const encoded = await this.encodeValues(
      items.map(([, v]) => v),
      await blobExecutionId("s", scopeId),
    );
    await this._base.statePutMany(
      scopeId,
      ns,
      items.map(([k], i) => [k, encoded[i]!]),
    );
  }

  /** Delete rows (their blobs stay until the scope is cleared). */
  async stateDelete(scopeId: Uint8Array, ns: Uint8Array, keys: Uint8Array[]): Promise<number> {
    if (typeof this._base.stateDelete !== "function") {
      throw new Error("the wrapped storage backend does not implement stateDelete");
    }
    return this._base.stateDelete(scopeId, ns, keys);
  }

  async stateAppend(scopeId: Uint8Array, ns: Uint8Array, key: Uint8Array, item: Uint8Array): Promise<number> {
    const [encoded] = await this.encodeValues([item], await blobExecutionId("s", scopeId));
    return this._base.stateAppend(scopeId, ns, key, encoded!);
  }

  async stateLogScan(
    scopeId: Uint8Array,
    ns: Uint8Array,
    key: Uint8Array,
    afterId: number = -1,
    limit: number | null = null,
  ): Promise<Array<[number, Uint8Array]>> {
    const rows = await this._base.stateLogScan(scopeId, ns, key, afterId, limit);
    const values = await this.decodeAll(rows.map(([, v]) => v));
    return rows.map(([id], i) => [id, values[i]!]);
  }

  /** Clear the scope and delete its blob scope. */
  async executionClear(scopeId: Uint8Array): Promise<number> {
    const deleted = await this._base.executionClear(scopeId);
    this.deleteBlobScope("s", scopeId, "executionClear");
    return deleted;
  }
}
