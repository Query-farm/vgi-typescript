// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Authenticated encryption of framework payloads written to FunctionStorage.
//
// The framework persists some of its own context to shared storage so any
// worker process can serve any RPC of an execution: the buffered-table init
// (the InitRequest, including `bind_call.secrets`) and the aggregate bind
// context (constant arguments, input schema). Storage may be a SQLite file or
// a Cloudflare Durable Object reached over the network, so none of that may
// sit there in plaintext, and nothing read back may be trusted unauthenticated.
//
// Sealed framework payload v1 (the same format in every VGI SDK; vgi-python's
// vgi/_storage_seal.py is the reference):
//
//   K    = HKDF-SHA256(ikm, salt = "", info = "vgi.storage.payload.v1", L = 32)
//          ikm is the worker's signing key exactly as this SDK resolved it (the
//          32 bytes decoded from the VGI_SIGNING_KEY hex, or the explicit
//          `signingKey` option). Without a signing key (stdio / AF_UNIX /
//          launcher) it is the host-local key file every VGI worker process of
//          this user on this host shares (./attach-secret-keyfile.ts), since one
//          query's RPCs are spread over several processes sharing one SQLite file.
//   AEAD = AES-256-GCM (WebCrypto, so it runs on Cloudflare Workers too),
//          random 12-byte nonce, 16-byte tag.
//   AAD  = "vgi.storage.payload.v1" || u32be(len)||shard_key
//          || u32be(len)||scope_id || u32be(len)||namespace || u32be(len)||key
//          so a payload copied to another execution, slot or attach shard fails.
//   wire = 0x01 || nonce(12) || ciphertext || tag(16)
//
// A payload that fails to open (unknown version, too short, tampered, wrong
// key, moved, or an unsealed value from an older release) throws
// StoragePayloadError. There is no plaintext fallback: executions are
// short-lived and their ids are never reused, so nothing needs migrating.
//
// Data user code writes to storage (buffered process() state, aggregate group
// states, table-in-out state, finalize output, work-queue items) is the
// function's own data and is not sealed.

import { hostLocalAttachSecretKey } from "../attach-secret-keyfile.js";

export const STORAGE_PAYLOAD_INFO = "vgi.storage.payload.v1";
export const STORAGE_PAYLOAD_VERSION = 0x01;
const KEY_LEN = 32;
const NONCE_LEN = 12;
const TAG_LEN = 16;
const UTF8 = new TextEncoder();
const INFO_BYTES = UTF8.encode(STORAGE_PAYLOAD_INFO);

/** A framework payload read from storage could not be authenticated. */
export class StoragePayloadError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "StoragePayloadError";
  }
}

/** The storage slot a payload is bound to. */
export interface StorageSlot {
  shardKey: string;
  scopeId: Uint8Array;
  ns: Uint8Array;
  key: Uint8Array;
}

function lp(b: Uint8Array): Uint8Array {
  const out = new Uint8Array(4 + b.length);
  new DataView(out.buffer).setUint32(0, b.length, false);
  out.set(b, 4);
  return out;
}

function concat(parts: Uint8Array[]): Uint8Array {
  let n = 0;
  for (const p of parts) n += p.length;
  const out = new Uint8Array(n);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

function hex(b: Uint8Array): string {
  let s = "";
  for (let i = 0; i < b.length; i++) s += b[i]!.toString(16).padStart(2, "0");
  return s;
}

/** The associated data binding a payload to its storage slot. */
export function storagePayloadAad(slot: StorageSlot): Uint8Array {
  return concat([INFO_BYTES, lp(UTF8.encode(slot.shardKey)), lp(slot.scopeId), lp(slot.ns), lp(slot.key)]);
}

/** `HKDF-SHA256(ikm, salt = "", info = "vgi.storage.payload.v1", 32)` as raw bytes. */
export async function deriveStoragePayloadKey(ikm: Uint8Array): Promise<Uint8Array> {
  const base = await crypto.subtle.importKey("raw", ikm as BufferSource, "HKDF", false, ["deriveBits"]);
  const bits = await crypto.subtle.deriveBits(
    { name: "HKDF", hash: "SHA-256", salt: new Uint8Array(0), info: INFO_BYTES },
    base,
    KEY_LEN * 8,
  );
  return new Uint8Array(bits);
}

// Derived AES keys, by ikm. Tiny (one or two entries in practice: the signing
// key, or the host-local key), bounded so a test that churns keys cannot grow it.
const KEY_CACHE_MAX = 16;
const keyCache = new Map<string, Promise<CryptoKey>>();

function aesKey(ikm: Uint8Array): Promise<CryptoKey> {
  const id = hex(ikm);
  let k = keyCache.get(id);
  if (!k) {
    k = (async () => {
      const raw = await deriveStoragePayloadKey(ikm);
      return crypto.subtle.importKey("raw", raw as BufferSource, "AES-GCM", false, ["encrypt", "decrypt"]);
    })();
    if (keyCache.size >= KEY_CACHE_MAX) keyCache.delete(keyCache.keys().next().value!);
    keyCache.set(id, k);
    // A failed derivation must not stay cached.
    k.catch(() => keyCache.delete(id));
  }
  return k;
}

/** Seal under an explicit `ikm`. `nonce` exists for test vectors only. */
export async function sealWith(
  payload: Uint8Array,
  ikm: Uint8Array,
  slot: StorageSlot,
  nonce?: Uint8Array,
): Promise<Uint8Array> {
  if (nonce !== undefined && nonce.length !== NONCE_LEN) throw new Error(`nonce must be ${NONCE_LEN} bytes`);
  const iv = nonce ?? crypto.getRandomValues(new Uint8Array(NONCE_LEN));
  const ct = new Uint8Array(
    await crypto.subtle.encrypt(
      { name: "AES-GCM", iv: iv as BufferSource, additionalData: storagePayloadAad(slot) as BufferSource, tagLength: TAG_LEN * 8 },
      await aesKey(ikm),
      payload as BufferSource,
    ),
  );
  const out = new Uint8Array(1 + NONCE_LEN + ct.length);
  out[0] = STORAGE_PAYLOAD_VERSION;
  out.set(iv, 1);
  out.set(ct, 1 + NONCE_LEN);
  return out;
}

/** Open under an explicit `ikm`; {@link StoragePayloadError} on any failure. */
export async function openWith(blob: Uint8Array, ikm: Uint8Array, slot: StorageSlot): Promise<Uint8Array> {
  const what = `framework storage payload ${JSON.stringify(new TextDecoder().decode(slot.ns))} for execution ${hex(slot.scopeId)}`;
  if (blob.length < 1 + NONCE_LEN + TAG_LEN) {
    throw new StoragePayloadError(`${what} is too short to be a sealed payload`);
  }
  if (blob[0] !== STORAGE_PAYLOAD_VERSION) {
    throw new StoragePayloadError(`${what} has unknown seal version 0x${blob[0]!.toString(16).padStart(2, "0")}`);
  }
  const iv = blob.subarray(1, 1 + NONCE_LEN);
  const body = blob.subarray(1 + NONCE_LEN);
  try {
    return new Uint8Array(
      await crypto.subtle.decrypt(
        { name: "AES-GCM", iv: iv as BufferSource, additionalData: storagePayloadAad(slot) as BufferSource, tagLength: TAG_LEN * 8 },
        await aesKey(ikm),
        body as BufferSource,
      ),
    );
  } catch {
    throw new StoragePayloadError(`${what} failed authentication (tampered, moved, or sealed under another key)`);
  }
}

/**
 * The input key material: the worker's signing key, else the host-local key.
 * Overridable for tests (`setStorageSealHostLocalKey`).
 */
let hostLocalOverride: (() => Promise<Uint8Array>) | null = null;

/** Test hook: replace the host-local key source (null restores the key file). */
export function setStorageSealHostLocalKey(fn: (() => Promise<Uint8Array>) | null): void {
  hostLocalOverride = fn;
}

async function ikmFor(signingKey: Uint8Array | undefined): Promise<Uint8Array> {
  if (signingKey && signingKey.length > 0) return signingKey;
  try {
    return await (hostLocalOverride ?? hostLocalAttachSecretKey)();
  } catch (e: any) {
    throw new StoragePayloadError(`no key to seal framework storage payloads: ${e?.message ?? e}`);
  }
}

/** Seal a framework payload bound to its storage slot. */
export async function sealPayload(
  payload: Uint8Array,
  signingKey: Uint8Array | undefined,
  slot: StorageSlot,
): Promise<Uint8Array> {
  return sealWith(payload, await ikmFor(signingKey), slot);
}

/** Open an envelope from {@link sealPayload} for the same slot. */
export async function openPayload(
  blob: Uint8Array,
  signingKey: Uint8Array | undefined,
  slot: StorageSlot,
): Promise<Uint8Array> {
  return openWith(blob, await ikmFor(signingKey), slot);
}
