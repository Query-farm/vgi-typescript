// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Framework-owned delivery of ATTACH options to functions and transaction
// hooks — secret ones (`AttachOptionSpec.secret`) encrypted with a key the
// client never has.
//
// The client hands `attach_opaque_data` back on every function call and hook,
// so the attach value is the only thing that can carry an option from
// `catalog_attach` to a later call: a catalog_attach and the function calls of
// its queries may run in different worker processes (launcher pools, HTTP). On
// HTTP the whole value is sealed, but on stdio / AF_UNIX it is raw bytes the
// client can read, and the sealing spec's rule 5 forbids a secret in plaintext
// in any opaque value. So the framework packs the options itself, after the
// catalog's own bytes, and encrypts the secret ones (SPEC-secret-options.md):
//
//   framework attach plaintext = uuid(16) || catalog_bytes || trailer
//   trailer = body || u32le(len(body)) || TRAILER_MAGIC(8)
//   body    = u32le(len(plain)) || plain || secret
//     plain  = Arrow IPC batch (1 row) of the non-secret options, or empty
//     secret = empty, or SECRET_VERSION(1) || nonce(24) || XChaCha20-Poly1305(
//                ciphertext || tag) of an Arrow IPC batch of the secret options
//   AEAD key = HKDF-SHA256(root, salt = "", info = "vgi.attach_secret_options.v1", 32)
//   AAD      = "vgi.attach_secret_options.v1" 0x00 || uuid
//
// `root` is the explicitly configured `attachSecretKey`, else the HTTP signing
// key (when the worker seals), else the host-local key file shared by every
// VGI worker of this user on this host (./attach-secret-keyfile.ts). The AAD
// binds the blob to its attach's framework UUID, so a blob copied into another
// attach value does not open. XChaCha20-Poly1305 comes from @noble/ciphers
// (pure JS: Node, Bun, browsers, workerd); HKDF from WebCrypto.
//
// Catalog and function code never sees the trailer: the framework strips it
// (with the UUID) before any catalog or function body runs, and records the
// decoded options against the opened bytes, where `attachOptions()` finds
// them. An attach with no options carries no trailer at all, so its bytes are
// exactly what they were before this module existed.

import { xchacha20poly1305 } from "@noble/ciphers/chacha.js";
import {
  batchFromColumns,
  deserializeBatch,
  iterRows,
  schema as makeSchema,
  serializeBatch,
  type VgiField,
} from "./arrow/index.js";
import { OpaqueDataRejectedError } from "./crypto.js";
import { hostLocalAttachSecretKey } from "./attach-secret-keyfile.js";

const UTF8 = new TextEncoder();

/** HKDF `info`, and the AAD prefix (before the 0x00 and the attach uuid). */
export const ATTACH_SECRET_OPTIONS_INFO = "vgi.attach_secret_options.v1";

/** First byte of the secret blob: XChaCha20-Poly1305 with a 24-byte nonce. */
export const ATTACH_SECRET_OPTIONS_VERSION = 1;

/** Last 8 bytes of a framework attach plaintext that carries options. */
const TRAILER_MAGIC = Uint8Array.of(0x00, 0x56, 0x47, 0x49, 0x41, 0x4f, 0x01, 0xff); // \0 V G I A O \1 \xff
const TRAILER_FIXED = 4 + TRAILER_MAGIC.length;
const NONCE_LEN = 24;
const TAG_LEN = 16;
const AAD_PREFIX = UTF8.encode(`${ATTACH_SECRET_OPTIONS_INFO}\0`);
const UUID_LEN = 16;
const FIELD = "attach_opaque_data";

/** A decoded ATTACH options record: option name → value, as `attach()` got it. */
export type AttachOptionValues = Readonly<Record<string, unknown>>;

/**
 * Where the secret-options root key comes from. `explicit` is the optional
 * `attachSecretKey` configuration; `signingKey` the HTTP sealing key.
 * `hostLocal` is resolved lazily, and only when a secret option has a value.
 */
export interface AttachSecretKeySource {
  explicit?: Uint8Array | undefined;
  signingKey?: Uint8Array | undefined;
  /** Override for tests; defaults to the host-local key file. */
  hostLocal?: (() => Promise<Uint8Array>) | undefined;
}

/** Raised when a secret option has a value but no key can be obtained. */
export class AttachSecretKeyUnavailableError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AttachSecretKeyUnavailableError";
  }
}

/** Raised by {@link attachOptions} for bytes the framework did not hand out. */
export class AttachOptionsUnavailableError extends Error {
  constructor() {
    super(
      "attachOptions(): these bytes are not an attach_opaque_data value the framework handed to this call. " +
        "Pass the attach value exactly as received (bindCall.attach_opaque_data, a TableBuffering attachId, or a " +
        "catalog method's attachOpaqueData argument, or a subarray of it), not a copy.",
    );
    this.name = "AttachOptionsUnavailableError";
  }
}

// ----------------------------------------------------------------- the key

const subkeys = new Map<string, Promise<Uint8Array>>();

function hexOf(b: Uint8Array): string {
  let s = "";
  for (let i = 0; i < b.length; i++) s += b[i]!.toString(16).padStart(2, "0");
  return s;
}

/**
 * The AEAD key for secret ATTACH options derived from `root`:
 * HKDF-SHA256(ikm = root, salt = empty, info = "vgi.attach_secret_options.v1", L = 32).
 * The root itself is never used as the cipher key.
 */
export function deriveAttachSecretKey(root: Uint8Array): Promise<Uint8Array> {
  const id = hexOf(root);
  let derived = subkeys.get(id);
  if (!derived) {
    derived = (async () => {
      const ikm = await crypto.subtle.importKey("raw", root as BufferSource, "HKDF", false, ["deriveBits"]);
      const bits = await crypto.subtle.deriveBits(
        { name: "HKDF", hash: "SHA-256", salt: new Uint8Array(0), info: UTF8.encode(ATTACH_SECRET_OPTIONS_INFO) },
        ikm,
        256,
      );
      return new Uint8Array(bits);
    })();
    subkeys.set(id, derived);
    // A failed derivation must not stick.
    derived.catch(() => subkeys.delete(id));
  }
  return derived;
}

function defaultHostLocal(): Promise<Uint8Array> {
  return hostLocalAttachSecretKey();
}

function noKeyMessage(): string {
  return (
    "A secret ATTACH option has a value but this worker has no key to encrypt it with: configure " +
    "`attachSecretKey`, run with a signing key (VGI_SIGNING_KEY), or run where the host-local key file " +
    "(VGI_ATTACH_SECRET_KEY_FILE) can be used."
  );
}

/** The root key: explicit, else the signing key, else the host-local key. */
async function rootKey(source: AttachSecretKeySource): Promise<Uint8Array> {
  if (source.explicit && source.explicit.length > 0) return source.explicit;
  if (source.signingKey && source.signingKey.length > 0) return source.signingKey;
  return (source.hostLocal ?? defaultHostLocal)();
}

function aadFor(uuid: Uint8Array): Uint8Array {
  const out = new Uint8Array(AAD_PREFIX.length + uuid.length);
  out.set(AAD_PREFIX, 0);
  out.set(uuid, AAD_PREFIX.length);
  return out;
}

// ------------------------------------------------------------------ packing

function u32le(n: number): Uint8Array {
  const b = new Uint8Array(4);
  new DataView(b.buffer).setUint32(0, n, true);
  return b;
}

function readU32le(b: Uint8Array, off: number): number {
  return new DataView(b.buffer, b.byteOffset, b.byteLength).getUint32(off, true);
}

function concat(...parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const p of parts) total += p.length;
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}

/**
 * Split a catalog_attach `options` IPC batch into one batch of the non-secret
 * options and one of the secret options that have a value (a NULL secret is
 * not a secret). `secretNames` are compared case-insensitively. Returns null
 * for absent / empty options.
 */
function splitOptions(
  optionsIpc: Uint8Array | null | undefined,
  secretNames: ReadonlySet<string>,
): { plain: Uint8Array | null; secret: Uint8Array | null } | null {
  if (!optionsIpc || optionsIpc.length === 0) return null;
  const batch = deserializeBatch(optionsIpc);
  if (batch.numRows === 0 || batch.schema.fields.length === 0) return null;
  const row = (iterRows(batch).next().value ?? {}) as Record<string, unknown>;
  const plainFields: VgiField[] = [];
  const secretFields: VgiField[] = [];
  const plainCols: Record<string, unknown[]> = {};
  const secretCols: Record<string, unknown[]> = {};
  for (const f of batch.schema.fields) {
    const value = row[f.name];
    if (secretNames.has(f.name.toLowerCase()) && value !== null && value !== undefined) {
      secretFields.push(f);
      secretCols[f.name] = [value];
    } else {
      plainFields.push(f);
      plainCols[f.name] = [value ?? null];
    }
  }
  return {
    plain: plainFields.length ? serializeBatch(batchFromColumns(plainCols, makeSchema(plainFields))) : null,
    secret: secretFields.length ? serializeBatch(batchFromColumns(secretCols, makeSchema(secretFields))) : null,
  };
}

/**
 * The trailer the framework appends to `uuid || catalog_bytes` for an attach
 * with options, or an empty array when there are none. Encrypts the secret
 * options (obtaining the key only if there is one); never logs anything.
 *
 * @throws {AttachSecretKeyUnavailableError} A secret option has a value and no key can be obtained.
 */
export async function packAttachOptionsTrailer(
  uuid: Uint8Array,
  optionsIpc: Uint8Array | null | undefined,
  secretNames: ReadonlySet<string>,
  keys: AttachSecretKeySource,
  fixedNonce?: Uint8Array,
): Promise<Uint8Array> {
  const split = splitOptions(optionsIpc, secretNames);
  if (split === null) return new Uint8Array(0);
  const plain = split.plain ?? new Uint8Array(0);
  let secret: Uint8Array = new Uint8Array(0);
  if (split.secret) {
    let root: Uint8Array;
    try {
      root = await rootKey(keys);
    } catch (e) {
      if (e instanceof AttachSecretKeyUnavailableError) throw e;
      // The key file's own messages name the file and the remedies; keep them.
      throw new AttachSecretKeyUnavailableError(`${noKeyMessage()} ${(e as Error).message}`);
    }
    const key = await deriveAttachSecretKey(root);
    const nonce = fixedNonce ?? crypto.getRandomValues(new Uint8Array(NONCE_LEN));
    const ct = xchacha20poly1305(key, nonce, aadFor(uuid)).encrypt(split.secret);
    secret = concat(Uint8Array.of(ATTACH_SECRET_OPTIONS_VERSION), nonce, ct);
  }
  const body = concat(u32le(plain.length), plain, secret);
  return concat(body, u32le(body.length), TRAILER_MAGIC);
}

function endsWithMagic(b: Uint8Array): boolean {
  if (b.length < TRAILER_MAGIC.length) return false;
  const off = b.length - TRAILER_MAGIC.length;
  for (let i = 0; i < TRAILER_MAGIC.length; i++) if (b[off + i] !== TRAILER_MAGIC[i]) return false;
  return true;
}

function decodeOptions(ipc: Uint8Array): Record<string, unknown> {
  const batch = deserializeBatch(ipc);
  if (batch.numRows === 0) return {};
  return { ...((iterRows(batch).next().value ?? {}) as Record<string, unknown>) };
}

// --------------------------------------------------------------- accessor

/** Opened attach buffers → their options. Keyed by the ArrayBuffer so any view
 *  of the bytes the framework handed out (a composite's subarray) resolves. */
const registry = new WeakMap<ArrayBufferLike, AttachOptionValues>();
const EMPTY: AttachOptionValues = Object.freeze({});

/**
 * Turn a FULL framework attach plaintext (`uuid || catalog_bytes [|| trailer]`,
 * already unsealed on HTTP) into the catalog's own bytes, decrypting any
 * secret options and recording all options for {@link attachOptions}.
 *
 * Returns a view of a private copy, so the record cannot be confused with
 * other bytes of the request. A value shorter than a UUID passes through
 * as-is (with no options), as before.
 *
 * @throws {OpaqueDataRejectedError} The trailer is malformed or the secret blob
 *   does not open (wrong key, tampered, or moved from another attach).
 */
export async function openFrameworkAttach(full: Uint8Array, keys: AttachSecretKeySource): Promise<Uint8Array> {
  if (full.length < UUID_LEN) return full;
  const copy = full.slice();
  let end = copy.length;
  let options: AttachOptionValues = EMPTY;
  if (copy.length >= UUID_LEN + TRAILER_FIXED && endsWithMagic(copy)) {
    const bodyLen = readU32le(copy, copy.length - TRAILER_FIXED);
    const bodyStart = copy.length - TRAILER_FIXED - bodyLen;
    if (bodyLen < 4 || bodyStart < UUID_LEN) throw new OpaqueDataRejectedError(FIELD);
    const body = copy.subarray(bodyStart, copy.length - TRAILER_FIXED);
    const plainLen = readU32le(body, 0);
    if (4 + plainLen > body.length) throw new OpaqueDataRejectedError(FIELD);
    const plain = body.subarray(4, 4 + plainLen);
    const secret = body.subarray(4 + plainLen);
    const values: Record<string, unknown> = {};
    try {
      if (plain.length) Object.assign(values, decodeOptions(plain));
    } catch {
      throw new OpaqueDataRejectedError(FIELD);
    }
    if (secret.length) {
      if (secret.length < 1 + NONCE_LEN + TAG_LEN || secret[0] !== ATTACH_SECRET_OPTIONS_VERSION) {
        throw new OpaqueDataRejectedError(FIELD);
      }
      let opened: Uint8Array;
      try {
        const key = await deriveAttachSecretKey(await rootKey(keys));
        const nonce = secret.subarray(1, 1 + NONCE_LEN);
        opened = xchacha20poly1305(key, nonce, aadFor(copy.subarray(0, UUID_LEN))).decrypt(secret.subarray(1 + NONCE_LEN));
      } catch {
        // Uniform: no key, wrong key, tampered and moved all read the same.
        throw new OpaqueDataRejectedError(FIELD);
      }
      try {
        Object.assign(values, decodeOptions(opened));
      } catch {
        throw new OpaqueDataRejectedError(FIELD);
      }
    }
    options = Object.freeze(values);
    end = bodyStart;
  }
  registry.set(copy.buffer, options);
  return copy.subarray(UUID_LEN, end);
}

/**
 * The ATTACH options of the attach a call belongs to — every supplied option,
 * secret ones included, decoded exactly as `CatalogInterface.attach()`
 * received them.
 *
 * Pass the attach value the framework handed the call: `bindCall.attach_opaque_data`
 * (bind, and `initCall.bind_call` at init/process, for every function kind), a
 * table-buffering function's `attachId`, or the `attachOpaqueData` argument of
 * a catalog method or transaction hook (`transactionBegin` / `transactionCommit`
 * / `transactionRollback`). Any subarray of those bytes works too; a copy does
 * not.
 *
 * Secret options (declared `secret: true`) reach this accessor on every
 * transport without ever being in plaintext in `attach_opaque_data`: the
 * framework encrypts them inside the attach value with a worker-held key. A
 * catalog therefore never needs to pack options into its own attach bytes,
 * and must not put a secret there.
 *
 * Returns `{}` for a null attach value (a call outside any attach) and for
 * an attach that had no options.
 *
 * @throws {AttachOptionsUnavailableError} The bytes were not handed out by the framework.
 */
export function attachOptions(attachOpaqueData: Uint8Array | null | undefined): AttachOptionValues {
  if (attachOpaqueData == null) return EMPTY;
  // Looked up even when empty: a catalog whose own attach bytes are empty
  // still gets a (zero-length) view of the framework value.
  const found = registry.get(attachOpaqueData.buffer);
  if (found !== undefined) return found;
  if (attachOpaqueData.length === 0) return EMPTY;
  throw new AttachOptionsUnavailableError();
}

/**
 * One option from {@link attachOptions}, looked up case-insensitively (DuckDB
 * option names are case-insensitive). `undefined` when it was not supplied.
 */
export function attachOption(attachOpaqueData: Uint8Array | null | undefined, name: string): unknown {
  const all = attachOptions(attachOpaqueData);
  if (Object.hasOwn(all, name)) return all[name];
  const lower = name.toLowerCase();
  for (const key of Object.keys(all)) if (key.toLowerCase() === lower) return all[key];
  return undefined;
}

/**
 * Names (lower-cased) of the options `catalog` declares `secret: true` for
 * catalog `name`, read from what it advertises in `catalogsInfo()` — the same
 * specs the extension redacts by — or its `attachOptionSpecs(name)`.
 */
export async function secretAttachOptionNames(
  catalog: {
    catalogsInfo?(): unknown;
    attachOptionSpecs?(name: string): Array<{ name: string; secret?: boolean }>;
  },
  name: string,
  deserialize: (bytes: Uint8Array) => { name: string; secret?: boolean },
): Promise<Set<string>> {
  const names = new Set<string>();
  if (typeof catalog.attachOptionSpecs === "function") {
    for (const spec of catalog.attachOptionSpecs(name) ?? []) if (spec.secret) names.add(spec.name.toLowerCase());
  }
  if (typeof catalog.catalogsInfo === "function") {
    let infos: unknown;
    try {
      infos = await catalog.catalogsInfo();
    } catch {
      infos = [];
    }
    for (const info of (infos ?? []) as Array<{ name?: string; attach_option_specs?: unknown[] | null }>) {
      if (info.name !== name) continue;
      for (const raw of info.attach_option_specs ?? []) {
        try {
          const spec = deserialize(raw as Uint8Array);
          if (spec.secret) names.add(spec.name.toLowerCase());
        } catch {
          // An undecodable spec declares nothing.
        }
      }
    }
  }
  return names;
}
