// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Shared param schemas and helpers for catalog protocol handlers.

import { iterRows } from "../../../arrow/index.js";
import type { AuthContext, CallContext } from "@query-farm/vgi-rpc";
import type { CatalogInterface } from "../../../catalog/interface.js";
import { NoCatalogError } from "../../../errors.js";
import { deserializeBatch } from "../../../util/arrow/index.js";
import { toUint8Array } from "../../../util/bytes.js";
import { normalizeRequestSchemaPaths, unwrapRequest } from "../shared.js";
import {
  sealBytes,
  openBytes,
  attachAad,
  transactionAad,
  ATTACH_ENVELOPE_VERSION,
  ATTACH_UUID_LEN,
  TRANSACTION_ENVELOPE_VERSION,
} from "../../../crypto.js";
import { openFrameworkAttach, packAttachOptionsTrailer } from "../../../attach-secret-options.js";

/**
 * The keys a catalog handler group opens opaque values with: the HTTP signing
 * key (absent on stdio / unix) and the optional explicit root key for secret
 * ATTACH options (`attachSecretKey`; see ../../../attach-secret-options.ts).
 */
export interface OpaqueKeys {
  signingKey?: Uint8Array | undefined;
  attachSecretKey?: Uint8Array | undefined;
}

/**
 * The options a catalog_attach packs into the attach value after the catalog's
 * own bytes (secret ones encrypted). See ../../../attach-secret-options.ts.
 */
export interface AttachOptionsToPack {
  /** The request's `options` IPC batch (null / empty: no options, no trailer). */
  optionsIpc: Uint8Array | null | undefined;
  /** Lower-cased names of the options declared `secret: true`. */
  secretNames: ReadonlySet<string>;
  /** Explicit root key for the secret options, if configured. */
  attachSecretKey?: Uint8Array | undefined;
}

export type GetCatalog = () => CatalogInterface;

// ---------------------------------------------------------------------------
// Catalog opaque-data AEAD sealing
//
// On HTTP transport (signingKey present) the worker seals attach_opaque_data
// and transaction_opaque_data into AEAD envelopes bound to the caller's
// identity. Subprocess / unix workers have no signing key — every helper is a
// transparent pass-through. The catalog implementation always sees plaintext;
// the client only ever sees sealed envelopes.
// ---------------------------------------------------------------------------

/**
 * Mint and seal a catalog_attach value: prepend a fresh framework UUID
 * (uuid(16) || catalog_bytes), then seal. Storage shards on the UUID — stable
 * across re-seals and globally unique, unlike the random-nonce ciphertext or
 * the (possibly non-unique) catalog bytes. openAttach strips the UUID back off
 * so the catalog only ever sees its own bytes. Mirrors vgi-python/-go.
 *
 * With `options`, the attach's ATTACH options are packed after the catalog's
 * bytes (secret ones encrypted, never plaintext) so `attachOptions()` can hand
 * them to every function call and transaction hook of this attach.
 */
export async function sealAttach(
  plaintext: Uint8Array,
  auth: AuthContext | undefined,
  signingKey: Uint8Array | undefined,
  options?: AttachOptionsToPack,
): Promise<Uint8Array> {
  const uuid = new Uint8Array(ATTACH_UUID_LEN);
  crypto.getRandomValues(uuid);
  // The attach's options ride after the catalog's bytes, the secret ones
  // encrypted under a key bound to this uuid (empty when there are none).
  const trailer = options
    ? await packAttachOptionsTrailer(uuid, options.optionsIpc, options.secretNames, {
        explicit: options.attachSecretKey,
        signingKey,
      })
    : new Uint8Array(0);
  const minted = new Uint8Array(ATTACH_UUID_LEN + plaintext.length + trailer.length);
  minted.set(uuid, 0);
  minted.set(plaintext, ATTACH_UUID_LEN);
  minted.set(trailer, ATTACH_UUID_LEN + plaintext.length);
  if (!signingKey) return minted;
  return sealBytes(minted, signingKey, attachAad(auth), ATTACH_ENVELOPE_VERSION);
}

/**
 * Open an attach envelope returning the FULL framework plaintext
 * uuid(16) || catalog_bytes (not stripped). Storage shards on the leading UUID.
 */
export async function openAttachFull(
  envelope: Uint8Array,
  auth: AuthContext | undefined,
  signingKey: Uint8Array | undefined,
): Promise<Uint8Array> {
  if (!signingKey || envelope.length === 0) return envelope;
  return openBytes(envelope, signingKey, attachAad(auth), ATTACH_ENVELOPE_VERSION, "attach_opaque_data");
}

/**
 * Open an attach_opaque_data envelope, returning the catalog's own bytes — the
 * framework UUID prefix is stripped. This is what catalog/function bodies see;
 * storage routing uses openAttachFull / deriveShardKey to reach the UUID.
 */
export async function openAttach(
  envelope: Uint8Array,
  auth: AuthContext | undefined,
  signingKey: Uint8Array | undefined,
  attachSecretKey?: Uint8Array,
): Promise<Uint8Array> {
  const full = await openAttachFull(envelope, auth, signingKey);
  // Strips the uuid and the options trailer, decrypting secret options and
  // recording all of them for attachOptions().
  return openFrameworkAttach(full, { explicit: attachSecretKey, signingKey });
}

/** Hex of a byte array (lowercase). */
function toHex(b: Uint8Array): string {
  let s = "";
  for (let i = 0; i < b.length; i++) s += b[i]!.toString(16).padStart(2, "0");
  return s;
}

/**
 * deriveShardKey returns the Cloudflare-DO routing key for an attach:
 * "att-" + hex(the 16-byte framework UUID at the head of the unwrapped attach).
 * One DO per logical ATTACH. Throws if the uuid is not 16 bytes (the storage
 * path is always bound to a logical ATTACH).
 */
export function deriveShardKey(attachUuid: Uint8Array): string {
  if (attachUuid.length !== ATTACH_UUID_LEN) {
    throw new Error(`shard_key requires a ${ATTACH_UUID_LEN}-byte attach uuid, got ${attachUuid.length}`);
  }
  return "att-" + toHex(attachUuid);
}

/**
 * shardKeyForAttach unwraps the sealed attach and derives its shard key. An
 * empty/absent attach yields "" (non-sharding backends ignore the key; the
 * CfDo backend rejects an empty key server-side, the "must not happen" case).
 */
export async function shardKeyForAttach(
  sealed: Uint8Array | undefined,
  auth: AuthContext | undefined,
  signingKey: Uint8Array | undefined,
): Promise<string> {
  if (!sealed || sealed.length === 0) return "";
  const full = await openAttachFull(sealed, auth, signingKey);
  if (full.length < ATTACH_UUID_LEN) return "";
  return deriveShardKey(full.subarray(0, ATTACH_UUID_LEN));
}

/**
 * Seal a plaintext transaction value (catalog_transaction_begin output), bound
 * to the caller's identity and the parent attach envelope it was minted under.
 */
export async function sealTransaction(
  plaintext: Uint8Array,
  attachEnvelope: Uint8Array,
  auth: AuthContext | undefined,
  signingKey: Uint8Array | undefined,
): Promise<Uint8Array> {
  if (!signingKey) return plaintext;
  return sealBytes(plaintext, signingKey, transactionAad(auth, attachEnvelope), TRANSACTION_ENVELOPE_VERSION);
}

/**
 * Open a transaction_opaque_data envelope (sealed on HTTP only, no UUID prefix).
 * `attachEnvelope` is the request's *sealed* attach, which the transaction
 * envelope's AAD binds. A pass-through without a signing key or for an empty
 * value. Catalog hooks and function bodies must see the same plaintext, or a
 * worker cannot tie a function call to its transaction's commit/rollback.
 */
export async function openTransaction(
  envelope: Uint8Array,
  attachEnvelope: Uint8Array,
  auth: AuthContext | undefined,
  signingKey: Uint8Array | undefined,
): Promise<Uint8Array> {
  if (!signingKey || envelope.length === 0) return envelope;
  return openBytes(envelope, signingKey, transactionAad(auth, attachEnvelope), TRANSACTION_ENVELOPE_VERSION, "transaction_opaque_data");
}

/**
 * Unwrap the attach_opaque_data / transaction_opaque_data fields of a catalog
 * request in place, so handler bodies always see plaintext. The transaction
 * envelope is opened with the *sealed* attach value as part of its AAD, so it
 * stays bound to its parent attach. A no-op when there is no signing key.
 */
async function unwrapParamsOpaque(
  params: Record<string, any>,
  ctx: CallContext | undefined,
  keys: OpaqueKeys,
): Promise<void> {
  const { signingKey, attachSecretKey } = keys;
  const auth = ctx?.auth;
  let sealedAttach: Uint8Array = new Uint8Array(0);
  if (params.attach_opaque_data != null) {
    const env = toUint8Array(params.attach_opaque_data);
    if (env.length > 0) {
      sealedAttach = env;
      // The framework mints every attach as uuid(16) || catalog_bytes —
      // sealed on HTTP, plaintext on subprocess/unix (sealAttach prepends the
      // UUID regardless of transport). Catalog handlers — including
      // CompositeCatalog's route-byte at byte 0 of catalog_bytes — must see
      // the catalog's own bytes, so open the envelope when sealed, then strip
      // the framework UUID prefix on EVERY transport, not just HTTP.
      const full = signingKey
        ? await openBytes(env, signingKey, attachAad(auth), ATTACH_ENVELOPE_VERSION, "attach_opaque_data")
        : env;
      params.attach_opaque_data = await openFrameworkAttach(full, { explicit: attachSecretKey, signingKey });
    }
  }
  // transaction_opaque_data is sealed only on HTTP (no UUID prefix); subprocess
  // values are raw plaintext and pass through untouched.
  if (signingKey && params.transaction_opaque_data != null) {
    const env = toUint8Array(params.transaction_opaque_data);
    if (env.length > 0) {
      params.transaction_opaque_data = await openBytes(
        env,
        signingKey,
        transactionAad(auth, sealedAttach),
        TRANSACTION_ENVELOPE_VERSION,
        "transaction_opaque_data",
      );
    }
  }
}

/**
 * Wrap a catalog unary handler so its request opaque-data fields are unwrapped
 * before the body runs. Used for every catalog_* method except catalog_attach
 * / catalog_transaction_begin, which seal their outputs and are written
 * bespoke. The body sees the flattened params as a plain record.
 */
export function catalogHandler<R>(
  keysOrSigningKey: OpaqueKeys | Uint8Array | undefined,
  handler: (params: Record<string, any>, ctx: CallContext) => R | Promise<R>,
): (params: object, ctx: CallContext) => Promise<R> {
  const keys: OpaqueKeys =
    keysOrSigningKey instanceof Uint8Array || keysOrSigningKey === undefined
      ? { signingKey: keysOrSigningKey }
      : keysOrSigningKey;
  return async (wire: object, ctx: CallContext) => {
    let params = wire as Record<string, any>;
    // catalog_create / catalog_table_create / catalog_macro_create (and
    // catalog_index_create) send one `request: binary` column holding the
    // IPC-serialized request record, so their attach/transaction values ride
    // INSIDE it. Flatten it first, so handlers read the request's fields
    // directly and those values are opened (and the framework UUID stripped)
    // like every other catalog call's. Before this, table_create read every
    // field off the envelope (all undefined) and macro_create handed the
    // catalog an unopened attach value.
    if (params.request != null && params.attach_opaque_data === undefined && params.name === undefined) {
      params = unwrapRequest(params.request);
    }
    normalizeRequestSchemaPaths(params);
    await unwrapParamsOpaque(params, ctx, keys);
    return handler(params, ctx);
  };
}

export function makeGetCatalog(catalog: CatalogInterface | undefined): GetCatalog {
  return () => {
    if (!catalog) throw new NoCatalogError();
    return catalog;
  };
}

/**
 * Decode the `options` field of a catalog_attach request into a plain
 * {name: value} dict. The wire field is either null (no options) or an
 * IPC-serialized RecordBatch of typed columns — one column per option,
 * one row with the value. Returns `{}` for null / empty input.
 */
export function decodeOptionsBatch(bytes: any): Record<string, unknown> {
  if (bytes == null) return {};
  const buf = toUint8Array(bytes);
  if (buf.byteLength === 0) return {};
  const batch = deserializeBatch(buf);
  if (batch.numRows === 0) return {};
  // Decode the single row through the codec path (iterRows yields RICH values):
  // date columns come back as JS Date, temporal types as bigint, etc. — the
  // same representation a producer would feed back into batchFromColumns. The
  // old `Math.floor(ms / 86_400_000)` date32 hack is gone; the codec owns it.
  const row = iterRows(batch).next().value as Record<string, unknown> | undefined;
  return row ?? {};
}

// Eight hand-written catalog param schemas used to live here, restating shapes
// codegen already emits. Every method's schemas now come from the generated
// registration table (src/generated/vgi-service.ts) and these were left behind, referenced by nothing but their
// own import lines. A stale duplicate is worse than no duplicate: the copies
// this SDK kept had drifted on `attach_opaque_data` nullability (45 generated
// schemas say non-null, every copy said nullable) and on `type` being
// dictionary-encoded utf8 rather than plain utf8, and the result was that a
// CORRECT client got rejected at the very first catalog call. Never revive one:
// the registration table is generated, schemas and all.
