// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Shared param schemas and helpers for catalog protocol handlers.

import { type VgiSchema, schema, type VgiField, field, type VgiDataType, binary, utf8, bool } from "../../../arrow/index.js";
import { Protocol, type AuthContext, type CallContext } from "@query-farm/vgi-rpc";
import type { CatalogInterface } from "../../../catalog/interface.js";
import { NoCatalogError } from "../../../errors.js";
import { deserializeBatch } from "../../../util/arrow/index.js";
import { toUint8Array } from "../../../util/bytes.js";
import {
  sealBytes,
  openBytes,
  attachAad,
  transactionAad,
  ATTACH_ENVELOPE_VERSION,
  ATTACH_UUID_LEN,
  TRANSACTION_ENVELOPE_VERSION,
} from "../../../crypto.js";

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
 */
export async function sealAttach(
  plaintext: Uint8Array,
  auth: AuthContext | undefined,
  signingKey: Uint8Array | undefined,
): Promise<Uint8Array> {
  const uuid = new Uint8Array(ATTACH_UUID_LEN);
  crypto.getRandomValues(uuid);
  const minted = new Uint8Array(ATTACH_UUID_LEN + plaintext.length);
  minted.set(uuid, 0);
  minted.set(plaintext, ATTACH_UUID_LEN);
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
): Promise<Uint8Array> {
  const full = await openAttachFull(envelope, auth, signingKey);
  return full.length < ATTACH_UUID_LEN ? full : full.subarray(ATTACH_UUID_LEN);
}

/** Hex of a byte array (lowercase). */
function toHex(b: Uint8Array): string {
  let s = "";
  for (let i = 0; i < b.length; i++) s += b[i].toString(16).padStart(2, "0");
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
 * Unwrap the attach_opaque_data / transaction_opaque_data fields of a catalog
 * request in place, so handler bodies always see plaintext. The transaction
 * envelope is opened with the *sealed* attach value as part of its AAD, so it
 * stays bound to its parent attach. A no-op when there is no signing key.
 */
async function unwrapParamsOpaque(
  params: Record<string, any>,
  ctx: CallContext | undefined,
  signingKey: Uint8Array | undefined,
): Promise<void> {
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
      params.attach_opaque_data = full.length < ATTACH_UUID_LEN ? full : full.subarray(ATTACH_UUID_LEN);
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
 * Register a catalog unary handler whose request opaque-data fields are
 * unwrapped before the handler body runs. Used for every catalog_* method
 * except catalog_attach / catalog_transaction_begin, which seal their outputs
 * and are registered bespoke.
 */
export function catalogUnary(
  protocol: Protocol,
  signingKey: Uint8Array | undefined,
  name: string,
  config: {
    params: unknown;
    result: unknown;
    handler: (params: Record<string, any>, ctx: CallContext) => unknown;
    doc?: string;
  },
): void {
  protocol.unary(name, {
    params: config.params as any,
    result: config.result as any,
    doc: config.doc,
    handler: async (params: Record<string, any>, ctx: any) => {
      await unwrapParamsOpaque(params, ctx, signingKey);
      return config.handler(params, ctx);
    },
  });
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
  const out: Record<string, unknown> = {};
  for (const field of batch.schema.fields) {
    const col = batch.getChild(field.name);
    out[field.name] = col ? col.get(0) : null;
  }
  return out;
}

// ---------------------------------------------------------------------------
// Common param schemas
// ---------------------------------------------------------------------------

export const emptyResultSchema = schema([]);

export const attachOpaqueDataParam = schema([
  field("attach_opaque_data", binary(), true),
]);

export const attachOpaqueDataTxnParams = schema([
  field("attach_opaque_data", binary(), true),
  field("transaction_opaque_data", binary(), true),
]);

export const attachOpaqueDataNameTxnParams = schema([
  field("attach_opaque_data", binary(), true),
  field("name", utf8(), false),
  field("transaction_opaque_data", binary(), true),
]);

export const attachOpaqueDataSchemaNameTxnParams = schema([
  field("attach_opaque_data", binary(), true),
  field("schema_name", utf8(), false),
  field("name", utf8(), false),
  field("transaction_opaque_data", binary(), true),
]);

export const schemaNameIgnoreNotFoundTxnParams = schema([
  field("attach_opaque_data", binary(), true),
  field("schema_name", utf8(), false),
  field("name", utf8(), false),
  field("ignore_not_found", bool(), true),
  field("transaction_opaque_data", binary(), true),
]);

export const schemaNameCommentParams = schema([
  field("attach_opaque_data", binary(), true),
  field("schema_name", utf8(), false),
  field("name", utf8(), false),
  field("comment", utf8(), true),
  field("ignore_not_found", bool(), true),
  field("transaction_opaque_data", binary(), true),
]);

export const schemaNameRenameParams = schema([
  field("attach_opaque_data", binary(), true),
  field("schema_name", utf8(), false),
  field("name", utf8(), false),
  field("new_name", utf8(), false),
  field("ignore_not_found", bool(), true),
  field("transaction_opaque_data", binary(), true),
]);

export const columnOpParams = schema([
  field("attach_opaque_data", binary(), true),
  field("schema_name", utf8(), false),
  field("name", utf8(), false),
  field("column_name", utf8(), false),
  field("ignore_not_found", bool(), true),
  field("transaction_opaque_data", binary(), true),
]);
