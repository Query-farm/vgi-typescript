// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Catalog opaque-data AEAD envelopes.
//
// `attach_opaque_data` and `transaction_opaque_data` are implementation-chosen
// byte strings the catalog returns and the client round-trips back. On HTTP
// transport (where one worker authenticates many principals) the worker seals
// each value in an authenticated-encrypted envelope whose AAD binds the
// caller's (domain, principal); the transaction envelope additionally binds
// its parent attach envelope. A value sealed for one principal — or one
// attach — cannot be opened by another.
//
// Workers with no signing key (subprocess / unix transports) pass values
// through unchanged, since OS process ownership already enforces identity.
//
// Wire format: version(1 byte) || nonce(24 bytes) || ciphertext+tag.
// Matches vgi-python's vgi_rpc.crypto / vgi.worker and vgi-go's vgi/crypto.go.

import { xchacha20poly1305 } from "@noble/ciphers/chacha.js";
import type { AuthContext } from "@query-farm/vgi-rpc";

const _UTF8 = new TextEncoder();

// v2: the inner attach plaintext is uuid(16) || catalog_bytes — catalog_attach
// prepends a framework-minted 16-byte UUID that storage shards on. Bumped from
// 1 so a stale v1 token (no uuid prefix) is cleanly rejected at open.
export const ATTACH_ENVELOPE_VERSION = 2;
export const TRANSACTION_ENVELOPE_VERSION = 2;

/** Width of the framework UUID prepended to every attach plaintext. */
export const ATTACH_UUID_LEN = 16;

const NONCE_LEN = 24; // XChaCha20-Poly1305 nonce
const TAG_LEN = 16; // Poly1305 tag
const MIN_ENVELOPE_LEN = 1 + NONCE_LEN + TAG_LEN;

const ATTACH_AAD_PREFIX = _UTF8.encode("vgi.attach_opaque_data.v1\0");
const TRANSACTION_AAD_PREFIX = _UTF8.encode("vgi.transaction_opaque_data.v1\0");

/**
 * Thrown when an envelope cannot be opened — wrong principal, wrong parent
 * attach, tampered, malformed, or simply unknown. Every failure mode maps to
 * this single error so a probing caller cannot distinguish them.
 */
export class OpaqueDataRejectedError extends Error {
  constructor(field = "opaque data") {
    super(`${field} not recognized`);
    this.name = "OpaqueDataRejectedError";
  }
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
 * Stretch/compress an operator-supplied key to the 32 bytes XChaCha20-Poly1305
 * requires. Exactly-32-byte keys pass through unchanged; any other length is
 * collapsed via SHA-256. Matches the vgi-python / vgi-go ports.
 */
async function normalizeKey(key: Uint8Array): Promise<Uint8Array> {
  if (key.length === 32) return key;
  return new Uint8Array(await crypto.subtle.digest("SHA-256", key as BufferSource));
}

/**
 * Build the identity portion of an opaque-data AAD. Unauthenticated requests
 * get a fixed anonymous tail, so an anonymous caller cannot open an envelope
 * sealed for a real principal.
 */
export function identityTail(auth: AuthContext | undefined): Uint8Array {
  if (!auth || !auth.authenticated) return _UTF8.encode("\0anonymous");
  const domain = _UTF8.encode(auth.domain ?? "");
  const principal = _UTF8.encode(auth.principal ?? "");
  return concat(Uint8Array.of(0x01), domain, Uint8Array.of(0x00), principal);
}

/** AAD for an `attach_opaque_data` envelope: prefix + caller identity. */
export function attachAad(auth: AuthContext | undefined): Uint8Array {
  return concat(ATTACH_AAD_PREFIX, identityTail(auth));
}

/**
 * AAD for a `transaction_opaque_data` envelope. Binds both the caller identity
 * and the parent attach envelope, so a transaction value minted under one
 * attach cannot be replayed against a different attach even by the same
 * principal.
 */
export function transactionAad(
  auth: AuthContext | undefined,
  attachEnvelope: Uint8Array,
): Uint8Array {
  return concat(TRANSACTION_AAD_PREFIX, identityTail(auth), Uint8Array.of(0x00), attachEnvelope);
}

/** Seal `payload` into an AEAD envelope: version || nonce || ciphertext+tag. */
export async function sealBytes(
  payload: Uint8Array,
  key: Uint8Array,
  aad: Uint8Array,
  version: number,
): Promise<Uint8Array> {
  const nonce = crypto.getRandomValues(new Uint8Array(NONCE_LEN));
  const ciphertext = xchacha20poly1305(await normalizeKey(key), nonce, aad).encrypt(payload);
  const out = new Uint8Array(1 + NONCE_LEN + ciphertext.length);
  out[0] = version & 0xff;
  out.set(nonce, 1);
  out.set(ciphertext, 1 + NONCE_LEN);
  return out;
}

/**
 * Open and verify an envelope produced by {@link sealBytes}. Every failure
 * mode — malformed, wrong version, tampered, wrong key, wrong AAD
 * (cross-principal / cross-attach replay) — throws {@link OpaqueDataRejectedError}.
 */
export async function openBytes(
  token: Uint8Array,
  key: Uint8Array,
  aad: Uint8Array,
  version: number,
  field = "opaque data",
): Promise<Uint8Array> {
  if (token.length < MIN_ENVELOPE_LEN || token[0] !== (version & 0xff)) {
    throw new OpaqueDataRejectedError(field);
  }
  const nonce = token.subarray(1, 1 + NONCE_LEN);
  const ciphertext = token.subarray(1 + NONCE_LEN);
  try {
    return xchacha20poly1305(await normalizeKey(key), nonce, aad).decrypt(ciphertext);
  } catch {
    throw new OpaqueDataRejectedError(field);
  }
}
