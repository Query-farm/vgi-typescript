// Copyright 2025, 2026 Query Farm LLC - https://query.farm

/**
 * Split-token envelope: the framework's wrapper around a worker's split payload.
 *
 * A split token *names* a unit of scan work so a distributed engine can
 * re-request exactly the work it was handed. The worker supplies only the
 * payload; everything around it is stamped here, so an author cannot forget the
 * consistency anchor or mis-bind the fingerprint, and never writes crypto.
 *
 * Layout (little-endian, fixed prefix) — byte-identical across every SDK:
 *
 * ```
 * offset  size  field
 * 0       1     format_version      currently 1
 * 1       1     flags               bit0 = payload_sealed; bits 1-7 reserved, MUST be 0
 * 2       2     anchor_len          u16 LE
 * 4       16    bind_fingerprint    truncated SHA-256 of the bind identity
 * 20      var   consistency_anchor  anchor_len bytes
 * 20+n    var   payload             the worker's own bytes
 * ```
 *
 * **The header is plaintext on every transport; only the payload is sealed.**
 * That is not a preference: a worker has no signing key on subprocess and unix,
 * which is DuckDB's primary path, so a header readable only through AEAD would be
 * unreadable exactly where DuckDB runs. It also matters for streaming — a
 * checkpointed position must survive key rotation.
 *
 * @module
 */

import type { AuthContext } from "@query-farm/vgi-rpc";

import { identityTail, openBytes, sealBytes } from "./crypto.js";

/** Envelope format version. Checked unconditionally, before anything else. */
export const SPLIT_TOKEN_FORMAT_VERSION = 1;

/** bit0 of `flags`: the payload is AEAD-sealed rather than plaintext. */
const FLAG_PAYLOAD_SEALED = 0x01;

/** bits 1-7 are reserved and MUST be zero; a set bit is a forward-compat violation. */
const RESERVED_FLAGS_MASK = 0xfe;

const FINGERPRINT_LEN = 16;
const HEADER_LEN = 4 + FINGERPRINT_LEN;

/**
 * Matches the reference default (`crypto.seal_bytes` version=1), so a token
 * sealed by one SDK opens in another.
 */
const SEAL_VERSION = 1;

const AAD_PREFIX = new TextEncoder().encode("vgi.split_token.v1\u0000");

/** Stable error-kind strings, identical across SDKs. */
export const SPLIT_TOKEN_INVALID = "SPLIT_TOKEN_INVALID";
export const SPLIT_SNAPSHOT_EXPIRED = "SPLIT_SNAPSHOT_EXPIRED";
export const SPLIT_TRANSACTION_ENDED = "SPLIT_TRANSACTION_ENDED";

/**
 * Why a split token was refused.
 *
 * The kind matters to a connector: only `SPLIT_SNAPSHOT_EXPIRED` means "re-run
 * the query", and neither kind is retriable in place. Keeping the anchor in the
 * PLAINTEXT header rather than in the AAD is what makes that distinction
 * expressible — inside the AAD both collapse into one tag-check failure.
 */
export class SplitTokenError extends Error {
  readonly kind: string;

  constructor(kind: string, message: string) {
    // The message carries the stable KIND, because the kind is the part a caller
    // acts on: only SPLIT_SNAPSHOT_EXPIRED means "re-run the query", and a
    // connector several layers up sees the message string rather than this
    // class. Without it the three failures are indistinguishable to everyone
    // downstream.
    super(`[${kind}] ${message}`);
    this.name = "SplitTokenError";
    this.kind = kind;
  }
}

function concat(...parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((n, p) => n + p.length, 0);
  const out = new Uint8Array(total);
  let at = 0;
  for (const p of parts) {
    out.set(p, at);
    at += p.length;
  }
  return out;
}

function equalBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a[i] ^ b[i];
  return diff === 0;
}

/**
 * Derive the 16-byte binding check for a bind call.
 *
 * Minted **and** verified by the same worker, so it needs self-consistency only
 * — it does not have to agree with any client, which is why the cross-SDK byte
 * fixtures do not cover it. 16 bytes is a binding check, not a MAC: forgery
 * resistance comes from the seal where a key exists, and from the uid trust
 * boundary where one does not.
 *
 * Uses Web Crypto rather than a hash dependency so this works unchanged in the
 * browser and Cloudflare Workers builds.
 */
export async function bindFingerprint(
  schemaName: string,
  functionName: string,
  args: Uint8Array,
  settings: Uint8Array,
  projection: Uint8Array,
): Promise<Uint8Array> {
  const enc = new TextEncoder();
  const zero = Uint8Array.of(0);
  const field = (label: string, value: Uint8Array) => concat(enc.encode(label), zero, value, zero);
  const message = concat(
    AAD_PREFIX,
    field("schema_name", enc.encode(schemaName)),
    field("function_name", enc.encode(functionName)),
    field("arguments", args),
    field("settings", settings),
    field("projection_ids", projection),
  );
  const digest = await crypto.subtle.digest("SHA-256", message as BufferSource);
  return new Uint8Array(digest).slice(0, FINGERPRINT_LEN);
}

/**
 * AAD for a sealed split payload: the plaintext header plus the caller identity.
 *
 * The identity half is load-bearing, not incidental — it stops a token minted for
 * one principal being replayed by another, exactly as the attach envelope does. A
 * split token names data (files, offsets, tenant partitions), so dropping it here
 * while keeping it on attach would be a regression.
 */
function splitTokenAad(header: Uint8Array, auth: AuthContext | undefined): Uint8Array {
  return concat(header, identityTail(auth));
}

/**
 * Encode the consistency anchor.
 *
 * `catalog_version` is the counter that MOVES within an attach, so it is what a
 * plan is pinned to; `resolved_data_version` is fixed at attach and would say
 * nothing about staleness.
 */
export function splitAnchor(catalogVersion: bigint | number): Uint8Array {
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigInt64(0, BigInt(catalogVersion), true);
  return out;
}

/** Stamp (and, when a key exists, seal) a worker payload into a split token. */
export async function buildSplitToken(opts: {
  payload: Uint8Array;
  fingerprint: Uint8Array;
  anchor: Uint8Array;
  signingKey?: Uint8Array;
  auth?: AuthContext;
}): Promise<Uint8Array> {
  const { payload, fingerprint, anchor, signingKey, auth } = opts;
  if (fingerprint.length !== FINGERPRINT_LEN) {
    throw new SplitTokenError(
      SPLIT_TOKEN_INVALID,
      `bind_fingerprint must be ${FINGERPRINT_LEN} bytes, got ${fingerprint.length}`,
    );
  }
  if (anchor.length > 0xffff) {
    throw new SplitTokenError(
      SPLIT_TOKEN_INVALID,
      `consistency_anchor too long: ${anchor.length} bytes exceeds u16`,
    );
  }

  const head = new Uint8Array(4);
  head[0] = SPLIT_TOKEN_FORMAT_VERSION;
  head[1] = signingKey ? FLAG_PAYLOAD_SEALED : 0;
  new DataView(head.buffer).setUint16(2, anchor.length, true);
  const body = concat(head, fingerprint, anchor);

  if (!signingKey) return concat(body, payload);
  const sealed = await sealBytes(payload, signingKey, splitTokenAad(body, auth), SEAL_VERSION);
  return concat(body, sealed);
}

/**
 * Verify a split token and return the worker's payload.
 *
 * `expectedFingerprint` and `currentAnchor` are optional; omitting one skips that
 * check.
 */
export async function openSplitToken(
  token: Uint8Array,
  opts: {
    signingKey?: Uint8Array;
    auth?: AuthContext;
    expectedFingerprint?: Uint8Array;
    currentAnchor?: Uint8Array;
  } = {},
): Promise<Uint8Array> {
  const { signingKey, auth, expectedFingerprint, currentAnchor } = opts;

  if (token.length < HEADER_LEN) {
    throw new SplitTokenError(
      SPLIT_TOKEN_INVALID,
      `split token too short: ${token.length} bytes, need at least ${HEADER_LEN}`,
    );
  }
  const version = token[0];
  const flags = token[1];
  const view = new DataView(token.buffer, token.byteOffset, token.byteLength);
  const anchorLen = view.getUint16(2, true);

  if (version !== SPLIT_TOKEN_FORMAT_VERSION) {
    throw new SplitTokenError(
      SPLIT_TOKEN_INVALID,
      `unsupported split-token format_version ${version}; this worker speaks ` +
        `${SPLIT_TOKEN_FORMAT_VERSION}`,
    );
  }
  if (flags & RESERVED_FLAGS_MASK) {
    throw new SplitTokenError(
      SPLIT_TOKEN_INVALID,
      `split token sets reserved flag bits (flags=0x${flags.toString(16).padStart(2, "0")})`,
    );
  }
  const sealed = (flags & FLAG_PAYLOAD_SEALED) !== 0;

  // ---- The alg:none refusal. Load-bearing; do not relax. ----
  // `flags` is attacker-controlled plaintext, so it may say "not sealed" on a
  // token an attacker wrote by hand. A keyed worker that honoured that would
  // redeem forged work without ever opening an envelope. The WORKER'S OWN KEY
  // STATE decides, never the token.
  if (signingKey && !sealed) {
    throw new SplitTokenError(
      SPLIT_TOKEN_INVALID,
      "split token is unsealed but this worker holds a signing key; refusing. An unsealed " +
        "token cannot be authenticated, so accepting one here would let any caller forge a " +
        "split (alg:none).",
    );
  }
  if (!signingKey && sealed) {
    throw new SplitTokenError(
      SPLIT_TOKEN_INVALID,
      "split token is sealed but this worker holds no signing key; cannot open it",
    );
  }

  const endOfAnchor = HEADER_LEN + anchorLen;
  if (token.length < endOfAnchor) {
    throw new SplitTokenError(
      SPLIT_TOKEN_INVALID,
      `split token truncated: anchor_len=${anchorLen} exceeds token length ${token.length}`,
    );
  }

  const fingerprint = token.subarray(4, HEADER_LEN);
  const anchor = token.subarray(HEADER_LEN, endOfAnchor);
  const body = token.subarray(0, endOfAnchor);
  const rest = token.subarray(endOfAnchor);

  if (expectedFingerprint && !equalBytes(fingerprint, expectedFingerprint)) {
    throw new SplitTokenError(
      SPLIT_TOKEN_INVALID,
      "split token was minted for a different bind (fingerprint mismatch)",
    );
  }
  // Anchor check AFTER the bind check, and as its own kind: "read version N" is
  // a different situation from "this token is not yours".
  if (currentAnchor && !equalBytes(anchor, currentAnchor)) {
    throw new SplitTokenError(SPLIT_SNAPSHOT_EXPIRED, "split snapshot expired; re-run the query");
  }

  if (!sealed) return new Uint8Array(rest);
  try {
    return await openBytes(
      rest,
      signingKey as Uint8Array,
      splitTokenAad(body, auth),
      SEAL_VERSION,
      "split token",
    );
  } catch (err) {
    throw new SplitTokenError(
      SPLIT_TOKEN_INVALID,
      `split token failed authentication: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}
