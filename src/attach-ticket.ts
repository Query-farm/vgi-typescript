// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Attach tickets: a user's ATTACH, sealed so a runner can replay it later as
// that user (`vgi.attach_tickets.v1`).
//
// A ticket is the *what* half of an unattended session; a sealed grant
// (`vgi_rpc.Identity.v1` `issue_grant`) is the *who*. While the user is
// attached and logged in, the client asks the worker to seal the options it
// attached with -- secret ones included -- into a ticket only this worker can
// open. Later any runner holding the user's grant attaches with the single
// option `vgi_attach_ticket`, and `catalog_attach` restores the sealed attach
// before any catalog code runs. The runner never sees an option.
//
// A ticket carries no authority: it opens only under the *caller's* principal.
//
// Normative spec: vgi-python `docs/protocol/vgi-attach-tickets.md`. Byte-exact
// vectors: `src/__tests__/data/attach_ticket_vectors.json`.
//
//   token    = "vgia1." || base64url_nopad(envelope)
//   envelope = 0x01 || nonce(24) || XChaCha20-Poly1305(key, nonce, payload, aad)
//   key      = this worker's signing key (the attach_opaque_data key), normalized
//   aad      = "vgi.attach_ticket.v1" 0x00 || UTF-8(principal)
//
// The AAD binds the principal only, not the (domain, principal) pair the
// attach envelope binds: a ticket is sealed while the user is logged in
// (domain `jwt`, say) and opened when a runner presents their grant (domain
// `grant`).

import {
  type AuthContext,
  type CallContext,
  Protocol,
  badRequest,
  errorInfo,
  preconditionFailure,
} from "@query-farm/vgi-rpc";
import { field, float64, schema, utf8, binary, int64, batchFromColumns, serializeBatch } from "./arrow/index.js";
import { deserializeBatch } from "./util/arrow/index.js";
import { openBytes, OpaqueDataRejectedError, sealBytes } from "./crypto.js";
import type { CatalogInterface } from "./catalog/interface.js";
import { deserializeAttachOptionSpecs, RESERVED_ATTACH_OPTION } from "./catalog/attach-option.js";
import { toUint8Array } from "./util/bytes.js";
import { decodeOptionsBatch } from "./protocol/handlers/catalog/shared.js";
import { REQUEST_PARAMS_SCHEMA, RESULT_BINARY_SCHEMA, unwrapRequest } from "./protocol/handlers/shared.js";

/** Token prefix. The version is in the prefix, so an incompatible format is a
 *  different prefix -- never half-parsed. */
export const ATTACH_TICKET_PREFIX = "vgia1.";
/** The reserved ATTACH option a runner presents a ticket in. */
export const ATTACH_TICKET_OPTION = RESERVED_ATTACH_OPTION;
/** Wire name of the protocol hosting `seal_attach`. */
export const ATTACH_TICKETS_PROTOCOL_NAME = "vgi.attach_tickets.v1";
/** Its declared version. */
export const ATTACH_TICKETS_PROTOCOL_VERSION = "1.0.0";
/** The envelope's version byte, fixed by this format. */
export const TICKET_ENVELOPE_VERSION = 0x01;
/** Largest options record (serialized Arrow IPC bytes) a ticket may carry. */
export const MAX_OPTIONS_BYTES = 16 * 1024;
/** Longest ticket text considered at all. */
export const MAX_TICKET_CHARS = 32 * 1024;
/** Allowance for clocks disagreeing between the sealing and redeeming worker. */
export const CLOCK_SKEW_SECONDS = 60;

const UTF8 = new TextEncoder();
const STRICT_UTF8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const TICKET_AAD_DOMAIN = UTF8.encode("vgi.attach_ticket.v1\0");
const TICKET_ID = /^[0-9a-f]{32}$/;
const B64URL = /^[A-Za-z0-9_-]+$/;
const MAX_TEXT = 0xffff;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/**
 * A `vgi_attach_ticket` this worker cannot accept.
 *
 * One type for every cause -- malformed, wrong prefix, non-canonical, wrong
 * key, wrong principal, tampered, bad payload -- so a caller cannot tell a
 * forged ticket from another user's. The message never contains the ticket.
 */
export class AttachTicketInvalidError extends Error {
  static readonly errorKind = "attach_ticket_invalid";
  readonly errorKind = "attach_ticket_invalid";
  readonly errorCode = "INVALID_ARGUMENT";
  readonly errorDetails: Record<string, unknown>[];
  constructor(detail = "attach ticket not accepted") {
    super(detail);
    this.name = "AttachTicketInvalidError";
    this.errorDetails = [badRequest([{ field: ATTACH_TICKET_OPTION, description: detail }]) as never];
  }
}

/**
 * An authentic ticket outside its lifetime. Only raised once the ticket has
 * opened under the caller's principal, so it reveals nothing a forger could use.
 */
export class AttachTicketExpiredError extends Error {
  static readonly errorKind = "attach_ticket_expired";
  readonly errorKind = "attach_ticket_expired";
  readonly errorCode = "FAILED_PRECONDITION";
  readonly errorDetails: Record<string, unknown>[];
  constructor(detail = "attach ticket has expired") {
    super(detail);
    this.name = "AttachTicketExpiredError";
    this.errorDetails = [
      preconditionFailure([{ type: "ATTACH_TICKET", subject: ATTACH_TICKET_OPTION, description: detail }]) as never,
    ];
  }
}

/** `invalid_request` / `INVALID_ARGUMENT` with one `BadRequest` violation per bad field. */
export class AttachTicketRequestError extends Error {
  readonly errorKind = "invalid_request";
  readonly errorCode = "INVALID_ARGUMENT";
  readonly errorDetails: Record<string, unknown>[];
  constructor(message: string, violations: ReadonlyArray<readonly [string, string]>) {
    super(message);
    this.name = "AttachTicketRequestError";
    this.errorDetails = [
      badRequest(violations.map(([f, description]) => ({ field: f, description }))) as never,
    ];
  }
}

/** `action_denied` / `PERMISSION_DENIED`, naming the refused action. */
export class SealAttachDeniedError extends Error {
  readonly errorKind = "action_denied";
  readonly errorCode = "PERMISSION_DENIED";
  readonly errorDetails: Record<string, unknown>[] = [errorInfo({ action: "seal_attach" }) as never];
  constructor(message: string) {
    super(message);
    this.name = "SealAttachDeniedError";
  }
}

// ---------------------------------------------------------------------------
// Token format
// ---------------------------------------------------------------------------

/** What a ticket carries. */
export interface AttachTicketClaims {
  /** Seconds since the Unix epoch. */
  issuedAt: number;
  /** Seconds since the Unix epoch; `0` means no expiry. */
  expiresAt: number;
  /** 32 lowercase hex; a correlation handle, not a secret. */
  ticketId: string;
  catalogName: string;
  /** `""` when the user gave none. */
  dataVersionSpec: string;
  /** `""` when the user gave none. */
  implementationVersion: string;
  /** Arrow IPC stream of the one-row options record; empty for none. */
  optionsIpc: Uint8Array;
}

/** The ticket AAD: `"vgi.attach_ticket.v1" 0x00 || UTF-8(principal)`. */
export function attachTicketAad(principal: string): Uint8Array {
  const p = UTF8.encode(principal);
  const out = new Uint8Array(TICKET_AAD_DOMAIN.length + p.length);
  out.set(TICKET_AAD_DOMAIN, 0);
  out.set(p, TICKET_AAD_DOMAIN.length);
  return out;
}

function packText(value: string, name: string): Uint8Array {
  const raw = UTF8.encode(value);
  if (raw.length > MAX_TEXT) throw new Error(`${name} is longer than 65535 bytes`);
  const out = new Uint8Array(2 + raw.length);
  new DataView(out.buffer).setUint16(0, raw.length, true);
  out.set(raw, 2);
  return out;
}

/** Encode a ticket payload (§2.1). Exported for the vectors' byte-exact
 *  `payload_hex` check; not part of the public API. @internal */
export function encodeAttachTicketPayload(c: AttachTicketClaims): Uint8Array {
  return encodePayload(c);
}

function encodePayload(c: AttachTicketClaims): Uint8Array {
  if (c.optionsIpc.length > MAX_OPTIONS_BYTES) {
    throw new Error(`options are ${c.optionsIpc.length} bytes; a ticket carries at most ${MAX_OPTIONS_BYTES}`);
  }
  const head = new Uint8Array(16);
  const hv = new DataView(head.buffer);
  hv.setBigInt64(0, BigInt(c.issuedAt), true);
  hv.setBigInt64(8, BigInt(c.expiresAt), true);
  const optLen = new Uint8Array(4);
  new DataView(optLen.buffer).setUint32(0, c.optionsIpc.length, true);
  const parts = [
    head,
    packText(c.ticketId, "ticket_id"),
    packText(c.catalogName, "catalog_name"),
    packText(c.dataVersionSpec, "data_version_spec"),
    packText(c.implementationVersion, "implementation_version"),
    optLen,
    c.optionsIpc,
  ];
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

/** Parse strictly: exact lengths, valid UTF-8, field rules, no trailing bytes. */
function decodePayload(payload: Uint8Array): AttachTicketClaims {
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  let pos = 0;
  const need = (n: number): void => {
    if (pos + n > payload.length) throw new AttachTicketInvalidError("attach ticket payload is truncated");
  };
  const text = (): string => {
    need(2);
    const len = view.getUint16(pos, true);
    pos += 2;
    need(len);
    const raw = payload.subarray(pos, pos + len);
    pos += len;
    try {
      return STRICT_UTF8.decode(raw);
    } catch {
      throw new AttachTicketInvalidError("attach ticket payload is not UTF-8");
    }
  };
  need(16);
  const issuedAt = Number(view.getBigInt64(0, true));
  const expiresAt = Number(view.getBigInt64(8, true));
  pos = 16;
  const ticketId = text();
  const catalogName = text();
  const dataVersionSpec = text();
  const implementationVersion = text();
  need(4);
  const optionsLen = view.getUint32(pos, true);
  pos += 4;
  if (optionsLen > MAX_OPTIONS_BYTES) throw new AttachTicketInvalidError("attach ticket options exceed 16 KiB");
  need(optionsLen);
  const optionsIpc = payload.slice(pos, pos + optionsLen);
  pos += optionsLen;
  if (pos !== payload.length) throw new AttachTicketInvalidError("attach ticket payload has trailing bytes");
  if (!TICKET_ID.test(ticketId)) throw new AttachTicketInvalidError("attach ticket id is not 32 lowercase hex");
  if (!catalogName) throw new AttachTicketInvalidError("attach ticket names no catalog");
  if (expiresAt !== 0 && expiresAt <= issuedAt) throw new AttachTicketInvalidError("attach ticket lifetime is empty");
  return { issuedAt, expiresAt, ticketId, catalogName, dataVersionSpec, implementationVersion, optionsIpc };
}

function b64url(data: Uint8Array): string {
  let bin = "";
  for (let i = 0; i < data.length; i++) bin += String.fromCharCode(data[i]);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/** Decode unpadded base64url, rejecting any spelling but the canonical one. */
function b64urlStrict(text: string): Uint8Array {
  if (!B64URL.test(text) || text.length % 4 === 1) {
    throw new AttachTicketInvalidError("attach ticket is not unpadded base64url");
  }
  const padded = text.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - (text.length % 4)) % 4);
  const bin = atob(padded);
  const raw = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) raw[i] = bin.charCodeAt(i);
  if (b64url(raw) !== text) throw new AttachTicketInvalidError("attach ticket is not canonical base64url");
  return raw;
}

function randomHex(bytes: number): string {
  const b = crypto.getRandomValues(new Uint8Array(bytes));
  let s = "";
  for (const x of b) s += x.toString(16).padStart(2, "0");
  return s;
}

/** Inputs to {@link mintAttachTicket}. */
export interface MintAttachTicketOptions {
  principal: string;
  catalogName: string;
  optionsIpc: Uint8Array;
  dataVersionSpec?: string;
  implementationVersion?: string;
  issuedAt: number;
  expiresAt: number;
  /** Override the random id -- for vectors only. */
  ticketId?: string;
  /** A fixed 24-byte nonce -- for vectors only. */
  nonce?: Uint8Array;
}

/**
 * Seal a ticket for `principal` under `signingKey` (the attach_opaque_data
 * key; any length, normalized exactly as the attach envelope's is).
 *
 * @throws Error An empty principal or catalog, an empty lifetime, a bad
 *   ticket id, or a field too long to encode.
 */
export async function mintAttachTicket(
  signingKey: Uint8Array,
  opts: MintAttachTicketOptions,
): Promise<{ token: string; claims: AttachTicketClaims }> {
  if (!opts.principal) throw new Error("a ticket needs a principal");
  if (!opts.catalogName) throw new Error("a ticket needs a catalog name");
  const claims: AttachTicketClaims = {
    issuedAt: Math.trunc(opts.issuedAt),
    expiresAt: Math.trunc(opts.expiresAt),
    ticketId: opts.ticketId ?? randomHex(16),
    catalogName: opts.catalogName,
    dataVersionSpec: opts.dataVersionSpec ?? "",
    implementationVersion: opts.implementationVersion ?? "",
    optionsIpc: opts.optionsIpc,
  };
  if (!TICKET_ID.test(claims.ticketId)) throw new Error("ticket_id must be 32 lowercase hex");
  if (claims.expiresAt !== 0 && claims.expiresAt <= claims.issuedAt) {
    throw new Error("expires_at must be 0 or after issued_at");
  }
  const envelope = await sealBytes(
    encodePayload(claims),
    signingKey,
    attachTicketAad(opts.principal),
    TICKET_ENVELOPE_VERSION,
    opts.nonce,
  );
  const token = ATTACH_TICKET_PREFIX + b64url(envelope);
  if (token.length > MAX_TICKET_CHARS) {
    throw new Error(`the ticket would be ${token.length} characters; at most ${MAX_TICKET_CHARS} are accepted`);
  }
  return { token, claims };
}

/**
 * Verify a ticket for the calling `principal` and return what it carries.
 *
 * Order, normative: prefix, length, canonical base64url, caller, AEAD open
 * under the caller's principal, strict payload parse, then lifetime with a
 * 60 s skew. The lifetime is inside the ciphertext, so it is trusted only
 * after the tag verified.
 *
 * @param now Override the clock (seconds), for tests and vectors.
 * @throws AttachTicketInvalidError Any cause but expiry.
 * @throws AttachTicketExpiredError Authentic but outside its lifetime.
 */
export async function openAttachTicket(
  signingKey: Uint8Array,
  token: string,
  principal: string | null | undefined,
  now?: number,
): Promise<AttachTicketClaims> {
  if (!token.startsWith(ATTACH_TICKET_PREFIX)) throw new AttachTicketInvalidError("not an attach ticket");
  if (token.length > MAX_TICKET_CHARS) throw new AttachTicketInvalidError("attach ticket is too long");
  const envelope = b64urlStrict(token.slice(ATTACH_TICKET_PREFIX.length));
  if (!principal) throw new AttachTicketInvalidError("an anonymous caller cannot redeem an attach ticket");
  let payload: Uint8Array;
  try {
    payload = await openBytes(envelope, signingKey, attachTicketAad(principal), TICKET_ENVELOPE_VERSION);
  } catch (e) {
    if (e instanceof OpaqueDataRejectedError) {
      throw new AttachTicketInvalidError("attach ticket failed verification");
    }
    throw e;
  }
  const claims = decodePayload(payload);
  const current = now ?? Date.now() / 1000;
  if (claims.issuedAt > current + CLOCK_SKEW_SECONDS) throw new AttachTicketExpiredError("attach ticket is not yet valid");
  if (claims.expiresAt !== 0 && current >= claims.expiresAt + CLOCK_SKEW_SECONDS) {
    throw new AttachTicketExpiredError("attach ticket has expired");
  }
  return claims;
}

// ---------------------------------------------------------------------------
// Redemption: what catalog_attach does with `vgi_attach_ticket`
// ---------------------------------------------------------------------------

/** The catalog_attach request fields a ticket replaces. */
export interface RestoredAttach {
  name: string;
  /** Raw Arrow IPC options bytes, or `null` for none. */
  optionsIpc: Uint8Array | null;
  /** The same options, decoded. */
  options: Record<string, unknown>;
  dataVersionSpec: string | null;
  implementationVersion: string | null;
}

/** The caller's principal, or `null` for anonymous. */
export function callerPrincipal(auth: AuthContext | undefined | null): string | null {
  if (!auth || !auth.authenticated) return null;
  return auth.principal || null;
}

/**
 * Replace a ticket-carrying attach with the attach it seals.
 *
 * Returns `null` when `options` carries no `vgi_attach_ticket` (the request is
 * untouched). Otherwise the ticket must be the only option
 * ({@link AttachTicketRequestError} -- checked before the ticket is opened),
 * must open under the caller's principal and `signingKey`
 * ({@link AttachTicketInvalidError}; no key, as off HTTP, never opens one),
 * and must be within its lifetime ({@link AttachTicketExpiredError}).
 * Never logs the ticket or a restored option.
 */
export async function redeemAttachTicket(
  options: Record<string, unknown>,
  signingKey: Uint8Array | undefined | null,
  auth: AuthContext | undefined | null,
  now?: number,
): Promise<RestoredAttach | null> {
  const keys = Object.keys(options);
  const ticketKeys = keys.filter((k) => k.toLowerCase() === ATTACH_TICKET_OPTION);
  if (ticketKeys.length === 0) return null;
  const others = keys.filter((k) => k !== ticketKeys[0]);
  if (others.length > 0) {
    throw new AttachTicketRequestError(
      `${ATTACH_TICKET_OPTION} must be the only attach option`,
      others.map((k) => [`options.${k}`, `not allowed alongside ${ATTACH_TICKET_OPTION}`] as const),
    );
  }
  const token = options[ticketKeys[0]];
  if (typeof token !== "string") throw new AttachTicketInvalidError(`${ATTACH_TICKET_OPTION} must be a string`);
  if (!signingKey) throw new AttachTicketInvalidError("this worker does not redeem attach tickets");
  const claims = await openAttachTicket(signingKey, token, callerPrincipal(auth), now);
  let restored: Record<string, unknown> = {};
  if (claims.optionsIpc.length > 0) {
    try {
      restored = decodeOptionsBatch(claims.optionsIpc);
    } catch {
      throw new AttachTicketInvalidError("attach ticket options are not an Arrow IPC record");
    }
  }
  return {
    name: claims.catalogName,
    optionsIpc: claims.optionsIpc.length > 0 ? claims.optionsIpc : null,
    options: restored,
    dataVersionSpec: claims.dataVersionSpec || null,
    implementationVersion: claims.implementationVersion || null,
  };
}

// ---------------------------------------------------------------------------
// vgi.attach_tickets.v1
// ---------------------------------------------------------------------------

/** `SealAttachRequest`, field for field with vgi-python. */
export const SEAL_ATTACH_REQUEST_SCHEMA = schema([
  field("catalog_name", utf8(), false),
  field("options", binary(), true),
  field("data_version_spec", utf8(), false),
  field("implementation_version", utf8(), false),
  field("ttl_seconds", int64(), false),
]);

/** `AttachTicket`, field for field with vgi-python. */
export const ATTACH_TICKET_SCHEMA = schema([
  field("ticket", utf8(), false),
  field("expires_at", float64(), false),
]);

/**
 * The ticket lifetime ceiling: the worker's grant maximum, or `null`.
 *
 * With grant keys configured it is their `maxTtlSeconds`. Otherwise (a worker
 * minting its own grants) `VGI_RPC_GRANT_MAX_TTL_SECONDS` when set, else none.
 *
 * @throws Error The environment value is not a positive integer.
 */
export function resolveTicketMaxTtl(
  grantKeys: { readonly maxTtlSeconds: number } | null | undefined,
  env: Record<string, string | undefined> | undefined,
): number | null {
  if (grantKeys) return Math.trunc(grantKeys.maxTtlSeconds);
  const raw = (env?.VGI_RPC_GRANT_MAX_TTL_SECONDS ?? "").trim();
  if (!raw) return null;
  const value = /^\d+$/.test(raw) ? Number(raw) : 0;
  if (!(value > 0)) throw new Error(`VGI_RPC_GRANT_MAX_TTL_SECONDS=${JSON.stringify(raw)} must be a positive integer`);
  return value;
}

/** What `seal_attach` needs from the worker. */
export interface AttachTicketsConfig {
  /** The worker's signing key -- the key that seals attach_opaque_data. */
  signingKey: Uint8Array;
  /** The catalog(s) the worker serves; validation reads their declared attach options. */
  catalogInterface: CatalogInterface | undefined;
  /** Lifetime ceiling in seconds, or `null` for no maximum. */
  maxTtlSeconds: number | null;
  /** Clock override (seconds), for tests. */
  now?: () => number;
}

/** The attach options `catalogName` declares, or `null` if no such catalog. */
async function declaredSpecs(
  catalog: CatalogInterface | undefined,
  catalogName: string,
): Promise<Array<{ name: string; required?: boolean }> | null> {
  if (!catalog) return null;
  const infos = catalog.catalogsInfo
    ? await catalog.catalogsInfo()
    : catalog.catalogs().map((name) => ({ name, attach_option_specs: [] as Uint8Array[] }));
  for (const info of infos) {
    if (info.name === catalogName) {
      return deserializeAttachOptionSpecs(
        ((info.attach_option_specs ?? []) as unknown[]).map((b) => toUint8Array(b as Uint8Array)),
      );
    }
  }
  return null;
}

/** Validate and seal; the body of `seal_attach`. */
export async function sealAttach(
  config: AttachTicketsConfig,
  request: Record<string, unknown>,
  auth: AuthContext | undefined,
): Promise<{ ticket: string; expires_at: number }> {
  const principal = callerPrincipal(auth);
  if (principal === null) throw new SealAttachDeniedError("an anonymous caller cannot seal an attach ticket");

  const violations: Array<readonly [string, string]> = [];
  const ttl = Number(request.ttl_seconds ?? 0);
  if (!Number.isFinite(ttl) || ttl < 0) violations.push(["ttl_seconds", "must be 0 (as long as allowed) or positive"]);

  const catalogName = String(request.catalog_name ?? "");
  let optionsIpc: Uint8Array = new Uint8Array(0);
  let options: Record<string, unknown> = {};
  if (request.options != null) {
    const raw = toUint8Array(request.options as Uint8Array);
    if (raw.length > 0) {
      const batch = deserializeBatch(raw);
      if (batch.numRows > 1) {
        violations.push(["options", "must be a one-row record"]);
      } else if (batch.numRows === 1) {
        options = decodeOptionsBatch(raw);
        optionsIpc = raw;
      }
    }
  }

  const specs = await declaredSpecs(config.catalogInterface, catalogName);
  if (specs === null) {
    violations.push(["catalog_name", `no catalog named ${JSON.stringify(catalogName)}`]);
  } else {
    const declared = new Set(specs.map((s) => s.name.toLowerCase()));
    for (const name of Object.keys(options)) {
      if (name.toLowerCase() === ATTACH_TICKET_OPTION) {
        violations.push([`options.${name}`, "a ticket cannot seal another ticket"]);
      } else if (!declared.has(name.toLowerCase())) {
        violations.push([`options.${name}`, "not an attach option this catalog declares"]);
      }
    }
    const supplied = new Set(Object.keys(options).map((n) => n.toLowerCase()));
    for (const spec of specs) {
      if (spec.required && !supplied.has(spec.name.toLowerCase())) violations.push([`options.${spec.name}`, "required"]);
    }
  }
  if (optionsIpc.length > MAX_OPTIONS_BYTES) {
    violations.push(["options", `${optionsIpc.length} bytes; a ticket carries at most ${MAX_OPTIONS_BYTES}`]);
  }
  if (violations.length > 0) throw new AttachTicketRequestError("seal_attach request is invalid", violations);

  const issuedAt = Math.trunc(config.now ? config.now() : Date.now() / 1000);
  const ceiling = config.maxTtlSeconds;
  // 0 asks for the ceiling; otherwise the request, capped at the ceiling.
  const lifetime = ttl === 0 ? ceiling : ceiling === null ? Math.trunc(ttl) : Math.min(Math.trunc(ttl), ceiling);
  const expiresAt = lifetime === null ? 0 : issuedAt + lifetime;
  let token: string;
  try {
    ({ token } = await mintAttachTicket(config.signingKey, {
      principal,
      catalogName,
      optionsIpc,
      dataVersionSpec: String(request.data_version_spec ?? ""),
      implementationVersion: String(request.implementation_version ?? ""),
      issuedAt,
      expiresAt,
    }));
  } catch (e) {
    throw new AttachTicketRequestError("seal_attach request is invalid", [["request", (e as Error).message]]);
  }
  return { ticket: token, expires_at: expiresAt === 0 ? Number.POSITIVE_INFINITY : expiresAt };
}

/**
 * Build `vgi.attach_tickets.v1`: one unary method, `seal_attach(request:
 * SealAttachRequest) -> AttachTicket`, each carried as serialized Arrow IPC
 * in a single binary column -- the same wire, and protocol hash, as
 * vgi-python's.
 */
export function buildAttachTicketsProtocol(config: AttachTicketsConfig): Protocol {
  const p = new Protocol(ATTACH_TICKETS_PROTOCOL_NAME, { protocolVersion: ATTACH_TICKETS_PROTOCOL_VERSION });
  p.unary("seal_attach", {
    params: REQUEST_PARAMS_SCHEMA as never,
    result: RESULT_BINARY_SCHEMA as never,
    doc: "Seal the caller's attach of request.catalog_name into a ticket.",
    handler: async (params: Record<string, unknown>, ctx: unknown) => {
      const request = unwrapRequest(params.request);
      const sealed = await sealAttach(config, request, (ctx as CallContext | undefined)?.auth);
      const batch = batchFromColumns(
        { ticket: [sealed.ticket], expires_at: [sealed.expires_at] },
        ATTACH_TICKET_SCHEMA,
      );
      return { result: serializeBatch(batch) };
    },
  });
  return p;
}
