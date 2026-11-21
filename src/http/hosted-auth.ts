// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Orchard hosted mode: the Pass verifier and the request gate.
//
// The verifier checks an RFC 9068 JWT access token against the Front Desk's
// JWKS with `jose` (Web Crypto based, so the same code runs on Node, Bun and
// Cloudflare Workers; no `node:*` imports here). The JWKS cache is our own,
// not `createRemoteJWKSet`, because the contract pins behaviour jose's cache
// does not have: a hard 10-minute lifetime that a shorter
// `Cache-Control: max-age` lowers, keys discarded (never reused stale) once it
// passes, a failed re-fetch answered as 503 rather than 401, and unknown-`kid`
// re-fetches limited to one per minute.
//
// The gate sits in front of vgi-rpc's HTTP handler. It authenticates every
// request except `HEAD /health` and CORS preflights *before* the handler sees
// it, so the body is never read, size-checked or decompressed for a caller
// without a Pass, and the landing page, describe page, `GET /health` and
// unknown paths (which vgi-rpc serves without consulting `authenticate`) all
// need one too. Its 401 carries `WWW-Authenticate: Bearer
// error="invalid_token"`, `VGI-Auth-Reason` and the usual VGI capability
// headers. The `Authorization` header is never logged.

import { createLocalJWKSet, jwtVerify, type JWTPayload, type JWTVerifyGetKey } from "jose";
import {
  AUTH_REASON_HEADER,
  AuthContext,
  AuthFailure,
  AuthReason,
  AuthUnavailableError,
  type AuthenticateFn,
} from "@query-farm/vgi-rpc";
import {
  HOSTED_AUTH_DOMAIN,
  HOSTED_CHALLENGE,
  JWKS_FETCH_TIMEOUT_SECONDS,
  JWKS_MAX_AGE_SECONDS,
  JWKS_REFRESH_MIN_INTERVAL_SECONDS,
  LEEWAY_SECONDS,
  REQUIRED_TYP,
  type HostedConfig,
} from "../hosted.js";

/** The slice of `fetch` the JWKS client uses. */
export type JwksFetch = (url: string, init: RequestInit) => Promise<Response>;

/** Injection points for tests. Production uses the global `fetch` and clock. */
export interface HostedVerifierDeps {
  /** Fetch used for the JWKS (default `globalThis.fetch`). */
  fetch?: JwksFetch | undefined;
  /** Clock, milliseconds since the epoch (default `Date.now`). */
  now?: (() => number) | undefined;
  /** Operator warnings (JWKS failures). Never receives a credential. Default `console.error`. */
  warn?: ((message: string) => void) | undefined;
  /** Debug lines (per-request rejections). Never receives a credential. Default: off. */
  debug?: ((message: string) => void) | undefined;
}

const HOSTED_MARK: unique symbol = Symbol.for("@query-farm/vgi.orchard-hosted-authenticate");

const MAX_AGE_RE = /(?:^|,)\s*max-age\s*=\s*"?(\d+)"?\s*(?=,|$)/i;

/** The `max-age` directive of a `Cache-Control` header, seconds, or `null`. */
export function cacheControlMaxAge(value: string | null | undefined): number | null {
  if (!value) return null;
  const match = MAX_AGE_RE.exec(value);
  return match ? Number(match[1]) : null;
}

/** Media types compare case-insensitively; `application/` is optional (RFC 9068 §2.1). */
function normalizeTyp(value: string): string {
  const lowered = value.trim().toLowerCase();
  return lowered.startsWith("application/") ? lowered.slice("application/".length) : lowered;
}

function errorCode(error: unknown): string {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === "string" ? code : error instanceof Error ? error.name : "unknown";
}

interface CachedKeys {
  readonly keys: JWTVerifyGetKey;
  /** Epoch ms at which the keys stop being usable. */
  readonly expiresAt: number;
}

/**
 * Build the Pass verifier for `config`: an `AuthenticateFn` returning an
 * `AuthContext` with domain `"orchard"`, principal `sub` and the Pass's claims
 * exactly as decoded (booleans stay booleans, numbers stay numbers).
 *
 * It throws `AuthFailure` (401) with reason `missing_credential`,
 * `invalid_credential` or `expired_credential`, and `AuthUnavailableError`
 * (503 with `Retry-After`) when no JWKS within its maximum age can be fetched.
 */
export function hostedAuthenticate(config: HostedConfig, deps: HostedVerifierDeps = {}): AuthenticateFn {
  const doFetch: JwksFetch = deps.fetch ?? ((url, init) => globalThis.fetch(url, init));
  const now = deps.now ?? Date.now;
  const warn = deps.warn ?? ((m: string) => console.error(m));
  const debug = deps.debug;
  const algorithms = [...config.algorithms];

  let cache: CachedKeys | null = null;
  let inflight: Promise<JWTVerifyGetKey> | null = null;
  let lastFetchAttempt: number | null = null;

  const usable = (entry: CachedKeys | null, at: number): entry is CachedKeys => entry !== null && at < entry.expiresAt;

  async function fetchOnce(): Promise<JWTVerifyGetKey> {
    const started = now();
    lastFetchAttempt = started;
    try {
      // No redirects: the JWKS URI is configuration, and following one would
      // let whoever controls the redirect choose the keys.
      const response = await doFetch(config.jwksUri, {
        headers: { Accept: "application/json" },
        redirect: "manual",
        signal: AbortSignal.timeout(JWKS_FETCH_TIMEOUT_SECONDS * 1000),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const keys = createLocalJWKSet(await response.json());
      const advertised = cacheControlMaxAge(response.headers.get("cache-control"));
      const ttl = advertised === null ? JWKS_MAX_AGE_SECONDS : Math.min(JWKS_MAX_AGE_SECONDS, advertised);
      // Measured from when the fetch started, so a slow response cannot
      // stretch the bound.
      cache = { keys, expiresAt: started + ttl * 1000 };
      return keys;
    } catch (error) {
      // Never fall back to stale keys: drop an expired entry so nothing can
      // read it, and make the caller fail closed (503, not 401).
      if (!usable(cache, now())) cache = null;
      warn(`[vgi] Orchard hosted mode: JWKS fetch failed (${errorCode(error)})`);
      throw new AuthUnavailableError("JWKS endpoint unavailable", 5);
    }
  }

  /** One fetch at a time; concurrent callers share it. */
  function fetchKeys(): Promise<JWTVerifyGetKey> {
    if (inflight) return inflight;
    const pending = fetchOnce();
    inflight = pending;
    pending.then(
      () => { if (inflight === pending) inflight = null; },
      () => { if (inflight === pending) inflight = null; },
    );
    return pending;
  }

  /** Keys within their maximum age, fetching when there are none. */
  function currentKeys(): Promise<JWTVerifyGetKey> {
    if (usable(cache, now())) return Promise.resolve(cache.keys);
    cache = null;
    return fetchKeys();
  }

  /** Re-fetch after an unknown `kid`; `null` when rate limited. A refresh
   *  another request already made is reused instead of stampeding the JWKS
   *  endpoint during a key rotation. */
  function refreshForUnknownKid(seen: JWTVerifyGetKey): Promise<JWTVerifyGetKey> | null {
    const at = now();
    if (usable(cache, at) && cache.keys !== seen) return Promise.resolve(cache.keys);
    if (inflight) return inflight;
    if (lastFetchAttempt !== null && at - lastFetchAttempt < JWKS_REFRESH_MIN_INTERVAL_SECONDS * 1000) return null;
    return fetchKeys();
  }

  async function verifyWith(token: string, keys: JWTVerifyGetKey): Promise<JWTPayload> {
    const { payload, protectedHeader } = await jwtVerify(token, keys, {
      algorithms,
      issuer: config.issuer,
      audience: config.audience,
      clockTolerance: LEEWAY_SECONDS,
      requiredClaims: ["exp"],
      currentDate: new Date(now()),
    });
    const typ = protectedHeader.typ;
    if (typeof typ !== "string" || normalizeTyp(typ) !== REQUIRED_TYP) {
      throw new AuthFailure(AuthReason.InvalidCredential, `JOSE header 'typ' must be '${REQUIRED_TYP}'`);
    }
    // Exactly one audience: a Pass names one Desk Address. A token naming
    // several is refused even if one of them is ours.
    const aud = payload.aud;
    if (!(typeof aud === "string" || (Array.isArray(aud) && aud.length === 1))) {
      throw new AuthFailure(AuthReason.InvalidCredential, "Pass must name exactly one audience");
    }
    return payload;
  }

  function reject(error: unknown): AuthFailure {
    if (error instanceof AuthFailure) return error;
    const code = errorCode(error);
    const reason = code === "ERR_JWT_EXPIRED" ? AuthReason.ExpiredCredential : AuthReason.InvalidCredential;
    debug?.(`[vgi] Orchard hosted mode: Pass rejected (${reason}: ${code})`);
    return new AuthFailure(reason, "invalid Pass");
  }

  const authenticate = async function orchardHostedAuthenticate(request: Request): Promise<AuthContext> {
    const header = request.headers.get("Authorization") ?? "";
    if (!header.trim()) {
      debug?.("[vgi] Orchard hosted mode: request without a Pass");
      throw new AuthFailure(AuthReason.MissingCredential, "Missing Authorization header");
    }
    // The scheme is case-insensitive (RFC 9110 §11.1).
    const match = /^bearer +(\S+) *$/i.exec(header);
    if (!match) {
      // Worded so no log line ever reads "Bearer <something>", which a leak
      // scanner rightly treats as a logged credential.
      debug?.("[vgi] Orchard hosted mode: Pass rejected (invalid_credential: unsupported Authorization scheme)");
      throw new AuthFailure(AuthReason.InvalidCredential, "Authorization header is not a Bearer credential");
    }
    const token = match[1]!;

    const keys = await currentKeys();
    let claims: JWTPayload;
    try {
      claims = await verifyWith(token, keys);
    } catch (error) {
      if (errorCode(error) !== "ERR_JWKS_NO_MATCHING_KEY") throw reject(error);
      // Unknown kid: refresh the JWKS (rate limited) and retry once. A failed
      // refresh is an AuthUnavailableError (503), not a rejection.
      const refreshed = refreshForUnknownKid(keys);
      if (refreshed === null) throw reject(error);
      const fresh = await refreshed;
      try {
        claims = await verifyWith(token, fresh);
      } catch (retryError) {
        throw reject(retryError);
      }
    }
    const principal = typeof claims.sub === "string" ? claims.sub : String(claims.sub ?? "");
    return new AuthContext(HOSTED_AUTH_DOMAIN, true, principal, { ...claims });
  };
  Object.defineProperty(authenticate, HOSTED_MARK, { value: true });
  return authenticate;
}

/** Whether `fn` was built by {@link hostedAuthenticate}. */
export function isHostedAuthenticate(fn: unknown): boolean {
  return typeof fn === "function" && (fn as unknown as Record<symbol, unknown>)[HOSTED_MARK] === true;
}

/** Headers of the capability probe that do not belong on a 401/503. */
const PROBE_HEADERS_DROPPED: ReadonlySet<string> = new Set(["content-type", "content-length", "x-request-id"]);

/** A hosted gate: the verifier, the `authenticate` to hand vgi-rpc, and the wrapper. */
export interface HostedGate {
  /** The Pass verifier. */
  readonly verify: AuthenticateFn;
  /**
   * The `authenticate` to give `createHttpHandler`. It returns the identity
   * the gate already verified for this request (re-verifying only if the
   * handler was given a different `Request` object), so a Pass is checked
   * once per request.
   */
  readonly authenticate: AuthenticateFn;
  /** Put the gate in front of a vgi-rpc HTTP handler mounted at `prefix`. */
  wrap(handler: (req: Request) => Promise<Response> | Response, prefix: string): (req: Request) => Promise<Response>;
}

/** Build the hosted-mode request gate for `config`. */
export function createHostedGate(config: HostedConfig, deps: HostedVerifierDeps = {}): HostedGate {
  const verify = hostedAuthenticate(config, deps);
  const verified = new WeakMap<Request, AuthContext>();
  const authenticate: AuthenticateFn = async (request) => verified.get(request) ?? (await verify(request));
  Object.defineProperty(authenticate, HOSTED_MARK, { value: true });

  function wrap(handler: (req: Request) => Promise<Response> | Response, prefix: string) {
    const healthPath = `${prefix}/health`;
    let probe: Array<[string, string]> | undefined;

    /** The capability (and CORS) headers the handler puts on `HEAD /health`.
     *  They depend only on configuration, so they are read once. */
    async function capabilityHeaders(request: Request): Promise<Array<[string, string]>> {
      if (probe) return probe;
      const url = new URL(request.url);
      url.pathname = healthPath;
      url.search = "";
      const response = await handler(new Request(url, { method: "HEAD" }));
      const headers: Array<[string, string]> = [];
      response.headers.forEach((value, name) => {
        if (!PROBE_HEADERS_DROPPED.has(name.toLowerCase())) headers.push([name, value]);
      });
      probe = headers;
      return headers;
    }

    async function refusal(error: unknown, request: Request): Promise<Response> {
      const headers = new Headers();
      for (const [name, value] of await capabilityHeaders(request)) headers.set(name, value);
      headers.set("Content-Type", "application/json");
      headers.set("Cache-Control", "no-store");
      if (error instanceof AuthUnavailableError) {
        const retryAfter = Number.isFinite(error.retryAfter) && error.retryAfter >= 0 ? Math.ceil(error.retryAfter) : 5;
        headers.set("Retry-After", String(retryAfter));
        const body = { error: "authentication_unavailable", detail: "authentication authority unavailable" };
        return new Response(JSON.stringify(body), { status: 503, headers });
      }
      // Anything that is not a classified rejection (a bug in the verifier)
      // still fails closed, as an invalid credential.
      const reason = error instanceof AuthFailure ? error.reason : AuthReason.InvalidCredential;
      headers.set("WWW-Authenticate", HOSTED_CHALLENGE);
      headers.set(AUTH_REASON_HEADER, reason);
      const body = { error: "unauthorized", reason, detail: "authentication rejected" };
      return new Response(JSON.stringify(body), { status: 401, headers });
    }

    return async function orchardHostedGate(request: Request): Promise<Response> {
      const pathname = new URL(request.url).pathname;
      const path = pathname.includes("//") ? pathname.replace(/\/{2,}/g, "/") : pathname;
      // The Front Desk's liveness probe, and CORS preflights (which browsers
      // send without credentials), are the only anonymous requests.
      if (request.method === "HEAD" && path === healthPath) return handler(request);
      if (request.method === "OPTIONS" && request.headers.has("Access-Control-Request-Method")) {
        return handler(request);
      }
      let auth: AuthContext;
      try {
        auth = await verify(request);
      } catch (error) {
        return refusal(error, request);
      }
      verified.set(request, auth);
      return handler(request);
    };
  }

  return { verify, authenticate, wrap };
}
