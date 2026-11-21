// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Orchard hosted mode (`VGI_ORCHARD_HOSTED=1`): configuration and the
// fail-closed startup checks.
//
// A hosted worker (a *Field Worker*) serves VGI over HTTP to callers who hold a
// *Pass*: a short-lived RFC 9068 access token (`typ: at+jwt`, ES256) issued by
// the Orchard Front Desk for exactly one canonical Desk Address. The worker
// verifies Passes locally against the Front Desk's JWKS and accepts nothing
// else. Everything here fails closed:
//
// - Startup is refused unless `VGI_JWT_ISSUER`, `VGI_JWT_AUDIENCE` and
//   `VGI_JWT_JWKS_URI` are all set to a single non-empty value.
// - Startup is refused when any other way in is configured: static bearer
//   tokens, sealed-grant keys, a `resolveToken` / `mintGrant` hook, OAuth
//   resource metadata, a proxy-proof gate, Iroh or other peer identity, a
//   custom `authenticate` callback, or a non-HTTP transport.
// - Startup is refused without a stable, operator-configured signing key.
//
// The Pass verifier and the request gate live in `http/hosted-auth.ts`; this
// module has no dependencies so the stdio `Worker` can refuse hosted mode
// without loading a JOSE library. Neither module imports `node:*`: hosted mode
// runs on Node, Bun and Cloudflare Workers alike.
//
// The reference implementation is vgi-python's `vgi/hosted.py`; the design is
// in the extension repository's `docs/location-delegation.md` ("The Pass" and
// "The Field Worker: SDK hosted mode").

/** Environment variable that turns hosted mode on. Baked into the hosted base image. */
export const HOSTED_ENV = "VGI_ORCHARD_HOSTED";

/** The challenge on every hosted-mode 401. No `resource_metadata`: a Field
 *  Worker never sends a client off to discover an authorization server. */
export const HOSTED_CHALLENGE = 'Bearer error="invalid_token"';

/** `AuthContext.domain` of a caller authenticated by a Pass. */
export const HOSTED_AUTH_DOMAIN = "orchard";

/** Default (and only-by-override) signing algorithm for Passes. */
export const DEFAULT_HOSTED_ALGORITHMS: readonly string[] = Object.freeze(["ES256"]);
/** Required JOSE `typ` (RFC 9068). */
export const REQUIRED_TYP = "at+jwt";
/** Clock skew allowed on `exp` / `nbf`, seconds. */
export const LEEWAY_SECONDS = 60;
/** Hard upper bound on how long JWKS keys are trusted, seconds. */
export const JWKS_MAX_AGE_SECONDS = 600;
/** Minimum time between JWKS re-fetches triggered by an unknown `kid`, seconds. */
export const JWKS_REFRESH_MIN_INTERVAL_SECONDS = 60;
/** Timeout for each JWKS request, seconds. */
export const JWKS_FETCH_TIMEOUT_SECONDS = 10;
/** The shortest signing key a hosted worker accepts, bytes. */
export const MIN_SIGNING_KEY_BYTES = 32;

const TRUE_VALUES: ReadonlySet<string> = new Set(["1", "true", "yes", "on"]);
const FALSE_VALUES: ReadonlySet<string> = new Set(["", "0", "false", "no", "off"]);

/** An environment: `process.env`, a Cloudflare Worker's `env`, or a test map. */
export type HostedEnv = Readonly<Record<string, string | undefined>>;

/**
 * A hosted-mode configuration error. The worker must not start.
 *
 * `serveVgiWorker` and `Worker.run` print the message and exit non-zero;
 * `createVgiFetch` / `createVgiWorkerFetch` throw it at construction.
 */
export class OrchardHostedConfigError extends Error {
  constructor(reason: string, options?: { raw?: boolean }) {
    super(options?.raw ? reason : `${HOSTED_ENV}=1 (Orchard hosted mode): ${reason}`);
    this.name = "OrchardHostedConfigError";
  }
}

/** Verified hosted-mode settings, read from the environment. */
export interface HostedConfig {
  /** The Front Desk issuer (`iss`), matched exactly. */
  readonly issuer: string;
  /** The one canonical Desk Address (`aud`), matched exactly. */
  readonly audience: string;
  /** Where the Front Desk publishes its signing keys. */
  readonly jwksUri: string;
  /** Accepted JWS algorithms (`ES256` unless overridden). */
  readonly algorithms: readonly string[];
}

function processEnv(): HostedEnv {
  return ((globalThis as { process?: { env?: HostedEnv } }).process?.env ?? {}) as HostedEnv;
}

/**
 * Whether `VGI_ORCHARD_HOSTED` turns hosted mode on.
 *
 * True for `1`/`true`/`yes`/`on` (any case), false when unset or
 * `0`/`false`/`no`/`off`/empty. Any other value throws
 * {@link OrchardHostedConfigError}: a typo must not quietly start a worker
 * without authentication.
 */
export function hostedModeEnabled(env: HostedEnv = processEnv()): boolean {
  const raw = env[HOSTED_ENV];
  if (raw === undefined) return false;
  const value = raw.trim().toLowerCase();
  if (TRUE_VALUES.has(value)) return true;
  if (FALSE_VALUES.has(value)) return false;
  throw new OrchardHostedConfigError(
    `${HOSTED_ENV} must be 1 or 0 (or true/false), got ${JSON.stringify(raw)}`,
    { raw: true },
  );
}

function singleValue(env: HostedEnv, name: string): string {
  const value = (env[name] ?? "").trim();
  if (!value) {
    throw new OrchardHostedConfigError(`${name} must be set (unset and empty are both errors).`);
  }
  if (value.includes(",")) {
    throw new OrchardHostedConfigError(
      `${name} must be exactly one value; got a comma-separated list. ` +
        "A Pass is issued for one canonical Desk Address and one issuer.",
    );
  }
  return value;
}

/**
 * Parse a `VGI_JWT_ALGORITHMS` value: comma-separated JWS algorithm names.
 * `none` and `HS*` are always refused: an HMAC algorithm checked against a
 * JWKS lets anyone holding the *public* key mint tokens.
 *
 * Returns the default (`ES256`) when unset or blank.
 */
export function parseJwtAlgorithms(raw: string | undefined): readonly string[] {
  if (raw === undefined || !raw.trim()) return DEFAULT_HOSTED_ALGORITHMS;
  const algorithms = raw.split(",").map((a) => a.trim()).filter(Boolean);
  const forbidden = algorithms.some((a) => {
    const lowered = a.toLowerCase();
    return lowered === "none" || lowered.startsWith("hs");
  });
  if (algorithms.length === 0 || forbidden) {
    throw new OrchardHostedConfigError(
      `VGI_JWT_ALGORITHMS=${JSON.stringify(raw)} is not allowed; Passes are asymmetric ` +
        "(default ES256), never 'none' or HS*.",
    );
  }
  return Object.freeze(algorithms);
}

/**
 * Read and validate the hosted-mode environment.
 *
 * @throws OrchardHostedConfigError when a required variable is missing, empty
 *   or multi-valued, or `VGI_JWT_ALGORITHMS` is unusable.
 */
export function resolveHostedConfig(env: HostedEnv = processEnv()): HostedConfig {
  const issuer = singleValue(env, "VGI_JWT_ISSUER");
  const audience = singleValue(env, "VGI_JWT_AUDIENCE");
  const jwksUri = singleValue(env, "VGI_JWT_JWKS_URI");
  const algorithms = parseJwtAlgorithms(env.VGI_JWT_ALGORITHMS);
  return Object.freeze({ issuer, audience, jwksUri, algorithms });
}

/**
 * Refuse when the environment configures another credential.
 *
 * The TypeScript SDK reads none of these itself, but a hosted image built from
 * another SDK's conventions may carry them, and each one means the operator
 * expects a second way in. Refusing is the only answer that cannot be wrong.
 *
 * @throws OrchardHostedConfigError on `VGI_BEARER_TOKENS`, `VGI_RPC_GRANT_KEYS`,
 *   `VGI_OAUTH_RESOURCE` or an active `VGI_PROXY_PROOF_MODE`.
 */
export function checkHostedEnvironment(env: HostedEnv = processEnv()): void {
  const conflicts: ReadonlyArray<readonly [string, string]> = [
    ["VGI_BEARER_TOKENS", "static bearer tokens"],
    ["VGI_RPC_GRANT_KEYS", "sealed-grant keys"],
    ["VGI_OAUTH_RESOURCE", "OAuth resource metadata (a Field Worker never advertises resource_metadata)"],
  ];
  for (const [name, what] of conflicts) {
    if ((env[name] ?? "").trim()) {
      throw new OrchardHostedConfigError(
        `${name} is set (${what}); the only credential a hosted worker accepts is a Pass.`,
      );
    }
  }
  const proofMode = (env.VGI_PROXY_PROOF_MODE ?? "off").trim().toLowerCase() || "off";
  if (proofMode !== "off") {
    throw new OrchardHostedConfigError("VGI_PROXY_PROOF_MODE must be off; hosted workers authenticate by Pass only.");
  }
}

/**
 * Refuse a hosted worker whose signing key is not stable across restarts.
 *
 * The key seals catalog opaque data and HTTP state tokens. A Field Worker's
 * machines stop and start, and a deployment can run on several of them: a key
 * generated per process makes every restart (and every other machine) reject
 * the sessions DuckDB already holds.
 *
 * @param env the environment (`VGI_SIGNING_KEY`, `VGI_SIGNING_KEY_MINTED`).
 * @param explicit a key passed in code, if any. It counts as configured.
 * @throws OrchardHostedConfigError when there is no configured key, the
 *   environment marks it as minted by a parent process, or it is shorter than
 *   {@link MIN_SIGNING_KEY_BYTES}.
 */
export function checkHostedSigningKey(env: HostedEnv, explicit?: Uint8Array | string): void {
  const minted = (env.VGI_SIGNING_KEY_MINTED ?? "").trim();
  const raw = (env.VGI_SIGNING_KEY ?? "").trim();
  if ((explicit === undefined && !raw) || (minted && minted !== "0")) {
    throw new OrchardHostedConfigError(
      "VGI_SIGNING_KEY is not set; hosted workers need a stable key shared by every machine of the " +
        "deployment, or every restart breaks attached sessions.",
    );
  }
  const lengths: number[] = [];
  if (raw) lengths.push(new TextEncoder().encode(raw).byteLength);
  if (typeof explicit === "string") lengths.push(new TextEncoder().encode(explicit).byteLength);
  else if (explicit !== undefined) lengths.push(explicit.byteLength);
  if (lengths.some((n) => n < MIN_SIGNING_KEY_BYTES)) {
    throw new OrchardHostedConfigError(`VGI_SIGNING_KEY must be at least ${MIN_SIGNING_KEY_BYTES} bytes.`);
  }
}

/**
 * Hosted mode from an environment, for `createVgiFetch({ orchardHosted })`.
 *
 * Returns `undefined` when `VGI_ORCHARD_HOSTED` is off. When it is on, returns
 * the validated configuration after checking the environment for any other
 * credential. Cloudflare Workers pass the Worker's `env`; `serveVgiWorker`
 * reads `process.env` itself.
 *
 * @throws OrchardHostedConfigError on any misconfiguration.
 */
export function orchardHostedFromEnv(env: HostedEnv = processEnv()): HostedConfig | undefined {
  if (!hostedModeEnabled(env)) return undefined;
  const config = resolveHostedConfig(env);
  checkHostedEnvironment(env);
  return config;
}

/** Refuse a non-HTTP transport (stdio, AF_UNIX, TCP, the Iroh raw upstream,
 *  a caller-provided byte stream) when hosted mode is on: none carries a Pass. */
export function refuseNonHttpTransportInHostedMode(transport: string, env: HostedEnv = processEnv()): void {
  if (hostedModeEnabled(env)) {
    throw new OrchardHostedConfigError(
      `only HTTP is allowed (serveVgiWorker / createVgiWorkerFetch / createVgiFetch); the ${transport} ` +
        "transport carries no Pass (stdio, --unix, --tcp and --iroh-raw-upstream are all refused).",
    );
  }
}
