// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// The one place a VGI worker's vgi-rpc server is built.
//
// Every transport a worker serves -- stdin/stdout, AF_UNIX (the launcher), raw
// TCP, the Iroh raw upstream, a caller-provided byte stream (Web Worker), and
// HTTP (Bun.serve and Cloudflare Workers alike) -- builds its server through
// `buildRpcServer`. Before this existed each built its own, five times over,
// which is how "what is hosted on which transport" drifts.
//
// What a server hosts, in reflection order:
//
//   1. The worker's own protocol, `vgi.v2`.
//   2. The worker's `hostedProtocols()`, in the order returned -- on EVERY
//      transport.
//   3. `vgi_rpc.Reflection.v1` -- on every transport.
//   4. `vgi_rpc.Identity.v1` -- HTTP only, and only when the worker supplies
//      `resolveToken` and/or `mintGrant`.
//
// Extra protocols cannot change `vgi.v2` behaviour: vgi-rpc routes every
// request on its `vgi_rpc.protocol` key with no fallback to the primary, so a
// client that only ever names `vgi.v2` (the DuckDB extension) dispatches
// exactly as it would against a single-protocol server.

import {
  type GrantMinter,
  IdentityImpl,
  type Protocol,
  type TokenResolver,
  VgiRpcServer,
} from "@query-farm/vgi-rpc";

/** The transport a server is being built for. Only `"http"` changes what is
 *  hosted (identity); the rest are recorded so the decision lives here rather
 *  than at each call site. `"iroh"` is the raw TCP upstream of a
 *  `vgi-iroh-bridge`; `"stream"` a caller-provided byte-stream pair. */
export type ServerTransport = "pipe" | "unix" | "tcp" | "iroh" | "stream" | "http";

const TRANSPORTS: ReadonlySet<string> = new Set<ServerTransport>(["pipe", "unix", "tcp", "iroh", "stream", "http"]);

/** Transports that authenticate their callers, and so may host
 *  `vgi_rpc.Identity.v1`. Identity answers questions about *other* callers'
 *  credentials; its allowlist is a list of principals, which a transport with
 *  no caller identity cannot check. */
const IDENTITY_TRANSPORTS: ReadonlySet<string> = new Set<ServerTransport>(["http"]);

const RESERVED_PREFIX = "vgi_rpc.";

/** Environment variable naming the principals allowed to call `introspect_token`. */
export const INTROSPECT_PRINCIPALS_ENV = "VGI_INTROSPECT_PRINCIPALS";

/**
 * What a worker hosts beyond `vgi.v2`, and whether it opts into identity.
 *
 * Accepted by `new Worker({...})`, `serveVgiWorker`, `createVgiWorkerFetch`
 * and `createVgiFetch` (Cloudflare Workers) alike, so one worker definition
 * hosts the same set on every transport.
 */
export interface HostingOptions {
  /**
   * Additional vgi-rpc protocols to host beside `vgi.v2`, on the **same**
   * listener, whatever the transport.
   *
   * A vgi-rpc `Protocol` carries its handlers, so each entry is a whole
   * `(protocol, implementation)` pair. Called **once**, when the server is
   * built; it may consult configuration or the environment, but the answer is
   * fixed for the life of the process, so reflection output and protocol
   * hashes stay stable. Hosted after `vgi.v2`, in the order returned.
   *
   * The protocol is the unit of optionality: there is no way to host a subset
   * of a protocol's methods. A capability that is optional is its own
   * protocol, returned here or not.
   *
   * Each needs a distinct wire name, which may not be `vgi.v2` and may not
   * use the reserved `vgi_rpc.` prefix: reflection is hosted automatically and
   * `vgi_rpc.Identity.v1` is enabled through {@link resolveToken} /
   * {@link mintGrant}. Violations are a startup error naming this hook.
   */
  hostedProtocols?: () => Iterable<Protocol>;

  /**
   * Resolve an opaque bearer credential to the identity it authenticates as.
   * Supplying it hosts `vgi_rpc.Identity.v1`'s `introspect_token` -- on HTTP,
   * the transport that authenticates callers. Absent, the method is absent
   * (not hosted-and-refusing), which keeps a dependency upgrade from growing a
   * credential-to-identity oracle on every existing worker.
   *
   * Return `null` for "the store answered and this credential is unknown".
   * For "I could not find out" (a store or sidecar outage, a timeout, a 5xx)
   * throw `AuthUnavailableError` from `@query-farm/vgi-rpc` -- the same error
   * an `authenticate` callback throws for an outage. The framework translates
   * it to `identity_unavailable` with that error's retry hint
   * (`vgi_rpc.RetryInfo`), so callers know to retry rather than cache a
   * refusal. `IdentityUnavailableError` works too. Never throw a plain
   * `Error` for an outage: a caller cannot tell it from a definitive answer.
   *
   * Requires {@link introspectPrincipals} (or `VGI_INTROSPECT_PRINCIPALS`);
   * there is no permissive default, and a worker without one refuses to start.
   */
  resolveToken?: TokenResolver;

  /**
   * Mint a standing delegation credential for the *calling* user. Supplying
   * it hosts `issue_grant` (HTTP only). Throw `GrantRefusedError` to decline,
   * and `AuthUnavailableError` for a transient failure (translated as for
   * {@link resolveToken}). Needs no allowlist: it is not an oracle about
   * anybody else.
   */
  mintGrant?: GrantMinter;

  /**
   * Principals permitted to call `introspect_token`. When omitted,
   * `VGI_INTROSPECT_PRINCIPALS` (comma-separated) is read. Only consulted when
   * {@link resolveToken} is supplied.
   */
  introspectPrincipals?: Iterable<string>;

  /** Maximum age of the caller's authentication for `issue_grant`, in
   *  seconds. vgi-rpc's default when omitted. */
  maxAuthAge?: number;
}

/** Server-level options passed through to `VgiRpcServer`. */
export interface BuildRpcServerOptions {
  /** Which transport this server will serve. */
  transport: ServerTransport;
  /** Server identifier, surfaced through reflection. */
  serverId?: string;
  /** Environment to read `VGI_INTROSPECT_PRINCIPALS` from. Default
   *  `process.env` where it exists (not on Cloudflare Workers). */
  env?: Record<string, string | undefined>;
}

/**
 * Build the vgi-rpc server for a worker's `vgi.v2` protocol on `transport`.
 *
 * Calls `hosting.hostedProtocols()` exactly once and hosts the result beside
 * `primary`; see the module comment for the full hosted set.
 *
 * @throws TypeError `hostedProtocols()` returned something other than an
 *   iterable of vgi-rpc `Protocol`s.
 * @throws Error A hosted protocol's name is invalid, reserved, `vgi.v2`'s, or
 *   repeated; the transport is unknown; or the worker supplies `resolveToken`
 *   on HTTP with no introspector allowlist.
 */
export function buildRpcServer(
  primary: Protocol,
  hosting: HostingOptions,
  options: BuildRpcServerOptions,
): VgiRpcServer {
  if (!TRANSPORTS.has(options.transport)) {
    throw new Error(`Unknown transport ${JSON.stringify(options.transport)}; expected one of ${[...TRANSPORTS].join(", ")}.`);
  }
  const protocols = validatedHostedProtocols(hosting, primary);
  const identity = IDENTITY_TRANSPORTS.has(options.transport) ? buildIdentity(hosting, options.env) : undefined;
  return new VgiRpcServer(primary, {
    serverId: options.serverId,
    protocols,
    identity,
  });
}

/** Call `hostedProtocols()` once and check its shape and names.
 *
 *  vgi-rpc checks these too, but its messages name a protocol and not the
 *  worker hook that supplied it; checking here first lets the error say which
 *  option to fix. */
function validatedHostedProtocols(hosting: HostingOptions, primary: Protocol): Protocol[] {
  if (hosting.hostedProtocols === undefined) return [];
  const owner = "hostedProtocols()";
  if (typeof hosting.hostedProtocols !== "function") {
    throw new TypeError(`${owner} must be a function returning vgi-rpc Protocols, got ${typeof hosting.hostedProtocols}.`);
  }
  const raw = hosting.hostedProtocols();
  if (raw == null || typeof (raw as Iterable<Protocol>)[Symbol.iterator] !== "function" || typeof raw === "string") {
    throw new TypeError(`${owner} must return an iterable of vgi-rpc Protocols, got ${String(raw)}.`);
  }
  const seen = new Set<string>();
  const out: Protocol[] = [];
  let index = 0;
  for (const proto of raw as Iterable<Protocol>) {
    const p = proto as Partial<Protocol> | null;
    if (p == null || typeof p.name !== "string" || typeof p.getMethod !== "function") {
      throw new TypeError(`${owner} entry ${index} must be a vgi-rpc Protocol, got ${String(proto)}.`);
    }
    const name = p.name;
    if (name.startsWith(RESERVED_PREFIX)) {
      throw new Error(
        `${owner} entry ${index} is named '${name}', which claims the reserved '${RESERVED_PREFIX}' prefix. ` +
          "Framework protocols are not supplied through this hook: reflection is hosted automatically, and " +
          "vgi_rpc.Identity.v1 is enabled by supplying resolveToken and/or mintGrant.",
      );
    }
    if (!/^[A-Za-z_][A-Za-z0-9_.]*$/.test(name)) {
      throw new Error(
        `${owner} entry ${index}: protocol name '${name}' is not an identifier, optionally dot-qualified ` +
          "(e.g. 'acme.Reports.v1').",
      );
    }
    if (name === primary.name) {
      throw new Error(
        `${owner} entry ${index} is named '${name}', the worker's own protocol. Give it a distinct name.`,
      );
    }
    if (seen.has(name)) {
      throw new Error(
        `${owner} lists protocol name '${name}' twice. The name is the routing key, so each hosted protocol ` +
          "needs a distinct name.",
      );
    }
    seen.add(name);
    out.push(proto);
    index++;
  }
  return out;
}

/** Build the `vgi_rpc.Identity.v1` implementation, or `undefined`.
 *
 *  `undefined` unless the worker supplies at least one hook -- and that is the
 *  point: the server then does not host the protocol at all, rather than
 *  hosting it and refusing every call. The two hooks are independent. */
function buildIdentity(
  hosting: HostingOptions,
  env: Record<string, string | undefined> | undefined,
): IdentityImpl | undefined {
  const { resolveToken, mintGrant } = hosting;
  if (!resolveToken && !mintGrant) return undefined;
  return new IdentityImpl({
    resolveToken,
    mintGrant,
    // Only meaningful for `introspect_token`; a worker that mints but resolves
    // nothing is not an oracle and needs no allowlist.
    introspectPrincipals: resolveToken ? resolveIntrospectPrincipals(hosting.introspectPrincipals, env) : undefined,
    maxAuthAge: hosting.maxAuthAge,
  });
}

/**
 * Resolve the introspector allowlist, or refuse to start.
 *
 * Fail-closed rather than defaulting to "any authenticated caller":
 * authenticating and introspecting are different capabilities, and a
 * permissive default lets any user resolve any other user's credential to its
 * owner. A worker that supplies `resolveToken` and forgets the allowlist must
 * not start.
 */
function resolveIntrospectPrincipals(
  explicit: Iterable<string> | undefined,
  env: Record<string, string | undefined> | undefined,
): string[] {
  let principals: string[];
  if (explicit !== undefined) {
    principals = [...explicit].map((p) => p.trim()).filter((p) => p !== "");
  } else {
    const source = env ?? (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env;
    const raw = source?.[INTROSPECT_PRINCIPALS_ENV] ?? "";
    principals = raw
      .split(",")
      .map((p) => p.trim())
      .filter((p) => p !== "");
  }
  if (principals.length === 0) {
    throw new Error(
      "This worker supplies resolveToken, which hosts the vgi_rpc.Identity.v1 protocol, but no " +
        `introspector allowlist was configured. Set ${INTROSPECT_PRINCIPALS_ENV} (comma-separated) or pass ` +
        "introspectPrincipals.\n\n" +
        "There is no permissive default on purpose: introspection is a separate capability from " +
        "authentication, and allowing every authenticated caller lets any user resolve any other user's " +
        "credential to its owner. Remove resolveToken to leave the protocol unhosted entirely.",
    );
  }
  return principals;
}
