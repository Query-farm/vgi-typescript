// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Shared HTTP fetch-handler factory for VGI workers.
//
// Both HTTP entrypoints funnel through here:
//   - `vgi/worker-cf`  -> createVgiFetch, exported as `default.fetch` for workerd
//   - `vgi/serve`      -> serveVgiWorker, which hands the handler to Bun.serve
//
// Centralising the wiring means the state-token key reaches `createHttpHandler`
// as `tokenKey` (the option the handler actually reads) and reaches
// `buildVgiProtocol` as `signingKey`, from one source. Passing different keys to
// those two seams produces a worker that mints tokens it cannot recover.

import { ARGUMENTS_REF_BYTES_HEADER, argumentsRefHeaderValue } from "../protocol/arguments-ref.js";
import { omitUndefined } from "../util/defined.js";
import {
  AuthContext,
  createHttpHandler,
  type AuthenticateFn,
  type DispatchHook,
  type DispatchInfo,
  type HookToken,
  type PeerAuthenticationPolicy,
  type PeerIdentityProvider,
  type PeerResolutionOptions,
  type Protocol,
} from "@query-farm/vgi-rpc";
import { arrowStateSerializer } from "../protocol/state-serializer.js";
import { buildVgiProtocol, type ProtocolConfig } from "../protocol/dispatch.js";
import { createLandingRoutes, type LandingInfo } from "./landing.js";
import { buildRpcServer, type HostingOptions } from "../rpc-server.js";
import { setRequestAuthScope, type RequestAuthHolder } from "../request-auth.js";
import { checkReservedAttachOptions } from "../catalog/attach-option.js";
import type { BlobVendorHttpConfig } from "./blob-vendor.js";
import { OrchardHostedConfigError, MIN_SIGNING_KEY_BYTES, type HostedConfig } from "../hosted.js";
import { createHostedGate, isHostedAuthenticate, type HostedGate, type HostedVerifierDeps } from "./hosted-auth.js";

/**
 * The external-location fields ({@link BlobVendorHttpConfig}) are forwarded to
 * vgi-rpc's handler: spread `blobVendorHttpConfigFromEnv(env)` in to serve
 * through the blob vendor (results over the threshold become pointers, large
 * requests arrive through vended upload URLs).
 */
export interface VgiFetchOptions extends HostingOptions, BlobVendorHttpConfig {
  /** Wire-protocol config (registry + catalogInterface). */
  protocol: ProtocolConfig;
  /** HMAC key for state-token signing. Pass a stable, secret 32-byte key
   *  (e.g. derived from a Wrangler secret). Required because Workers don't
   *  preserve in-memory state across requests/instances. */
  signingKey: Uint8Array;
  /** Whether {@link signingKey} came from explicit configuration (a secret,
   *  `VGI_SIGNING_KEY`) rather than being generated for this process. Only a
   *  configured key hosts `vgi.attach_tickets.v1` (with the ability to issue
   *  grants): tickets sealed under a generated key would die on restart.
   *  Defaults to `true`, since this factory requires the key to be passed. */
  signingKeyConfigured?: boolean | undefined;
  /**
   * Input key material for sealed framework storage payloads. Pass the
   * configured `VGI_SIGNING_KEY` *string* (its UTF-8 bytes are the cross-SDK
   * ikm, so Python, Java and TypeScript workers of one deployment agree), or
   * bytes to use unchanged. Defaults to {@link signingKey}'s bytes.
   */
  storageSealKey?: Uint8Array | string | undefined;
  /** State-token TTL in seconds (default 21600, 6 h: vgi-rpc STATE_REF_V1.md). */
  tokenTtl?: number | undefined;
  /**
   * vgi-rpc's cursor rule for a stream state token over its turn budget
   * (STATE_REF_V1.md §9). Defaults to `"error"` (`cursor_too_large`): this SDK
   * carries its init request as an `init` StateRef, so a cursor over budget
   * is a bug to surface, not a size to tolerate.
   */
  cursorRule?: "warn" | "error" | undefined;
  /**
   * The `VGI-Arguments-Ref-Bytes` value advertised when the server vends
   * upload URLs and its storage is a vgi-rpc ObjectStore (protocol 3.4
   * argument references). Defaults to `$VGI_ARGUMENTS_REF_BYTES`, else 4 MiB.
   */
  argumentsRefBytes?: number | undefined;
  /**
   * Environment read for `VGI_ARGUMENTS_REF_BYTES` and the test switch
   * `VGI_TEST_DISABLE_ARGUMENTS_REF` (a Cloudflare Worker passes its `env`).
   * Defaults to `process.env` where there is one.
   */
  env?: Readonly<Record<string, string | undefined>> | undefined;
  /** URL path prefix for VGI requests (default "/vgi"). Pass "" to mount at
   *  the root, which is what a dedicated single-worker HTTP server wants. */
  prefix?: string | undefined;
  /** Server ID for state-token attribution (default "vgi-cf"). */
  serverId?: string | undefined;
  /** CORS allowed origins, added to all responses so the preflight `OPTIONS`
   *  that browser clients send before `__describe__` succeeds.
   *
   *  **Defaults to `"*"`.** A VGI worker exists to be attached and explored, and
   *  the hosted Cupola UI reaches it from another origin, so cross-origin is the
   *  normal case rather than the exception — and a worker that omits this gets
   *  no CORS headers at all, which fails only in a browser and only for someone
   *  else's page, i.e. nowhere its author is looking. `serveVgiWorker` has
   *  always defaulted this way; this matches it so both entries agree.
   *
   *  Pass `null` to disable CORS. */
  corsOrigins?: string | null | undefined;
  /** Public source-repository URL, surfaced on the landing page. */
  repositoryUrl?: string | undefined;
  /** Worker identity for the standardized VGI landing surface: `GET /` serves
   *  the shared landing.html plus a JSON status document carrying this
   *  identity, and `GET /vgi-client.js` serves the browser client build the
   *  page reads the catalog with.
   *
   *  Required, and deliberately so. It used to be optional, and omitting it
   *  silently downgraded `GET /` to vgi-rpc's generic "this is an RPC
   *  endpoint" placeholder while `GET /vgi-client.js` 404'd — a worker that
   *  looked deployed but had no catalog tree, no Cupola link, and no ATTACH
   *  snippet. Nothing failed loudly, so the only way to notice was to open the
   *  page. `serveVgiWorker` never had the problem because it builds this from
   *  its required name/doc/version; the CF entry had to remember, and
   *  vgi-open-meteo shipped for weeks without it. */
  landingInfo: LandingInfo;
  /** Authenticates each incoming request, returning the caller's
   *  {@link AuthContext}. Forwarded verbatim to `createHttpHandler`; omit it
   *  and every caller is anonymous, which is what this package did before the
   *  option existed.
   *
   *  Supplying it is what makes a worker able to serve more than one principal,
   *  and the identity it returns is the one the worker seals `attach_opaque_data`
   *  and split tokens under. */
  authenticate?: AuthenticateFn | undefined;
  /** Off-wire transport identities, including a trusted Iroh HTTP bridge. */
  peerIdentityProviders?: readonly PeerIdentityProvider[] | undefined;
  /** Policy composing peer evidence with application authentication. */
  peerAuthenticationPolicy?: PeerAuthenticationPolicy | undefined;
  /** Host adapter supplying the physical peer and multiplicity-preserving headers. */
  peerResolutionContext?: ((request: Request) => PeerResolutionOptions | Promise<PeerResolutionOptions>) | undefined;
  /**
   * Orchard hosted mode: accept only Orchard Passes (RFC 9068 JWT access
   * tokens) on every request except `HEAD /health` and CORS preflights. Pass
   * `orchardHostedFromEnv(env)`, which is `undefined` unless
   * `VGI_ORCHARD_HOSTED` is on. `serveVgiWorker` / `createVgiWorkerFetch` set
   * it from `process.env` themselves.
   *
   * Construction throws `OrchardHostedConfigError` when any other way in is
   * configured (`authenticate`, `grantKeys`, `resolveToken`, `mintGrant`, peer
   * identity) or the signing key is not a configured one. CORS is forced open
   * (`*`): the only credential is the bearer Pass.
   */
  orchardHosted?: HostedConfig | undefined;
  /** Test seam for hosted mode's JWKS fetch and clock. */
  orchardHostedDeps?: HostedVerifierDeps | undefined;
}

/**
 * Refuse a `createVgiFetch` configuration that admits a caller without a Pass.
 * Called by `createVgiFetch` whenever `orchardHosted` is set; throws
 * {@link OrchardHostedConfigError}.
 */
export function checkHostedFetchOptions(opts: VgiFetchOptions): void {
  if (opts.authenticate !== undefined && !isHostedAuthenticate(opts.authenticate)) {
    throw new OrchardHostedConfigError(
      "a custom authenticate callback is configured; hosted workers verify Passes only.",
    );
  }
  if (opts.grantKeys !== undefined && opts.grantKeys !== null) {
    throw new OrchardHostedConfigError("sealed-grant keys (--grant-key / VGI_RPC_GRANT_KEYS / grantKeys) are configured.");
  }
  if (opts.resolveToken !== undefined) {
    throw new OrchardHostedConfigError("the worker supplies a resolveToken hook (resolve_token); hosted workers accept Passes only.");
  }
  if (opts.mintGrant !== undefined) {
    throw new OrchardHostedConfigError("the worker supplies a mintGrant hook (mint_grant / issue_grant); hosted workers accept Passes only.");
  }
  if (
    (opts.peerIdentityProviders?.length ?? 0) > 0 ||
    opts.peerAuthenticationPolicy !== undefined ||
    opts.peerResolutionContext !== undefined
  ) {
    throw new OrchardHostedConfigError("peer identity (Iroh, Tailscale, ...) is configured; hosted workers accept Passes only.");
  }
  if (opts.signingKeyConfigured === false) {
    throw new OrchardHostedConfigError(
      "VGI_SIGNING_KEY is not set; hosted workers need a stable key shared by every machine of the " +
        "deployment, or every restart breaks attached sessions.",
    );
  }
  if (opts.signingKey.byteLength < MIN_SIGNING_KEY_BYTES) {
    throw new OrchardHostedConfigError(`VGI_SIGNING_KEY must be at least ${MIN_SIGNING_KEY_BYTES} bytes.`);
  }
}

function hostedDebugEnabled(): boolean {
  const flag = (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env?.VGI_DEBUG;
  return flag !== undefined && flag !== "" && flag !== "0";
}

// ---------------------------------------------------------------------------
// Request-scoped caller identity
//
// `@query-farm/vgi-rpc` types stream initialization as `(params) => state` —
// no CallContext — on both dispatchers, so the `init` handler cannot be handed
// the principal as an argument the way every unary handler is. But `init` is
// exactly where the identity-bound envelopes minted by `catalog_attach` and
// `table_function_plan` (both unary, both sealed under the REAL caller) are
// opened. Opening them under the anonymous identity works for anonymous
// callers and fails for everyone else.
//
// So the identity travels the way vgi-python's `current_auth()` ContextVar
// does: an ambient per-request scope. `AsyncLocalStorage.run` wraps the whole
// handler call, and a dispatch hook fills the holder in with whatever identity
// the framework actually dispatched under — the hook is read AFTER
// authentication, the PKCE cookie chain and bearer introspection have all had
// their say, so it reports the final principal rather than re-deriving one.
//
// A mutable holder rather than `enterWith`: `run` is the only form Cloudflare
// Workers supports, and the identity is not known until authentication has run
// inside the handler.
// ---------------------------------------------------------------------------

/** The `AsyncLocalStorage` surface used here, structurally typed so this file
 *  compiles where `node:async_hooks` has no types. */
interface AsyncLocalStorageLike<T> {
  run<R>(store: T, fn: () => R): R;
  getStore(): T | undefined;
}

let requestAuthStorage: AsyncLocalStorageLike<RequestAuthHolder> | null | undefined;
let requestAuthStoragePending: Promise<AsyncLocalStorageLike<RequestAuthHolder> | null> | undefined;

/**
 * Resolve (once) the AsyncLocalStorage this process scopes requests with.
 *
 * `node:async_hooks` is loaded dynamically and defensively: it exists on Node
 * and Bun, and on Cloudflare Workers only with the `nodejs_compat` /
 * `nodejs_als` compatibility flag. Where it is missing this resolves to `null`
 * and the worker keeps serving exactly as it did before — anonymous callers
 * unaffected, authenticated ones still unable to open their sealed attach.
 * Degrading is the right failure here: refusing to start would break every
 * anonymous deployment to fix a case it does not have.
 */
function ensureRequestAuthStorage(): Promise<AsyncLocalStorageLike<RequestAuthHolder> | null> {
  if (requestAuthStorage !== undefined) return Promise.resolve(requestAuthStorage);
  requestAuthStoragePending ??= import("node:async_hooks")
    .then((mod) => {
      const storage = new mod.AsyncLocalStorage<RequestAuthHolder>() as AsyncLocalStorageLike<RequestAuthHolder>;
      requestAuthStorage = storage;
      setRequestAuthScope(storage);
      return storage;
    })
    .catch(() => {
      requestAuthStorage = null;
      return null;
    });
  return requestAuthStoragePending;
}

/**
 * Dispatch hook that records the dispatching principal on the open scope.
 *
 * Reads the identity off `DispatchInfo`, which the framework populates from the
 * resolved auth context for every method — so this sees the same principal the
 * access log and the unary handlers see, without this package having to know
 * how authentication was configured.
 */
const REQUEST_AUTH_DISPATCH_HOOK: DispatchHook = {
  onDispatchStart(info: DispatchInfo): HookToken {
    const holder = requestAuthStorage?.getStore();
    if (holder) {
      holder.auth = info.authenticated
        ? new AuthContext(info.authDomain ?? "", true, info.principal ?? "", info.claims ?? {})
        : AuthContext.anonymous();
    }
    return undefined;
  },
  onDispatchEnd() {
    // Nothing to record; the scope ends when the request does.
  },
};

/** Default state-token TTL: 6 h (vgi-rpc STATE_REF_V1.md, protocol 3.4). */
export const DEFAULT_TOKEN_TTL = 21600;

/**
 * Add `VGI-Arguments-Ref-Bytes` to every response, next to vgi-rpc's own
 * capability headers (which it sets on every response, capability discovery
 * included), and expose it to browsers alongside them.
 */
function withArgumentsRefHeader(
  handler: (req: Request) => Promise<Response> | Response,
  value: number,
): (req: Request) => Promise<Response> {
  const text = String(value);
  return async (req: Request) => {
    const response = await handler(req);
    try {
      response.headers.set(ARGUMENTS_REF_BYTES_HEADER, text);
      return addExposed(response);
    } catch {
      // Immutable headers (a fetched Response): rebuild around the same body.
      const copy = new Response(response.body, response);
      copy.headers.set(ARGUMENTS_REF_BYTES_HEADER, text);
      return addExposed(copy);
    }
  };
}

function addExposed(response: Response): Response {
  const exposed = response.headers.get("Access-Control-Expose-Headers");
  if (exposed && exposed !== "*" && !exposed.toLowerCase().includes(ARGUMENTS_REF_BYTES_HEADER.toLowerCase())) {
    response.headers.set("Access-Control-Expose-Headers", `${exposed}, ${ARGUMENTS_REF_BYTES_HEADER}`);
  }
  return response;
}

/**
 * Build a fetch handler suitable for `export default { fetch }` in a CF
 * Worker module, or for `Bun.serve({ fetch })`. The returned handler is
 * async-safe across Workers' isolate-per-request execution model — all state
 * round-trips through the signed state token created here.
 */
export function createVgiFetch(opts: VgiFetchOptions): (req: Request) => Promise<Response> {
  // The type makes this required, but a plain-JS caller (or one that predates
  // the change) reaches here with it undefined, and the failure mode this
  // guards is silent: a worker that serves a placeholder page forever. Fail
  // at construction, where the stack points at the caller.
  if (!opts.landingInfo) {
    throw new Error(
      "createVgiFetch requires `landingInfo` ({ name, doc, version }). Without it " +
        "GET / serves vgi-rpc's generic placeholder instead of the VGI landing page " +
        "and GET /vgi-client.js 404s.",
    );
  }
  // Orchard hosted mode: refuse every other way in, then put the Pass gate in
  // front of the handler and hand the handler an authenticate that returns the
  // identity the gate verified.
  let gate: HostedGate | undefined;
  if (opts.orchardHosted) {
    checkHostedFetchOptions(opts);
    const debug = hostedDebugEnabled() ? (m: string) => console.error(m) : undefined;
    gate = createHostedGate(opts.orchardHosted, { debug, ...opts.orchardHostedDeps });
  }
  const authenticate = gate ? gate.authenticate : opts.authenticate;
  const tokenTtl = opts.tokenTtl ?? DEFAULT_TOKEN_TTL;
  // Protocol 3.4: argument references are on whenever the server can mint and
  // resolve them, and the header is the mandatory signal that they are.
  const argumentsRefBytes = argumentsRefHeaderValue({
    uploadUrlProvider: opts.uploadUrlProvider,
    externalLocation: opts.externalLocation,
    explicit: opts.argumentsRefBytes,
    ...(opts.env ? { env: opts.env } : {}),
  });
  // `?? "/vgi"` and not `|| "/vgi"`: an explicit "" means "mount at root".
  const prefix = opts.prefix ?? "/vgi";
  const serverId = opts.serverId ?? "vgi-cf";

  const protocol: Protocol = buildVgiProtocol({
    signingKey: opts.signingKey,
    storageSealKey:
      typeof opts.storageSealKey === "string" ? new TextEncoder().encode(opts.storageSealKey) : opts.storageSealKey,
    ...opts.protocol,
  });

  // The same builder every other transport uses: `vgi.v3`, the worker's
  // hostedProtocols(), reflection, and -- HTTP being the transport that
  // authenticates callers -- vgi_rpc.Identity.v2 when the worker opts in.
  // Built at construction, so a worker that opts into introspection without an
  // allowlist fails here rather than serving traffic.
  // Hosted mode never reads sealed-grant keys from the environment (the
  // hosted checks refused them; this keeps a Worker-less process.env out too).
  const server = buildRpcServer(protocol, gate ? { ...opts, grantKeys: null } : opts, {
    transport: "http",
    serverId,
    attachTickets: {
      signingKey: opts.signingKey,
      signingKeyConfigured: opts.signingKeyConfigured ?? true,
      catalogInterface: opts.protocol.catalogInterface,
    },
  });
  // A catalog declaring the reserved `vgi_attach_ticket` attach option must
  // not serve. This factory is synchronous, so the check runs once up front
  // and every request waits on it: a refusal fails each request (and is
  // logged once) rather than letting a ticket be read as an ordinary option.
  const reservedCheck = checkReservedAttachOptions(opts.protocol.catalogInterface);
  reservedCheck.catch((e) => console.error(`VGI worker refused to serve: ${(e as Error).message}`));

  // vgi-rpc's own types disagree under exactOptionalPropertyTypes:
  // `VgiRpcServer.identity` is `IdentityImpl | undefined` while `ProtocolHost`
  // declares `identity?: {...}` without `| undefined`. Both mean "may be
  // absent"; the cast only bridges the declarations.
  const rpcHandler = createHttpHandler(server as Parameters<typeof createHttpHandler>[0], omitUndefined({
    prefix,
    serverId,
    tokenKey: opts.signingKey,
    tokenTtl,
    cursorRule: opts.cursorRule ?? "error",
    stateSerializer: arrowStateSerializer,
    // Records the dispatching principal on the request scope opened below, so
    // stream `init` — the one handler the framework passes no CallContext to —
    // can still open the identity-bound attach and split envelopes.
    dispatchHook: REQUEST_AUTH_DISPATCH_HOOK,
    authenticate,
    // Hosted mode: a Pass is the only bearer; nothing is composed beside it.
    identityBearer: gate ? false : undefined,
    peerIdentityProviders: opts.peerIdentityProviders,
    peerAuthenticationPolicy: opts.peerAuthenticationPolicy,
    peerResolutionContext: opts.peerResolutionContext,
    // `null` is the explicit opt-out; `undefined` (omitted) means "*". Hosted
    // mode is always open: the only credential is the bearer Pass.
    corsOrigins: gate ? "*" : opts.corsOrigins === null ? undefined : (opts.corsOrigins ?? "*"),
    repositoryUrl: opts.repositoryUrl,
    externalLocation: opts.externalLocation,
    uploadUrlProvider: opts.uploadUrlProvider,
    maxRequestBytes: opts.maxRequestBytes,
    maxUploadBytes: opts.maxUploadBytes,
    // The landing surface is ours, contributed into vgi-rpc's routing rather
    // than built there. `enableLandingPage: false` drops vgi-rpc's generic
    // "this is an RPC endpoint" page, which would otherwise sit behind ours for
    // no reason — a VGI worker always has the real thing.
    extraRoutes: createLandingRoutes(opts.landingInfo),
    enableLandingPage: false,
  }));
  // Hosted mode authenticates before vgi-rpc routes anything, so no page,
  // probe or body handling is reachable without a Pass.
  const gated = gate ? gate.wrap(rpcHandler, prefix) : rpcHandler;
  const handler = argumentsRefBytes === null ? gated : withArgumentsRefHeader(gated, argumentsRefBytes);
  return async (req: Request) => {
    await reservedCheck;
    const storage = await ensureRequestAuthStorage();
    // No AsyncLocalStorage on this host: serve exactly as before.
    if (!storage) return handler(req);
    // One holder per request. The dispatch hook fills it in once the framework
    // has resolved the principal; `init` reads it back through
    // `currentRequestAuth()`.
    return storage.run({}, () => handler(req));
  };
}
