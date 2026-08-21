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

import {
  AuthContext,
  createHttpHandler,
  unpackStateToken,
  type AuthenticateFn,
  type DispatchHook,
  type DispatchInfo,
  type HookToken,
  type Protocol,
} from "@query-farm/vgi-rpc";
import { arrowStateSerializer } from "../protocol/state-serializer.js";
import { buildVgiProtocol, type ProtocolConfig } from "../protocol/dispatch.js";
import { createLandingRoutes, type LandingInfo } from "./landing.js";
import { setRequestAuthScope, type RequestAuthHolder } from "../request-auth.js";

export interface VgiFetchOptions {
  /** Wire-protocol config (registry + catalogInterface). */
  protocol: Omit<ProtocolConfig, "recoverExchangeState">;
  /** HMAC key for state-token signing. Pass a stable, secret 32-byte key
   *  (e.g. derived from a Wrangler secret). Required because Workers don't
   *  preserve in-memory state across requests/instances. */
  signingKey: Uint8Array;
  /** State-token TTL in seconds (default 3600). */
  tokenTtl?: number;
  /** URL path prefix for VGI requests (default "/vgi"). Pass "" to mount at
   *  the root, which is what a dedicated single-worker HTTP server wants. */
  prefix?: string;
  /** Server ID for state-token attribution (default "vgi-cf"). */
  serverId?: string;
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
  corsOrigins?: string | null;
  /** Public source-repository URL, surfaced on the landing page. */
  repositoryUrl?: string;
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
  authenticate?: AuthenticateFn;
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
  const tokenTtl = opts.tokenTtl ?? 3600;
  // `?? "/vgi"` and not `|| "/vgi"`: an explicit "" means "mount at root".
  const prefix = opts.prefix ?? "/vgi";
  const serverId = opts.serverId ?? "vgi-cf";

  const protocol: Protocol = buildVgiProtocol({
    signingKey: opts.signingKey,
    ...opts.protocol,
    recoverExchangeState: async (opaqueData: Uint8Array) => {
      const tokenString = new TextDecoder().decode(opaqueData);
      // principal binding is enforced by the HTTP handler on the request that
      // carried this token; the recovery path itself is not principal-scoped.
      const unpacked = await unpackStateToken(tokenString, opts.signingKey, tokenTtl, undefined);
      return arrowStateSerializer.deserialize(unpacked.stateBytes);
    },
  });

  const handler = createHttpHandler(protocol, {
    prefix,
    serverId,
    tokenKey: opts.signingKey,
    tokenTtl,
    stateSerializer: arrowStateSerializer,
    // Records the dispatching principal on the request scope opened below, so
    // stream `init` — the one handler the framework passes no CallContext to —
    // can still open the identity-bound attach and split envelopes.
    dispatchHook: REQUEST_AUTH_DISPATCH_HOOK,
    authenticate: opts.authenticate,
    // `null` is the explicit opt-out; `undefined` (omitted) means "*".
    corsOrigins: opts.corsOrigins === null ? undefined : (opts.corsOrigins ?? "*"),
    repositoryUrl: opts.repositoryUrl,
    // The landing surface is ours, contributed into vgi-rpc's routing rather
    // than built there. `enableLandingPage: false` drops vgi-rpc's generic
    // "this is an RPC endpoint" page, which would otherwise sit behind ours for
    // no reason — a VGI worker always has the real thing.
    extraRoutes: createLandingRoutes(opts.landingInfo),
    enableLandingPage: false,
  });
  return async (req: Request) => {
    const storage = await ensureRequestAuthStorage();
    // No AsyncLocalStorage on this host: serve exactly as before.
    if (!storage) return handler(req);
    // One holder per request. The dispatch hook fills it in once the framework
    // has resolved the principal; `init` reads it back through
    // `currentRequestAuth()`.
    return storage.run({}, () => handler(req));
  };
}
