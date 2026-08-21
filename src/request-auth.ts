// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Request-scoped caller identity, for the code paths the RPC framework cannot
// hand a CallContext to.
//
// Every unary VGI method receives the caller's `AuthContext` directly — the
// dispatcher passes the OutputCollector, which *is* a CallContext. The stream
// `init` method does not: `@query-farm/vgi-rpc` types stream initialization as
// `(params) => state`, with no context parameter, on both the pipe and HTTP
// dispatchers. That is a framework signature this package cannot change.
//
// It matters because `init` opens two identity-bound AEAD envelopes:
// `attach_opaque_data` (sealed under `attachAad(auth)` by `catalog_attach`) and
// every split token (sealed under `splitTokenAad(body, auth)` by
// `table_function_plan`). Both of those minting sites ARE unary and therefore
// seal under the REAL principal. Opening them under a hard-coded `undefined`
// yields the anonymous identity tail, so the AAD only ever matches for an
// anonymous caller: an authenticated one gets `OpaqueDataRejectedError:
// attach_opaque_data not recognized` on the first scan. Anonymous workers never
// saw it, which is why it survived — the example HTTP workers authenticate
// nobody.
//
// The reference implementations solve this with an ambient, request-scoped
// identity rather than a parameter: vgi-python reads a `ContextVar` via
// `current_auth()`; vgi-go's RPC framework threads a `*CallContext` into
// `handleInit`. This is the TypeScript equivalent — an `AsyncLocalStorage`
// scope opened per HTTP request by `createVgiFetch` (see `http/fetch.ts`),
// read here.
//
// Deliberately a registry rather than a direct `node:async_hooks` import: this
// module is reachable from the protocol handlers, which also run under stdio,
// AF_UNIX and browser builds. Only the HTTP entry points install a scope, and
// they load `node:async_hooks` defensively. With no scope installed
// `currentRequestAuth()` returns `undefined`, which is exactly the previous
// behaviour — and correct, because a worker with no signing key seals nothing.

import type { AuthContext } from "@query-farm/vgi-rpc";

/** Mutable per-request cell. The scope is opened before the identity is known
 *  (authentication happens inside the handler), so the holder is published
 *  first and filled in when the framework reports the principal. */
export interface RequestAuthHolder {
  auth?: AuthContext;
}

/** The slice of `AsyncLocalStorage` this module needs. Structural on purpose:
 *  it keeps `node:async_hooks` out of every non-HTTP build, and lets a test
 *  install a trivial stub. */
export interface RequestAuthScope {
  getStore(): RequestAuthHolder | undefined;
}

let scope: RequestAuthScope | undefined;

/**
 * Publish the scope `currentRequestAuth()` reads. Called once per process by
 * the HTTP entry point; idempotent for the same scope.
 */
export function setRequestAuthScope(next: RequestAuthScope | undefined): void {
  scope = next;
}

/**
 * The authenticated principal of the request being served on this async
 * context, or `undefined` when there is none — no scope installed (stdio /
 * AF_UNIX / a host that has no `AsyncLocalStorage`), or an unauthenticated
 * request.
 *
 * `undefined` and an anonymous `AuthContext` produce the same identity tail, so
 * the two cases need not be distinguished by callers.
 */
export function currentRequestAuth(): AuthContext | undefined {
  return scope?.getStore()?.auth;
}
