// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// The standardized VGI landing surface.
//
// This lives here, not in `@query-farm/vgi-rpc`, because everything it serves
// is VGI vocabulary: a page that renders catalogs, schemas, tables, functions
// and macros, and the browser build of *this package's* client that reads them
// over the protocol. vgi-rpc is generic RPC over Arrow and depends on nothing
// here — yet it used to vendor both assets, which put a compiled artifact of
// the higher layer inside the lower one and added ~900 KB to every consumer of
// vgi-rpc, whether or not they served a VGI page. vgi-python already drew the
// line this way: its landing surface is in `vgi/http/`, and `vgi_rpc/http/` has
// neither asset.
//
// It reaches the request through vgi-rpc's `extraRoutes` hook rather than by
// wrapping the returned fetch handler, because ordering matters. Contributed
// routes run at the same pipeline position the surface occupied when it lived
// inside vgi-rpc — after the OAuth browser-redirect branch, and before the
// generic landing page and the 404. Wrapping from outside would answer ahead of
// that redirect, so a worker with OAuth configured would show this page to a
// browser that should have been sent to its identity provider first.
//
// That redirect is the only thing guarding these two routes, and only for
// browsers: vgi-rpc gates GET pages on `authenticate && pkceConfig`, and a
// non-browser caller that fails auth falls through to normal page serving. So
// on a worker with no PKCE configured, the page and the client bundle are
// public — as the generic landing page always was. Worth knowing before
// treating anything on this surface as private; vgi-rpc-python is stricter and
// answers 401 here.

import type { ExtraRouteContext, ExtraRouteHandler } from "@query-farm/vgi-rpc";
import { LANDING_HTML_BYTES } from "./landing-html.js";
import { CLIENT_BUNDLE_BYTES } from "./client-bundle.js";

/**
 * Worker identity for the standardized VGI landing surface.
 *
 * The shared `landing.html` reads catalog metadata by speaking the VGI protocol
 * through the client bundle the worker serves beside it, so nothing about the
 * catalog belongs here. What the protocol has no method for — which worker this
 * is, what it is called, what version it runs — rides on the JSON status
 * document at `GET {prefix}/?format=json`.
 */
export interface LandingInfo {
  /** Worker name shown as the page heading, e.g. "ishares". */
  name: string;
  /** One-line description shown under the heading. */
  doc?: string;
  /** Worker version string shown in the footer. */
  version?: string;
  /** Override the Cupola base URL the "Explore" links point at. */
  cupolaBase?: string;
}

/** Default Cupola deployment the page's "Explore" links point at. */
export const DEFAULT_CUPOLA_BASE = "https://cupola.query-farm.services";

// Caching. Both assets are immutable for a given build but live at URLs that
// never change, which is the combination that goes wrong quietly: the page used
// to be served with no Cache-Control and no validator at all, so browsers
// applied heuristic caching with nothing to revalidate against and could hold a
// stale page indefinitely — every landing-page fix needed a shift-reload to
// see. The bundle was worse in a different way: `max-age=3600` with no
// validator meant a full hour where a client could not learn that a release had
// happened, which is how a bundle that could not decode workerd's compressed
// responses stayed live in a browser long after it was fixed.
//
// `no-cache` does not mean "do not store" — it means "store, but revalidate
// before reuse". With a strong ETag a repeat visit is a conditional GET that
// answers 304 with no body, so the bandwidth is the same as a long TTL and the
// staleness window closes to zero. `public` is kept so shared caches (the
// Cloudflare edge in front of a Workers deployment) can hold and revalidate it
// too, rather than each viewer paying the full transfer.
const REVALIDATE = "public, no-cache";

/**
 * FNV-1a over the asset bytes, as a strong ETag.
 *
 * Computed lazily and memoised rather than at module load: most requests to a
 * worker are RPC and never touch these routes, and hashing ~700 KB on every
 * isolate start would tax cold starts for a page nobody may open. Once per
 * isolate on first use is the right trade.
 */
function etagFor(bytes: Uint8Array): string {
  let hash = 0x811c9dc5;
  for (let i = 0; i < bytes.length; i++) {
    hash ^= bytes[i]!;
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  // Length is folded in so two builds that collide on the hash still differ.
  return `"${hash.toString(16)}-${bytes.length.toString(16)}"`;
}

let landingEtag: string | null = null;
let bundleEtag: string | null = null;

/** 304 when the client already holds this exact body, else null. */
function notModified(request: Request, etag: string, extra?: HeadersInit): Response | null {
  const ifNoneMatch = request.headers.get("If-None-Match");
  if (!ifNoneMatch) return null;
  // A conditional GET may list several validators, and a cache is allowed to
  // weaken ours to `W/"…"` on the way back.
  const matches = ifNoneMatch
    .split(",")
    .map((c) => c.trim().replace(/^W\//, ""))
    .includes(etag);
  if (!matches) return null;
  const headers = new Headers(extra);
  headers.set("ETag", etag);
  headers.set("Cache-Control", REVALIDATE);
  return new Response(null, { status: 304, headers });
}

/**
 * Build the VGI landing surface as a route contributed to vgi-rpc's HTTP
 * handler.
 *
 * Serves two paths, both relative to the handler's mount prefix:
 *
 * - `GET {prefix}/` — the shared `landing.html` for browsers, or a JSON status
 *   document for health checks, `?format=json`, and the page's own identity
 *   read. Content negotiation matches vgi-python's `LandingPageResource`.
 * - `GET {prefix}/vgi-client.js` — the browser build the page imports.
 *
 * Anything else returns `null`, which lets normal routing continue.
 */
export function createLandingRoutes(info: LandingInfo): ExtraRouteHandler {
  return (request: Request, ctx: ExtraRouteContext): Response | null => {
    const path = ctx.url.pathname;
    const { prefix } = ctx;

    if (path === prefix || path === `${prefix}/`) {
      const accept = request.headers.get("Accept") ?? "";
      const wantJson =
        ctx.url.searchParams.get("format") === "json" ||
        (accept.includes("application/json") && !accept.includes("text/html"));
      if (wantJson) {
        // Rebuilt per request rather than cached: `oauthActive` and `serverId`
        // come from the handler, so this cannot be pre-rendered at construction
        // time the way it was when it lived inside vgi-rpc.
        const body = JSON.stringify({
          status: "ok",
          server_id: ctx.serverId,
          protocol: "vgi",
          worker: info.name,
          doc: info.doc ?? "",
          version: info.version ?? "",
          lang: "typescript",
          oauth: ctx.oauthActive,
          cupola_base: info.cupolaBase ?? DEFAULT_CUPOLA_BASE,
        });
        const headers = new Headers({ "Content-Type": "application/json" });
        ctx.addCorsHeaders(headers);
        return new Response(body, { status: 200, headers });
      }
      landingEtag ??= etagFor(LANDING_HTML_BYTES);
      const conditional = notModified(request, landingEtag);
      if (conditional) {
        ctx.addCorsHeaders(conditional.headers);
        return conditional;
      }
      const headers = new Headers({
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": REVALIDATE,
        ETag: landingEtag,
      });
      ctx.addCorsHeaders(headers);
      return new Response(LANDING_HTML_BYTES as unknown as BodyInit, { status: 200, headers });
    }

    // Served by the worker rather than from a CDN: this page is same-origin
    // with an authenticated worker and carries its session cookie, so
    // third-party script here would run with full access to that origin — and a
    // CDN dependency would break air-gapped deployments that today need nothing
    // but the worker.
    if (path === `${prefix}/vgi-client.js`) {
      bundleEtag ??= etagFor(CLIENT_BUNDLE_BYTES);
      const conditional = notModified(request, bundleEtag);
      if (conditional) {
        ctx.addCorsHeaders(conditional.headers);
        return conditional;
      }
      const headers = new Headers({
        "Content-Type": "text/javascript; charset=utf-8",
        "Cache-Control": REVALIDATE,
        ETag: bundleEtag,
      });
      ctx.addCorsHeaders(headers);
      return new Response(CLIENT_BUNDLE_BYTES as unknown as BodyInit, { status: 200, headers });
    }

    return null;
  };
}
