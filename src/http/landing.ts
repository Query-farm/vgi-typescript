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
      const headers = new Headers({ "Content-Type": "text/html; charset=utf-8" });
      ctx.addCorsHeaders(headers);
      return new Response(LANDING_HTML_BYTES as unknown as BodyInit, { status: 200, headers });
    }

    // Served by the worker rather than from a CDN: this page is same-origin
    // with an authenticated worker and carries its session cookie, so
    // third-party script here would run with full access to that origin — and a
    // CDN dependency would break air-gapped deployments that today need nothing
    // but the worker.
    if (path === `${prefix}/vgi-client.js`) {
      const headers = new Headers({
        "Content-Type": "text/javascript; charset=utf-8",
        "Cache-Control": "public, max-age=3600",
      });
      ctx.addCorsHeaders(headers);
      return new Response(CLIENT_BUNDLE_BYTES as unknown as BodyInit, { status: 200, headers });
    }

    return null;
  };
}
