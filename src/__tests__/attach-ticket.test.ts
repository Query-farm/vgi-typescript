// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Attach tickets against the cross-SDK vectors (vgi-python's
// attach_ticket_vectors.json, copied verbatim), plus seal_attach validation,
// the reserved option name and the hosting conditions.

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { AuthContext, GrantKeys, Protocol } from "@query-farm/vgi-rpc";
import { Utf8 } from "@query-farm/apache-arrow";
import {
  ATTACH_TICKETS_PROTOCOL_NAME,
  attachTicketAad,
  encodeAttachTicketPayload,
  mintAttachTicket,
  openAttachTicket,
  redeemAttachTicket,
  sealAttach,
  type AttachTicketsConfig,
} from "../attach-ticket.js";
import {
  checkReservedAttachOptions,
  ReservedAttachOptionError,
  serializeAttachOptionSpec,
  serializeAttachOptionSpecs,
  validateRequiredAttachOptions,
} from "../catalog/attach-option.js";
import { serializeAttachOptions } from "../catalog/attach-options.js";
import { buildRpcServer, type ServerTransport } from "../rpc-server.js";
import { batchFromColumns, serializeBatch } from "../util/arrow/index.js";
import { schema, field, utf8 } from "../arrow/index.js";

// Read as UTF-8 explicitly: the vectors carry a non-ASCII principal.
const V = JSON.parse(readFileSync(join(import.meta.dir, "data", "attach_ticket_vectors.json"), "utf8"));
const DEFAULT_KEY: string = V.defaults.signing_key_b64;
const DEFAULT_NOW: number = V.defaults.now;

const b64 = (s: string): Uint8Array => new Uint8Array(Buffer.from(s, "base64"));
const hex = (s: string): Uint8Array => new Uint8Array(Buffer.from(s, "hex"));
const toHex = (b: Uint8Array): string => Buffer.from(b).toString("hex");
const toB64 = (b: Uint8Array): string => Buffer.from(b).toString("base64");

function auth(principal: string): AuthContext {
  return principal ? new AuthContext("grant", true, principal) : AuthContext.anonymous();
}

async function kindOf(p: Promise<unknown>): Promise<string> {
  try {
    await p;
    return "accepted";
  } catch (e) {
    return (e as { errorKind?: string }).errorKind ?? `untyped: ${(e as Error).message}`;
  }
}

describe("vectors: mint", () => {
  for (const c of V.mint) {
    test(c.name, async () => {
      const key = b64(c.signing_key_b64 ?? DEFAULT_KEY);
      expect(toHex(attachTicketAad(c.principal))).toBe(c.aad_hex);
      const claims = {
        issuedAt: c.issued_at,
        expiresAt: c.expires_at,
        ticketId: c.ticket_id,
        catalogName: c.catalog_name,
        dataVersionSpec: c.data_version_spec,
        implementationVersion: c.implementation_version,
        optionsIpc: b64(c.options_ipc_b64),
      };
      expect(toHex(encodeAttachTicketPayload(claims))).toBe(c.payload_hex);
      const { token } = await mintAttachTicket(key, {
        principal: c.principal,
        catalogName: c.catalog_name,
        optionsIpc: claims.optionsIpc,
        dataVersionSpec: c.data_version_spec,
        implementationVersion: c.implementation_version,
        issuedAt: c.issued_at,
        expiresAt: c.expires_at,
        ticketId: c.ticket_id,
        nonce: hex(c.nonce_hex),
      });
      expect(token).toBe(c.token);
    });
  }
});

describe("vectors: accept", () => {
  for (const c of V.accept) {
    test(c.name, async () => {
      const claims = await openAttachTicket(b64(c.signing_key_b64 ?? DEFAULT_KEY), c.token, c.principal, c.now ?? DEFAULT_NOW);
      expect({
        issued_at: claims.issuedAt,
        expires_at: claims.expiresAt,
        ticket_id: claims.ticketId,
        catalog_name: claims.catalogName,
        data_version_spec: claims.dataVersionSpec,
        implementation_version: claims.implementationVersion,
        options_ipc_b64: toB64(claims.optionsIpc),
      }).toEqual(c.claims);
    });
  }
});

describe("vectors: reject", () => {
  for (const c of V.reject) {
    test(c.name, async () => {
      const kind = await kindOf(
        openAttachTicket(b64(c.signing_key_b64 ?? DEFAULT_KEY), c.token, c.principal, c.now ?? DEFAULT_NOW),
      );
      expect(kind).toBe(c.error_kind);
    });
  }
});

describe("vectors: redeem", () => {
  for (const c of V.redeem) {
    test(c.name, async () => {
      const run = redeemAttachTicket(c.options, b64(c.signing_key_b64 ?? DEFAULT_KEY), auth(c.principal), c.now ?? DEFAULT_NOW);
      if (c.error_kind) {
        expect(await kindOf(run)).toBe(c.error_kind);
        return;
      }
      const restored = await run;
      if (c.result === null) {
        expect(restored).toBeNull();
        return;
      }
      expect(restored).not.toBeNull();
      expect({
        catalog_name: restored!.name,
        data_version_spec: restored!.dataVersionSpec,
        implementation_version: restored!.implementationVersion,
        options: restored!.options,
      }).toEqual(c.result);
    });
  }

  test("a worker without a signing key never opens a ticket", async () => {
    const ticket = V.redeem[0].options.vgi_attach_ticket;
    expect(await kindOf(redeemAttachTicket({ vgi_attach_ticket: ticket }, undefined, auth("alice"), DEFAULT_NOW))).toBe(
      "attach_ticket_invalid",
    );
  });

  test("a non-string ticket is invalid", async () => {
    expect(await kindOf(redeemAttachTicket({ vgi_attach_ticket: 7 }, b64(DEFAULT_KEY), auth("alice")))).toBe(
      "attach_ticket_invalid",
    );
  });

  test("a ticket sealed under the login domain opens under the grant domain", async () => {
    const key = b64(DEFAULT_KEY);
    const { token } = await mintAttachTicket(key, {
      principal: "alice",
      catalogName: "ticket_probe",
      optionsIpc: new Uint8Array(0),
      issuedAt: DEFAULT_NOW,
      expiresAt: 0,
    });
    const asLogin = new AuthContext("jwt", true, "alice");
    const asGrant = new AuthContext("grant", true, "alice");
    expect((await redeemAttachTicket({ vgi_attach_ticket: token }, key, asLogin))?.name).toBe("ticket_probe");
    expect((await redeemAttachTicket({ vgi_attach_ticket: token }, key, asGrant))?.name).toBe("ticket_probe");
  });
});

// ---------------------------------------------------------------------------
// seal_attach
// ---------------------------------------------------------------------------

const SPECS = [
  { name: "region", description: "r", type: new Utf8(), default: "us-east-1" },
  { name: "api_key", description: "k", type: new Utf8(), required: true, secret: true },
];

const fakeCatalog = {
  catalogs: () => ["ticket_probe"],
  catalogsInfo: () => [
    {
      name: "ticket_probe",
      implementation_version: null,
      data_version_spec: null,
      attach_option_specs: serializeAttachOptionSpecs(SPECS),
    },
  ],
} as never;

function sealConfig(maxTtlSeconds: number | null, now = DEFAULT_NOW): AttachTicketsConfig {
  return { signingKey: b64(DEFAULT_KEY), catalogInterface: fakeCatalog, maxTtlSeconds, now: () => now };
}

function request(options: Record<string, string> | null, extra: Record<string, unknown> = {}) {
  return {
    catalog_name: "ticket_probe",
    options: options ? serializeAttachOptions(options) : null,
    data_version_spec: "",
    implementation_version: "",
    ttl_seconds: 0,
    ...extra,
  };
}

async function violations(p: Promise<unknown>): Promise<string[]> {
  try {
    await p;
    return [];
  } catch (e) {
    const err = e as { errorKind?: string; errorDetails?: Array<{ field_violations?: Array<{ field: string }> }> };
    expect(err.errorKind).toBe("invalid_request");
    return (err.errorDetails?.[0]?.field_violations ?? []).map((v) => v.field);
  }
}

describe("seal_attach", () => {
  test("seals the caller's options, which redeem back for the same principal", async () => {
    const sealed = await sealAttach(sealConfig(3600), request({ region: "eu-west-2", api_key: "sk" }), auth("alice"));
    expect(sealed.ticket.startsWith("vgia1.")).toBe(true);
    expect(sealed.expires_at).toBe(DEFAULT_NOW + 3600);
    const restored = await redeemAttachTicket({ vgi_attach_ticket: sealed.ticket }, b64(DEFAULT_KEY), auth("alice"), DEFAULT_NOW);
    expect(restored?.options).toEqual({ region: "eu-west-2", api_key: "sk" });
    expect(await kindOf(redeemAttachTicket({ vgi_attach_ticket: sealed.ticket }, b64(DEFAULT_KEY), auth("bob"), DEFAULT_NOW))).toBe(
      "attach_ticket_invalid",
    );
  });

  test("anonymous is action_denied", async () => {
    expect(await kindOf(sealAttach(sealConfig(null), request({ api_key: "sk" }), AuthContext.anonymous()))).toBe(
      "action_denied",
    );
  });

  test("lifetime: 0 is the ceiling, a request is capped, no ceiling is +inf or the request", async () => {
    const a = auth("alice");
    expect((await sealAttach(sealConfig(600), request({ api_key: "k" }), a)).expires_at).toBe(DEFAULT_NOW + 600);
    expect((await sealAttach(sealConfig(600), request({ api_key: "k" }, { ttl_seconds: 60 }), a)).expires_at).toBe(DEFAULT_NOW + 60);
    expect((await sealAttach(sealConfig(600), request({ api_key: "k" }, { ttl_seconds: 6000 }), a)).expires_at).toBe(DEFAULT_NOW + 600);
    expect((await sealAttach(sealConfig(null), request({ api_key: "k" }), a)).expires_at).toBe(Number.POSITIVE_INFINITY);
    expect((await sealAttach(sealConfig(null), request({ api_key: "k" }, { ttl_seconds: 90 }), a)).expires_at).toBe(DEFAULT_NOW + 90);
  });

  test("validation reports every violation together", async () => {
    const a = auth("alice");
    expect(await violations(sealAttach(sealConfig(null), request({ api_key: "k" }, { ttl_seconds: -1 }), a))).toEqual([
      "ttl_seconds",
    ]);
    expect(await violations(sealAttach(sealConfig(null), request({ api_key: "k" }, { catalog_name: "nope" }), a))).toEqual([
      "catalog_name",
    ]);
    expect(await violations(sealAttach(sealConfig(null), request({ region: "x" }), a))).toEqual(["options.api_key"]);
    expect(
      await violations(sealAttach(sealConfig(null), request({ api_key: "k", bogus: "x", VGI_ATTACH_TICKET: "t" }), a)),
    ).toEqual(["options.bogus", "options.VGI_ATTACH_TICKET"]);
    const two = batchFromColumns(
      { api_key: ["a", "b"] },
      schema([field("api_key", utf8(), true)]),
    );
    expect(await violations(sealAttach(sealConfig(null), { ...request(null), options: serializeBatch(two) }, a))).toEqual([
      "options",
      // A record that is not one row supplies no options, so the required one is missing too.
      "options.api_key",
    ]);
    expect(
      await violations(sealAttach(sealConfig(null), request({ api_key: "k".repeat(17 * 1024) }), a)),
    ).toEqual(["options"]);
  });

  test("an expired ticket is attach_ticket_expired, only for its own principal", async () => {
    const sealed = await sealAttach(sealConfig(100), request({ api_key: "k" }), auth("alice"));
    const later = DEFAULT_NOW + 100 + 60;
    const key = b64(DEFAULT_KEY);
    expect(await kindOf(redeemAttachTicket({ vgi_attach_ticket: sealed.ticket }, key, auth("alice"), later))).toBe(
      "attach_ticket_expired",
    );
    expect(await kindOf(redeemAttachTicket({ vgi_attach_ticket: sealed.ticket }, key, auth("bob"), later))).toBe(
      "attach_ticket_invalid",
    );
  });
});

// ---------------------------------------------------------------------------
// Reserved option name
// ---------------------------------------------------------------------------

describe("the reserved attach option name", () => {
  for (const name of ["vgi_attach_ticket", "VGI_Attach_Ticket"]) {
    test(`declaring '${name}' is refused`, async () => {
      const spec = { name, description: "", type: new Utf8() };
      expect(() => serializeAttachOptionSpec(spec)).toThrow(/reserved name 'vgi_attach_ticket'/);
      expect(() => validateRequiredAttachOptions("c", [spec], {})).toThrow(/reserved/);
      const catalog = {
        catalogsInfo: () => [
          { name: "c", attach_option_specs: serializeAttachOptionSpecs([spec]) },
        ],
      };
      await expect(checkReservedAttachOptions(catalog)).rejects.toBeInstanceOf(ReservedAttachOptionError);
    });
  }

  test("an ordinary catalog passes the startup check", async () => {
    await checkReservedAttachOptions(fakeCatalog);
  });
});

// ---------------------------------------------------------------------------
// Hosting conditions
// ---------------------------------------------------------------------------

describe("hosting vgi.attach_tickets.v1", () => {
  const KEY = Buffer.from(new Uint8Array(32).fill(3)).toString("base64");
  const ALL: ServerTransport[] = ["pipe", "unix", "tcp", "iroh", "stream", "http"];
  function hosted(transport: ServerTransport, configured: boolean, env: Record<string, string>, mintGrant = false) {
    const server = buildRpcServer(
      new Protocol("vgi.v2", { protocolVersion: "2.0.0" }),
      mintGrant ? { mintGrant: () => ({ token: "t", expiresAt: 0 }) } : {},
      {
        transport,
        env,
        attachTickets: { signingKey: new Uint8Array(32).fill(1), signingKeyConfigured: configured, catalogInterface: fakeCatalog },
      },
    );
    return [...server.bindings().keys()];
  }

  test("HTTP with a configured key and grant keys hosts it after the framework protocols", () => {
    const names = hosted("http", true, { VGI_RPC_GRANT_KEYS: KEY });
    expect(names).toContain(ATTACH_TICKETS_PROTOCOL_NAME);
  });

  test("its own mintGrant also counts as able to issue grants", () => {
    expect(hosted("http", true, {}, true)).toContain(ATTACH_TICKETS_PROTOCOL_NAME);
  });

  test("absent with a generated key, without grants, or off HTTP", () => {
    expect(hosted("http", false, { VGI_RPC_GRANT_KEYS: KEY })).not.toContain(ATTACH_TICKETS_PROTOCOL_NAME);
    expect(hosted("http", true, {})).not.toContain(ATTACH_TICKETS_PROTOCOL_NAME);
    for (const t of ALL.filter((t) => t !== "http")) {
      expect(hosted(t, true, { VGI_RPC_GRANT_KEYS: KEY }, true)).not.toContain(ATTACH_TICKETS_PROTOCOL_NAME);
    }
  });

  test("the lifetime ceiling is the grant keys' maximum", async () => {
    const keys = GrantKeys.fromEnv({ VGI_RPC_GRANT_KEYS: KEY, VGI_RPC_GRANT_MAX_TTL_SECONDS: "120" });
    expect(keys?.maxTtlSeconds).toBe(120);
  });
});
