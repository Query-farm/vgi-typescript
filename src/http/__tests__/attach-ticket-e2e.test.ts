// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Attach tickets end to end over a real listening HTTP server, against the
// cross-SDK `ticket_probe` fixture catalog:
//
//   1. alice (a fresh login) attaches ticket_probe with region + api_key and
//      reads main.probe;
//   2. she calls seal_attach (vgi.attach_tickets.v1) and issue_grant
//      (vgi_rpc.Identity.v1);
//   3. a second client authenticated only by `Bearer <grant>` attaches with
//      nothing but {vgi_attach_ticket: <ticket>} and reads the same row;
//   4. bob's grant with alice's ticket is refused.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { AuthContext, GrantKeys, httpConnect } from "@query-farm/vgi-rpc";
import { VgiClient } from "../../client/client.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { CompositeCatalogInterface } from "../../catalog/composite.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { serializeAttachOptions } from "../../catalog/attach-options.js";
import { batchFromColumns, deserializeBatch, iterRows, serializeBatch } from "../../util/arrow/index.js";
import { SEAL_ATTACH_REQUEST_SCHEMA } from "../../attach-ticket.js";
import { createVgiFetch } from "../fetch.js";
import {
  apiKeyDigest,
  createTicketProbeCatalog,
  ticketProbeFunctions,
} from "../../../examples/ticket_probe.js";
import { narrowBindCatalog, narrowBindFunctions } from "../../../examples/narrow_bind.js";

const KEYS = new GrantKeys([new Uint8Array(32).fill(5)], { audience: "sdk-test", maxTtlSeconds: 3600 });
const TOKENS: Record<string, string> = { "vgi-test-alice": "alice", "vgi-test-bob": "bob" };
const API_KEY = "sk-test-0123456789";

let server: { port: number; stop(force?: boolean): void };
let baseUrl: string;

beforeAll(() => {
  const registry = new FunctionRegistry();
  for (const f of [...narrowBindFunctions, ...ticketProbeFunctions]) registry.register(f as never);
  // ticket_probe is the SECOND backend, so a composite has to route on the
  // sealed catalog name -- the name on a ticket-carrying request is ignored.
  const catalogInterface = new CompositeCatalogInterface([
    new ReadOnlyCatalogInterface(narrowBindCatalog, registry),
    createTicketProbeCatalog(registry),
  ]);
  const fetch = createVgiFetch({
    protocol: { registry, catalogInterface },
    signingKey: new Uint8Array(32).fill(9),
    signingKeyConfigured: true,
    prefix: "",
    serverId: "vgi-attach-ticket-test",
    landingInfo: { name: "demo", doc: "Attach-ticket test worker.", version: "0" },
    // The fixture's test bearers, as fresh logins; a sealed grant is not ours
    // and falls through to the grant authenticator.
    authenticate: (req: Request) => {
      const header = req.headers.get("authorization") ?? "";
      const token = header.replace(/^Bearer\s+/i, "");
      if (token.startsWith("vgig1.")) throw new Error("not mine");
      const principal = TOKENS[token];
      return principal
        ? new AuthContext("bearer", true, principal, { auth_time: Math.trunc(Date.now() / 1000) })
        : AuthContext.anonymous();
    },
    grantKeys: KEYS,
  });
  server = (globalThis as any).Bun.serve({ port: 0, fetch });
  baseUrl = `http://localhost:${server.port}`;
});

afterAll(() => server?.stop(true));

/** Attach (as `authorization`) with `options` under `name`, then read main.probe. */
async function probe(authorization: string, name: string, options: Record<string, string>): Promise<string[][]> {
  const client = new VgiClient(httpConnect(baseUrl, { authorization }));
  try {
    const attach = await client.catalogAttach(name, { options });
    const rows: string[][] = [];
    for await (const batch of client.tableFunction({
      functionName: "ticket_probe",
      attachOpaqueData: attach.attach_opaque_data,
    })) {
      for (const row of iterRows(batch as never)) {
        const r = row as Record<string, unknown>;
        rows.push([String(r.region), String(r.api_key_sha256)]);
      }
    }
    return rows;
  } finally {
    client.close();
  }
}

/** One unary call on `protocol`, returning the decoded `result` record. */
async function call(
  authorization: string,
  protocol: string,
  method: string,
  params: Record<string, unknown>,
): Promise<Record<string, unknown>> {
  const rpc = httpConnect(baseUrl, { protocol, authorization });
  try {
    const out = (await rpc.call(method, params)) as { result: Uint8Array };
    return iterRows(deserializeBatch(out.result) as never).next().value as Record<string, unknown>;
  } finally {
    rpc.close();
  }
}

async function sealAttachAs(authorization: string, options: Record<string, string>): Promise<string> {
  const request = serializeBatch(
    batchFromColumns(
      {
        catalog_name: ["ticket_probe"],
        options: [serializeAttachOptions(options)],
        data_version_spec: [""],
        implementation_version: [""],
        ttl_seconds: [0n],
      },
      SEAL_ATTACH_REQUEST_SCHEMA,
    ),
  );
  const result = await call(authorization, "vgi.attach_tickets.v1", "seal_attach", { request });
  expect(Number(result.expires_at)).toBeGreaterThan(Date.now() / 1000);
  return String(result.ticket);
}

async function grantFor(authorization: string): Promise<string> {
  const result = await call(authorization, "vgi_rpc.Identity.v1", "issue_grant", {
    purpose: "reattach",
    scopes: [],
    ttl_seconds: 600,
  });
  return String(result.token);
}

describe("attach tickets over HTTP", () => {
  test("a grant plus a ticket reattaches as the user without the options", async () => {
    const alice = "Bearer vgi-test-alice";
    const options = { region: "eu-west-2", api_key: API_KEY };
    const expected = [["eu-west-2", apiKeyDigest(API_KEY)]];
    expect(apiKeyDigest(API_KEY)).toBe("0d3b56072291");
    expect(await probe(alice, "ticket_probe", options)).toEqual(expected);

    const ticket = await sealAttachAs(alice, options);
    expect(ticket.startsWith("vgia1.")).toBe(true);
    expect(ticket).not.toContain(API_KEY);
    const grant = await grantFor(alice);
    expect(grant.startsWith("vgig1.")).toBe(true);

    // Only the grant and the ticket travel; the request's catalog name is ignored.
    expect(await probe(`Bearer ${grant}`, "narrow_bind", { vgi_attach_ticket: ticket })).toEqual(expected);
  });

  test("another principal's grant with the ticket is refused", async () => {
    const ticket = await sealAttachAs("Bearer vgi-test-alice", { api_key: API_KEY });
    const bobGrant = await grantFor("Bearer vgi-test-bob");
    await expect(probe(`Bearer ${bobGrant}`, "ticket_probe", { vgi_attach_ticket: ticket })).rejects.toThrow(
      /attach ticket/,
    );
  });

  test("an option beside the ticket is invalid_request", async () => {
    const ticket = await sealAttachAs("Bearer vgi-test-alice", { api_key: API_KEY });
    const grant = await grantFor("Bearer vgi-test-alice");
    await expect(
      probe(`Bearer ${grant}`, "ticket_probe", { vgi_attach_ticket: ticket, region: "us-west-1" }),
    ).rejects.toThrow(/must be the only attach option/);
  });

  test("reflection lists vgi.attach_tickets.v1 with vgi-python's protocol hash", async () => {
    const out = await call("Bearer vgi-test-alice", "vgi_rpc.Reflection.v1", "list_protocols", {});
    const protocols = (out.protocols ?? []) as Array<Record<string, unknown>>;
    const entry = [...protocols].find((p) => p.protocol === "vgi.attach_tickets.v1");
    expect(entry).toBeDefined();
    expect(entry!.protocol_version).toBe("1.0.0");
    // compute_protocol_hash("vgi.attach_tickets.v1", rpc_methods(AttachTickets)) in vgi-python f5e99c7.
    expect(entry!.protocol_hash).toBe("241fffa801dd073c76fa933b9ad5ac790a95b81fee02e73a8332da4d3526b4e0");
  });
});
