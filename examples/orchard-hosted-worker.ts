// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Orchard hosted mode fixture worker (VGI_ORCHARD_HOSTED=1).
//
// Kept apart from examples/http-worker.ts, whose fixture authenticator,
// resolveToken and mintGrant are exactly what hosted mode refuses. Serves one
// catalog, `orchard`, with one function:
//
//   SELECT orchard.main.pass_claims(1);
//
// which returns the caller's verified Pass as JSON:
//   {"claims": {...}, "domain": "orchard", "principal": "<sub>"}
// (keys sorted; claims keep their JSON types, so `email_verified` is a boolean
// and `exp` a number).
//
// Prints PORT:<n> on stdout once listening. Flags, for the startup-refusal
// tests:
//   --with-resolve-token  supply a resolveToken hook (must refuse)
//   --with-authenticate   supply a custom authenticate (must refuse)
//   --worker [ARGS...]    serve through Worker.run(ARGS) -- stdio, --unix,
//                         --tcp -- instead of HTTP (must refuse)
// Any other arguments (e.g. --grant-key KEY) reach serveVgiWorker's argv.

import { createHash } from "node:crypto";
import { AuthContext } from "@query-farm/vgi-rpc";
import { int64, utf8 } from "../src/arrow/index.js";
import { defineScalarFunction } from "../src/functions/scalar.js";
import { FunctionRegistry } from "../src/functions/registry.js";
import { ReadOnlyCatalogInterface } from "../src/catalog/read-only.js";
import { serveVgiWorker } from "../src/serve-entry.js";
import { Worker } from "../src/worker.js";

/** The Pass claims `pass_claims` returns, when present. */
export const PASS_CLAIM_NAMES = [
  "sub",
  "kind",
  "email",
  "email_verified",
  "name",
  "org_id",
  "org_name",
  "federated_iss",
  "federated_sub",
  "client_id",
  "iat",
  "nbf",
  "exp",
  "jti",
] as const;

function sortedJson(value: unknown): string {
  return JSON.stringify(value, (_key, v: unknown) =>
    v && typeof v === "object" && !Array.isArray(v)
      ? Object.fromEntries(Object.entries(v as Record<string, unknown>).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)))
      : v,
  );
}

const passClaims = defineScalarFunction({
  name: "pass_claims",
  description: "Return the caller's verified Pass claims as a JSON object.",
  params: { x: int64() },
  returns: utf8(),
  compute: (batch, _consts, info) => {
    const auth = info.auth;
    const claims: Record<string, unknown> = {};
    for (const name of PASS_CLAIM_NAMES) {
      if (auth.claims && name in auth.claims) claims[name] = auth.claims[name];
    }
    const payload = sortedJson({ principal: auth.principal, domain: auth.domain, claims });
    return Array.from({ length: batch.numRows }, () => payload);
  },
  categories: ["auth", "testing"],
});

const registry = new FunctionRegistry();
registry.register(passClaims);
const catalog = { name: "orchard", schemas: [{ name: "main", functions: [passClaims] }] };
const catalogInterface = new ReadOnlyCatalogInterface(catalog, registry);

// The framework takes VGI_SIGNING_KEY as 64 hex characters; the cross-SDK
// harnesses start fixtures with "any stable value", so (like http-worker.ts)
// anything else is used as SHA-256 of its UTF-8. Hosted mode still checks the
// raw value: it must be set and at least 32 bytes.
const rawSigningKey = process.env.VGI_SIGNING_KEY?.trim();
const signingKey =
  rawSigningKey && !/^[0-9a-fA-F]{64}$/.test(rawSigningKey)
    ? new Uint8Array(createHash("sha256").update(rawSigningKey, "utf8").digest())
    : undefined;

const argv = process.argv.slice(2);
const workerAt = argv.indexOf("--worker");
if (workerAt >= 0) {
  new Worker({ catalogInterface, registry }).run(argv.slice(workerAt + 1));
} else {
  const server = serveVgiWorker({
    name: "orchard",
    doc: "Orchard hosted-mode fixture worker.",
    version: "0.1.0",
    registry,
    catalogInterface,
    signingKey,
    prefix: "",
    serverId: "vgi-example-orchard-hosted",
    port: Number(process.env.PORT ?? 0),
    hostname: process.env.HOST ?? "127.0.0.1",
    quiet: true,
    argv: argv.filter((a) => a !== "--with-resolve-token" && a !== "--with-authenticate"),
    resolveToken: argv.includes("--with-resolve-token") ? () => null : undefined,
    authenticate: argv.includes("--with-authenticate") ? () => AuthContext.anonymous() : undefined,
  });
  console.log(`PORT:${server.port}`);
}
