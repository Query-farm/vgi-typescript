// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Orchard hosted mode on Node: run the built `@query-farm/vgi/worker-cf` bundle
// (the runtime-neutral createVgiFetch) under plain Node and check that the
// Pass gate verifies an ES256 Pass with Web Crypto, refuses a missing or
// forged one, and leaves HEAD /health anonymous. The Bun unit suite covers the
// contract in depth; this proves the hosted path has no Bun-only dependency.
//
//   bun run build && node scripts/hosted-node-smoke.mjs

import { createServer } from "node:http";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import {
  createVgiFetch,
  FunctionRegistry,
  orchardHostedFromEnv,
  ReadOnlyCatalogInterface,
} from "../dist/worker-cf-entry.js";

const ISSUER = "https://auth.orchard.test";
const AUDIENCE = "https://desk.orchard.test/d/dep_node";

const { publicKey, privateKey } = await generateKeyPair("ES256");
const jwk = { ...(await exportJWK(publicKey)), kid: "node-1", alg: "ES256", use: "sig" };
const jwks = createServer((_req, res) => {
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ keys: [jwk] }));
});
await new Promise((resolve) => jwks.listen(0, "127.0.0.1", resolve));
const { port } = jwks.address();

let failures = 0;
const check = (ok, what) => {
  console.log(`${ok ? "ok  " : "FAIL"} ${what}`);
  if (!ok) failures++;
};

try {
  const orchardHosted = orchardHostedFromEnv({
    VGI_ORCHARD_HOSTED: "1",
    VGI_JWT_ISSUER: ISSUER,
    VGI_JWT_AUDIENCE: AUDIENCE,
    VGI_JWT_JWKS_URI: `http://127.0.0.1:${port}/jwks`,
  });
  const registry = new FunctionRegistry();
  const handler = createVgiFetch({
    protocol: { registry, catalogInterface: new ReadOnlyCatalogInterface({ name: "node", schemas: [] }, registry) },
    signingKey: new Uint8Array(32).fill(3),
    prefix: "",
    landingInfo: { name: "node", doc: "Node smoke", version: "0" },
    orchardHosted,
  });
  const now = Math.floor(Date.now() / 1000);
  const token = await new SignJWT({ iss: ISSUER, aud: AUDIENCE, sub: "user_node", iat: now, exp: now + 300 })
    .setProtectedHeader({ alg: "ES256", typ: "at+jwt", kid: "node-1" })
    .sign(privateKey);
  const req = (method, auth) =>
    handler(new Request("http://worker.test/health", { method, headers: auth ? { Authorization: auth } : {} }));

  const head = await req("HEAD");
  check(head.status === 200, `HEAD /health anonymous (${head.status})`);
  const none = await req("GET");
  check(
    none.status === 401 && none.headers.get("www-authenticate") === 'Bearer error="invalid_token"',
    `GET /health without a Pass is 401 (${none.status})`,
  );
  const ok = await req("GET", `Bearer ${token}`);
  check(ok.status === 200, `GET /health with a Pass is 200 (${ok.status})`);
  const forged = await req("GET", `Bearer ${token.slice(0, -4)}AAAA`);
  check(forged.status === 401 && forged.headers.get("vgi-auth-reason") === "invalid_credential", `forged Pass is 401 (${forged.status})`);
} finally {
  jwks.close();
}
if (failures) {
  console.error(`${failures} check(s) failed`);
  process.exit(1);
}
console.log("hosted mode works on Node", process.version);
