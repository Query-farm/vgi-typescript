// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Orchard hosted mode with the blob vendor (VGI_ORCHARD_HOSTED=1 plus
// VGI_BLOB_VENDOR_URL, as vgi-python's hosted mode reads them):
//
// - the capability headers say so, on a 401 too: VGI-Upload-URL-Support: 2,
//   VGI-Arguments-Ref-Bytes, VGI-Externalization-Enabled: true, and the hosted
//   request cap (32 MiB unless VGI_RPC_MAX_REQUEST_BYTES sets one);
// - an upload grant carries object_id and a ref sealed for the Pass principal;
// - a bind presenting that ref resolves for the principal it was granted to
//   and is refused (invalid_arguments_ref) for any other;
// - a result over the threshold is externalized to the vendor.
//
// Runs against vgi-rpc-python's fake vendor: the lane's (VGI_BLOB_VENDOR_URL
// pointing at a fake vendor started with --test-routes), else one started with
// the reference Python (VGI_RPC_PYTHON_BIN, or python3) when
// vgi_rpc.conformance.fake_vendor is importable, else skipped, loudly.
// VGI_VENDOR_TESTS_REQUIRED=1 turns the skip into a failure.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createHash, randomBytes } from "node:crypto";
import { exportJWK, generateKeyPair, SignJWT, type JWK } from "jose";
import { httpConnect, type RpcClient } from "@query-farm/vgi-rpc";
import { blobCatalog, blobCatalogFunctions } from "../../examples/blob_catalog.js";
import { batchFromColumns, serializeBatch } from "../arrow/index.js";
import { Arguments } from "../arguments/arguments.js";
import { ReadOnlyCatalogInterface } from "../catalog/read-only.js";
import { unwrapResult, wrapRequest } from "../client/protocol.js";
import { FunctionRegistry } from "../functions/registry.js";
import { FunctionStorageSqlite, resolveStorageFromEnv, setStorage } from "../functions/storage.js";
import { TableFunctionPlanRequestSchema } from "../generated/vgi-protocol-schemas.js";
import { HOSTED_DEFAULT_MAX_REQUEST_BYTES } from "../hosted.js";
import { serializeArguments, serializeBindRequest } from "../protocol/serialize.js";
import type { BindRequest } from "../protocol/types.js";
import { serveVgiWorker, type VgiHttpServer } from "../serve-entry.js";
import { FunctionType } from "../types.js";

const ISSUER = "https://auth.orchard.test";
const AUDIENCE = "https://desk.orchard.test/d/dep_vendor";
const KID = "desk-es256-1";
const PYTHON = process.env.VGI_RPC_PYTHON_BIN ?? "python3";
const REQUIRED = process.env.VGI_VENDOR_TESTS_REQUIRED === "1";
const TOKEN = process.env.VGI_BLOB_VENDOR_TOKEN || "test-token";
const SPLITS = 400;

async function laneVendorUrl(): Promise<string | null> {
  const url = process.env.VGI_BLOB_VENDOR_URL?.trim();
  if (!url) return null;
  try {
    const r = await fetch(`${url}/__stats__`);
    await r.text();
    return r.ok ? url : null;
  } catch {
    return null;
  }
}

let vendorUrl = await laneVendorUrl();
let canSpawn = false;
if (!vendorUrl) {
  try {
    canSpawn = Bun.spawnSync([PYTHON, "-c", "import vgi_rpc.conformance.fake_vendor"]).exitCode === 0;
  } catch {}
}
const HAVE_VENDOR = vendorUrl !== null || canSpawn;
if (!HAVE_VENDOR) {
  const why = `no fake vendor: VGI_BLOB_VENDOR_URL unset and vgi_rpc.conformance.fake_vendor not importable with ${PYTHON}`;
  if (REQUIRED) throw new Error(`VGI_VENDOR_TESTS_REQUIRED=1 but ${why}`);
  console.warn(`SKIP Orchard hosted mode with the blob vendor: ${why}`);
}

const ec = await generateKeyPair("ES256", { extractable: true });
const jwk: JWK = { ...(await exportJWK(ec.publicKey)), kid: KID, alg: "ES256", use: "sig" };

async function pass(sub: string): Promise<string> {
  const now = Math.floor(Date.now() / 1000);
  return new SignJWT({ iss: ISSUER, aud: AUDIENCE, sub, iat: now, nbf: now, exp: now + 900, jti: `jti-${crypto.randomUUID()}` })
    .setProtectedHeader({ alg: "ES256", typ: "at+jwt", kid: KID })
    .sign(ec.privateKey);
}

async function stats(): Promise<Record<string, number>> {
  return JSON.parse(await (await fetch(`${vendorUrl}/__stats__`)).text());
}

describe.skipIf(!HAVE_VENDOR)("Orchard hosted mode with the blob vendor", () => {
  let proc: ReturnType<typeof Bun.spawn> | undefined;
  let jwks: ReturnType<typeof Bun.serve>;
  let server: VgiHttpServer;
  let base: string;
  let alice: string;
  let bob: string;

  beforeAll(async () => {
    if (!vendorUrl) {
      proc = Bun.spawn(
        [PYTHON, "-c", "from vgi_rpc.conformance.fake_vendor import main; main()", "--test-routes", "--token", `${TOKEN}=tenant-a`],
        { stdout: "pipe", stderr: "inherit" },
      );
      const reader = (proc.stdout as ReadableStream<Uint8Array>).getReader();
      let text = "";
      while (!/URL:\S+/.test(text)) {
        const { done, value } = await reader.read();
        if (done) throw new Error(`fake vendor exited: ${text}`);
        text += new TextDecoder().decode(value);
      }
      reader.releaseLock();
      vendorUrl = /URL:(\S+)/.exec(text)![1]!;
    }
    const body = JSON.stringify({ keys: [jwk] });
    jwks = Bun.serve({
      port: 0,
      hostname: "127.0.0.1",
      fetch: () => new Response(body, { headers: { "Content-Type": "application/json" } }),
    });
    setStorage(new FunctionStorageSqlite(":memory:"));
    const registry = new FunctionRegistry();
    for (const f of blobCatalogFunctions) registry.register(f);
    server = serveVgiWorker({
      name: "hosted-blob",
      doc: "Orchard hosted mode with the blob vendor.",
      version: "0.0.1",
      registry,
      catalogInterface: new ReadOnlyCatalogInterface(blobCatalog, registry),
      prefix: "",
      port: 0,
      hostname: "127.0.0.1",
      quiet: true,
      argv: [],
      // Exactly the deployment's environment: no request cap set, so the
      // hosted default applies.
      env: {
        VGI_ORCHARD_HOSTED: "1",
        VGI_JWT_ISSUER: ISSUER,
        VGI_JWT_AUDIENCE: AUDIENCE,
        VGI_JWT_JWKS_URI: `http://127.0.0.1:${jwks.port}/jwks`,
        VGI_SIGNING_KEY: randomBytes(32).toString("hex"),
        VGI_BLOB_VENDOR_URL: vendorUrl!,
        VGI_BLOB_VENDOR_TOKEN: TOKEN,
        VGI_RPC_EXTERNALIZE_THRESHOLD_BYTES: "4096",
      },
    } as Parameters<typeof serveVgiWorker>[0]);
    base = `http://127.0.0.1:${server.port}`;
    alice = await pass("user_alice");
    bob = await pass("user_bob");
  });

  afterAll(() => {
    server?.stop(true);
    jwks?.stop(true);
    proc?.kill();
    setStorage(resolveStorageFromEnv());
  });

  function client(token: string): RpcClient {
    return httpConnect(base, {
      prefix: "",
      authorization: `Bearer ${token}`,
      // Pointers name the fake vendor on loopback http.
      externalLocation: { storage: undefined as never, urlValidator: null },
    });
  }

  test("the capability headers advertise the vendor, behind the Pass and on a 401", async () => {
    for (const init of [{ headers: { Authorization: `Bearer ${alice}` } }, {}]) {
      const response = await fetch(`${base}/health`, init);
      await response.text();
      expect(response.status).toBe("headers" in init ? 200 : 401);
      expect(response.headers.get("VGI-Upload-URL-Support")).toBe("2");
      expect(response.headers.get("VGI-Arguments-Ref-Bytes")).toBe(String(4 * 1024 * 1024));
      expect(response.headers.get("VGI-Externalization-Enabled")).toBe("true");
      expect(response.headers.get("VGI-Max-Request-Bytes")).toBe(String(HOSTED_DEFAULT_MAX_REQUEST_BYTES));
    }
    expect(HOSTED_DEFAULT_MAX_REQUEST_BYTES).toBe(33554432);
  });

  test("a grant binds to the Pass principal and a large plan is externalized", async () => {
    const ipc = serializeArguments(new Arguments(["p".repeat(5000), BigInt(SPLITS)]));
    const sha256 = createHash("sha256").update(ipc).digest("hex");
    const [grant] = await client(alice).requestUploadUrls([{ size: ipc.byteLength, sha256 }]);
    expect(grant!.objectId).toBeTruthy();
    expect(grant!.ref?.byteLength ?? 0).toBeGreaterThan(0);
    const put = await fetch(grant!.uploadUrl, { method: "PUT", headers: grant!.uploadHeaders, body: ipc });
    await put.text();
    expect(put.ok).toBe(true);

    const call: BindRequest = {
      function_name: "blob_param_streams",
      arguments: new Arguments(),
      function_type: FunctionType.TABLE,
      input_schema: null,
      settings: null,
      secrets: null,
      attach_opaque_data: null,
      transaction_opaque_data: null,
      resolved_secrets_provided: false,
      schema_path: ["main"],
      arguments_location: { ref: grant!.ref!, size: ipc.byteLength, sha256 },
    };
    const bindBatch = serializeBindRequest(call);

    // The ref opens for the principal it was granted to, and for no other.
    await client(alice).call("bind", wrapRequest(bindBatch));
    let refused: any;
    try {
      await client(bob).call("bind", wrapRequest(bindBatch));
    } catch (error) {
      refused = error;
    }
    expect(refused?.errorKind).toBe("invalid_arguments_ref");

    // 400 splits are far over the 4096-byte threshold: a vendor pointer the
    // client fetches, built from the arguments resolved through the ref.
    const before = await stats();
    const request = batchFromColumns(
      Object.fromEntries(
        TableFunctionPlanRequestSchema.fields.map((f) => [f.name, [f.name === "bind_call" ? serializeBatch(bindBatch) : null]]),
      ),
      TableFunctionPlanRequestSchema,
    );
    const plan = unwrapResult((await client(alice).call("table_function_plan", wrapRequest(request))) as Record<string, any>);
    const splits = plan.splits?.toArray?.() ?? plan.splits;
    expect(splits.length).toBe(SPLITS);
    const after = await stats();
    expect(after.puts_rpc!).toBeGreaterThan(before.puts_rpc!);
    expect(after.gets_upload!).toBeGreaterThanOrEqual(1);
  });
});
