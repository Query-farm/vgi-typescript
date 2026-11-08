// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Secret ATTACH options reach functions and transaction hooks through
// attachOptions(), encrypted inside attach_opaque_data — never in plaintext
// (SPEC-secret-options.md, Conformance):
//   * secret absent from the attach bytes on stdio (raw) and HTTP (opened inner bytes)
//   * value returned by the accessor, in hooks and in function calls
//   * blob bound to its attach (moving it fails, uniformly)
//   * host-local key file: 0600 / 0700, reused, concurrent creation agrees, bad perms refused
//   * HKDF subkey used, not the root

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { hkdfSync } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { httpConnect, subprocessConnect } from "@query-farm/vgi-rpc";
import { xchacha20poly1305 } from "@noble/ciphers/chacha.js";
import {
  ATTACH_SECRET_OPTIONS_INFO,
  AttachOptionsUnavailableError,
  AttachSecretKeyUnavailableError,
  attachOption,
  attachOptions,
  deriveAttachSecretKey,
  openFrameworkAttach,
  packAttachOptionsTrailer,
} from "../attach-secret-options.js";
import { hostLocalAttachSecretKeyPath, loadHostLocalAttachSecretKey } from "../attach-secret-keyfile.js";
import { OpaqueDataRejectedError } from "../crypto.js";
import { serializeAttachOptions } from "../catalog/attach-options.js";
import { ReadOnlyCatalogInterface } from "../catalog/read-only.js";
import type { CatalogAttachResult } from "../catalog/interface.js";
import { FunctionRegistry } from "../functions/registry.js";
import { buildVgiProtocol } from "../protocol/dispatch.js";
import { openAttachFull } from "../protocol/handlers/catalog/shared.js";
import { wrapRequest } from "../client/protocol.js";
import { decodeASD } from "../codec/asd.js";
import { CatalogAttachResultSchema } from "../generated/vgi-protocol-schemas.js";
import { batchFromColumns, deserializeBatch, iterRows } from "../util/arrow/index.js";
import { binary, field, schema, utf8 } from "../arrow/index.js";
import { VgiClient } from "../client/client.js";
import { serveVgiWorker, type VgiHttpServer } from "../serve-entry.js";
import { TxfixCatalog, descriptor as txfixDescriptor } from "../../examples/writable-tx-worker.js";

const enc = new TextEncoder();
const TOKEN = "s3cr3t-token-value-xyz";

function contains(haystack: Uint8Array, needle: Uint8Array): boolean {
  outer: for (let i = 0; i + needle.length <= haystack.length; i++) {
    for (let j = 0; j < needle.length; j++) if (haystack[i + j] !== needle[j]) continue outer;
    return true;
  }
  return false;
}

const SECRET = new Set(["token"]);
const optionsIpc = (o: Record<string, string | boolean | null>) => serializeAttachOptions(o)!;
const uuidOf = (b: number) => new Uint8Array(16).fill(b);
const root = new Uint8Array(32).fill(7);
const keys = { explicit: root };

let scratch: string;
beforeAll(() => {
  scratch = mkdtempSync(join(tmpdir(), "vgi-secret-opts-"));
});
afterAll(() => rmSync(scratch, { recursive: true, force: true }));

// ---------------------------------------------------------------- envelope

describe("options trailer", () => {
  test("no options: no trailer, bytes unchanged", async () => {
    expect((await packAttachOptionsTrailer(uuidOf(1), null, SECRET, keys)).length).toBe(0);
    const full = new Uint8Array([...uuidOf(1), ...enc.encode("catalog")]);
    const opened = await openFrameworkAttach(full, keys);
    expect(new TextDecoder().decode(opened)).toBe("catalog");
    expect(attachOptions(opened)).toEqual({});
  });

  test("secret never in plaintext; accessor returns every option", async () => {
    const uuid = uuidOf(2);
    const trailer = await packAttachOptionsTrailer(uuid, optionsIpc({ token: TOKEN, region: "eu" }), SECRET, keys);
    expect(contains(trailer, enc.encode(TOKEN))).toBe(false);
    const full = new Uint8Array([...uuid, ...enc.encode("cat"), ...trailer]);
    const opened = await openFrameworkAttach(full, keys);
    expect(new TextDecoder().decode(opened)).toBe("cat");
    expect(attachOptions(opened)).toEqual({ token: TOKEN, region: "eu" });
    expect(attachOption(opened, "TOKEN")).toBe(TOKEN);
    // A subarray of the handed-out bytes resolves too (a composite's route strip)…
    expect(attachOptions(opened.subarray(1))).toEqual({ token: TOKEN, region: "eu" });
    // …a copy does not.
    expect(() => attachOptions(opened.slice())).toThrow(AttachOptionsUnavailableError);
    expect(attachOptions(null)).toEqual({});
  });

  test("no secret supplied (or NULL): no key is needed", async () => {
    const noKey = { hostLocal: async () => { throw new Error("must not be read"); } };
    const t1 = await packAttachOptionsTrailer(uuidOf(3), optionsIpc({ region: "eu" }), SECRET, noKey);
    const t2 = await packAttachOptionsTrailer(uuidOf(3), optionsIpc({ region: "eu", token: null }), SECRET, noKey);
    for (const t of [t1, t2]) {
      const opened = await openFrameworkAttach(new Uint8Array([...uuidOf(3), ...t]), noKey);
      expect(attachOptions(opened).region).toBe("eu");
    }
  });

  test("a secret with no obtainable key fails the attach with a clear message", async () => {
    const noKey = { hostLocal: async () => { throw new Error("no filesystem here"); } };
    const p = packAttachOptionsTrailer(uuidOf(4), optionsIpc({ token: TOKEN }), SECRET, noKey);
    await expect(p).rejects.toBeInstanceOf(AttachSecretKeyUnavailableError);
    await expect(p).rejects.toThrow(/VGI_SIGNING_KEY/);
    await expect(p).rejects.not.toThrow(new RegExp(TOKEN));
  });

  test("blob is bound to its attach: moving it into another attach fails uniformly", async () => {
    const tA = await packAttachOptionsTrailer(uuidOf(5), optionsIpc({ token: TOKEN }), SECRET, keys);
    const moved = new Uint8Array([...uuidOf(6), ...enc.encode("cat"), ...tA]);
    const err = await openFrameworkAttach(moved, keys).catch((e) => e);
    expect(err).toBeInstanceOf(OpaqueDataRejectedError);
    expect(err.message).toBe("attach_opaque_data not recognized");
  });

  test("tampered blob / wrong key / truncated trailer all read the same", async () => {
    const uuid = uuidOf(7);
    const t = await packAttachOptionsTrailer(uuid, optionsIpc({ token: TOKEN }), SECRET, keys);
    const full = new Uint8Array([...uuid, ...t]);
    const tampered = full.slice();
    tampered[tampered.length - 20] ^= 1; // inside the tag
    const truncated = new Uint8Array([...uuid, ...t.subarray(5)]);
    for (const [bytes, k] of [
      [tampered, keys],
      [full, { explicit: new Uint8Array(32).fill(8) }],
      [truncated, keys],
    ] as const) {
      const err = await openFrameworkAttach(bytes, k).catch((e) => e);
      expect(err).toBeInstanceOf(OpaqueDataRejectedError);
      expect(err.message).toBe("attach_opaque_data not recognized");
    }
  });

  test("the AEAD key is the HKDF-SHA256 subkey of the root, never the root", async () => {
    const derived = await deriveAttachSecretKey(root);
    const expected = new Uint8Array(hkdfSync("sha256", root, new Uint8Array(0), ATTACH_SECRET_OPTIONS_INFO, 32));
    expect(derived).toEqual(expected);
    expect(derived).not.toEqual(root);

    // Open the blob by hand: the subkey opens it, the root does not.
    const uuid = uuidOf(9);
    const nonce = new Uint8Array(24).fill(3);
    const t = await packAttachOptionsTrailer(uuid, optionsIpc({ token: TOKEN }), SECRET, keys, nonce);
    const magicAndLen = 12;
    const bodyLen = new DataView(t.buffer, t.byteOffset).getUint32(t.length - magicAndLen, true);
    const body = t.subarray(0, bodyLen);
    const plainLen = new DataView(body.buffer, body.byteOffset).getUint32(0, true);
    const blob = body.subarray(4 + plainLen);
    expect(blob[0]).toBe(1);
    expect(blob.subarray(1, 25)).toEqual(nonce);
    const aad = new Uint8Array([...enc.encode(`${ATTACH_SECRET_OPTIONS_INFO}\0`), ...uuid]);
    const pt = xchacha20poly1305(expected, nonce, aad).decrypt(blob.subarray(25));
    expect([...iterRows(deserializeBatch(pt))][0]).toEqual({ token: TOKEN });
    expect(() => xchacha20poly1305(root, nonce, aad).decrypt(blob.subarray(25))).toThrow();
  });
});

// ---------------------------------------------------------------- key file

describe("host-local key file", () => {
  test("path rules: explicit env, then $XDG_RUNTIME_DIR/vgi, then <tmpdir>/vgi-<user>", () => {
    expect(hostLocalAttachSecretKeyPath({ VGI_ATTACH_SECRET_KEY_FILE: "/x/y.key" })).toBe("/x/y.key");
    expect(hostLocalAttachSecretKeyPath({ VGI_ATTACH_SECRET_KEY_FILE: "", XDG_RUNTIME_DIR: scratch })).toBe(
      join(scratch, "vgi", "attach-secret-options.key"),
    );
    const fallback = hostLocalAttachSecretKeyPath({ XDG_RUNTIME_DIR: join(scratch, "missing") });
    expect(fallback.startsWith(tmpdir())).toBe(true);
    expect(fallback).toMatch(/\/vgi-[^/]+\/attach-secret-options\.key$/);
  });

  test("created 32 bytes, file 0600 in a 0700 directory, then reused", () => {
    const path = join(scratch, "fresh", "vgi", "attach-secret-options.key");
    const k1 = loadHostLocalAttachSecretKey(path);
    expect(k1.length).toBe(32);
    expect(statSync(path).mode & 0o777).toBe(0o600);
    expect(statSync(join(scratch, "fresh", "vgi")).mode & 0o777).toBe(0o700);
    expect(new Uint8Array(readFileSync(path))).toEqual(k1);
    expect(loadHostLocalAttachSecretKey(path)).toEqual(k1);
  });

  test("concurrent first starts agree on one key", async () => {
    const path = join(scratch, "race", "attach-secret-options.key");
    const script = join(scratch, "race-load.ts");
    writeFileSync(
      script,
      `import { loadHostLocalAttachSecretKey } from ${JSON.stringify(resolve(import.meta.dir, "../attach-secret-keyfile.ts"))};\n` +
        `process.stdout.write(Buffer.from(loadHostLocalAttachSecretKey(${JSON.stringify(path)})).toString("hex"));\n`,
    );
    const procs = Array.from({ length: 8 }, () => Bun.spawn(["bun", script], { stdout: "pipe", stderr: "inherit" }));
    const outs = await Promise.all(procs.map((p) => new Response(p.stdout).text()));
    await Promise.all(procs.map((p) => p.exited));
    expect(new Set(outs).size).toBe(1);
    expect(outs[0]).toBe(Buffer.from(readFileSync(path)).toString("hex"));
  });

  test("refuses group/other-accessible file or directory, and a wrong-length key", () => {
    const dir = join(scratch, "perms");
    const path = join(dir, "attach-secret-options.key");
    loadHostLocalAttachSecretKey(path);
    chmodSync(path, 0o644);
    expect(() => loadHostLocalAttachSecretKey(path)).toThrow(/group\/other/);
    chmodSync(path, 0o600);
    chmodSync(dir, 0o755);
    expect(() => loadHostLocalAttachSecretKey(path)).toThrow(/group\/other/);
    chmodSync(dir, 0o700);

    const shortDir = join(scratch, "short");
    mkdirSync(shortDir, { mode: 0o700 });
    const shortPath = join(shortDir, "attach-secret-options.key");
    writeFileSync(shortPath, "too short", { mode: 0o600 });
    expect(() => loadHostLocalAttachSecretKey(shortPath)).toThrow(new RegExp(`${shortPath}.*32.*VGI_SIGNING_KEY`));
    // Never overwritten.
    expect(readFileSync(shortPath, "utf8")).toBe("too short");
  });
});

// ---------------------------------------------------------- worker handlers

const SPECS = [
  { name: "token", description: "credential", type: utf8(), secret: true },
  { name: "region", description: "region", type: utf8() },
];

/** Records attachOptions() as the transaction hooks see them. */
class HookCatalog extends ReadOnlyCatalogInterface {
  seen: Array<[string, Readonly<Record<string, unknown>>]> = [];
  override async attach(name: string, options?: Record<string, unknown>): Promise<CatalogAttachResult> {
    return { ...(await super.attach(name, options)), supports_transactions: true };
  }
  override transactionBegin(a: Uint8Array): Uint8Array {
    this.seen.push(["begin", this.attachOptions(a)]);
    return new Uint8Array([1, 2, 3]);
  }
  override transactionCommit(a: Uint8Array): void {
    this.seen.push(["commit", this.attachOptions(a)]);
  }
}

async function attachVia(signingKey: Uint8Array | undefined) {
  const cat = new HookCatalog({ name: "c", attachOptions: SPECS, schemas: [{ name: "main" }] }, new FunctionRegistry());
  const protocol = buildVgiProtocol({ registry: new FunctionRegistry(), catalogInterface: cat, signingKey, attachSecretKey: root });
  const method = (name: string) => (protocol as any)._methods.get(name);
  const request = wrapRequest(
    batchFromColumns(
      { name: ["c"], options: [optionsIpc({ token: TOKEN, region: "eu" })], data_version_spec: [null], implementation_version: [null], client_capabilities: [null] },
      schema([
        field("name", utf8(), false),
        field("options", binary(), true),
        field("data_version_spec", utf8(), true),
        field("implementation_version", utf8(), true),
        field("client_capabilities", binary(), true),
      ]),
    ),
  );
  const result = decodeASD<CatalogAttachResult>(CatalogAttachResultSchema, (await method("catalog_attach").handler(request, {})).result);
  const attach = result.attach_opaque_data as Uint8Array;
  const begin = await method("catalog_transaction_begin").handler({ attach_opaque_data: attach }, {});
  const tx = (deserializeBatch(begin.result) as any).getChild("transaction_opaque_data").get(0);
  await method("catalog_transaction_commit").handler({ attach_opaque_data: attach, transaction_opaque_data: tx }, {});
  return { cat, attach };
}

describe("catalog_attach and the transaction hooks", () => {
  test("stdio / unix (no signing key): raw attach bytes never hold the secret; hooks read it", async () => {
    const { cat, attach } = await attachVia(undefined);
    expect(contains(attach, enc.encode(TOKEN))).toBe(false);
    expect(cat.seen).toEqual([
      ["begin", { token: TOKEN, region: "eu" }],
      ["commit", { token: TOKEN, region: "eu" }],
    ]);
  });

  test("HTTP (sealed): the opened inner bytes never hold the secret either", async () => {
    const signingKey = new Uint8Array(32).fill(4);
    const { cat, attach } = await attachVia(signingKey);
    const inner = await openAttachFull(attach, undefined, signingKey);
    expect(contains(inner, enc.encode(TOKEN))).toBe(false);
    expect(contains(attach, enc.encode(TOKEN))).toBe(false);
    expect(cat.seen.map(([, o]) => o.token)).toEqual([TOKEN, TOKEN]);
  });
});

// --------------------------------------------- end to end: function calls

async function whoami(client: VgiClient, token: string): Promise<{ attach: Uint8Array; row: Record<string, unknown> }> {
  const attach = await client.catalogAttach("txfix", { options: { token } });
  const rows: Record<string, unknown>[] = [];
  for await (const batch of client.tableFunction({
    functionName: "whoami",
    attachOpaqueData: attach.attach_opaque_data as Uint8Array,
  })) {
    rows.push(...(iterRows(batch as any) as Iterable<Record<string, unknown>>));
  }
  return { attach: attach.attach_opaque_data as Uint8Array, row: rows[0]! };
}

describe("the txfix fixture's functions read the token only through attachOptions()", () => {
  test("stdio: the token reaches whoami; the raw attach bytes never hold it", async () => {
    const dir = join(scratch, "stdio");
    mkdirSync(dir, { recursive: true });
    // The fixture over stdio, with its functions also registered unscoped:
    // VgiClient binds by bare name (DuckDB sends the schema path).
    const worker = join(dir, "txfix-stdio.ts");
    writeFileSync(
      worker,
      `import { FunctionRegistry, Worker } from ${JSON.stringify(resolve(import.meta.dir, "../index.js"))};\n` +
        `import { TxfixCatalog, descriptor } from ${JSON.stringify(resolve(import.meta.dir, "../../examples/writable-tx-worker.ts"))};\n` +
        `const registry = new FunctionRegistry();\n` +
        `for (const f of descriptor.schemas[0].functions) registry.register(f);\n` +
        `new Worker({ registry, catalogInterface: new TxfixCatalog(descriptor, registry) }).run([]);\n`,
    );
    const keyFile = join(dir, "keys", "attach-secret-options.key");
    const client = new VgiClient(
      subprocessConnect(["bun", worker], {
        cwd: dir,
        env: { ...process.env, VGI_TXFIX_STATE_DIR: join(dir, "state"), VGI_ATTACH_SECRET_KEY_FILE: keyFile } as any,
      }),
    );
    try {
      const { attach, row } = await whoami(client, TOKEN);
      expect(contains(attach, enc.encode(TOKEN))).toBe(false);
      expect(row).toEqual({ token: TOKEN, transactional: false, token_in_attach_data: false });
      expect(existsSync(keyFile)).toBe(true);
    } finally {
      client.close();
    }
  }, 30_000);

  test("HTTP: the token reaches whoami; neither the envelope nor its inner bytes hold it", async () => {
    const signingKey = new Uint8Array(32).fill(6);
    process.env.VGI_TXFIX_STATE_DIR = join(scratch, "http-state");
    const registry = new FunctionRegistry();
    for (const f of txfixDescriptor.schemas[0]!.functions ?? []) registry.register(f);
    const server: VgiHttpServer = serveVgiWorker({
      name: "txfix",
      doc: "test",
      version: "0",
      registry,
      catalogInterface: new TxfixCatalog(txfixDescriptor, registry),
      hostname: "127.0.0.1",
      port: 0,
      quiet: true,
      signingKey,
      argv: [],
      env: {},
    });
    const client = new VgiClient(httpConnect(`http://127.0.0.1:${server.port}`, { prefix: "" }));
    try {
      const { attach, row } = await whoami(client, TOKEN);
      expect(contains(attach, enc.encode(TOKEN))).toBe(false);
      expect(contains(await openAttachFull(attach, undefined, signingKey), enc.encode(TOKEN))).toBe(false);
      expect(row).toEqual({ token: TOKEN, transactional: false, token_in_attach_data: false });
    } finally {
      client.close();
      server.stop(true);
    }
  }, 30_000);
});
