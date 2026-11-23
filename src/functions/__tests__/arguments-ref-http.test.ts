// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Protocol 3.4 argument references and `init: StateRef`, end to end over HTTP
// (vgi docs/large-values-3.4.md §2, §5, §5a, §6).
//
// The test plays the extension: it "uploads" the arguments IPC stream into the
// worker's object store, mints the grant's sealed reference with the worker's
// key (what `__upload_url__/init` hands the client), and sends BindRequests
// whose `arguments` is null and whose `arguments_location` names that object.
// The worker must:
//   - advertise VGI-Arguments-Ref-Bytes (it vends upload URLs over an ObjectStore);
//   - resolve the reference at every request's entry, through vgi-rpc's object
//     cache (one storage GET for the whole stream, however many turns);
//   - keep the argument out of every cursor (the init request it carries as
//     its `init` StateRef holds the reference, never the value);
//   - refuse a gone object with the prepared-statement message, a tampered
//     reference with invalid_arguments_ref, and a bind setting neither or both
//     with invalid_bind_request.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  AuthContext,
  httpConnect,
  mintArgumentRef,
  ObjectMissingError,
  type RpcClient,
  STATE_KEY,
  type StreamSession,
} from "@query-farm/vgi-rpc";
import { blobCatalogFunctions } from "../../../examples/blob_catalog.js";
import { batchFromColumns, deserializeBatch, schema, type VgiBatch } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { unwrapResult, wrapRequest } from "../../client/protocol.js";
import { ARGUMENTS_REF_MISSING_MESSAGE, INVALID_BIND_REQUEST_MESSAGE } from "../../protocol/arguments-ref.js";
import {
  deserializeBindResponse,
  serializeArguments,
  serializeBindRequest,
  serializeInitRequest,
} from "../../protocol/serialize.js";
import type { BindRequest, BindResponse, InitRequest } from "../../protocol/types.js";
import { SIGNING_KEY_BYTES, serveVgiWorker, type VgiHttpServer } from "../../serve-entry.js";
import { FunctionType } from "../../types.js";
import { iterRows } from "../../util/arrow/index.js";
import { FunctionRegistry } from "../registry.js";
import { FunctionStorageSqlite, resolveStorageFromEnv, setStorage } from "../storage.js";

const SIGNING_KEY = new Uint8Array(SIGNING_KEY_BYTES).fill(11);

/** An in-memory object store: what the vendor is to a deployed worker. */
class MemoryObjectStore {
  readonly objects = new Map<string, Uint8Array>();
  gets = 0;
  private next = 0;
  async putObject(data: Uint8Array): Promise<string> {
    const id = `rpc/obj-${++this.next}-${crypto.randomUUID()}`;
    this.objects.set(id, data.slice());
    return id;
  }
  async get(objectId: string): Promise<Uint8Array> {
    this.gets++;
    const data = this.objects.get(objectId);
    if (!data) throw new ObjectMissingError();
    return data.slice();
  }
  async upload(): Promise<string> {
    throw new Error("results are never externalized in this test");
  }
}

const store = new MemoryObjectStore();
const uploadUrlProvider = {
  generateUploadUrl: () => ({
    objectId: "unused",
    uploadUrl: "http://127.0.0.1:9/put",
    downloadUrl: "http://127.0.0.1:9/get",
    expiresAt: new Date(Date.now() + 60_000),
  }),
};

let dir: string;
let server: VgiHttpServer;
let rpc: RpcClient;
const echoedCursorLengths: number[] = [];

beforeAll(() => {
  dir = mkdtempSync(join(tmpdir(), "vgi-arguments-ref-"));
  setStorage(new FunctionStorageSqlite(join(dir, "state.db")));
  const registry = new FunctionRegistry();
  for (const f of blobCatalogFunctions) registry.register(f);
  server = serveVgiWorker({
    name: "blob",
    doc: "Argument reference HTTP test worker.",
    version: "0.0.1",
    registry,
    catalogInterface: new ReadOnlyCatalogInterface(
      { name: "blob", schemas: [{ name: "main", functions: blobCatalogFunctions }] },
      registry,
    ),
    port: 0,
    signingKey: SIGNING_KEY,
    quiet: true,
    env: {},
    externalLocation: { storage: store as any, urlValidator: null },
    uploadUrlProvider,
  });
  const spy: typeof fetch = (async (input: any, init?: any) => {
    if (init?.body instanceof Uint8Array && String(input).endsWith("/exchange")) {
      const cursor = deserializeBatch(init.body).metadata?.get(STATE_KEY);
      if (cursor) echoedCursorLengths.push(cursor.length);
    }
    return fetch(input, init);
  }) as typeof fetch;
  rpc = httpConnect(`http://localhost:${server.port}`, { prefix: "", fetch: spy });
});

afterAll(() => {
  server?.stop(true);
  setStorage(resolveStorageFromEnv());
  rmSync(dir, { recursive: true, force: true });
});

/** Upload `args` the way the extension does and return the location it binds with. */
async function upload(args: Arguments): Promise<{ ref: Uint8Array; size: number; sha256: string; objectId: string }> {
  const ipc = serializeArguments(args);
  const sha256 = createHash("sha256").update(ipc).digest("hex");
  const objectId = await store.putObject(ipc);
  const ref = mintArgumentRef(SIGNING_KEY, AuthContext.anonymous(), { size: ipc.byteLength, sha256, objectId });
  return { ref, size: ipc.byteLength, sha256, objectId };
}

function bindCall(name: string, location: { ref: Uint8Array; size: number; sha256: string }): BindRequest {
  return {
    function_name: name,
    // Never sent: serializeBindRequest emits `arguments_location` instead.
    arguments: new Arguments(),
    function_type: FunctionType.TABLE,
    input_schema: null,
    settings: null,
    secrets: null,
    attach_opaque_data: null,
    transaction_opaque_data: null,
    resolved_secrets_provided: false,
    schema_path: ["main"],
    arguments_location: { ref: location.ref, size: location.size, sha256: location.sha256 },
  };
}

async function bind(request: BindRequest | VgiBatch): Promise<BindResponse> {
  const batch = "function_name" in request ? serializeBindRequest(request) : request;
  const result = await rpc.call("bind", wrapRequest(batch));
  return deserializeBindResponse(unwrapResult(result as Record<string, any>));
}

function initRequest(bind: BindRequest, bound: BindResponse): InitRequest {
  return {
    bind_call: bind,
    output_schema: bound.output_schema,
    bind_opaque_data: bound.opaque_data,
    projection_ids: null,
    pushdown_filters: null,
    join_keys: [],
    split_tokens: null,
    row_limit: null,
    phase: null,
    finalize_state_id: null,
    execution_id: null,
    init_opaque_data: null,
    substream_id: null,
    order_by_column_name: null,
    order_by_direction: null,
    order_by_null_order: null,
    order_by_limit: null,
    tablesample_percentage: null,
    tablesample_seed: null,
  };
}

/** Drain a producer stream turn by turn, returning every row. */
async function drain(session: StreamSession): Promise<Record<string, any>[]> {
  const out: Record<string, any>[] = [];
  const first = await (session as any).tickRaw();
  if (first?.batch) out.push(...iterRows(first.batch));
  for (let turn = 0; turn < 200 && (session as any)._stateToken != null; turn++) {
    const reply = await (session as any).exchangeRaw({ batch: batchFromColumns({}, schema([])), metadata: new Map() });
    if (reply?.batch) out.push(...iterRows(reply.batch));
  }
  return out;
}

async function refusal(fn: () => Promise<unknown>): Promise<any> {
  try {
    await fn();
  } catch (error) {
    return error;
  }
  throw new Error("expected a refusal");
}

describe("argument references over HTTP", () => {
  test("the worker advertises VGI-Arguments-Ref-Bytes (4 MiB)", async () => {
    const response = await fetch(`http://localhost:${server.port}/health`);
    expect(response.headers.get("VGI-Arguments-Ref-Bytes")).toBe(String(4 * 1024 * 1024));
  });

  test("a referenced argument binds, streams over many turns, and is fetched once", async () => {
    const payload = "p".repeat(65536);
    const batches = 12;
    const location = await upload(new Arguments([payload, BigInt(batches)]));
    const gets = store.gets;
    const call = bindCall("blob_param_batches", location);
    const bound = await bind(call);
    echoedCursorLengths.length = 0;
    const session = await rpc.stream("init", wrapRequest(serializeInitRequest(initRequest(call, bound))));
    const rows = await drain(session);

    const sha = createHash("sha256").update(payload).digest("hex");
    expect(rows.map((r) => Number(r.i))).toEqual(Array.from({ length: batches }, (_, i) => i));
    expect(rows.every((r) => Number(r.payload_len) === payload.length && r.payload_sha256 === sha)).toBe(true);
    // One storage GET for the bind, the init and every continuation: vgi-rpc's
    // object cache, keyed by object id.
    expect(store.gets - gets).toBe(1);
    // The cursor carries the init request holding the reference, never the
    // 64 KiB argument.
    expect(echoedCursorLengths.length).toBeGreaterThan(0);
    expect(Math.max(...echoedCursorLengths)).toBeLessThan(4096);
  });

  test("an object storage reports gone fails with the prepared-statement message", async () => {
    const location = await upload(new Arguments(["gone", 1n]));
    store.objects.delete(location.objectId);
    const error = await refusal(() => bind(bindCall("blob_param_batches", location)));
    expect(error.errorKind).toBe("arguments_ref_missing");
    expect(String(error.message)).toContain(ARGUMENTS_REF_MISSING_MESSAGE);
  });

  test("a tampered reference is invalid_arguments_ref", async () => {
    const location = await upload(new Arguments(["tampered", 1n]));
    const ref = location.ref.slice();
    ref[ref.length - 1]! ^= 0x01;
    const error = await refusal(() => bind(bindCall("blob_param_batches", { ...location, ref })));
    expect(error.errorKind).toBe("invalid_arguments_ref");
    expect(String(error.message)).toContain("invalid_arguments_ref");
  });

  test("a location whose size or digest differs from the grant is invalid_arguments_ref", async () => {
    const location = await upload(new Arguments(["sized", 1n]));
    const error = await refusal(() => bind(bindCall("blob_param_batches", { ...location, size: location.size + 1 })));
    expect(error.errorKind).toBe("invalid_arguments_ref");
  });

  for (const name of ["bind-both-null.arrow", "bind-both-set.arrow"]) {
    test(`${name} is refused with invalid_bind_request`, async () => {
      const bytes = new Uint8Array(readFileSync(new URL(`../../../conformance/nested-binary/${name}`, import.meta.url)));
      const error = await refusal(() => bind(deserializeBatch(bytes)));
      expect(error.errorKind).toBe("invalid_bind_request");
      expect(String(error.message)).toContain(INVALID_BIND_REQUEST_MESSAGE);
    });
  }
});
