// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// The init request an HTTP stream carries in its cursor is packed once and
// parsed once per process.
//
// Every HTTP turn used to re-parse the whole init request out of the cursor
// (~0.9 ms of a ~4 ms 1000-row turn), and the cursor carried it raw (~9 KB
// for a DuckDB scan), which every turn then re-sealed and base64-encoded.
// carried-init-request.ts compresses it once at /init and keeps a small
// process memo from the packed bytes to their parse.
//
// The HTTP half drives the real `sequence` fixture through the real in-process
// HTTP stack; the requests are hand-built, every response is the worker's.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { httpConnect, type RpcClient, STATE_KEY } from "@query-farm/vgi-rpc";
import { tableFunctions } from "../../../examples/table.js";
import { deserializeBatch, field, float64, int64, schema, serializeBatch, utf8 } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { unwrapResult, wrapRequest } from "../../client/protocol.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { SIGNING_KEY_BYTES, serveVgiWorker, type VgiHttpServer } from "../../serve-entry.js";
import { FunctionType } from "../../types.js";
import { batchFromColumns, batchToScalarDict } from "../../util/arrow/index.js";
import { carriedInitRequestStats, packInitRequest, parseCarriedInitRequest } from "../carried-init-request.js";
import {
  deserializeBindResponse,
  deserializeInitRequest,
  serializeBindRequest,
  serializeInitRequest,
} from "../serialize.js";
import type { BindRequest, InitRequest } from "../types.js";

/** A bind call shaped like DuckDB's: arguments plus a settings batch. */
function bindCall(count: number): BindRequest {
  return {
    function_name: "sequence",
    arguments: new Arguments([count], new Map([["batch_size", 100]])),
    function_type: FunctionType.TABLE,
    input_schema: null,
    settings: batchFromColumns(
      { greeting: ["hello"], multiplier: [2n], scale_factor: [1.5], threshold: [10n], vgi_verbose_mode: ["off"] },
      schema([
        field("greeting", utf8(), true),
        field("multiplier", int64(), true),
        field("scale_factor", float64(), true),
        field("threshold", int64(), true),
        field("vgi_verbose_mode", utf8(), true),
      ]),
    ),
    secrets: null,
    attach_opaque_data: null,
    transaction_opaque_data: null,
    resolved_secrets_provided: false,
  };
}

function initRequest(bind: BindRequest, outputSchema = schema([field("n", int64(), true)])): InitRequest {
  return {
    bind_call: bind,
    output_schema: outputSchema,
    bind_opaque_data: null,
    split_tokens: null,
    row_limit: null,
    projection_ids: null,
    pushdown_filters: null,
    join_keys: [],
    phase: null,
    finalize_state_id: null,
    order_by_column_name: null,
    order_by_direction: null,
    order_by_null_order: null,
    order_by_limit: null,
    tablesample_percentage: null,
    tablesample_seed: null,
    execution_id: null,
    init_opaque_data: null,
    substream_id: null,
  };
}

const ipcOf = (req: InitRequest) => serializeBatch(serializeInitRequest(req));

describe("packing", () => {
  test("an init request is compressed in the cursor", () => {
    const raw = ipcOf(initRequest(bindCall(1000)));
    const packed = packInitRequest(raw);
    // zstd on Bun: Arrow IPC framing (schemas, padding) compresses ~3x.
    expect(packed.byteLength).toBeLessThan(raw.byteLength / 2);
  });

  test("a packed request parses to what the raw one does", () => {
    const req = initRequest(bindCall(1234));
    const raw = ipcOf(req);
    const direct = deserializeInitRequest(batchToScalarDict(deserializeBatch(raw)));
    const { request } = parseCarriedInitRequest(packInitRequest(raw));
    expect(request.bind_call.function_name).toBe("sequence");
    expect(request.bind_call.arguments.get(0)).toEqual(direct.bind_call.arguments.get(0));
    expect(request.bind_call.arguments.get("batch_size")).toEqual(direct.bind_call.arguments.get("batch_size"));
    expect(request.output_schema.fields.map((f) => f.name)).toEqual(["n"]);
    expect(batchToScalarDict(request.bind_call.settings!)).toEqual(batchToScalarDict(direct.bind_call.settings!));
  });
});

describe("the parse memo", () => {
  test("the same packed bytes are parsed once; each call gets its own top-level objects", () => {
    const packed = packInitRequest(ipcOf(initRequest(bindCall(4321))));
    const before = carriedInitRequestStats().parses;
    const first = parseCarriedInitRequest(packed);
    // A fresh copy of the bytes, as a new turn's cursor would decode them.
    const second = parseCarriedInitRequest(packed.slice());
    expect(carriedInitRequestStats().parses - before).toBe(1);
    expect(second.request).not.toBe(first.request);
    expect(second.request.bind_call).not.toBe(first.request.bind_call);
    expect(second.request.output_schema).toBe(first.request.output_schema);
    // What the dispatcher assigns per turn stays on that turn's object.
    first.request.split_payloads = [new Uint8Array([1])];
    expect(parseCarriedInitRequest(packed).request.split_payloads).toBeUndefined();
  });

  test("different bytes are a different parse", () => {
    const a = parseCarriedInitRequest(packInitRequest(ipcOf(initRequest(bindCall(1)))));
    const b = parseCarriedInitRequest(packInitRequest(ipcOf(initRequest(bindCall(2)))));
    expect(a.request.bind_call.arguments.get(0)).not.toEqual(b.request.bind_call.arguments.get(0));
  });

  test("an unknown codec is refused", () => {
    expect(() => parseCarriedInitRequest(new Uint8Array([0xff, 1, 2, 3]))).toThrow(/unknown codec/);
  });
});

describe("over HTTP", () => {
  let server: VgiHttpServer;
  let rpc: RpcClient;
  const echoedCursorLengths: number[] = [];

  beforeAll(() => {
    const registry = new FunctionRegistry();
    const sequence = tableFunctions.find((f) => f.meta.name === "sequence")!;
    registry.register(sequence);
    server = serveVgiWorker({
      name: "demo",
      doc: "Carried init request test worker.",
      version: "0.0.1",
      registry,
      catalogInterface: new ReadOnlyCatalogInterface(
        { name: "demo", schemas: [{ name: "main", functions: [sequence] }] },
        registry,
      ),
      port: 0,
      signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(5),
      quiet: true,
      env: {},
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

  afterAll(() => server?.stop(true));

  test("a 20-turn scan parses its init request once, and the cursor is smaller than the request", async () => {
    const bind = bindCall(2000);
    const bound = deserializeBindResponse(
      unwrapResult((await rpc.call("bind", wrapRequest(serializeBindRequest(bind)))) as Record<string, any>),
    );
    const req = initRequest(bind, bound.output_schema);
    const rawLength = ipcOf(req).byteLength;
    const before = carriedInitRequestStats();
    const session = await rpc.stream("init", wrapRequest(serializeInitRequest(req)));
    let rows = 0;
    try {
      for await (const batch of session) rows += batch.length;
    } finally {
      session.close();
    }
    const after = carriedInitRequestStats();
    expect(rows).toBe(2000);
    // 20 batches of 100: the init turn plus 19 continuations (and the final,
    // finishing one). Only the first continuation parses.
    expect(after.parses - before.parses).toBe(1);
    expect(after.hits - before.hits).toBeGreaterThanOrEqual(18);
    // The cursor carries the request compressed: before, it carried it raw
    // and was always larger than the request itself.
    expect(echoedCursorLengths.length).toBeGreaterThanOrEqual(19);
    expect(Math.max(...echoedCursorLengths)).toBeLessThan(rawLength);
  });
});
