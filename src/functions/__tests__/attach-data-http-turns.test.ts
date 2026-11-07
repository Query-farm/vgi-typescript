// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// What a function sees of its catalog's attach_opaque_data, on every HTTP turn.
//
// Over HTTP the worker seals attach_opaque_data in an AEAD envelope bound to
// the caller (crypto.ts). Function bodies must see the catalog's own bytes,
// and the init turn strips the envelope before any user code runs. The turns
// after it rebuild the stream from the cursor (function.ts, exchange) and used
// to hand createStreamHandlers the request exactly as carried: still sealed.
// A function reading `initCall.bind_call.attach_opaque_data` -- the documented
// way to read attach-time state (examples/attach-options-worker.ts) -- got the
// catalog's bytes on turn 1 and a 73-byte envelope from turn 2 on.
//
// The same rebuild also re-ran `initialState` on every turn and then threw its
// result away in favour of the carried state. An initialState that read the
// (sealed) attach data and validated it therefore failed every later turn, and
// one with side effects repeated them per turn.
//
// Driven end to end over HTTP with the real client against an in-process
// worker with a signing key, so every turn after the first is a rebuild.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { httpConnect } from "@query-farm/vgi-rpc";
import { batchFromColumns, field, int64, schema, utf8 } from "../../arrow/index.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import type { CatalogAttachResult } from "../../catalog/interface.js";
import { VgiClient } from "../../client/client.js";
import { SIGNING_KEY_BYTES, serveVgiWorker, type VgiHttpServer } from "../../serve-entry.js";
import { FunctionRegistry } from "../registry.js";
import { defineScalarFunction } from "../scalar.js";
import { defineTableFunction } from "../table.js";
import { defineTableInOutFunction } from "../table-in-out.js";
import { FunctionStorageSqlite, resolveStorageFromEnv, setStorage } from "../storage.js";

const CATALOG = "attachdemo";
const MARKER = new TextEncoder().encode("catalog-bytes:hello");
const decode = (bytes: Uint8Array | null | undefined) => (bytes ? new TextDecoder().decode(bytes) : null);

/** A catalog whose attach_opaque_data is a known plaintext. */
class MarkerCatalog extends ReadOnlyCatalogInterface {
  override async attach(
    name: string,
    options?: Record<string, unknown>,
    dataVersionSpec?: string | null,
    implementationVersion?: string | null,
    clientCapabilities?: Uint8Array | null,
  ): Promise<CatalogAttachResult> {
    const base = await super.attach(name, options, dataVersionSpec, implementationVersion, clientCapabilities);
    return { ...base, attach_opaque_data: MARKER };
  }
}

let initialStateCalls = 0;

// Table in-out: one output row per input batch, naming the attach it saw.
const attachEcho = defineTableInOutFunction<Record<string, never>, { seen: string | null; batches: number }>({
  name: "attach_echo",
  description: "Echo the attach bytes every turn sees",
  onBind: () => ({ outputSchema: schema([field("turn", int64(), false), field("attach", utf8(), true), field("from_state", utf8(), true)]) }),
  initialState: (params) => {
    initialStateCalls++;
    const seen = decode(params.initCall.bind_call.attach_opaque_data);
    // Validating attach-time state here is the natural thing to do.
    if (seen !== decode(MARKER)) throw new Error(`initialState saw attach ${JSON.stringify(seen)}`);
    return { seen, batches: 0 };
  },
  process: (params, state, _batch, out) => {
    state.batches++;
    out.emit(
      batchFromColumns(
        {
          turn: [BigInt(state.batches)],
          attach: [decode(params.initCall.bind_call.attach_opaque_data)],
          from_state: [state.seen],
        },
        params.outputSchema,
      ),
    );
  },
});

// Table function emitting one row per producer turn.
const attachTicks = defineTableFunction<Record<string, never>, { emitted: number }>({
  name: "attach_ticks",
  description: "One row per turn, naming the attach it saw",
  onBind: () => ({ outputSchema: schema([field("turn", int64(), false), field("attach", utf8(), true)]) }),
  initialState: () => ({ emitted: 0 }),
  process: (params, state, out) => {
    if (state.emitted >= 3) {
      out.finish();
      return;
    }
    state.emitted++;
    out.emit(
      batchFromColumns(
        { turn: [BigInt(state.emitted)], attach: [decode(params.initCall.bind_call.attach_opaque_data)] },
        params.outputSchema,
      ),
    );
  },
});

// Scalar: async compute that reads the attach bytes.
const attachScalar = defineScalarFunction({
  name: "attach_scalar",
  description: "Append the attach bytes to each value, asynchronously",
  params: { value: utf8() },
  returns: utf8(),
  compute: async (batch, _consts, info) => {
    await new Promise((resolve) => setTimeout(resolve, 1));
    const attach = decode(info.bindCall.attach_opaque_data);
    return Array.from(batch.getChildAt(0) ?? [], (v) => (v == null ? null : `${v}@${attach}`));
  },
});

let dir: string;
let server: VgiHttpServer;
let client: VgiClient;
let attach: Uint8Array;

beforeAll(async () => {
  dir = mkdtempSync(join(tmpdir(), "vgi-attach-turns-"));
  setStorage(new FunctionStorageSqlite(join(dir, "state.db")));
  const registry = new FunctionRegistry();
  const functions = [attachEcho, attachTicks, attachScalar];
  for (const fn of functions) registry.register(fn);
  server = serveVgiWorker({
    name: CATALOG,
    doc: "attach data across HTTP turns",
    version: "0.0.1",
    registry,
    catalogInterface: new MarkerCatalog({ name: CATALOG, schemas: [{ name: "main", functions }] }, registry),
    port: 0,
    signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(7),
    quiet: true,
    env: {},
  });
  client = new VgiClient(httpConnect(`http://localhost:${server.port}`, { prefix: "" }));
  attach = (await client.catalogAttach(CATALOG)).attach_opaque_data;
});

afterAll(() => {
  client?.close();
  server?.stop(true);
  setStorage(resolveStorageFromEnv());
  rmSync(dir, { recursive: true, force: true });
});

describe("attach_opaque_data over HTTP turns", () => {
  test("the client holds a sealed envelope, not the catalog's bytes", () => {
    expect(decode(attach)).not.toBe(decode(MARKER));
    expect(attach.length).toBeGreaterThan(MARKER.length);
  });

  test("a table in-out function sees the catalog's bytes on every turn, and initialState runs once", async () => {
    initialStateCalls = 0;
    const inputSchema = schema([field("x", int64(), false)]);
    const inputs = [1, 2, 3, 4].map((x) => batchFromColumns({ x: [BigInt(x)] }, inputSchema));
    const rows: Record<string, unknown>[] = [];
    for await (const page of client.tableInOutFunctionRows({
      functionName: "attach_echo",
      input: inputs,
      attachOpaqueData: attach,
    })) {
      rows.push(...page);
    }
    expect(rows.map((r) => BigInt(r.turn as number | bigint))).toEqual([1n, 2n, 3n, 4n]);
    expect(rows.map((r) => r.attach)).toEqual(Array(4).fill(decode(MARKER)));
    expect(rows.map((r) => r.from_state)).toEqual(Array(4).fill(decode(MARKER)));
    expect(initialStateCalls).toBe(1);
  });

  test("a table function sees the catalog's bytes on every producer turn", async () => {
    const rows: Record<string, unknown>[] = [];
    for await (const page of client.tableFunctionRows({
      functionName: "attach_ticks",
      attachOpaqueData: attach,
      int64: "bigint",
    })) {
      rows.push(...page);
    }
    expect(rows.map((r) => r.turn)).toEqual([1n, 2n, 3n]);
    expect(rows.map((r) => r.attach)).toEqual(Array(3).fill(decode(MARKER)));
  });

  test("an async scalar sees the catalog's bytes on every batch", async () => {
    const inputSchema = schema([field("value", utf8(), true)]);
    const inputs = [["a", null], ["b"], ["c"]].map((vals) => batchFromColumns({ value: vals }, inputSchema));
    const out: unknown[] = [];
    for await (const page of client.scalarFunctionRows({
      functionName: "attach_scalar",
      input: inputs,
      attachOpaqueData: attach,
    })) {
      out.push(...page.map((r) => r.result));
    }
    expect(out).toEqual([
      `a@${decode(MARKER)}`,
      null,
      `b@${decode(MARKER)}`,
      `c@${decode(MARKER)}`,
    ]);
  });
});
