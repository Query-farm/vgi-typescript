// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// vgi.v2 is one protocol with one surface in every SDK ("the protocol is the
// unit of optionality"), so vgi_rpc.Reflection.v1 must report the reference's
// vgi.v2 hash here too. A method added, dropped, renamed, or a field whose
// name, order, type or nullability drifts from the reference changes the hash
// and fails this test. Driven end to end over a listening HTTP server through
// vgi-rpc's own reflection client, so it measures what a caller sees.
//
// The 13 methods this SDK does not implement are registered anyway
// (handlers/unimplemented.ts) and must refuse with UNIMPLEMENTED /
// method_not_implemented, never succeed silently.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { httpConnect, httpIntrospect, RpcError } from "@query-farm/vgi-rpc";
import { isBinary, isBool, isList } from "../../arrow/index.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { createVgiFetch } from "../../http/fetch.js";
import { VGI_PROTOCOL_NAME, VGI_V2_PROTOCOL_HASH } from "../dispatch.js";
import { UNIMPLEMENTED_METHODS, unimplementedMessage } from "../handlers/unimplemented.js";

/** The reference (vgi-python 0.43.0) hosts exactly this many vgi.v2 methods. */
const REFERENCE_METHOD_COUNT = 72;

let server: { port: number; stop(force?: boolean): void };
let baseUrl: string;

beforeAll(() => {
  const fetch = createVgiFetch({
    protocol: { registry: new FunctionRegistry() },
    signingKey: new Uint8Array(32).fill(3),
    prefix: "",
    serverId: "vgi-v2-hash-test",
    landingInfo: { name: "hash", doc: "vgi.v2 surface test.", version: "0" },
  });
  server = (globalThis as any).Bun.serve({ port: 0, fetch });
  baseUrl = `http://localhost:${server.port}`;
});

afterAll(() => server?.stop(true));

describe("vgi.v2 surface", () => {
  test("reflection reports the reference vgi.v2 hash and method count", async () => {
    const desc = await httpIntrospect(baseUrl, { protocol: VGI_PROTOCOL_NAME });
    expect(desc.protocolName).toBe(VGI_PROTOCOL_NAME);
    expect(desc.methods.length).toBe(REFERENCE_METHOD_COUNT);
    expect(desc.protocolHash).toBe(VGI_V2_PROTOCOL_HASH);
  });

  test("every unimplemented method refuses with UNIMPLEMENTED", async () => {
    const rpc = httpConnect(baseUrl, { protocol: VGI_PROTOCOL_NAME });
    try {
      for (const m of UNIMPLEMENTED_METHODS) {
        // The handler refuses before reading its params, so any well-typed
        // placeholder will do; build one from the declared schema.
        const params: Record<string, unknown> = {};
        for (const f of m.params.fields) {
          params[f.name] = f.nullable
            ? null
            : isList(f.type)
              ? []
              : isBool(f.type)
                ? false
                : isBinary(f.type)
                  ? new Uint8Array(0)
                  : "";
        }
        let caught: unknown;
        try {
          await rpc.call(m.name, params);
        } catch (e) {
          caught = e;
        }
        expect(caught, m.name).toBeInstanceOf(RpcError);
        const err = caught as RpcError;
        expect(err.errorCode, m.name).toBe("UNIMPLEMENTED");
        expect(err.errorKind, m.name).toBe("method_not_implemented");
        expect(err.errorMessage, m.name).toContain(unimplementedMessage(m.name));
      }
    } finally {
      rpc.close();
    }
  });
});
