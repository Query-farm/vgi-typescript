// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// The SDK's own errors carry the canonical vgi-rpc code (WIRE_PROTOCOL.md §8),
// so a client — and DuckDB's errors_as_json — can tell "your input was wrong"
// from a worker bug. The mapping is shared with every other VGI SDK.
//
// End to end: the example worker's catalog is served in-process over HTTP and
// driven through VgiClient; the code is read off the decoded vgi-rpc RpcError.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { errorCodeOf, httpConnect } from "@query-farm/vgi-rpc";
import { RpcError } from "@query-farm/vgi-rpc/connect";
import { batchFromColumns, field, schema, utf8 } from "../arrow/index.js";
import { Arguments } from "../arguments/arguments.js";
import { ReadOnlyCatalogInterface } from "../catalog/read-only.js";
import { FunctionRegistry } from "../functions/registry.js";
import { createVgiFetch } from "../http/fetch.js";
import { VgiClient } from "../client/client.js";
import {
  ArgumentValidationError,
  CatalogAlreadyExistsError,
  CatalogNotFoundError,
  CatalogReadOnlyError,
  FunctionNotFoundError,
  NoCatalogError,
  RowCountMismatchError,
  VgiError,
} from "../errors.js";
import { allFunctions, catalog } from "../../examples/common.js";

describe("SDK error classes declare their canonical code", () => {
  test.each([
    [new ArgumentValidationError("bad"), "INVALID_ARGUMENT"],
    [new FunctionNotFoundError("f"), "NOT_FOUND"],
    [new CatalogNotFoundError("Table", "t"), "NOT_FOUND"],
    [new CatalogAlreadyExistsError("Table", "t"), "ALREADY_EXISTS"],
    [new CatalogReadOnlyError("table_create"), "FAILED_PRECONDITION"],
    [new NoCatalogError(), "UNIMPLEMENTED"],
    // A worker bug, not the caller's fault: left unclassified.
    [new RowCountMismatchError(2, 3), "UNKNOWN"],
    [new VgiError("plain"), "UNKNOWN"],
    [new VgiError("lookup", "NOT_FOUND"), "NOT_FOUND"],
  ] as const)("%p -> %s", (err, code) => {
    expect(errorCodeOf(err)).toBe(code);
  });

  test("class identities are unchanged", () => {
    const e = new ArgumentValidationError("bad");
    expect(e).toBeInstanceOf(ArgumentValidationError);
    expect(e).toBeInstanceOf(VgiError);
    expect(e).toBeInstanceOf(Error);
    expect(e.name).toBe("ArgumentValidationError");
  });
});

let server: ReturnType<typeof Bun.serve>;
let baseUrl: string;

beforeAll(() => {
  const registry = new FunctionRegistry();
  for (const f of allFunctions) registry.register(f);
  const catalogInterface = new ReadOnlyCatalogInterface(catalog, registry);
  const fetch = createVgiFetch({
    protocol: { registry, catalogInterface },
    signingKey: new Uint8Array(32).fill(5),
    prefix: "",
    landingInfo: { name: "example", doc: "error-code test", version: "0.0.0" },
  });
  server = Bun.serve({ port: 0, fetch });
  baseUrl = `http://localhost:${server.port}`;
});

afterAll(() => {
  server?.stop(true);
});

/** Run `fn` against the example catalog and return the decoded wire error code. */
async function wireCode(fn: (client: VgiClient, attach: Uint8Array) => Promise<void>): Promise<{ code: string; message: string }> {
  const rpc = httpConnect(baseUrl, { prefix: "" });
  try {
    const client = new VgiClient(rpc);
    const { attach_opaque_data } = await client.catalogAttach("example");
    await fn(client, attach_opaque_data);
  } catch (e) {
    let cur: unknown = e;
    while (cur && !(cur instanceof RpcError)) cur = (cur as { cause?: unknown }).cause;
    if (!(cur instanceof RpcError)) throw e;
    return { code: cur.errorCode, message: cur.errorMessage };
  } finally {
    rpc.close();
  }
  throw new Error("expected the call to fail");
}

describe("example worker: input errors reach the client as INVALID_ARGUMENT", () => {
  test("double('abc') — scalar type rejection", async () => {
    const input = schema([field("x", utf8(), true)]);
    const { code, message } = await wireCode(async (client, attach) => {
      for await (const _ of client.scalarFunctionRows({
        functionName: "double",
        input: [batchFromColumns({ x: ["abc"] }, input)],
        attachOpaqueData: attach,
      })) {
        // drain
      }
    });
    expect(message).toContain("Unsupported numeric type for addition");
    expect(code).toBe("INVALID_ARGUMENT");
  });

  test("sequence(10, batch_size := 0) — argument constraint", async () => {
    const { code, message } = await wireCode(async (client, attach) => {
      for await (const _ of client.tableFunctionRows({
        functionName: "sequence",
        arguments: new Arguments([10n], new Map([["batch_size", 0n]])),
        attachOpaqueData: attach,
      })) {
        // drain
      }
    });
    expect(message).toContain("must be >= 1");
    expect(code).toBe("INVALID_ARGUMENT");
  });

  test("an unknown function is NOT_FOUND", async () => {
    const { code } = await wireCode(async (client, attach) => {
      for await (const _ of client.tableFunctionRows({ functionName: "no_such_function", attachOpaqueData: attach })) {
        // drain
      }
    });
    expect(code).toBe("NOT_FOUND");
  });
});
