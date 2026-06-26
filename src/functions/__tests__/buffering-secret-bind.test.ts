// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Conformance test: a table-buffering function can request DuckDB secrets via
// the two-phase secret bind, exactly like scalar/table functions.
//
// The buffering bind path previously dropped the onBind `lookupSecret*` return
// fields and never surfaced `resolvedSecretsProvided`, so a buffering sink
// could not authenticate to a cloud store from a DuckDB `CREATE SECRET`. These
// tests pin that the first bind pass emits the lookup request and the second
// pass (resolved_secrets_provided=true) binds normally.

import { describe, test, expect } from "bun:test";
import { schema, field, int64 } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { FunctionType } from "../../types.js";
import type { BindRequest } from "../../protocol/types.js";
import { defineTableBufferingFunction } from "../table-buffering.js";

function makeBindRequest(resolved: boolean): BindRequest {
  return {
    function_name: "secret_sink",
    arguments: new Arguments(),
    function_type: FunctionType.TABLE_BUFFERING,
    input_schema: null,
    settings: null,
    secrets: null,
    attach_opaque_data: null,
    transaction_opaque_data: null,
    resolved_secrets_provided: resolved,
  };
}

function makeSink() {
  return defineTableBufferingFunction({
    name: "secret_sink",
    onBind: (params) => {
      if (!params.resolvedSecretsProvided) {
        return {
          outputSchema: schema([field("x", int64(), true)]),
          lookupSecretTypes: ["s3"],
          lookupScopes: ["s3://bucket/out.dat"],
          lookupNames: [],
        };
      }
      return { outputSchema: schema([field("x", int64(), true)]) };
    },
    process: () => new Uint8Array(),
    combine: () => [],
    finalize: () => {},
  });
}

describe("table-buffering onBind two-phase secret lookup", () => {
  test("first pass returns the secret lookup request", async () => {
    const resp = await makeSink().bind(makeBindRequest(false));
    expect(resp.lookup_secret_types).toEqual(["s3"]);
    expect(resp.lookup_scopes).toEqual(["s3://bucket/out.dat"]);
  });

  test("second pass (resolved) binds with no further lookups", async () => {
    const resp = await makeSink().bind(makeBindRequest(true));
    expect(resp.lookup_secret_types ?? []).toEqual([]);
    expect((resp.output_schema as any).fields[0].name).toBe("x");
  });
});
