// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Conformance test: defineTableFunction.onBind may return a Promise.
//
// Until the recent async-bind patch, `onBind` was strictly synchronous —
// returning a Promise would cause `BindResponse.output_schema` to be a
// Promise instead of a Schema, which the bind handler then forwarded into
// `serializeSchema(promise)` and crashed the wire round-trip. This test pins
// that async-resolved schemas survive the bind path.

import { describe, test, expect } from "bun:test";
import { type VgiSchema, schema, type VgiField, field, type VgiDataType, int64 } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { FunctionType } from "../../types.js";
import type { BindRequest } from "../../protocol/types.js";
import { defineTableFunction } from "../table.js";

function makeBindRequest(): BindRequest {
  return {
    function_name: "async_bind",
    arguments: new Arguments(),
    function_type: FunctionType.TABLE,
    input_schema: null,
    settings: null,
    secrets: null,
    attach_opaque_data: null,
    transaction_opaque_data: null,
    resolved_secrets_provided: false,
  };
}

describe("table function onBind may be async", () => {
  test("Promise-returning onBind resolves to a real BindResponse", async () => {
    const fn = defineTableFunction({
      name: "async_bind",
      onBind: async () => {
        // Simulate an HTTP fetch that resolves on the next microtask.
        await Promise.resolve();
        return { outputSchema: schema([field("x", int64(), true)]) };
      },
      process: () => {},
    });

    const bindResp = await fn.bind(makeBindRequest());
    expect((bindResp.output_schema as any).fields[0].name).toBe("x");
  });

  test("Sync onBind still works (backwards compatibility)", async () => {
    const fn = defineTableFunction({
      name: "sync_bind",
      onBind: () => ({ outputSchema: schema([field("y", int64(), true)]) }),
      process: () => {},
    });

    const bindResp = await fn.bind(makeBindRequest());
    expect((bindResp.output_schema as any).fields[0].name).toBe("y");
  });
});
