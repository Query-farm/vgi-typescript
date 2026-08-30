// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Regression tests: calling a function via the wrong RPC method shape.
//
// Found live: a Python client called `elevation` (a blended row-transform /
// table-in-out function on a real deployed worker) via the plain producer
// path -- no input schema, no phase -- instead of an INPUT-phase exchange
// with an input stream. `createStreamHandlers` silently degraded the missing
// `input_schema` to an empty one (`request.bind_call.input_schema ?? undefined`),
// which disabled the transport layer's own schema-conformance check
// downstream. The result was not an error: it was a non-terminating
// continuation loop, since the worker kept issuing "not ready" tokens while
// the client kept polling, both sides correctly following their own local
// contract. These tests pin that the mismatch is now rejected immediately,
// with a message naming the fix -- in both directions, and for both the
// classic and blended (row-transform) table-in-out shapes.

import { describe, test, expect } from "bun:test";
import { schema, field, int64, float64 } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { FunctionType } from "../../types.js";
import type { BindRequest } from "../../protocol/types.js";
import type { InitRequest, GlobalInitResponse } from "../../protocol/types.js";
import { defineTableFunction } from "../table.js";
import { defineTableInOutFunction, defineRowTransformFunction } from "../table-in-out.js";

function makeBindRequest(overrides: Partial<BindRequest> = {}): BindRequest {
  return {
    function_name: "test_fn",
    arguments: new Arguments(),
    function_type: FunctionType.TABLE,
    input_schema: null,
    settings: null,
    secrets: null,
    attach_opaque_data: null,
    transaction_opaque_data: null,
    resolved_secrets_provided: false,
    ...overrides,
  };
}

function makeInitRequest(overrides: Partial<InitRequest> = {}): InitRequest {
  return {
    bind_call: makeBindRequest(),
    output_schema: schema([field("n", int64(), true)]),
    bind_opaque_data: null,
    projection_ids: null,
    pushdown_filters: null,
    join_keys: [],
    phase: null,
    finalize_state_id: null,
    split_tokens: null,
    split_payloads: null,
    row_limit: null,
    execution_id: null,
    init_opaque_data: null,
    substream_id: null,
    order_by_column_name: null,
    order_by_direction: null,
    order_by_null_order: null,
    order_by_limit: null,
    tablesample_percentage: null,
    tablesample_seed: null,
    ...overrides,
  };
}

const initResponse: GlobalInitResponse = {
  max_workers: 1,
  execution_id: new Uint8Array(16),
  opaque_data: null,
};

describe("table-in-out function called with no input schema (table_function() mismatch)", () => {
  test("classic defineTableInOutFunction rejects a missing input schema", () => {
    const fn = defineTableInOutFunction({
      name: "echo_test",
      onBind: () => ({ outputSchema: schema([field("n", int64(), true)]) }),
      process: () => {},
    });

    const request = makeInitRequest({
      bind_call: makeBindRequest({ function_name: "echo_test", input_schema: null }),
    });
    expect(() => fn.createStreamHandlers(request, initResponse)).toThrow(/table_in_out_function/);
  });

  test("blended defineRowTransformFunction rejects a missing input schema", () => {
    const fn = defineRowTransformFunction({
      name: "row_sum_test",
      args: { x: float64(), y: float64() },
      onBind: () => ({ outputSchema: schema([field("row_sum", float64(), true)]) }),
      process: () => {},
    } as any);

    const request = makeInitRequest({
      bind_call: makeBindRequest({ function_name: "row_sum_test", input_schema: null }),
    });
    expect(() => fn.createStreamHandlers(request, initResponse)).toThrow(/table_in_out_function/);
  });
});

describe("plain table function called with an init phase set (table_in_out_function() mismatch)", () => {
  test("defineTableFunction rejects a non-null phase", () => {
    const fn = defineTableFunction({
      name: "sequence_test",
      onBind: () => ({ outputSchema: schema([field("n", int64(), true)]) }),
      process: () => {},
    });

    const request = makeInitRequest({
      bind_call: makeBindRequest({ function_name: "sequence_test", input_schema: schema([field("n", int64(), true)]) }),
      phase: 0 as any, // TableInOutPhase.INPUT -- any non-null phase is the mismatch signal here
    });
    expect(() => fn.createStreamHandlers(request, initResponse)).toThrow(/table_in_out_function/);
  });
});
