// Conformance test: scalar function outputType callback sees the bind
// request's attach_id and transaction_id via params.bindCall. When a scalar
// is invoked through an ATTACHed catalog the C++ extension forwards those
// fields on the bind RPC, and the scalar surface must expose them so the
// function can route to the right backend.

import { describe, test, expect } from "bun:test";
import { type VgiSchema, schema, type VgiField, field, type VgiDataType, int64 } from "../../arrow/index.js";
import { defineScalarFunction, type ScalarBindParameters } from "../scalar.js";
import { Arguments } from "../../arguments/arguments.js";
import { FunctionType } from "../../types.js";
import type { BindRequest } from "../../protocol/types.js";

function makeBindRequest(opts: {
  attach_id?: Uint8Array | null;
  transaction_id?: Uint8Array | null;
}): BindRequest {
  return {
    function_name: "attach_id_echo",
    arguments: new Arguments(),
    function_type: FunctionType.SCALAR,
    input_schema: schema([field("x", int64(), true)]),
    settings: null,
    secrets: null,
    attach_id: opts.attach_id ?? null,
    transaction_id: opts.transaction_id ?? null,
    resolved_secrets_provided: false,
  };
}

describe("scalar bind exposes attach_id / transaction_id", () => {
  let captured: ScalarBindParameters | null = null;

  const fn = defineScalarFunction({
    name: "attach_id_echo",
    params: { x: int64() },
    outputType: (params: ScalarBindParameters): VgiDataType => {
      captured = params;
      return int64();
    },
    compute: () => [],
  });

  test("forwards attach_id when present", () => {
    captured = null;
    const attach = new Uint8Array(16).fill(0xaa);
    fn.bind!(makeBindRequest({ attach_id: attach }));
    expect(captured).not.toBeNull();
    expect(captured!.bindCall.attach_id).toEqual(attach);
    expect(captured!.bindCall.transaction_id).toBeNull();
  });

  test("forwards transaction_id when present", () => {
    captured = null;
    const attach = new Uint8Array(16).fill(0xaa);
    const tx = new Uint8Array(8).fill(0xbb);
    fn.bind!(makeBindRequest({ attach_id: attach, transaction_id: tx }));
    expect(captured!.bindCall.attach_id).toEqual(attach);
    expect(captured!.bindCall.transaction_id).toEqual(tx);
  });

  test("both null when invoked outside a catalog", () => {
    captured = null;
    fn.bind!(makeBindRequest({}));
    expect(captured!.bindCall.attach_id).toBeNull();
    expect(captured!.bindCall.transaction_id).toBeNull();
  });
});
