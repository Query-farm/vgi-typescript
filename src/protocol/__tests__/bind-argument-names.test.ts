// Copyright 2025, 2026 Query Farm LLC - https://query.farm

import { expect, test } from "bun:test";
import { deserializeBatch, iterRows, serializeBatch } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { FunctionType } from "../../types.js";
import { deserializeBindRequest, serializeBindRequest } from "../serializers/bind.js";
import type { BindRequest } from "../types.js";

test("BindRequest preserves complete argument names including unnamed varargs", () => {
  const request: BindRequest = {
    function_name: "probe",
    arguments: new Arguments(),
    function_type: FunctionType.SCALAR,
    input_schema: null,
    settings: null,
    secrets: null,
    attach_opaque_data: null,
    transaction_opaque_data: null,
    resolved_secrets_provided: false,
    argument_names: ["left", "right", null],
  };

  const batch = deserializeBatch(serializeBatch(serializeBindRequest(request)));
  const [row] = [...iterRows(batch)];
  expect(deserializeBindRequest(row).argument_names).toEqual(["left", "right", null]);
});
