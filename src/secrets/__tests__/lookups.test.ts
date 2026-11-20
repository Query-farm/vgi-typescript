// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// onBind secret lookups: the three lists are parallel and lookupSecretTypes
// drives the request (the extension reads one lookup per type). A name or
// scope with no type at its index used to be dropped silently, so the worker
// got no secret; it is now a bind error.

import { describe, test, expect } from "bun:test";
import { schema, field, int64 } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { FunctionType } from "../../types.js";
import type { BindRequest } from "../../protocol/types.js";
import { defineTableFunction } from "../../functions/table.js";
import { secretLookupWire, type SecretLookupFields } from "../lookups.js";

const out = schema([field("x", int64(), true)]);

function bindRequest(): BindRequest {
  return {
    function_name: "scan",
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

function scanReturning(lookups: SecretLookupFields) {
  return defineTableFunction({
    name: "scan",
    onBind: () => ({ outputSchema: out, ...lookups }),
    process: (_p, _s, o) => o.finish(),
  });
}

describe("secretLookupWire", () => {
  test("parallel lists pass through", () => {
    const wire = secretLookupWire("f", {
      lookupSecretTypes: ["shopify"],
      lookupScopes: [""],
      lookupNames: ["prod"],
    });
    expect(wire).toEqual({ lookup_secret_types: ["shopify"], lookup_scopes: [""], lookup_names: ["prod"] });
  });

  test("types alone, or shorter scope/name lists, are allowed (missing = unset)", () => {
    expect(() => secretLookupWire("f", { lookupSecretTypes: ["s3"] })).not.toThrow();
    expect(() => secretLookupWire("f", { lookupSecretTypes: ["s3", "gcs"], lookupNames: ["a"] })).not.toThrow();
    expect(() => secretLookupWire("f", {})).not.toThrow();
  });

  test("a name with no secret type is rejected, naming the fix", () => {
    expect(() => secretLookupWire("scan", { lookupNames: ["prod"] })).toThrow(
      /scan: onBind returned 1 lookupNames but 0 lookupSecretTypes/,
    );
  });

  test("more scopes than types is rejected", () => {
    expect(() =>
      secretLookupWire("scan", { lookupSecretTypes: ["s3"], lookupScopes: ["s3://a", "s3://b"] }),
    ).toThrow(/2 lookupScopes but 1 lookupSecretTypes/);
  });
});

describe("table function bind", () => {
  test("sends a name-scoped lookup", async () => {
    const resp = await scanReturning({
      lookupSecretTypes: ["shopify"],
      lookupScopes: [""],
      lookupNames: ["prod"],
    }).bind(bindRequest());
    expect(resp.lookup_secret_types).toEqual(["shopify"]);
    expect(resp.lookup_names).toEqual(["prod"]);
  });

  test("fails the bind instead of silently requesting nothing", async () => {
    await expect(scanReturning({ lookupNames: ["prod"] }).bind(bindRequest())).rejects.toThrow(
      /lookupSecretTypes/,
    );
  });
});
