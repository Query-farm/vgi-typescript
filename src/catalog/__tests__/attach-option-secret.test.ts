// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Unit tests for the `secret` attach-option flag — the wire column and its
// round trip. The contract is shared with vgi-python, vgi-go and the C++
// extension: a nullable Bool column "secret" appended after "required",
// looked up by name, with absent or null reading as false.

import { describe, test, expect } from "bun:test";
import {
  type AttachOptionSpec,
  deserializeAttachOptionSpec,
  deserializeAttachOptionSpecs,
  serializeAttachOptionSpec,
  serializeAttachOptionSpecs,
  validateRequiredAttachOptions,
} from "../attach-option.js";
import {
  batchFromColumns,
  deserializeBatch,
  iterRows,
  schema as makeSchema,
  field,
  binary,
  serializeBatch,
  serializeSchema,
  utf8,
  bool,
  type VgiField,
} from "../../arrow/index.js";

/** Read the single serialized spec row back as a plain object, through the
 *  facade so this holds on either Arrow backend. */
function wireRow(spec: AttachOptionSpec): Record<string, unknown> {
  const rows = [...iterRows(deserializeBatch(serializeAttachOptionSpec(spec)))];
  expect(rows).toHaveLength(1);
  return rows[0];
}

/** Build a spec batch the way a peer that predates `secret` (or `required`)
 *  would: only the columns named, with `secretValue` optionally present as a
 *  nullable Bool column. */
function legacySpecBytes(opts: { withRequired: boolean; secretValue?: boolean | null }): Uint8Array {
  const typeBytes = serializeSchema(makeSchema([field("value", utf8(), true)]));
  const fields: VgiField[] = [
    field("name", utf8(), false),
    field("description", utf8(), false),
    field("type", binary(), false),
    field("default_value", binary(), true),
  ];
  const columns: Record<string, unknown[]> = {
    name: ["api_key"],
    description: ["API key"],
    type: [typeBytes],
    default_value: [null],
  };
  if (opts.withRequired) {
    fields.push(field("required", bool(), true));
    columns.required = [true];
  }
  if (opts.secretValue !== undefined) {
    fields.push(field("secret", bool(), true));
    columns.secret = [opts.secretValue];
  }
  return serializeBatch(batchFromColumns(columns, makeSchema(fields)));
}

describe("secret on the wire", () => {
  test("defaults to false rather than null", () => {
    const row = wireRow({ name: "region", description: "Region", type: utf8(), default: "us-east-1" });
    expect(row.secret).toBe(false);
  });

  test("secret: true survives the wire", () => {
    const row = wireRow({ name: "api_key", description: "API key", type: utf8(), secret: true });
    expect(row.secret).toBe(true);
  });

  test("secret is appended after required", () => {
    // Column order is the compatibility contract: peers read by name, and a
    // peer predating `secret` simply doesn't see it.
    const batch = deserializeBatch(
      serializeAttachOptionSpec({ name: "api_key", description: "API key", type: utf8(), secret: true }),
    );
    expect(batch.schema.fields.map((f) => f.name)).toEqual([
      "name",
      "description",
      "type",
      "default_value",
      "required",
      "secret",
    ]);
    const secretField = batch.schema.fields.find((f) => f.name === "secret")!;
    expect(secretField.nullable).toBe(true);
  });
});

describe("secret round trip", () => {
  test.each([true, false])("secret: %p round-trips", (secret) => {
    const spec = deserializeAttachOptionSpec(
      serializeAttachOptionSpec({ name: "api_key", description: "API key", type: utf8(), secret }),
    );
    expect(spec.secret).toBe(secret);
    expect(spec.required).toBe(false);
  });

  test("an unset flag deserializes as false", () => {
    const spec = deserializeAttachOptionSpec(
      serializeAttachOptionSpec({ name: "region", description: "Region", type: utf8() }),
    );
    expect(spec.secret).toBe(false);
  });

  test("a batch without the secret column reads false", () => {
    const spec = deserializeAttachOptionSpec(legacySpecBytes({ withRequired: true }));
    expect(spec.secret).toBe(false);
    expect(spec.required).toBe(true);
  });

  test("a batch with neither required nor secret reads both false", () => {
    const spec = deserializeAttachOptionSpec(legacySpecBytes({ withRequired: false }));
    expect(spec.secret).toBe(false);
    expect(spec.required).toBe(false);
  });

  test("an explicit null secret reads false", () => {
    const spec = deserializeAttachOptionSpec(legacySpecBytes({ withRequired: true, secretValue: null }));
    expect(spec.secret).toBe(false);
  });

  test("a peer's secret: true is read by name", () => {
    const spec = deserializeAttachOptionSpec(legacySpecBytes({ withRequired: false, secretValue: true }));
    expect(spec.secret).toBe(true);
    expect(spec.required).toBe(false);
  });

  test("secret with a default is allowed and keeps both", () => {
    const spec = deserializeAttachOptionSpec(
      serializeAttachOptionSpec({ name: "token", description: "Token", type: utf8(), secret: true, default: "dev" }),
    );
    expect(spec.secret).toBe(true);
    expect(spec.default).toBe("dev");
  });
});

describe("secret combined with required", () => {
  test.each([
    [true, true],
    [true, false],
    [false, true],
    [false, false],
  ])("required: %p, secret: %p round-trip independently", (required, secret) => {
    const spec = deserializeAttachOptionSpec(
      serializeAttachOptionSpec({ name: "api_key", description: "API key", type: utf8(), required, secret }),
    );
    expect(spec.required).toBe(required);
    expect(spec.secret).toBe(secret);
  });

  test("a required secret option is still enforced at attach", () => {
    const specs = deserializeAttachOptionSpecs(
      serializeAttachOptionSpecs([
        { name: "api_key", description: "API key", type: utf8(), required: true, secret: true },
        { name: "region", description: "Region", type: utf8(), default: "us-east-1" },
      ]),
    );
    expect(specs.map((s) => [s.name, s.required, s.secret])).toEqual([
      ["api_key", true, true],
      ["region", false, false],
    ]);
    expect(() => validateRequiredAttachOptions("gated", specs, {})).toThrow(/required option 'api_key'/);
    expect(() => validateRequiredAttachOptions("gated", specs, { api_key: "sk-test" })).not.toThrow();
  });

  test("secret does not make an option required", () => {
    const specs: AttachOptionSpec[] = [
      { name: "token", description: "Optional token", type: utf8(), secret: true },
    ];
    expect(() => validateRequiredAttachOptions("open", specs, {})).not.toThrow();
  });
});
