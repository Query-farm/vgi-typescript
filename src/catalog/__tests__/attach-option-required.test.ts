// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Unit tests for the `required` attach-option machinery — the wire column and
// the ATTACH-time validator. Mirrors vgi-python's tests/catalog/test_attach_option.py
// so both implementations enforce the same contract.

import { describe, test, expect } from "bun:test";
import { utf8 } from "../../arrow/index.js";
import {
  type AttachOptionSpec,
  MissingAttachOptionsError,
  serializeAttachOptionSpec,
  validateRequiredAttachOptions,
} from "../attach-option.js";
import { deserializeBatch } from "../../util/arrow/index.js";

/** Read the single serialized spec row back as a plain object. */
function roundTrip(spec: AttachOptionSpec): Record<string, unknown> {
  const batch = deserializeBatch(serializeAttachOptionSpec(spec));
  const row = batch.get(0);
  expect(row).not.toBeNull();
  return (row as { toJSON(): Record<string, unknown> }).toJSON();
}

describe("required on the wire", () => {
  test("defaults to false rather than null", () => {
    // The extension does `SELECT DISTINCT opt.required` and expects `false`,
    // so an unset flag must serialize explicitly, not as NULL.
    const row = roundTrip({ name: "region", description: "Region", type: utf8(), default: "us-east-1" });
    expect(row.required).toBe(false);
  });

  test("required survives the round trip", () => {
    const row = roundTrip({ name: "api_key", description: "API key", type: utf8(), required: true });
    expect(row.required).toBe(true);
  });

  test("a required option carries no default", () => {
    const row = roundTrip({ name: "api_key", description: "API key", type: utf8(), required: true });
    expect(row.default_value).toBeNull();
  });

  test("required + default is rejected as a declaration bug", () => {
    expect(() =>
      serializeAttachOptionSpec({
        name: "api_key",
        description: "API key",
        type: utf8(),
        required: true,
        default: "fallback",
      }),
    ).toThrow(/required but also declares a default/);
  });

  test("required is appended last, after the shared four columns", () => {
    // Column order is the compatibility contract: a peer predating `required`
    // reads the batch by name and simply doesn't see it.
    const batch = deserializeBatch(
      serializeAttachOptionSpec({ name: "region", description: "Region", type: utf8() }),
    );
    expect(batch.schema.fields.map((f) => f.name)).toEqual([
      "name",
      "description",
      "type",
      "default_value",
      "required",
    ]);
  });
});

describe("validateRequiredAttachOptions", () => {
  const specs: AttachOptionSpec[] = [
    { name: "api_key", description: "API key", type: utf8(), required: true },
    { name: "region", description: "Region", type: utf8(), default: "us-east-1" },
  ];

  test("missing required option throws with the name in the message", () => {
    expect(() => validateRequiredAttachOptions("gated", specs, {})).toThrow(
      /required option 'api_key'/,
    );
  });

  test("the error carries the names without message parsing", () => {
    try {
      validateRequiredAttachOptions("gated", specs, {});
      throw new Error("expected a throw");
    } catch (e) {
      expect(e).toBeInstanceOf(MissingAttachOptionsError);
      expect((e as MissingAttachOptionsError).missing).toEqual(["api_key"]);
    }
  });

  test("supplying the option passes", () => {
    expect(() => validateRequiredAttachOptions("gated", specs, { api_key: "secret" })).not.toThrow();
  });

  test("option names match case-insensitively, as DuckDB treats ATTACH keys", () => {
    expect(() => validateRequiredAttachOptions("gated", specs, { API_KEY: "secret" })).not.toThrow();
  });

  test("no required specs always passes", () => {
    expect(() => validateRequiredAttachOptions("plain", [specs[1]!], {})).not.toThrow();
  });

  test("multiple missing options are reported together and pluralized", () => {
    const two: AttachOptionSpec[] = [
      { name: "api_key", description: "API key", type: utf8(), required: true },
      { name: "tenant", description: "Tenant", type: utf8(), required: true },
    ];
    try {
      validateRequiredAttachOptions("gated", two, {});
      throw new Error("expected a throw");
    } catch (e) {
      expect((e as MissingAttachOptionsError).missing).toEqual(["api_key", "tenant"]);
      expect((e as Error).message).toMatch(/required options 'api_key', 'tenant'/);
    }
  });
});
