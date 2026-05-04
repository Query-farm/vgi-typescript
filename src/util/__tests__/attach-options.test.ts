// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Unit tests for serializeAttachOptions — the JS→RecordBatch converter
// that powers VgiClient.catalogAttach({ options }). These tests don't
// need a running worker; they round-trip through deserializeBatch and
// inspect the column types + values.

import { describe, test, expect } from "bun:test";
import {
  Bool,
  Binary,
  Float64,
  Int64,
  Null,
  Utf8,
  DataType,
} from "@query-farm/apache-arrow";
import {
  inferAttachOptionArrowType,
  serializeAttachOptions,
} from "../attach-options.js";
import { deserializeBatch } from "../arrow/index.js";

describe("inferAttachOptionArrowType", () => {
  test("string → Utf8", () => {
    expect(DataType.isUtf8(inferAttachOptionArrowType("hello"))).toBe(true);
  });
  test("bigint → Int64", () => {
    expect(DataType.isInt(inferAttachOptionArrowType(42n))).toBe(true);
    expect((inferAttachOptionArrowType(42n) as any).bitWidth).toBe(64);
  });
  test("number → Float64", () => {
    expect(DataType.isFloat(inferAttachOptionArrowType(1.5))).toBe(true);
  });
  test("integer number still → Float64 (JS doesn't distinguish)", () => {
    expect(DataType.isFloat(inferAttachOptionArrowType(42))).toBe(true);
  });
  test("boolean → Bool", () => {
    expect(DataType.isBool(inferAttachOptionArrowType(true))).toBe(true);
  });
  test("Uint8Array → Binary", () => {
    expect(DataType.isBinary(inferAttachOptionArrowType(new Uint8Array([1, 2])))).toBe(true);
  });
  test("null → Null", () => {
    expect(DataType.isNull(inferAttachOptionArrowType(null))).toBe(true);
  });
  test("NaN/Infinity number throws", () => {
    expect(() => inferAttachOptionArrowType(Number.NaN)).toThrow(/finite/);
    expect(() => inferAttachOptionArrowType(Number.POSITIVE_INFINITY)).toThrow(/finite/);
    expect(() => inferAttachOptionArrowType(Number.NEGATIVE_INFINITY)).toThrow(/finite/);
  });
  test("symbol / function / object throws with helpful message", () => {
    // @ts-expect-error — intentionally passing unsupported types
    expect(() => inferAttachOptionArrowType(Symbol("x"))).toThrow(/Unsupported/);
    // @ts-expect-error
    expect(() => inferAttachOptionArrowType(() => 1)).toThrow(/Unsupported/);
    // @ts-expect-error
    expect(() => inferAttachOptionArrowType({ a: 1 })).toThrow(/Unsupported/);
  });
});

describe("serializeAttachOptions", () => {
  test("undefined or null → null (no batch produced)", () => {
    expect(serializeAttachOptions(undefined)).toBeNull();
    expect(serializeAttachOptions(null)).toBeNull();
  });

  test("empty object → null (zero-column IPC would fail downstream)", () => {
    expect(serializeAttachOptions({})).toBeNull();
  });

  test("single string option round-trips exactly", () => {
    const bytes = serializeAttachOptions({ region: "us-east-1" });
    expect(bytes).not.toBeNull();
    const batch = deserializeBatch(bytes!);
    expect(batch.schema.fields).toHaveLength(1);
    expect(batch.schema.fields[0].name).toBe("region");
    expect(batch.schema.fields[0].type).toBeInstanceOf(Utf8);
    expect(batch.numRows).toBe(1);
    expect(batch.getChild("region")?.get(0)).toBe("us-east-1");
  });

  test("mixed types get distinct column types + values round-trip", () => {
    const bytes = serializeAttachOptions({
      region: "us-east-1",
      maxRows: 1000n,
      timeout: 30.5,
      readOnly: true,
      token: new Uint8Array([0xca, 0xfe]),
    });
    const batch = deserializeBatch(bytes!);
    const byName = Object.fromEntries(batch.schema.fields.map((f) => [f.name, f.type]));
    // Use the DataType.is* predicates — arrow-js's deserializer returns
    // internal subclasses (e.g. `Int_`) that aren't `instanceof Int64`.
    expect(DataType.isUtf8(byName["region"])).toBe(true);
    expect(DataType.isInt(byName["maxRows"])).toBe(true);
    expect((byName["maxRows"] as any).bitWidth).toBe(64);
    expect(DataType.isFloat(byName["timeout"])).toBe(true);
    expect(DataType.isBool(byName["readOnly"])).toBe(true);
    expect(DataType.isBinary(byName["token"])).toBe(true);

    expect(batch.getChild("region")?.get(0)).toBe("us-east-1");
    expect(batch.getChild("maxRows")?.get(0)).toBe(1000n);
    expect(batch.getChild("timeout")?.get(0)).toBe(30.5);
    expect(batch.getChild("readOnly")?.get(0)).toBe(true);
    const token = batch.getChild("token")?.get(0);
    expect(token).toBeInstanceOf(Uint8Array);
    expect(Array.from(token as Uint8Array)).toEqual([0xca, 0xfe]);
  });

  test("null value produces a Null-typed column with null value", () => {
    const bytes = serializeAttachOptions({ maybe: null });
    const batch = deserializeBatch(bytes!);
    expect(DataType.isNull(batch.schema.fields[0].type)).toBe(true);
    expect(batch.getChild("maybe")?.get(0)).toBeNull();
  });

  test("fields preserve insertion order", () => {
    const bytes = serializeAttachOptions({ c: 3n, a: "1", b: true });
    const batch = deserializeBatch(bytes!);
    expect(batch.schema.fields.map((f) => f.name)).toEqual(["c", "a", "b"]);
  });

  test("column types are nullable (so future callers can pass null for a known-typed key)", () => {
    const bytes = serializeAttachOptions({ region: "us-east-1" });
    const batch = deserializeBatch(bytes!);
    expect(batch.schema.fields[0].nullable).toBe(true);
  });
});
