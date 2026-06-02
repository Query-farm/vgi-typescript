// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import { describe, test, expect } from "bun:test";
import { inferFieldType, arrowStateSerializer } from "../state-serializer.js";
import { type VgiDataType } from "../../arrow/index.js";
// ============================================================================
// inferFieldType
// ============================================================================

describe("inferFieldType", () => {
  test("null → Null", () => {
    expect(inferFieldType(null)).toMatchObject({ typeId: expect.any(Number) });
  });

  test("undefined → Null", () => {
    expect(inferFieldType(undefined)).toMatchObject({ typeId: expect.any(Number) });
  });

  test("number → Float64", () => {
    expect(inferFieldType(42)).toMatchObject({ typeId: expect.any(Number) });
    expect(inferFieldType(3.14)).toMatchObject({ typeId: expect.any(Number) });
  });

  test("bigint → Int64", () => {
    expect(inferFieldType(42n)).toMatchObject({ typeId: expect.any(Number) });
  });

  test("string → Utf8", () => {
    expect(inferFieldType("hello")).toMatchObject({ typeId: expect.any(Number) });
  });

  test("boolean → Bool", () => {
    expect(inferFieldType(true)).toMatchObject({ typeId: expect.any(Number) });
  });

  test("Uint8Array → Binary", () => {
    expect(inferFieldType(new Uint8Array([1, 2, 3]))).toMatchObject({ typeId: expect.any(Number) });
  });

  test("ArrayBuffer → Binary", () => {
    expect(inferFieldType(new ArrayBuffer(4))).toMatchObject({ typeId: expect.any(Number) });
  });

  test("plain object → Struct", () => {
    const type = inferFieldType({ a: 1, b: "x" });
    expect(type).toMatchObject({ typeId: expect.any(Number) });
    expect((type as any).children.length).toBe(2);
    expect((type as any).children[0].name).toBe("a");
    expect((type as any).children[1].name).toBe("b");
  });

  test("Array throws", () => {
    expect(() => inferFieldType([1, 2, 3])).toThrow(/arrays are not supported/);
  });

  test("Map throws", () => {
    expect(() => inferFieldType(new Map())).toThrow(/Map is not supported/);
  });

  test("Set throws", () => {
    expect(() => inferFieldType(new Set())).toThrow(/Set is not supported/);
  });

  test("Date throws", () => {
    expect(() => inferFieldType(new Date())).toThrow(/Date is not supported/);
  });

  test("RegExp throws", () => {
    expect(() => inferFieldType(/foo/)).toThrow(/RegExp is not supported/);
  });
});

// ============================================================================
// arrowStateSerializer round-trips
// ============================================================================

describe("arrowStateSerializer", () => {
  const baseState = {
    functionName: "test_func",
    initRequestIpc: new Uint8Array([0xde, 0xad]),
    executionId: new Uint8Array([0xbe, 0xef]),
    maxWorkers: 4,
    opaqueData: null,
    isProducer: false,
  };

  test("null userState round-trips", () => {
    const state = { ...baseState, userState: null };
    const bytes = arrowStateSerializer.serialize(state);
    const result = arrowStateSerializer.deserialize(bytes);
    expect(result.userState).toBeNull();
    expect(result.functionName).toBe("test_func");
  });

  test("empty object userState round-trips", () => {
    const state = { ...baseState, userState: {} };
    const bytes = arrowStateSerializer.serialize(state);
    const result = arrowStateSerializer.deserialize(bytes);
    expect(result.userState).toEqual({});
  });

  test("primitive userState round-trips", () => {
    const state = {
      ...baseState,
      userState: { remaining: 42, name: "test", active: true },
    };
    const bytes = arrowStateSerializer.serialize(state);
    const result = arrowStateSerializer.deserialize(bytes);
    expect(result.userState.remaining).toBe(42);
    expect(result.userState.name).toBe("test");
    expect(result.userState.active).toBe(true);
  });

  test("BigInt userState round-trips", () => {
    const state = {
      ...baseState,
      userState: { counter: 9007199254740993n },
    };
    const bytes = arrowStateSerializer.serialize(state);
    const result = arrowStateSerializer.deserialize(bytes);
    expect(result.userState.counter).toBe(9007199254740993n);
  });

  test("nested struct userState round-trips", () => {
    const state = {
      ...baseState,
      userState: { outer: { inner: 99 } },
    };
    const bytes = arrowStateSerializer.serialize(state);
    const result = arrowStateSerializer.deserialize(bytes);
    expect(result.userState.outer.inner).toBe(99);
  });

  test("Uint8Array userState round-trips", () => {
    const data = new Uint8Array([1, 2, 3, 4]);
    const state = { ...baseState, userState: { blob: data } };
    const bytes = arrowStateSerializer.serialize(state);
    const result = arrowStateSerializer.deserialize(bytes);
    expect(new Uint8Array(result.userState.blob)).toEqual(data);
  });

  test("top-level fields round-trip correctly", () => {
    const state = {
      ...baseState,
      isProducer: true,
      maxWorkers: 8,
      userState: null,
    };
    const bytes = arrowStateSerializer.serialize(state);
    const result = arrowStateSerializer.deserialize(bytes);
    expect(result.functionName).toBe("test_func");
    expect(result.isProducer).toBe(true);
    expect(result.__isProducer).toBe(true);
    expect(result.maxWorkers).toBe(8);
    expect(new Uint8Array(result.executionId)).toEqual(new Uint8Array([0xbe, 0xef]));
  });
});
