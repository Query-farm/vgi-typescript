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

  test("array → List of the element type", () => {
    const type = inferFieldType([1n, 2n, 3n]);
    expect((type as any).children.length).toBe(1);
    expect((type as any).children[0].name).toBe("item");
  });

  test("empty array → List (placeholder element type, never Null)", () => {
    // A Null-typed list child is what an empty array "means", but flechette's
    // column builder rejects it — so the placeholder must build on both
    // backends. Asserted through a real round-trip below.
    const type = inferFieldType([]);
    expect((type as any).children.length).toBe(1);
  });

  test("heterogeneous array throws, naming both element types", () => {
    expect(() => inferFieldType([1n, "a"])).toThrow(/must be homogeneous/);
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

  // ------------------------------------------------------------------------
  // Arrays. Every HTTP producer continuation serializes userState, so a
  // producer holding an array in its state (a pending value buffer, a resolved
  // key set, a row cursor) round-trips through here on every turn. Serialize
  // then deserialize MUST give the same value back — a lossy round-trip
  // silently resumes the producer with the wrong state instead of failing.
  // ------------------------------------------------------------------------

  test("bigint array userState round-trips", () => {
    const state = { ...baseState, userState: { values: [1n, 2n, 3n], offset: 2 } };
    const result = arrowStateSerializer.deserialize(arrowStateSerializer.serialize(state));
    expect(result.userState.values).toEqual([1n, 2n, 3n]);
    expect(result.userState.offset).toBe(2);
  });

  test("number array userState round-trips", () => {
    const state = { ...baseState, userState: { values: [5, 50, 95] } };
    const result = arrowStateSerializer.deserialize(arrowStateSerializer.serialize(state));
    expect(result.userState.values).toEqual([5, 50, 95]);
  });

  test("string array with nulls round-trips", () => {
    const state = { ...baseState, userState: { tags: ["i", null, "s"] } };
    const result = arrowStateSerializer.deserialize(arrowStateSerializer.serialize(state));
    expect(result.userState.tags).toEqual(["i", null, "s"]);
  });

  test("boolean array round-trips", () => {
    const state = { ...baseState, userState: { flags: [true, false, true] } };
    const result = arrowStateSerializer.deserialize(arrowStateSerializer.serialize(state));
    expect(result.userState.flags).toEqual([true, false, true]);
  });

  test("empty array round-trips as an empty array", () => {
    const state = { ...baseState, userState: { values: [], done: false } };
    const result = arrowStateSerializer.deserialize(arrowStateSerializer.serialize(state));
    expect(result.userState.values).toEqual([]);
    expect(result.userState.done).toBe(false);
  });

  test("all-null array round-trips element-for-element", () => {
    const state = { ...baseState, userState: { tags: [null, null] } };
    const result = arrowStateSerializer.deserialize(arrowStateSerializer.serialize(state));
    expect(result.userState.tags).toEqual([null, null]);
  });

  test("array of arrays round-trips (repeat_value's row buffer)", () => {
    const state = { ...baseState, userState: { rows: [[1n, 2n], [3n, 4n]], offset: 0 } };
    const result = arrowStateSerializer.deserialize(arrowStateSerializer.serialize(state));
    expect(result.userState.rows).toEqual([[1n, 2n], [3n, 4n]]);
  });

  test("array of empty arrays round-trips", () => {
    const state = { ...baseState, userState: { rows: [[], []] } };
    const result = arrowStateSerializer.deserialize(arrowStateSerializer.serialize(state));
    expect(result.userState.rows).toEqual([[], []]);
  });

  test("array of structs round-trips", () => {
    const state = { ...baseState, userState: { items: [{ a: 1n }, { a: 2n }] } };
    const result = arrowStateSerializer.deserialize(arrowStateSerializer.serialize(state));
    expect(result.userState.items).toEqual([{ a: 1n }, { a: 2n }]);
  });

  test("array nested inside a struct round-trips", () => {
    const state = { ...baseState, userState: { outer: { inner: [1n, 2n] } } };
    const result = arrowStateSerializer.deserialize(arrowStateSerializer.serialize(state));
    expect(result.userState.outer.inner).toEqual([1n, 2n]);
  });

  test("union_varargs-shaped state round-trips whole", () => {
    // The exact state shape that broke the http lanes: three parallel arrays
    // plus a done flag, serialized on the producer's continuation turn.
    const userState = {
      idx: [0n, 1n],
      tags: ["i", "s"] as (string | null)[],
      values: ["1", "x"],
      done: false,
    };
    const result = arrowStateSerializer.deserialize(
      arrowStateSerializer.serialize({ ...baseState, userState }),
    );
    expect(result.userState).toEqual(userState);
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
