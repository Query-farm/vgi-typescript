// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Backend-parity smoke test for the Arrow facade.
//
// Exercises the facade's schema/factory/IPC/iterate surface so the same
// assertions pass whether the active backend is impl-arrowjs (default) or
// impl-flechette (`bun --conditions=worker test ...`). Run via:
//
//   bun test src/arrow/__tests__/parity.test.ts
//   bun --conditions=worker test src/arrow/__tests__/parity.test.ts
//
// Both invocations must pass. The Makefile's `test-facade-parity` target
// runs both back-to-back.

import { describe, test, expect } from "bun:test";
import {
  schema, field, utf8, int32, int64, struct, list, dictionary, decimal128,
  timestamp, TimeUnit,
  isList, isStruct, isDecimal, isDictionary, isTimestamp,
  TypeId,
  batchFromColumns, serializeBatch, deserializeBatch, iterRows,
  backend,
} from "../index.js";

describe(`facade (backend=${backend.name})`, () => {
  test("typeIds match the Arrow Type enum (backend-agnostic)", () => {
    expect(utf8().typeId).toBe(TypeId.Utf8);
    expect(int32().typeId).toBe(TypeId.Int);
    expect(struct([field("a", int32(), true)]).typeId).toBe(TypeId.Struct);
    expect(list(field("item", utf8(), true)).typeId).toBe(TypeId.List);
    expect(dictionary(utf8(), int32()).typeId).toBe(TypeId.Dictionary);
    expect(decimal128(38, 4).typeId).toBe(TypeId.Decimal);
    expect(timestamp(TimeUnit.NANOSECOND).typeId).toBe(TypeId.Timestamp);
  });

  test("predicates dispatch by typeId, not by class identity", () => {
    expect(isList(list(field("item", utf8(), true)))).toBe(true);
    expect(isStruct(struct([field("a", int32(), true)]))).toBe(true);
    expect(isDecimal(decimal128(38, 4))).toBe(true);
    expect(isDictionary(dictionary(utf8(), int32()))).toBe(true);
    expect(isTimestamp(timestamp(TimeUnit.NANOSECOND))).toBe(true);
    expect(isList(int32())).toBe(false);
    expect(isStruct(utf8())).toBe(false);
  });

  test("primitive round-trip (utf8, int32, int64, bool)", () => {
    const sch = schema([
      field("name", utf8(), true),
      field("v", int32(), true),
      field("big", int64(), true),
    ]);
    const batch = batchFromColumns(
      { name: ["a", null, "c"], v: [1, 2, null], big: [10n, null, 30n] },
      sch,
    );
    const bytes = serializeBatch(batch);
    const round = deserializeBatch(bytes);
    expect(round.numRows).toBe(3);
    const rows = [...iterRows(round)];
    expect(rows[0].name).toBe("a");
    expect(rows[1].name).toBe(null);
    expect(rows[2].v).toBe(null);
    expect(rows[0].big).toBe(10n);
  });

  test("decimal128 BigInt round-trip", () => {
    const sch = schema([field("price", decimal128(38, 4), true)]);
    const batch = batchFromColumns(
      { price: [12345n, null, -99999n] },
      sch,
    );
    const round = deserializeBatch(serializeBatch(batch));
    const rows = [...iterRows(round)];
    // Both backends materialize Decimal differently (arrow-js: Uint32Array,
    // flechette: BigInt). Normalize to BigInt for comparison.
    const normDec = (v: any): bigint | null => {
      if (v === null || v === undefined) return null;
      if (typeof v === "bigint") return v;
      if (ArrayBuffer.isView(v)) {
        const u8 = new Uint8Array(
          (v as ArrayBufferView).buffer,
          (v as ArrayBufferView).byteOffset,
          (v as ArrayBufferView).byteLength,
        );
        let bi = 0n;
        for (let i = u8.length - 1; i >= 0; i--) bi = (bi << 8n) | BigInt(u8[i]);
        if (u8[u8.length - 1] & 0x80) bi -= 1n << BigInt(u8.length * 8);
        return bi;
      }
      return BigInt(v);
    };
    expect(normDec(rows[0].price)).toBe(12345n);
    expect(normDec(rows[1].price)).toBe(null);
    expect(normDec(rows[2].price)).toBe(-99999n);
  });

  test("nested list-of-struct", () => {
    const sch = schema([
      field(
        "rows",
        list(field("item", struct([
          field("k", utf8(), true),
          field("v", int32(), true),
        ]), true)),
        true,
      ),
    ]);
    const batch = batchFromColumns(
      { rows: [[{ k: "a", v: 1 }, { k: "b", v: 2 }], null, []] },
      sch,
    );
    const round = deserializeBatch(serializeBatch(batch));
    expect(round.numRows).toBe(3);
    const rows = [...iterRows(round)];
    // Both backends expose list/struct values in slightly different shapes:
    //   arrow-js List -> Vector (iterable yielding StructRow per element)
    //   arrow-js StructRow -> object that's ALSO iterable (yields [k,v] pairs)
    //   flechette List -> array of objects
    //   flechette Struct -> plain object
    // We canonicalize to plain JS arrays/objects: prefer Object.keys (works
    // for both StructRow and plain object) over [...iter].
    // arrow-js List values are Vectors (iterable, with numeric `length`,
    // internal _offsets/data keys); arrow-js Struct values are StructRow
    // objects (iterable yielding [k,v], BUT have field-name getters). flechette
    // uses plain arrays/objects. Heuristic: numeric .length + iterable -> array.
    const norm = (v: any): any => {
      if (v === null || v === undefined) return null;
      if (Array.isArray(v)) return v.map(norm);
      if (typeof v === "object" && !ArrayBuffer.isView(v)) {
        // Vector / list-shaped: numeric length and iterable
        if (
          typeof (v as any).length === "number" &&
          typeof v[Symbol.iterator] === "function"
        ) {
          return [...v].map(norm);
        }
        // Otherwise: object/struct — enumerate user-facing keys only
        const keys = Object.keys(v).filter((k) => !k.startsWith("_"));
        const out: any = {};
        for (const k of keys.sort()) out[k] = norm(v[k]);
        return out;
      }
      return v;
    };
    expect(norm(rows[0].rows)).toEqual([{ k: "a", v: 1 }, { k: "b", v: 2 }]);
    expect(rows[1].rows).toBe(null);
    expect(norm(rows[2].rows)).toEqual([]);
  });

  test("dictionary<utf8> auto-decodes on read", () => {
    const sch = schema([field("color", dictionary(utf8(), int32()), true)]);
    const batch = batchFromColumns(
      { color: ["red", "green", "red", null, "blue"] },
      sch,
    );
    const round = deserializeBatch(serializeBatch(batch));
    const rows = [...iterRows(round)];
    expect(rows.map((r) => r.color)).toEqual(["red", "green", "red", null, "blue"]);
  });

  test("backend reports its own name", () => {
    expect(backend.name).toMatch(/^(arrow-js|flechette)$/);
  });
});
