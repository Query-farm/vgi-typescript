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
  schema, field, utf8, int32, int64, uint64, struct, list, dictionary, decimal128,
  timestamp, TimeUnit, dateDay,
  isList, isStruct, isDecimal, isDictionary, isTimestamp,
  TypeId,
  batchFromColumns, serializeBatch, deserializeBatch, deserializeSchema, iterRows,
  withBatchMetadata,
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

  test("date32 columns store raw day-numbers (non-null and null)", () => {
    // Regression: the arrow-js backend used to route non-null date32 columns
    // through vectorFromArray, which silently zeroed raw day-number inputs — so
    // `easter_date(2025)` came back as 1970-01-01. Day-numbers must round-trip
    // for both the all-non-null case and the null-propagation case.
    const sch = schema([
      field("d", dateDay(), true),
      field("dn", dateDay(), true),
    ]);
    // 20198 = 2025-04-20; 0 = 1970-01-01.
    const batch = batchFromColumns(
      { d: [20198, 0, 19813], dn: [20198, null, 19813] },
      sch,
    );
    const round = deserializeBatch(serializeBatch(batch));
    const rows = [...iterRows(round)];
    // date32 reads come back as a Date, a day-number, or epoch-ms depending on
    // backend — normalize all three to a day-number. (Magnitude disambiguates a
    // raw day-number ~2e4 from epoch-ms ~1.7e12.)
    const dayNum = (v: any) => {
      if (v == null) return null;
      if (v instanceof Date) return Math.round(v.getTime() / 86_400_000);
      const n = typeof v === "bigint" ? Number(v) : (v as number);
      return Math.abs(n) >= 1e6 ? Math.round(n / 86_400_000) : n;
    };
    expect(rows.map((r) => dayNum(r.d))).toEqual([20198, 0, 19813]);
    expect(rows.map((r) => dayNum(r.dn))).toEqual([20198, null, 19813]);
  });

  test("accepts foreign (arrow-js) DataType instances incl. variable-width", async () => {
    // Real worker code (e.g. examples/common.ts) builds schemas with concrete
    // arrow-js types — `new Int64()`/`new Utf8()` from @query-farm/apache-arrow
    // — NOT the facade constructors above. The flechette backend must accept
    // these "foreign" Arrow type objects. Two regressions lived here:
    //   - Int64: a stale flechette dist threw "Conversion from 'BigInt' to
    //     'number'".
    //   - Utf8/List/Struct: flechette's columnFromArray only *tolerates* a
    //     foreign type for values; the resulting Column.type isn't writer-safe,
    //     so serialize corrupted them (Utf8 → ["", "xy\0\0…"]). Surfaced
    //     against the C++ extension as "Invalid Error: basic_string".
    // impl-flechette/build.ts now normalizes foreign types → flechette-native.
    const A: any = await import("@query-farm/apache-arrow");
    const sch: any = new A.Schema([
      new A.Field("i", new A.Int64(), true),
      new A.Field("s", new A.Utf8(), true),
      new A.Field("lst", new A.List(new A.Field("item", new A.Int32(), true)), true),
      new A.Field("st", new A.Struct([new A.Field("a", new A.Utf8(), true)]), true),
    ]);
    const batch = batchFromColumns(
      { i: [10n, 20n], s: ["x", "y"], lst: [[1, 2], [3]], st: [{ a: "p" }, { a: "q" }] },
      sch,
    );
    const rows = [...iterRows(deserializeBatch(serializeBatch(batch)))];
    expect(rows[0].i).toBe(10n);
    expect(rows[1].i).toBe(20n);
    expect(rows[0].s).toBe("x");
    expect(rows[1].s).toBe("y");
    expect([...rows[0].lst]).toEqual([1, 2]);
    expect(rows[0].st.a).toBe("p");
    expect(rows[1].st.a).toBe("q");
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

  test("withBatchMetadata surfaces the map on batch.metadata (incl. 0-row)", () => {
    const sch = schema([field("v", int64(), true)]);
    const md = new Map([["vgi.cache.not_modified", "1"], ["vgi.cache.etag", '"e1"']]);

    // 0-row batch — the load-bearing case (a not_modified reply).
    const empty = batchFromColumns({ v: [] as bigint[] }, sch);
    const stamped = withBatchMetadata(empty, md);
    expect(stamped.numRows).toBe(0);
    expect((stamped as any).metadata?.get("vgi.cache.not_modified")).toBe("1");
    // The original batch is not mutated.
    expect((empty as any).metadata?.get?.("vgi.cache.not_modified")).toBeUndefined();

    // Non-empty batch keeps its data.
    const full = batchFromColumns({ v: [1n, 2n] }, sch);
    const stamped2 = withBatchMetadata(full, md);
    expect(stamped2.numRows).toBe(2);
    expect((stamped2 as any).metadata?.get("vgi.cache.etag")).toBe('"e1"');
  });

  test("backend reports its own name", () => {
    expect(backend.name).toMatch(/^(arrow-js|flechette)$/);
  });

  // ---------------------------------------------------------------------
  // arrow-js-shaped surface the workers and examples read directly.
  // Each of these was a live flechette-only integration failure.
  // ---------------------------------------------------------------------

  const compatSchema = schema([
    field("name", utf8(), true),
    field("v", int32(), true),
    field("big", int64(), true),
    field("ub", uint64(), true),
  ]);
  const compatBytes = serializeBatch(
    batchFromColumns(
      { name: ["a", null, "c"], v: [1, 2, null], big: [10n, null, 30n], ub: [1n, 2n, 3n] },
      compatSchema,
    ),
  );

  test("IPC decode accepts a stream at a non-8-byte-aligned offset", () => {
    // Length-prefixed payloads (`splitLenPrefixed`) and nested IPC carried in
    // Arrow Binary values land at arbitrary byteOffsets. flechette builds its
    // buffer views zero-copy off `bytes.byteOffset`, so a misaligned stream
    // throws "RangeError: Byte offset is not aligned" unless the facade
    // re-bases it.
    for (let skew = 1; skew < 8; skew++) {
      const backing = new Uint8Array(compatBytes.length + skew);
      backing.set(compatBytes, skew);
      const round = deserializeBatch(backing.subarray(skew));
      expect(round.numRows).toBe(3);
      expect([...iterRows(round)].length).toBe(3);
    }
  });

  test("column exposes isValid(index)", () => {
    const col = (deserializeBatch(compatBytes) as any).getChild("name");
    expect(typeof col.isValid).toBe("function");
    expect([col.isValid(0), col.isValid(1), col.isValid(2)]).toEqual([true, false, true]);
  });

  test("batch exposes slice(begin, end)", () => {
    const round = deserializeBatch(compatBytes) as any;
    const empty = round.slice(0, 0);
    expect(empty.numRows).toBe(0);
    expect(empty.schema.fields.length).toBe(4);
    const mid = round.slice(1, 3);
    expect(mid.numRows).toBe(2);
    expect(mid.getChild("name").at(1)).toBe("c");
    expect(mid.getChild("name").isValid(0)).toBe(false);
  });

  test("int types report isSigned (decoded and constructed)", () => {
    const s = deserializeSchema(compatBytes) as any;
    expect(s.fields[2].type.isSigned).toBe(true);
    expect(s.fields[3].type.isSigned).toBe(false);
    const b = deserializeBatch(compatBytes) as any;
    expect(b.schema.fields[2].type.isSigned).toBe(true);
    expect(b.schema.fields[3].type.isSigned).toBe(false);
    expect((int64() as any).isSigned).toBe(true);
    expect((uint64() as any).isSigned).toBe(false);
  });
});
