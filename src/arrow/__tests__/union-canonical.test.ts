// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Backend-parity test for union canonical decode.
//
// DuckDB serializes a SQL UNION as a SPARSE Arrow union and drops the active
// member discriminator unless the reader recovers it from the per-row type
// code. This test feeds a sparse-union column (built with arrow-js, the wire
// producer) through the facade's `deserializeBatch` + `readCanonicalValue`, so
// the same assertions run against whichever backend is active:
//
//   bun test src/arrow/__tests__/union-canonical.test.ts
//   bun --conditions=worker test src/arrow/__tests__/union-canonical.test.ts
//
// Both must pass. Each row must decode into a TaggedUnion { tag, value } whose
// tag is the active member's field name and whose value is the member's
// canonical value (int64 -> bigint, utf8 -> string).

import { describe, test, expect } from "bun:test";
import * as arrow from "@query-farm/apache-arrow";
import {
  deserializeBatch,
  readCanonicalValue,
  TypeId,
  backend,
} from "../index.js";
import type { TaggedUnion } from "../types.js";

/** Build IPC bytes for a single `u: UNION(i BIGINT, s VARCHAR)` column whose
 *  rows alternate the active member, mirroring DuckDB's sparse-union output. */
function sparseUnionIpc(rows: Array<["i", bigint] | ["s", string]>): Uint8Array {
  const iField = new arrow.Field("i", new arrow.Int64(), true);
  const sField = new arrow.Field("s", new arrow.Utf8(), true);
  const ut = new arrow.SparseUnion([0, 1], [iField, sField]);

  const iVals = rows.map((r) => (r[0] === "i" ? (r[1] as bigint) : 0n));
  const sVals = rows.map((r) => (r[0] === "s" ? (r[1] as string) : ""));
  const typeCodes = Int8Array.from(rows.map((r) => (r[0] === "i" ? 0 : 1)));

  const iData = arrow.makeData({ type: new arrow.Int64(), data: BigInt64Array.from(iVals) });
  const sData = arrow.vectorFromArray(sVals, new arrow.Utf8()).data[0];
  const uData = arrow.makeData({
    type: ut,
    length: rows.length,
    typeIds: typeCodes,
    children: [iData, sData],
  });

  const schema = new arrow.Schema([new arrow.Field("u", ut, true)]);
  const batch = new arrow.RecordBatch(
    schema,
    arrow.makeData({
      type: new arrow.Struct(schema.fields),
      length: rows.length,
      children: [uData],
    }),
  );
  return arrow.tableToIPC(new arrow.Table([batch]), "stream");
}

describe(`union canonical decode (backend=${backend.name})`, () => {
  test("recovers the active member tag and value per row", () => {
    const ipc = sparseUnionIpc([
      ["i", 1n],
      ["s", "x"],
      ["i", 42n],
      ["s", "hello"],
    ]);
    const batch = deserializeBatch(ipc);
    const col = batch.getChild("u")!;
    expect(col.type.typeId).toBe(TypeId.Union);

    const decoded = [0, 1, 2, 3].map(
      (i) => readCanonicalValue(col.type, col, i) as TaggedUnion,
    );

    expect(decoded[0]).toEqual({ tag: "i", value: 1n });
    expect(decoded[1]).toEqual({ tag: "s", value: "x" });
    expect(decoded[2]).toEqual({ tag: "i", value: 42n });
    expect(decoded[3]).toEqual({ tag: "s", value: "hello" });
  });

  test("a union nested in a struct keeps its tag", () => {
    // Mirrors the argument-decode path: args arrive as a struct of positional_N
    // columns, one of which is a union.
    const iField = new arrow.Field("i", new arrow.Int64(), true);
    const sField = new arrow.Field("s", new arrow.Utf8(), true);
    const ut = new arrow.SparseUnion([0, 1], [iField, sField]);
    const iData = arrow.makeData({ type: new arrow.Int64(), data: BigInt64Array.from([7n, 0n]) });
    const sData = arrow.vectorFromArray(["", "z"], new arrow.Utf8()).data[0];
    const uData = arrow.makeData({
      type: ut,
      length: 2,
      typeIds: Int8Array.from([0, 1]),
      children: [iData, sData],
    });
    const structType = new arrow.Struct([new arrow.Field("positional_0", ut, true)]);
    const sData2 = arrow.makeData({ type: structType, length: 2, children: [uData] });
    const schema = new arrow.Schema([new arrow.Field("args", structType, true)]);
    const batch = new arrow.RecordBatch(
      schema,
      arrow.makeData({ type: new arrow.Struct(schema.fields), length: 2, children: [sData2] }),
    );
    const ipc = arrow.tableToIPC(new arrow.Table([batch]), "stream");

    const dbatch = deserializeBatch(ipc);
    const argsCol = dbatch.getChild("args")!;
    const row0 = readCanonicalValue(argsCol.type, argsCol, 0) as Record<string, TaggedUnion>;
    const row1 = readCanonicalValue(argsCol.type, argsCol, 1) as Record<string, TaggedUnion>;
    expect(row0.positional_0).toEqual({ tag: "i", value: 7n });
    expect(row1.positional_0).toEqual({ tag: "s", value: "z" });
  });
});
