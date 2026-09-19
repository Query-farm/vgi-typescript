// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// withBatchMetadata on a zero-column batch that carries rows.
//
// DuckDB sends one as the input of a scalar whose arguments are all constants
// (`hash_seed(42)`): no columns, N rows. flechette derives a Table's row count
// from its columns, so a reader has to carry N some other way, and a clone that
// rewrites the batch's metadata has to keep it. Run under both backends:
//
//   bun test src/arrow/__tests__/zero-column-metadata.test.ts
//   bun --conditions=flechette test src/arrow/__tests__/zero-column-metadata.test.ts

import { describe, test, expect } from "bun:test";
import * as arrow from "@query-farm/apache-arrow";
import { tableFromIPC } from "@query-farm/flechette";
import { backend, deserializeBatch, withBatchMetadata } from "../index.js";

const ROWS = 3;

/** Arrow IPC stream bytes of a zero-column batch with `rows` rows. */
function zeroColumnIpc(rows: number): Uint8Array {
  const data = arrow.makeData({ type: new arrow.Struct([]), length: rows, nullCount: 0, children: [] });
  const batch = new arrow.RecordBatch(new arrow.Schema([]), data);
  return arrow.tableToIPC(new arrow.Table([batch]), "stream");
}

describe(`withBatchMetadata on a zero-column batch (backend=${backend.name})`, () => {
  test("the IPC bytes carry the row count", () => {
    expect(arrow.tableFromIPC(zeroColumnIpc(ROWS)).numRows).toBe(ROWS);
  });

  test("a batch read by this backend keeps its row count and takes the new metadata", () => {
    const batch = deserializeBatch(zeroColumnIpc(ROWS));
    expect(batch.numRows).toBe(ROWS);
    const out = withBatchMetadata(batch, new Map([["k", "v"]]));
    expect(out.numRows).toBe(ROWS);
    expect((out as any).metadata?.get("k")).toBe("v");
  });

  // vgi-rpc's flechette reader carries the row count as an own `numRows`
  // property over Table.prototype's getter-only accessor (vgi-rpc
  // impl-flechette deserializeBatch), and a batch it read reaches this SDK's
  // helpers as a method's input. Cloning one by assignment threw.
  test.if(backend.name === "flechette")("a batch carrying an own numRows (vgi-rpc's reader) does not throw", () => {
    const table: any = tableFromIPC(zeroColumnIpc(ROWS));
    Object.defineProperty(table, "numRows", { value: ROWS, configurable: true, enumerable: true });
    const out = withBatchMetadata(table, new Map([["k", "v"]]));
    expect(out.numRows).toBe(ROWS);
    expect((out as any).metadata?.get("k")).toBe("v");
  });
});
