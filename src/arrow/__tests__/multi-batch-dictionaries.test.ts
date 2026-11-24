// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// serializeBatches over batches that each carry their own dictionary.
//
// The HTTP exchange writes one IPC stream per reply: zero or more zero-row
// log/state batches (metadata only) followed by the result batches. Every
// batch is built on its own (batchFromColumns), so a dictionary column's
// values differ batch to batch. Under the flechette backend this crashed
// ("Cannot convert undefined to a BigInt") when a zero-row metadata batch led
// the stream, and silently decoded later batches against the first batch's
// dictionary. Run under both backends:
//
//   bun test src/arrow/__tests__/multi-batch-dictionaries.test.ts
//   bun --conditions=flechette test src/arrow/__tests__/multi-batch-dictionaries.test.ts

import { describe, expect, test } from "bun:test";
import { tableFromIPC } from "@query-farm/apache-arrow";
import {
  backend,
  batchFromColumns,
  dictionary,
  emptyBatch,
  field,
  int8,
  int64,
  schema,
  serializeBatches,
  utf8,
  withBatchMetadata,
} from "../index.js";

const SCHEMA = schema([field("n", int64(), true), field("kind", dictionary(utf8(), int8()), true)]);

const batch = (n: bigint[], kind: (string | null)[]) => batchFromColumns({ n, kind }, SCHEMA);
const logBatch = () => withBatchMetadata(emptyBatch(SCHEMA), new Map([["vgi_rpc.log_level", "WARN"]]));

/** Decode with Apache Arrow JS (an independent reader) into [n, kind] rows. */
function rows(bytes: Uint8Array): [number, string | null][] {
  const t = tableFromIPC(bytes);
  const n = t.getChild("n")!;
  const kind = t.getChild("kind")!;
  return Array.from({ length: t.numRows }, (_, i) => [Number(n.get(i)), kind.get(i) as string | null]);
}

describe(`serializeBatches with per-batch dictionaries (backend=${backend.name})`, () => {
  test("a zero-row metadata batch before an all-NULL dictionary batch", () => {
    expect(rows(serializeBatches([logBatch(), batch([1n], [null])], SCHEMA))).toEqual([[1, null]]);
  });

  test("a zero-row metadata batch before data", () => {
    expect(rows(serializeBatches([logBatch(), batch([1n, 2n], ["a", null])], SCHEMA))).toEqual([
      [1, "a"],
      [2, null],
    ]);
  });

  test("each batch decodes against its own dictionary", () => {
    const bytes = serializeBatches([batch([1n], ["a"]), batch([2n, 3n], ["b", "c"]), batch([4n], [null])], SCHEMA);
    expect(rows(bytes)).toEqual([
      [1, "a"],
      [2, "b"],
      [3, "c"],
      [4, null],
    ]);
  });
});
