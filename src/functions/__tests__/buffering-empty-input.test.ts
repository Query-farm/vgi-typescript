// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Empty-input table buffering: the DuckDB extension (vgi 63eb257) runs the
// buffering init, then combine with an EMPTY state_ids list, then finalize,
// when a table-buffering function's input is empty at runtime. A whole-input
// reduction must still answer (sum_all_columns -> one zero row) and a
// pass-through must still emit nothing. Pins the empty-combine path at the
// unit level; table_buffering_empty_input.test pins it end to end.

import { describe, expect, test } from "bun:test";
import { Field, Int64, Schema } from "@query-farm/apache-arrow";
import { OutputCollector } from "@query-farm/vgi-rpc";
import { tableBufferingFunctions } from "../../../examples/table_buffering.js";
import { BoundStorage, FunctionStorageSqlite } from "../storage.js";
import type { TableBufferingParams } from "../table-buffering.js";

const OUTPUT = new Schema([new Field("a", new Int64(), true), new Field("b", new Int64(), true)]);

function fixture(name: string) {
  const fn = tableBufferingFunctions.find((f) => (f as any).meta?.name === name) as any;
  if (!fn?.bufferingConfig) throw new Error(`no table-buffering fixture ${name}`);
  return fn.bufferingConfig;
}

function params(storage: BoundStorage, executionId: Uint8Array): TableBufferingParams {
  return {
    args: { logging: false },
    initCall: {} as never,
    outputSchema: OUTPUT as never,
    settings: {},
    secrets: {},
    storage,
    executionId,
    attachId: new Uint8Array(0),
    transactionId: null,
    function_name: "test",
    batchIndex: null,
    clientLog: () => {},
  };
}

/** combine([]) then drain finalize for every finalize_state_id; return all rows. */
async function emptyInputRun(name: string): Promise<Record<string, unknown>[]> {
  const config = fixture(name);
  const executionId = crypto.getRandomValues(new Uint8Array(16));
  const p = params(new BoundStorage(new FunctionStorageSqlite(":memory:"), executionId), executionId);
  const finalizeIds: Uint8Array[] = await config.combine([], p);
  const rows: Record<string, unknown>[] = [];
  for (const fid of finalizeIds) {
    const state = config.initialFinalizeState ? await config.initialFinalizeState(fid, p) : undefined;
    for (let tick = 0; tick < 10; tick++) {
      const out = new OutputCollector(OUTPUT as never, true);
      await config.finalize(p, fid, state, out);
      // Read by column so the test runs on both Arrow backends (arrow-js rows
      // have toJSON, flechette rows are plain objects).
      for (const { batch } of out.batches) {
        const b = batch as any;
        for (let i = 0; i < b.numRows; i++) {
          rows.push(Object.fromEntries(OUTPUT.fields.map((f) => [f.name, b.getChild(f.name).get(i)])));
        }
      }
      if (out.finished) break;
    }
  }
  return rows;
}

describe("table buffering on empty input (combine with no state_ids)", () => {
  test("sum_all_columns answers with its zero row", async () => {
    expect(await emptyInputRun("sum_all_columns")).toEqual([{ a: 0n, b: 0n }]);
  });

  test("buffer_input still returns no rows", async () => {
    expect(await emptyInputRun("buffer_input")).toEqual([]);
  });
});
