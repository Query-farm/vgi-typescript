// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// narrow_bind reproducer fixture — TypeScript port of
// vgi-python/vgi/_test_fixtures/narrow_bind/worker.py.
//
// Two virtual tables, each backed by a table function:
//
//   * `mismatch`   — advertises columns {id, val} in its catalog listing but
//                    its scan function `narrow_scan` binds to {id} only. This
//                    inconsistency used to SIGSEGV the client at scan time
//                    (ArrowToDuckDB walking off the end of the 1-column batch).
//                    The C++ extension must now refuse it at bind with a clear
//                    BinderException.
//   * `consistent` — advertises {id, val} and its scan function `wide_scan`
//                    binds to {id, val}. Positive control: must keep working.
//
// Driven by test/sql/integration/narrow_bind_mismatch.test in ~/Development/vgi.

import { Schema, Field, Int64 } from "@query-farm/apache-arrow";
import {
  defineTableFunction,
  batchFromColumns,
  Arguments,
  type CatalogDescriptor,
  type VgiFunction,
} from "../src/index.js";

// What the catalog advertises for both tables: two columns.
const TABLE_SCHEMA = new Schema([
  new Field("id", new Int64(), true),
  new Field("val", new Int64(), true),
]);
// What the narrow scan function actually binds to: one column.
const NARROW_BIND_SCHEMA = new Schema([new Field("id", new Int64(), true)]);

interface NarrowArgs {
  count: number;
}

// Binds to a NARROWER schema than the catalog advertises (the bug).
const narrow_scan = defineTableFunction<NarrowArgs, { done: boolean }>({
  name: "narrow_scan",
  description: "bind reports a narrower schema than the table advertises",
  args: { count: new Int64() },
  onBind: () => ({ outputSchema: NARROW_BIND_SCHEMA }),
  initialState: () => ({ done: false }),
  process: (params, state, out) => {
    if (state.done) {
      out.finish();
      return;
    }
    state.done = true;
    out.emit(batchFromColumns({ id: [0n, 1n, 2n] }, params.outputSchema));
  },
});

// Binds to the full advertised schema (positive control — must work).
const wide_scan = defineTableFunction<NarrowArgs, { done: boolean }>({
  name: "wide_scan",
  description: "bind matches the table's advertised schema",
  args: { count: new Int64() },
  onBind: () => ({ outputSchema: TABLE_SCHEMA }),
  initialState: () => ({ done: false }),
  process: (params, state, out) => {
    if (state.done) {
      out.finish();
      return;
    }
    state.done = true;
    out.emit(
      batchFromColumns({ id: [0n, 1n, 2n], val: [10n, 20n, 30n] }, params.outputSchema),
    );
  },
});

export const narrowBindFunctions: VgiFunction[] = [narrow_scan, wide_scan];

export const narrowBindCatalog: CatalogDescriptor = {
  name: "narrow_bind",
  defaultSchema: "main",
  comment: "narrow-bind reproducer catalog",
  schemas: [
    {
      name: "main",
      comment: "narrow-bind reproducer catalog",
      tables: [
        {
          name: "mismatch",
          columns: TABLE_SCHEMA,
          function: narrow_scan,
          arguments: new Arguments([3]),
          comment: "narrow-bind reproducer table -> narrow_scan",
        },
        {
          name: "consistent",
          columns: TABLE_SCHEMA,
          function: wide_scan,
          arguments: new Arguments([3]),
          comment: "narrow-bind reproducer table -> wide_scan",
        },
      ],
    },
  ],
};
