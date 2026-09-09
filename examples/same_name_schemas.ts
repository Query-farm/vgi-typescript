// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Same-name-in-two-schemas *declarative-table* producer fixtures
// (`test_same_name_table_scan`).
//
// The TABLE-DISPATCH member of the schema-disambiguation family (see
// `examples/same_name.ts` for the scalar dispatch probe,
// `examples/same_name_exchange.ts` for the table-in-out / buffered / aggregate
// probes, and `examples/same_name_cached.ts` for the result-cache probe). Those
// are reached by calling a function directly; this one is reached ONLY through
// `catalog_table_scan_function_get` / `catalog_table_scan_branches_get` — the
// RPC pair that tells a client which function backs a *declarative catalog
// table*.
//
// `test_same_name_table_scan` is a one-row producer registered under the SAME
// NAME in BOTH the `main` and `data` schemas of the `example` catalog, each
// emitting a single row tagged with its own schema. A declarative
// `test_same_name_table` is declared in each schema too (see `common.ts`),
// backed by that schema's own implementation.
//
// This is also the end-to-end regression guard for protocol 2.0.0's
// `ScanFunctionResult.schema_path` / `ScanBranch.schema_path`: the C++
// extension now prefers the worker-declared schema over its old
// table-schema/default-schema heuristic when resolving which catalog entry
// `function_name` refers to. That heuristic still happens to get *this*
// two-schema case right (the table's own schema is tried first and always
// matches here), so a worker that stopped setting `schema_path` shows up not as
// a wrong row but as the filter/projection-pushdown breakage
// `cache/filter_pushdown_keys.test` and `cache/projection_pushdown.test` catch.
// What this file catches is a worker/client that stopped agreeing on which
// schema's table gets which schema's function.
//
// Mirrors vgi-python's `vgi/_test_fixtures/table/same_name_schemas.py` and
// vgi-rust's `vgi-example-worker/src/same_name.rs`; driven by
// `test/sql/integration/table/same_name_schemas.test`.

import { Schema, Field, Utf8 } from "@query-farm/apache-arrow";
import {
  defineTableFunction,
  batchFromColumns,
  type VgiFunction,
} from "../src/index.js";

// Deliberately shared across the two schemas — the collision is the point.
const FUNCTION_NAME = "test_same_name_table_scan";

// The name of the declarative table each schema backs with its own half of the
// pair. Exported so `common.ts` declares both tables under one spelling.
export const SAME_NAME_TABLE = "test_same_name_table";

// The single VARCHAR column every implementation here emits.
const TAG_SCHEMA = new Schema([new Field("tag", new Utf8(), true)]);

// One-shot emit latch for the single output row.
interface ScanState {
  done: boolean;
}

function makeTableScan(owningSchema: string): VgiFunction {
  return defineTableFunction<Record<string, never>, ScanState>({
    name: FUNCTION_NAME,
    description: `Schema-disambiguation probe; the ${owningSchema}-schema table producer`,
    onBind: () => ({ outputSchema: TAG_SCHEMA }),
    initialState: () => ({ done: false }),
    process: (params, state, out) => {
      if (state.done) {
        out.finish();
        return;
      }
      out.emit(batchFromColumns({ tag: [owningSchema] }, params.outputSchema));
      state.done = true;
    },
    examples: [
      {
        sql: `SELECT * FROM example.${owningSchema}.test_same_name_table`,
        description: `One row tagged '${owningSchema}'`,
      },
    ],
    categories: ["generator", "testing"],
  });
}

// The `main`-schema half (advertised in `main`, registered on the worker) and
// the `data`-schema half (advertised in `data`). The worker registers both; the
// catalog schemas scope which surfaces where.
export const sameNameTableMainScan = makeTableScan("main");
export const sameNameTableDataScan = makeTableScan("data");

export const sameNameTableScanMainFunctions = [sameNameTableMainScan];
export const sameNameTableScanDataFunctions = [sameNameTableDataScan];
export const sameNameTableScanFunctions = [sameNameTableMainScan, sameNameTableDataScan];
