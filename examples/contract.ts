// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// `example.contract`: the cross-SDK protocol 3.3 contract fixtures every SDK's
// standard example worker serves (vgi test/sql/integration/visibility/
// example_table_only.test and write_contract/example_*.test).
//
// Tables (schema `contract`, no comment):
//   good     read-only; table_only scan with a row id and projection pushdown
//   ranged   read-only; table_only scan named exactly `range` -- the worker's
//            function binds, not DuckDB's range()
//   notes    INSERT only (count); a NOT_NULL record on id the insert enforces,
//            the count reported by finalize only
//   amounts  INSERT (count and rows) into decimal32 / decimal64 / decimal128
//            columns, which must arrive at exactly their declared widths
//
// None of these functions is listed in a schema's `functions`, so the SDK
// sends each `table_only` (protocol 3.3). Nothing is stored: the inserts
// validate, count, and discard.

import {
  constraintRecord,
  defineTableFunction,
  defineTableInOutFunction,
  FunctionStability,
  rowIdField,
  type SchemaDescriptor,
  type VgiFunction,
} from "../src/index.js";
import {
  batchFromColumns,
  decimal,
  field,
  int64,
  iterRows,
  schema,
  typeSignature,
  utf8,
  type VgiBatch,
  type VgiSchema,
} from "../src/arrow/index.js";
import { writeCountBatch, writeOptions, writeResultBatch, writeResultSchema } from "../src/write-results.js";

const SCHEMA = "contract";

interface Once {
  done: boolean;
}

/** A no-argument, VOLATILE table function that emits `columns` once, projected. */
function fixedScan(name: string, description: string, outputSchema: VgiSchema, columns: Record<string, unknown[]>): VgiFunction {
  return defineTableFunction<Record<string, never>, Once>({
    name,
    description,
    stability: FunctionStability.VOLATILE,
    // DuckDB reads `rowid` as a virtual column, which needs projection pushdown:
    // params.outputSchema is then exactly the requested columns.
    projectionPushdown: true,
    onBind: () => ({ outputSchema }),
    initialState: () => ({ done: false }),
    process: (params, state, out) => {
      if (state.done) return out.finish();
      state.done = true;
      const projected: Record<string, unknown[]> = {};
      for (const f of params.outputSchema.fields) projected[f.name] = columns[f.name]!;
      out.emit(batchFromColumns(projected, params.outputSchema));
    },
  });
}

// ------------------------------------------------------------------- good

const goodScan = fixedScan(
  "contract_good_scan",
  "Scan contract.good (table_only, row id = id)",
  schema([field("id", int64(), true), field("label", utf8(), true), rowIdField()]),
  { id: [1n, 2n, 3n], label: ["a", "b", "c"], rowid: [1n, 2n, 3n] },
);

// ----------------------------------------------------------------- ranged

const rangedScan = fixedScan(
  "range",
  "Scan contract.ranged (table_only; named range on purpose)",
  schema([field("who", utf8(), true)]),
  { who: ["vgi example worker"] },
);

// ------------------------------------------------------------------ notes

const NOTES = schema([field("id", int64(), false), field("note", utf8(), true)]);
const COUNT = writeResultSchema("count", schema([]));

const notesScan = fixedScan("contract_notes_scan", "Scan contract.notes (always empty)", NOTES, { id: [], note: [] });

/** Per write call: the rows its batches carried (the SDK collects every state for finalize). */
interface NotesCallState {
  rows: number;
}

const notesInsert = defineTableInOutFunction<Record<string, never>, NotesCallState>({
  name: "contract_notes_insert",
  description: "INSERT into contract.notes: enforce NOT NULL id, count in finalize, discard",
  stability: FunctionStability.VOLATILE,
  onBind: () => ({ outputSchema: COUNT }),
  initialState: () => ({ rows: 0 }),
  process: (_params, state, batch: VgiBatch, out) => {
    for (const f of batch.schema.fields) {
      if (!f.nullable) throw new Error(`contract: notes_insert input field ${f.name} is non-nullable`);
    }
    const ids = batch.getChild("id");
    for (let i = 0; i < batch.numRows; i++) {
      if (ids == null || ids.get(i) == null) throw new Error("contract: notes.id required");
    }
    // Pooled worker processes may serve one statement's batches: the count
    // lives in this call's state, which the SDK persists per execution and
    // hands to finalize (from every process) as `states`.
    state.rows += batch.numRows;
    out.emit(batchFromColumns({ count: [] }, COUNT));
  },
  finalize: (_params, states) => {
    const total = states.reduce((sum, st) => sum + (st?.rows ?? 0), 0);
    // collect() drained the states: nothing remains to clear.
    return [writeCountBatch(total)];
  },
});

// ---------------------------------------------------------------- amounts

const AMOUNTS = schema([
  field("id", int64(), true),
  field("d32", decimal(7, 2, 32), true),
  field("d64", decimal(15, 3, 64), true),
  field("d128", decimal(9, 2, 128), true),
]);

const amountsScan = fixedScan("contract_amounts_scan", "Scan contract.amounts (fixed rows)", AMOUNTS, {
  id: [1n, 2n, 3n],
  // Unscaled values at each column's scale.
  d32: [1234567n, -9999999n, null],
  d64: [-123456789012345n, 999999999999999n, 1n],
  d128: [123456789n, -999999999n, null],
});

const amountsInsert = defineTableInOutFunction({
  name: "contract_amounts_insert",
  description: "INSERT into contract.amounts: check decimal widths, echo or count, discard",
  stability: FunctionStability.VOLATILE,
  onBind: (params) => ({ outputSchema: writeResultSchema(writeOptions(params.bindCall).resultMode, AMOUNTS) }),
  process: (params, _state, batch: VgiBatch, out) => {
    for (const declared of AMOUNTS.fields) {
      const arrived = batch.schema.fields.find((f) => f.name === declared.name);
      if (arrived == null) throw new Error(`contract: amounts.${declared.name} missing from the input`);
      const got = typeSignature(arrived.type);
      const want = typeSignature(declared.type);
      if (got !== want) throw new Error(`contract: amounts.${declared.name} arrived as ${got}, declared ${want}`);
    }
    const rows = [...iterRows(batch)] as Record<string, unknown>[];
    const mode = writeOptions(params.initCall.bind_call).resultMode;
    out.emit(writeResultBatch(mode, AMOUNTS, rows.map(() => null), rows));
  },
});

// ----------------------------------------------------------------- schema

const notNullId = constraintRecord({
  schemaPath: [SCHEMA],
  table: "notes",
  id: "notes_id_not_null",
  kind: "NOT_NULL",
  columns: [["id"]],
  enforcement: "ENFORCED",
  validation: "ALL_ROWS",
});

/** The `contract` schema of the example catalog. Deliberately no comment. */
export const contractSchema: SchemaDescriptor = {
  name: SCHEMA,
  tables: [
    { name: "good", function: goodScan },
    { name: "ranged", function: rangedScan },
    { name: "notes", function: notesScan, insertFunction: notesInsert, constraints: [notNullId] },
    { name: "amounts", function: amountsScan, insertFunction: amountsInsert, writeResultModes: { insert: "rows" } },
  ],
};
