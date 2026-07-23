// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Same-name-in-two-schemas scalar fixtures (`test_same_name_bind`).
//
// Two distinct scalar functions register under the *same* name but live in
// different schemas of the `example` catalog (`main` and `data`). They prove
// that VGI resolves a schema-qualified call to the implementation in that
// schema — `example.main.test_same_name_bind(x)` must reach the `main` function
// and `example.data.test_same_name_bind(x)` the `data` one — rather than
// collapsing both into one flat by-name registry entry.
//
// Each returns a VARCHAR tagged with its own schema, so a mis-routed call is
// visible in the query result rather than silently plausible.
//
// Mirrors vgi-python's `vgi/_test_fixtures/scalar/same_name.py`; driven by
// `test/sql/integration/scalar/same_name_schemas.test`.

import { Int64, Utf8, RecordBatch } from "@query-farm/apache-arrow";
import { defineScalarFunction } from "../src/index.js";

/** Render `<schemaName>:<value>` for every row of column 0, preserving nulls. */
function tag(schemaName: string, batch: RecordBatch): (string | null)[] {
  const col = batch.getChildAt(0);
  if (!col) return [];
  const out: (string | null)[] = [];
  for (let i = 0; i < col.length; i++) {
    const v = col.get(i);
    out.push(v === null || v === undefined ? null : `${schemaName}:${v}`);
  }
  return out;
}

export const sameNameMain = defineScalarFunction({
  name: "test_same_name_bind",
  description: "Schema-disambiguation probe; the main-schema implementation",
  params: { value: new Int64() },
  argDocs: { value: "Integer value to tag" },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => tag("main", batch),
  examples: [
    { sql: "SELECT example.main.test_same_name_bind(1)", description: "Returns 'main:1'" },
  ],
});

export const sameNameData = defineScalarFunction({
  name: "test_same_name_bind",
  description: "Schema-disambiguation probe; the data-schema implementation",
  params: { value: new Int64() },
  argDocs: { value: "Integer value to tag" },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => tag("data", batch),
  examples: [
    { sql: "SELECT example.data.test_same_name_bind(1)", description: "Returns 'data:1'" },
  ],
});

export const sameNameFunctions = [sameNameMain, sameNameData];
