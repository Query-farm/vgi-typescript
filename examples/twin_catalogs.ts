// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Two catalogs, one worker, colliding function names.
//
// `twin_a` and `twin_b` are separate VGI catalogs served by the same worker
// process (composed via CompositeCatalogInterface). Each declares a schema
// literally named `main` holding a scalar literally named
// `test_same_name_catalog` — so neither the function name nor the schema name
// distinguishes them. Only the catalog does.
//
// Attaching both from the same worker LOCATION and calling
// `a.main.test_same_name_catalog(1)` vs `b.main.test_same_name_catalog(1)` must
// reach different implementations. The routing key is the per-attach
// attach_opaque_data, which identifies which catalog the caller attached.
//
// Companion to `same_name.ts`, which collides two names within a *single*
// catalog across two schemas. Mirrors vgi-python's
// `vgi/_test_fixtures/twin_catalogs.py`; driven by
// `test/sql/integration/scalar/same_name_catalogs.test`.

import { Int64, Utf8, RecordBatch } from "@query-farm/apache-arrow";
import { defineScalarFunction, type CatalogDescriptor } from "../src/index.js";

// Deliberately identical in both catalogs — the collision is the point.
const FUNCTION_NAME = "test_same_name_catalog";
const SCHEMA_NAME = "main";

export const CATALOG_A = "twin_a";
export const CATALOG_B = "twin_b";

/** Render `<catalogName>:<value>` for every row of column 0, preserving nulls. */
function tag(catalogName: string, batch: RecordBatch): (string | null)[] {
  const col = batch.getChildAt(0);
  if (!col) return [];
  const out: (string | null)[] = [];
  for (let i = 0; i < col.length; i++) {
    const v = col.get(i);
    out.push(v === null || v === undefined ? null : `${catalogName}:${v}`);
  }
  return out;
}

const twinA = defineScalarFunction({
  name: FUNCTION_NAME,
  description: "Catalog-disambiguation probe; the twin_a implementation",
  params: { value: new Int64() },
  argDocs: { value: "Integer value to tag" },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => tag(CATALOG_A, batch),
  examples: [
    { sql: "SELECT a.main.test_same_name_catalog(1)", description: "Returns 'twin_a:1'" },
  ],
});

const twinB = defineScalarFunction({
  name: FUNCTION_NAME,
  description: "Catalog-disambiguation probe; the twin_b implementation",
  params: { value: new Int64() },
  argDocs: { value: "Integer value to tag" },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => tag(CATALOG_B, batch),
  examples: [
    { sql: "SELECT b.main.test_same_name_catalog(1)", description: "Returns 'twin_b:1'" },
  ],
});

function twinCatalog(name: string, fn: typeof twinA): CatalogDescriptor {
  return {
    name,
    defaultSchema: SCHEMA_NAME,
    comment: `Catalog-disambiguation twin (${name})`,
    schemas: [
      {
        name: SCHEMA_NAME,
        comment: `Colliding function name served by ${name}`,
        functions: [fn],
      },
    ],
  };
}

export const twinACatalog = twinCatalog(CATALOG_A, twinA);
export const twinBCatalog = twinCatalog(CATALOG_B, twinB);

// Registered on the worker so both are routable; the catalogs above scope them.
export const twinCatalogFunctions = [twinA, twinB];
