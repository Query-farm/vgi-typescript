// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Cacheable same-name-in-two-schemas producer fixtures (`test_same_name_cached`).
//
// The RESULT-CACHE member of the schema-disambiguation family (see
// `examples/same_name.ts` for the scalar dispatch probe and
// `examples/same_name_exchange.ts` for the table-in-out / buffered / aggregate
// probes). Those probe *dispatch*; this one probes the *result cache*, a
// distinct layer.
//
// `test_same_name_cached` is a one-row producer table function that advertises
// `vgi.cache.ttl` and is registered in BOTH the `main` and `data` schemas of the
// `example` catalog. Each schema's implementation emits a single row tagged with
// its own schema name.
//
// The result cache keyed on catalog + auth + function name with no schema
// dimension, so the two implementations produced byte-identical cache keys and
// one schema's memoized rows cross-served the other — the caching-layer twin of
// the (schema, name) dispatch bug. The tag makes a cross-serve visible:
// `example.data.test_same_name_cached()` would return a `main` row. With the
// schema in the key each schema gets its own entry (so `vgi_result_cache()` holds
// two rows for the one function name) and returns its own tag. Driven by
// `test/sql/integration/cache/same_name_schemas.test`.
//
// Mirrors vgi-python's `vgi/_test_fixtures/table/same_name_cached.py`.

import { Schema, Field, Utf8 } from "@query-farm/apache-arrow";
import {
  defineTableFunction,
  batchFromColumns,
  cacheControlMetadata,
  type VgiFunction,
} from "../src/index.js";

// Deliberately shared across the two schemas — the collision is the point.
const FUNCTION_NAME = "test_same_name_cached";

// Long enough that the TTL never lapses mid-test.
const TTL_SECONDS = 300;

// The single VARCHAR column every implementation here emits.
const TAG_SCHEMA = new Schema([new Field("tag", new Utf8(), true)]);

// One-shot emit latch for the single output row.
interface CachedState {
  done: boolean;
}

function makeCached(owningSchema: string): VgiFunction {
  return defineTableFunction<Record<string, never>, CachedState>({
    name: FUNCTION_NAME,
    description: `Schema-disambiguation probe; the ${owningSchema}-schema cacheable producer`,
    onBind: () => ({ outputSchema: TAG_SCHEMA }),
    initialState: () => ({ done: false }),
    process: (params, state, out) => {
      if (state.done) {
        out.finish();
        return;
      }
      out.emit(
        batchFromColumns({ tag: [owningSchema] }, params.outputSchema),
        cacheControlMetadata({ ttl: TTL_SECONDS }),
      );
      state.done = true;
    },
    examples: [
      {
        sql: `SELECT * FROM example.${owningSchema}.test_same_name_cached()`,
        description: `One cacheable row tagged '${owningSchema}'`,
      },
    ],
    categories: ["generator", "cache", "testing"],
  });
}

// The `main`-schema half (advertised in `main`, registered on the worker) and the
// `data`-schema half (advertised in `data`). The worker registers both; the
// catalog schemas scope which surfaces where.
export const sameNameMainCached = makeCached("main");
export const sameNameDataCached = makeCached("data");

export const sameNameCachedMainFunctions = [sameNameMainCached];
export const sameNameCachedDataFunctions = [sameNameDataCached];
export const sameNameCachedFunctions = [sameNameMainCached, sameNameDataCached];
