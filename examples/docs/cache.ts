// Copyright 2025, 2026 Query Farm LLC - https://query.farm

// cache is the result-caching example for the vgi-typescript documentation.
//
// Caching is advertised, not requested: the worker attaches vgi.cache.*
// metadata to the FIRST data batch it emits, and the client (the DuckDB
// extension) decides what to do with it. Nothing is cached unless you say so.
//
// This worker exposes rates(), standing in for a slow upstream whose answer is
// worth reusing, and shows the whole vocabulary: a freshness lifetime, a
// validator plus revalidatable so the client can ask "still good?" instead of
// paying for a recompute, and the 304-equivalent reply to such a request.
//
//   bun run cache.ts
//   # then, in a Haybarn shell:
//   ATTACH 'rates' (TYPE vgi, LOCATION 'bun run /abs/path/cache.ts');
//   SELECT * FROM rates.rates();   -- repeat calls inside the TTL never land here
//   SELECT hits, misses, inserts FROM vgi_result_cache_stats();
//   SELECT * FROM rates.upstream_calls();   -- proves the worker was not re-run

import {
  Worker,
  defineTableFunction,
  batchFromColumns,
  cacheControlMetadata,
  toSchema,
  int,
  str,
} from "@query-farm/vgi";

const ratesSchema = toSchema({ pair: str, rate: int });

// A strong validator for the payload below. Anything opaque and stable works —
// a content hash, a database version, an upstream ETag — as long as it changes
// exactly when the payload does.
const ETAG = '"rates-v1"';

const TTL_SECONDS = 300;

// Counts real invocations so the caching can be observed rather than assumed.
let calls = 0;

export const rates = defineTableFunction({
  name: "rates",
  description: "Exchange rates from a slow upstream, cached on the client",

  onBind: () => ({ outputSchema: ratesSchema }),
  initialState: () => ({ emitted: false }),

  process: (params, state, out) => {
    if (state.emitted) return out.finish();
    state.emitted = true;

    // A conditional request: the client already has a payload and is asking
    // whether it is still good. Answering costs nothing here, which is exactly
    // when `revalidatable` is worth advertising.
    if (params.ifNoneMatch === ETAG) {
      out.emit(
        batchFromColumns({ pair: [], rate: [] }, ratesSchema),
        // A zero-row batch carrying notModified is the 304 equivalent: keep
        // what you have. The client re-uses its stored rows without a restream.
        cacheControlMetadata({
          notModified: true,
          ttl: TTL_SECONDS,
          etag: ETAG,
          revalidatable: true,
        }),
      );
      return;
    }

    calls++;
    out.emit(
      batchFromColumns(
        { pair: ["EURUSD", "GBPUSD", "USDJPY"], rate: [108n, 127n, 15700n] },
        ratesSchema,
      ),
      // Metadata rides the FIRST data batch. It cannot go on the schema — the
      // IPC stream fixes that when the stream opens, before this runs.
      cacheControlMetadata({
        ttl: TTL_SECONDS,
        etag: ETAG,
        revalidatable: true,
        // Grace windows: serve stale immediately while refreshing in the
        // background, and keep serving stale if a refresh RPC fails.
        staleWhileRevalidate: 60,
        staleIfError: 3600,
      }),
    );
  },
});

// Reports how many times the upstream was actually hit, so a query can prove
// the cache engaged rather than take it on faith.
export const upstreamCalls = defineTableFunction({
  name: "upstream_calls",
  description: "How many times rates() actually computed a result",
  onBind: () => ({ outputSchema: toSchema({ calls: int }) }),
  initialState: () => ({ emitted: false }),
  process: (_params, state, out) => {
    if (state.emitted) return out.finish();
    state.emitted = true;
    out.emit(batchFromColumns({ calls: [BigInt(calls)] }, toSchema({ calls: int })));
  },
});

export const worker = new Worker({
  catalog: {
    name: "rates",
    comment: "Documentation example: advertising a cacheable result",
    schemas: [{ name: "main", functions: [rates, upstreamCalls] }],
  },
});

if (import.meta.main) worker.run();
