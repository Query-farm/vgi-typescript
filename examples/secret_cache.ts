// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Cacheable fixtures whose results depend on a secret.
// Ports vgi-python/vgi/_test_fixtures/secret_cache.py.
//
// The C++ result cache keys a secret-dependent result on a fingerprint of the
// secrets its bind resolved (never their values), so a result is reused while
// the secret is unchanged and recomputed the moment it is rotated, re-fielded
// or dropped. Each fixture here reads the `vgi_example` secret's
// `secret_string` and advertises cacheability, one per cache path the
// fingerprint has to reach:
//
// * `secret_cache_nonce()` — producer table function; the secret is declared in
//   `requiredSecrets`. Also exposed as the `data.secret_cache_nonce` table
//   (examples/common.ts). vgi-python pre-binds that table (inline_bind), which
//   covers the client's no-bind-RPC path; this SDK has no inline bind, so here
//   the table takes the ordinary bind RPC.
// * `secret_cached_scalar(value)` — scalar, per-value memoized; secret declared
//   via `requiredSecrets`.
// * `secret_cached_lateral(x)` — blended map, per-value memoized, called under
//   LATERAL. vgi-python requests the secret from `on_bind` (the two-phase
//   bind); this SDK's blended onBind cannot return secret lookups, so the
//   secret is declared in `requiredSecrets` instead — as `secret_in_out` does
//   for the same reason. The cache still keys on the secrets that bind
//   resolved; only the route they arrive by differs.
//
// Every output carries a nonce minted only when the worker really runs. It is
// random rather than a counter because a pooled worker may run several
// processes, and a per-process counter repeats across them: equal nonces prove
// a cache HIT, different ones a MISS, on any pool size.
//
// Backs vgi's test/sql/integration/cache/secret_scope.test.

import {
  defineTableFunction,
  defineScalarFunction,
  defineRowTransformFunction,
  batchFromColumns,
  cacheControlMetadata,
  secretsOfType,
  schema,
  field,
  int64,
  utf8,
  type VgiFunction,
} from "../src/index.js";

// The secret type every fixture here reads. Declared by the example catalog's
// `secretTypes` (CREATE SECRET ... (TYPE vgi_example, secret_string '...')).
const SECRET_TYPE = "vgi_example";

// Long enough that TTL never lapses mid-test.
const TTL_SECONDS = 300;

// Both non-scalar fixtures emit the same two columns.
const SECRET_NONCE_SCHEMA = schema([
  field("secret_string", utf8(), true),
  field("nonce", int64(), true),
]);

/**
 * A value unique to this invocation, across every process in a worker pool.
 *
 * 56 random bits, as vgi-python mints them: exact as a bigint and well inside
 * BIGINT.
 */
function mintNonce(): bigint {
  const bytes = crypto.getRandomValues(new Uint8Array(7));
  let n = 0n;
  for (const b of bytes) n = (n << 8n) | BigInt(b);
  return n;
}

/** The `secret_string` of the resolved `vgi_example` secret, or null when none resolved. */
function secretString(secrets: Record<string, Record<string, any>>): string | null {
  const value = secretsOfType(secrets, SECRET_TYPE)[0]?.secret_string;
  return value === null || value === undefined ? null : String(value);
}

// ---------------------------------------------------------------------------
// secret_cache_nonce — producer
// ---------------------------------------------------------------------------

// The secret itself is NOT kept here: state rides the HTTP state token between
// turns, and process() reads the secret off its params on every turn anyway.
interface SecretNonceState {
  nonce: bigint;
  done: boolean;
}

// One row: the secret's secret_string and a per-invocation nonce; cacheable.
// initialState runs only on a cache MISS, so the nonce is stable across HITs. A
// rotated secret must MISS and report the new value; restoring the original
// secret must HIT the entry it produced. maxWorkers is left at its default of
// one, so exactly one stream mints exactly one nonce.
const secret_cache_nonce = defineTableFunction<Record<string, never>, SecretNonceState>({
  name: "secret_cache_nonce",
  description: "One row with a secret's value and a per-invocation nonce; cacheable per secret",
  requiredSecrets: [SECRET_TYPE],
  onBind: () => ({ outputSchema: SECRET_NONCE_SCHEMA }),
  initialState: () => ({ nonce: mintNonce(), done: false }),
  process: (params, state, out) => {
    if (state.done) {
      out.finish();
      return;
    }
    out.emit(
      batchFromColumns(
        { secret_string: [secretString(params.secrets)], nonce: [state.nonce] },
        params.outputSchema,
      ),
      cacheControlMetadata({ ttl: TTL_SECONDS }),
    );
    state.done = true;
  },
  examples: [
    {
      sql: "SELECT * FROM secret_cache_nonce()",
      description: "The nonce is stable while the vgi_example secret is unchanged",
    },
  ],
  categories: ["generator", "cache", "secret", "testing"],
});

// ---------------------------------------------------------------------------
// secret_cached_scalar — scalar, per-value
// ---------------------------------------------------------------------------

// value -> '<secret_string>|<nonce>', memoized per value per secret. With no
// secret resolved the label is '|<nonce>' — a dropped secret is a state the
// test drives, so it must not error. One nonce per compute call, shared by the
// batch, so a served value keeps the nonce of the call that produced it.
// `perValue` is a test choice, as on cached_double_scalar: the point is
// coverage of the tier, not economics. Stability is the default, CONSISTENT.
const secret_cached_scalar = defineScalarFunction({
  name: "secret_cached_scalar",
  description: "Returns '<secret_string>|<nonce>' per value; memoized per value per secret",
  params: { value: int64() },
  argDocs: { value: "Any value; the output ignores it" },
  returns: utf8(),
  requiredSecrets: [SECRET_TYPE],
  cacheControl: { ttl: TTL_SECONDS, perValue: true },
  compute: (batch, _consts, info) => {
    const label = `${secretString(info.secrets) ?? ""}|${mintNonce()}`;
    return new Array<string>(batch.numRows).fill(label);
  },
  examples: [
    {
      sql: "SELECT secret_cached_scalar(1)",
      description: "Stable while the vgi_example secret is unchanged",
    },
  ],
});

// ---------------------------------------------------------------------------
// secret_cached_lateral — blended map, per-value
// ---------------------------------------------------------------------------

// 1->1 map emitting the secret's secret_string and a per-call nonce on every
// row, advertising `perValue` so a correlated LATERAL call is memoized per
// input value per secret. The input value is never read, so nothing here
// decodes a column — the output is built from the row count alone.
const secret_cached_lateral = defineRowTransformFunction({
  name: "secret_cached_lateral",
  description: "Blended map emitting a secret's value and a per-call nonce; memoized per secret",
  args: { x: int64() },
  argDocs: { x: "Input column" },
  requiredSecrets: [SECRET_TYPE],
  onBind: () => ({ outputSchema: SECRET_NONCE_SCHEMA }),
  process: (params, batch, out) => {
    const rows = batch.numRows;
    out.emit(
      batchFromColumns(
        {
          secret_string: new Array(rows).fill(secretString(params.secrets)),
          nonce: new Array(rows).fill(mintNonce()),
        },
        SECRET_NONCE_SCHEMA,
      ),
      cacheControlMetadata({ ttl: TTL_SECONDS, perValue: true }),
    );
  },
  categories: ["blended", "cache", "secret", "test"],
});

/** The producer, also backing the `data.secret_cache_nonce` table. */
export const secretCacheNonceFunction: VgiFunction = secret_cache_nonce;

export const secretCacheFunctions: VgiFunction[] = [
  secret_cache_nonce,
  secret_cached_scalar,
  secret_cached_lateral,
];
