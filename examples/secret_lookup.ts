// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Secret-lookup conformance fixtures.
//
// Implements the language-neutral contract that the shared
// `test/sql/integration/secret/secret_lookup_*.test` files run against every
// SDK's example worker. The contract (function names, declared secret
// requirements, output columns, value rendering) is written down in the vgi repo
// at `docs/secret_lookup_fixtures.md`; keep this module in step with it. Ported
// from vgi-python `vgi/_test_fixtures/secret_lookup.py`.
//
// The resolved-secrets batch is keyed by each secret's unique DuckDB NAME, not
// its type (an unnamed secret arrives as `__default_vgi_example`), so every
// fixture selects through the SDK's public selectors — never by indexing the
// batch with a type:
//
// * secret_value_by_type(field)   requiredSecrets: ["vgi_example"]; secretOfType
// * secret_value_required(field)  same, plus requireSecretOfType in onBind
// * secret_value_by_name(name, field)  two-phase bind by NAME; secretByName
// * secret_value_for_scope(path, field)  three static same-type requirements
//                                 (unscoped, a/, a/b/); secretForScopeOfType
// * secret_types_seen()           vgi_example + s3 scoped to s3://vgi-conformance/;
//                                 one row per delivered secret
//
// Requirements are declared statically except for secret_value_by_name, which
// is dynamic on purpose: the static declaration is what the others test, and a
// dynamic request would mask a dropped one.

import {
  defineTableFunction,
  batchFromColumns,
  emptyBatch,
  secretOfType,
  requireSecretOfType,
  secretByName,
  secretForScopeOfType,
  schema,
  field,
  utf8,
  type SecretFields,
  type VgiFunction,
} from "../src/index.js";
// `bool` from the package root is vgi-rpc's argument builder; the Arrow type
// factory comes from the arrow facade.
import { bool } from "../src/arrow/index.js";

/** The secret type every value fixture reads (registered by the example catalog). */
const SECRET_TYPE = "vgi_example";

/** Static scopes for secret_value_for_scope — nested, so longest-prefix is observable. */
const SCOPE_A = "vgi-conformance://a/";
const SCOPE_AB = "vgi-conformance://a/b/";

/**
 * The foreign type secret_types_seen requires, and its scope. DuckDB's `s3`
 * secrets default to the scope `s3://`, so an unscoped lookup never matches one.
 */
const S3_SCOPE = "s3://vgi-conformance/";

const VALUE_SCHEMA = schema([
  field("found", bool(), true),
  field("secret_name", utf8(), true),
  field("value", utf8(), true),
]);

const SEEN_SCHEMA = schema([
  field("secret_name", utf8(), true),
  field("secret_type", utf8(), true),
  field("scope", utf8(), true),
]);

/** Render a secret field as text: NULL stays NULL, booleans lowercase, numbers plain decimal. */
function render(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "boolean") return value ? "true" : "false";
  return String(value);
}

interface ValueRow {
  found: boolean;
  secretName: string | null;
  value: string | null;
}

function rowFor(secret: SecretFields | undefined, fieldName: string): ValueRow {
  if (!secret) return { found: false, secretName: null, value: null };
  return {
    found: true,
    secretName: render(secret.name),
    value: Object.prototype.hasOwnProperty.call(secret, fieldName) ? render(secret[fieldName]) : null,
  };
}

// The row is recomputed from params.secrets in process() rather than carried in
// state: secrets reach every process() turn on params, and state rides the HTTP
// state token, which must never hold a secret value.
interface DoneState {
  done: boolean;
}

function emitValueRow(row: ValueRow, outputSchema: any, state: DoneState, out: any): void {
  if (state.done) {
    out.finish();
    return;
  }
  out.emit(
    batchFromColumns(
      { found: [row.found], secret_name: [row.secretName], value: [row.value] },
      outputSchema,
    ),
  );
  state.done = true;
}

// ---------------------------------------------------------------------------
// secret_value_by_type
// ---------------------------------------------------------------------------
const secret_value_by_type = defineTableFunction<{ field: string }, DoneState>({
  name: "secret_value_by_type",
  description: "Conformance: a field of the vgi_example secret, selected by type",
  args: { field: utf8() },
  argDocs: { field: "Secret field to return" },
  // The simplest declaration form: a bare type string.
  requiredSecrets: [SECRET_TYPE],
  onBind: () => ({ outputSchema: VALUE_SCHEMA }),
  initialState: () => ({ done: false }),
  process: (params, state, out) => {
    emitValueRow(rowFor(secretOfType(params.secrets, SECRET_TYPE), params.args.field), params.outputSchema, state, out);
  },
  examples: [{ sql: "SELECT * FROM secret_value_by_type('api_key')", description: "By type" }],
  categories: ["secret", "testing"],
});

// ---------------------------------------------------------------------------
// secret_value_required
// ---------------------------------------------------------------------------
const secret_value_required = defineTableFunction<{ field: string }, DoneState>({
  name: "secret_value_required",
  description: "Conformance: a field of the required vgi_example secret; errors when absent",
  args: { field: utf8() },
  argDocs: { field: "Secret field to return" },
  requiredSecrets: [SECRET_TYPE],
  onBind: (params) => {
    // Fails the bind, naming the type, when no vgi_example secret was resolved.
    requireSecretOfType(params.secrets, SECRET_TYPE);
    return { outputSchema: VALUE_SCHEMA };
  },
  initialState: () => ({ done: false }),
  process: (params, state, out) => {
    emitValueRow(rowFor(secretOfType(params.secrets, SECRET_TYPE), params.args.field), params.outputSchema, state, out);
  },
  examples: [{ sql: "SELECT * FROM secret_value_required('api_key')", description: "Required" }],
  categories: ["secret", "testing"],
});

// ---------------------------------------------------------------------------
// secret_value_by_name — dynamic (two-phase bind) request by NAME
// ---------------------------------------------------------------------------
const secret_value_by_name = defineTableFunction<{ name: string; field: string }, DoneState>({
  name: "secret_value_by_name",
  description: "Conformance: a field of the named vgi_example secret",
  args: { name: utf8(), field: utf8() },
  argDocs: { name: "Secret name to look up", field: "Secret field to return" },
  onBind: (params) => {
    if (!params.resolvedSecretsProvided) {
      return {
        outputSchema: VALUE_SCHEMA,
        lookupSecretTypes: [SECRET_TYPE],
        lookupScopes: [""],
        lookupNames: [params.args.name],
      };
    }
    return { outputSchema: VALUE_SCHEMA };
  },
  initialState: () => ({ done: false }),
  process: (params, state, out) => {
    emitValueRow(
      rowFor(secretByName(params.secrets, params.args.name, SECRET_TYPE), params.args.field),
      params.outputSchema,
      state,
      out,
    );
  },
  examples: [{ sql: "SELECT * FROM secret_value_by_name('my_secret', 'api_key')", description: "By name" }],
  categories: ["secret", "testing"],
});

// ---------------------------------------------------------------------------
// secret_value_for_scope — three static same-type requirements
// ---------------------------------------------------------------------------
const secret_value_for_scope = defineTableFunction<{ path: string; field: string }, DoneState>({
  name: "secret_value_for_scope",
  description: "Conformance: a field of the vgi_example secret whose scope best matches a path",
  args: { path: utf8(), field: utf8() },
  argDocs: { path: "Path to select a secret for (longest scope prefix)", field: "Secret field to return" },
  requiredSecrets: [
    SECRET_TYPE,
    { secretType: SECRET_TYPE, scope: SCOPE_A },
    { secretType: SECRET_TYPE, scope: SCOPE_AB },
  ],
  onBind: () => ({ outputSchema: VALUE_SCHEMA }),
  initialState: () => ({ done: false }),
  process: (params, state, out) => {
    emitValueRow(
      rowFor(secretForScopeOfType(params.secrets, params.args.path, SECRET_TYPE), params.args.field),
      params.outputSchema,
      state,
      out,
    );
  },
  examples: [
    {
      sql: "SELECT * FROM secret_value_for_scope('vgi-conformance://a/b/x', 'api_key')",
      description: "Longest-prefix scope selection",
    },
  ],
  categories: ["secret", "testing"],
});

// ---------------------------------------------------------------------------
// secret_types_seen — vgi_example + scoped s3; one row per delivered secret
// ---------------------------------------------------------------------------
const secret_types_seen = defineTableFunction<Record<string, never>, DoneState>({
  name: "secret_types_seen",
  description: "Conformance: the secrets delivered for a vgi_example + s3 requirement",
  requiredSecrets: [SECRET_TYPE, { secretType: "s3", scope: S3_SCOPE }],
  onBind: () => ({ outputSchema: SEEN_SCHEMA }),
  initialState: () => ({ done: false }),
  process: (params, state, out) => {
    if (state.done) {
      out.finish();
      return;
    }
    state.done = true;
    const names = Object.keys(params.secrets).sort();
    if (names.length === 0) {
      out.emit(emptyBatch(params.outputSchema));
      return;
    }
    out.emit(
      batchFromColumns(
        {
          secret_name: names,
          secret_type: names.map((n) => render(params.secrets[n]!.type)),
          scope: names.map((n) => render(params.secrets[n]!.scope) ?? ""),
        },
        params.outputSchema,
      ),
    );
  },
  examples: [{ sql: "SELECT * FROM secret_types_seen()", description: "Delivered secrets" }],
  categories: ["secret", "testing"],
});

export const secretLookupFunctions: VgiFunction[] = [
  secret_value_by_type,
  secret_value_by_name,
  secret_value_for_scope,
  secret_value_required,
  secret_types_seen,
];
