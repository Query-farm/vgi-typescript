// Type- and scope-aware selection over resolved secrets.
//
// Resolved secrets reach a worker as a plain object keyed by each secret's
// unique DuckDB secret NAME, not its type: an unnamed
// `CREATE SECRET (TYPE brave, ...)` arrives as `__default_brave`, and several
// secrets of one type (e.g. one per S3 bucket) coexist. Each secret carries its
// connector-serialized `type` (the DuckDB secret type), `name`, and `scope`
// (newline-joined scope prefixes) fields, plus type-specific fields.
//
// So never look a TYPE up as a key (`secrets["brave"]` finds the secret only
// when the user happened to name it after its type). Select with these helpers,
// which apply the same rules the client used to resolve the request
// (vgi-python `docs/protocol/vgi-secrets.md` section 5):
//
//   by type   -> secretOfType / secretsOfType / requireSecretOfType
//   by name   -> secretByName
//   by scope  -> secretForScopeOfType (longest prefix, else unscoped, else none)
//
// Every by-type selector matches the secret's `type` field. Only when no
// secret carries that type does it fall back to a secret whose KEY is the type
// and that has no `type` field at all: a bare key/value struct from a client
// that keys the batch by type (as vgi-python's standalone Client allows).
// These helpers mirror `vgi::Secrets` in the Rust SDK.

export type SecretFields = Record<string, any>;
export type SecretsDict = Record<string, SecretFields>;

function scalarStr(v: any): string {
  if (v === null || v === undefined) return "";
  return typeof v === "string" ? v : String(v);
}

/** The DuckDB secret type of the named secret (its serialized `type` field). */
export function secretType(secrets: SecretsDict, name: string): string | undefined {
  const f = secrets?.[name];
  return f && "type" in f ? scalarStr(f.type) : undefined;
}

/** True when the secret's serialized `type` field is absent or empty. */
function untyped(fields: SecretFields | undefined): boolean {
  return !!fields && scalarStr(fields.type) === "";
}

/**
 * The legacy by-key fallback: a secret stored under the key `type` that has no
 * `type` field of its own (a client that keys the batch by type and sends bare
 * key/value structs). A secret under that key whose `type` field names another
 * type is NOT a match.
 */
function keyedByType(secrets: SecretsDict, type: string): SecretFields | undefined {
  const f = secrets?.[type];
  return untyped(f) ? f : undefined;
}

/**
 * Every resolved secret whose serialized `type` field matches `type`, in
 * delivery order. Falls back to an untyped secret keyed by `type` only when no
 * secret carries the type.
 */
export function secretsOfType(secrets: SecretsDict, type: string): SecretFields[] {
  const typed = Object.values(secrets ?? {}).filter((f) => scalarStr(f.type) === type);
  if (typed.length > 0) return typed;
  const legacy = keyedByType(secrets, type);
  return legacy ? [legacy] : [];
}

/**
 * The secret a by-TYPE request refers to: a secret of `type` with an empty
 * scope, else the first secret of `type`, else undefined. This is the rule the
 * client applies to a `requiredSecrets: ["<type>"]` declaration, so it picks the
 * secret that declaration resolved even when other same-type secrets (from
 * scoped declarations) were delivered alongside it.
 */
export function secretOfType(secrets: SecretsDict, type: string): SecretFields | undefined {
  const all = secretsOfType(secrets, type);
  return all.find((f) => scalarStr(f.scope) === "") ?? all[0];
}

/**
 * Like {@link secretOfType}, but throws when no secret of `type` was resolved.
 * Call it from `onBind` to fail the bind of a function that cannot run without
 * credentials. The message names the type and never includes a secret value.
 */
export function requireSecretOfType(secrets: SecretsDict, type: string): SecretFields {
  const found = secretOfType(secrets, type);
  if (!found) {
    throw new Error(
      `required secret of type '${type}' was not found; create one with CREATE SECRET (TYPE ${type}, ...)`,
    );
  }
  return found;
}

/**
 * The secret with DuckDB name `name` (its key in the resolved batch). With
 * `type`, only if that secret's `type` field is `type` (or absent, for a bare
 * key/value struct) — the rule the client applies to a by-name request.
 */
export function secretByName(
  secrets: SecretsDict,
  name: string,
  type?: string,
): SecretFields | undefined {
  if (!secrets || !Object.prototype.hasOwnProperty.call(secrets, name)) return undefined;
  const f = secrets[name];
  if (!f) return undefined;
  if (type === undefined || untyped(f) || scalarStr(f.type) === type) return f;
  return undefined;
}

/**
 * The secret whose `scope` is the longest prefix of `path`. The connector
 * serializes each secret's scope as a newline-joined list of prefixes; a secret
 * with no (or empty) scope matches as a last-resort fallback. Returns undefined
 * when no scope is a prefix of `path` and no unscoped secret was delivered.
 */
export function secretForScope(secrets: SecretsDict, path: string): SecretFields | undefined {
  return selectForScope(secrets, path, undefined);
}

/**
 * Like {@link secretForScope} but only over secrets of `type`: the one whose
 * scope is the longest prefix of `path`, else one of `type` with an empty scope,
 * else undefined.
 */
export function secretForScopeOfType(
  secrets: SecretsDict,
  path: string,
  type: string,
): SecretFields | undefined {
  return selectForScope(secrets, path, type) ?? keyedByType(secrets, type);
}

function selectForScope(
  secrets: SecretsDict,
  path: string,
  type: string | undefined,
): SecretFields | undefined {
  let best: SecretFields | undefined;
  let bestLen = -1;
  let fallback: SecretFields | undefined;
  for (const fields of Object.values(secrets ?? {})) {
    if (type !== undefined && scalarStr(fields.type) !== type) continue;
    const scope = scalarStr(fields.scope);
    if (!scope) {
      if (!fallback) fallback = fields;
      continue;
    }
    for (const prefix of scope.split("\n")) {
      if (prefix && path.startsWith(prefix) && prefix.length > bestLen) {
        bestLen = prefix.length;
        best = fields;
      }
    }
  }
  return best ?? fallback;
}
