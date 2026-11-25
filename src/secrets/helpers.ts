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
 * Matching follows DuckDB exactly (see {@link scopeMatches}): an
 * authority-only scope such as `snmp://10.0.0.1` covers only that host and
 * paths under it, never `snmp://10.0.0.15` or `snmp://10.0.0.1:161`.
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
      if (prefix && scopeMatches(prefix, path) && prefix.length > bestLen) {
        bestLen = prefix.length;
        best = fields;
      }
    }
  }
  return best ?? fallback;
}

/**
 * The URI authority of `text`, or `""`; mirrors DuckDB's
 * `StringUtil::GetURIAuthority`. `text` is a URI only if it contains `://`
 * after a non-empty scheme and its first `/` or `\\` is the one that starts
 * that `://`. The authority runs from after `://` to the first `/`, `\\`, `?`
 * or `#` (or the end).
 */
function uriAuthority(text: string): string {
  const schemeEnd = text.indexOf("://");
  if (schemeEnd <= 0) return "";
  const slashes = [text.indexOf("/"), text.indexOf("\\")].filter((i) => i !== -1);
  const firstSlash = slashes.length ? Math.min(...slashes) : -1;
  if (firstSlash !== schemeEnd + 1) return "";
  const start = schemeEnd + 3;
  const ends = ["/", "\\", "?", "#"].map((c) => text.indexOf(c, start)).filter((i) => i !== -1);
  return ends.length ? text.slice(start, Math.min(...ends)) : text.slice(start);
}

/**
 * Whether a secret scope `prefix` covers `path`, exactly as DuckDB decides.
 *
 * A scope matches when it is a string prefix of `path`, except that an
 * authority-only URI scope (`scheme://authority` with nothing after the
 * authority) has an implicit trailing `/`: `snmp://10.0.0.1` covers
 * `snmp://10.0.0.1` and `snmp://10.0.0.1/x` but not `snmp://10.0.0.15` or
 * `snmp://10.0.0.1:161`, and `s3://bucket` does not cover `s3://bucket-evil/`.
 * This is DuckDB's `BaseSecret::MatchScore` as of the Haybarn 2.0.1 engine
 * (duckdb#28684). Indexing is by UTF-16 unit rather than DuckDB's byte, which
 * agrees here: every delimiter is ASCII, `startsWith` aligns the prefix, and
 * the unit after it is `/` exactly when the next UTF-8 byte is `/`.
 */
export function scopeMatches(prefix: string, path: string): boolean {
  if (!prefix || !path.startsWith(prefix)) return false;
  const authority = uriAuthority(prefix);
  const authorityOnly =
    authority.length > 0 && prefix.length === prefix.indexOf("://") + 3 + authority.length;
  if (!authorityOnly) return true;
  return path.length === prefix.length || path[prefix.length] === "/";
}
