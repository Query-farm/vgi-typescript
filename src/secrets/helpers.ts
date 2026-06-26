// Type- and scope-aware selection over resolved secrets.
//
// Resolved secrets reach a worker as a plain object keyed by each secret's
// unique DuckDB secret name (so several secrets of the same type — e.g. one per
// S3 bucket — coexist). Each secret carries its connector-serialized `type` (the
// DuckDB secret type) and `scope` (newline-joined scope prefixes) fields, plus
// type-specific fields. These helpers mirror `vgi::Secrets` in the Rust SDK.

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

/** Every resolved secret whose serialized `type` field matches `type`. */
export function secretsOfType(secrets: SecretsDict, type: string): SecretFields[] {
  return Object.values(secrets ?? {}).filter((f) => scalarStr(f.type) === type);
}

/**
 * The secret whose `scope` is the longest prefix of `path`. The connector
 * serializes each secret's scope as a newline-joined list of prefixes; a secret
 * with no (or empty) scope matches as a last-resort fallback. Returns undefined
 * only when there are no candidate secrets.
 */
export function secretForScope(secrets: SecretsDict, path: string): SecretFields | undefined {
  return selectForScope(secrets, path, undefined);
}

/** Like {@link secretForScope} but only over secrets of `type`. */
export function secretForScopeOfType(
  secrets: SecretsDict,
  path: string,
  type: string,
): SecretFields | undefined {
  return selectForScope(secrets, path, type);
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
