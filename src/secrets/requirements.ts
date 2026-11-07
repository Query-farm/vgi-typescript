// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Static secret requirements: what a function declares in `requiredSecrets`.
//
// Each requirement becomes one FunctionInfo `required_secrets` entry
// (`{secret_type, scope, secret_name}`). At bind the client resolves every entry
// with DuckDB's secret manager and sends the matches, one struct column per
// secret, keyed by the secret's unique NAME (see `docs/protocol/vgi-secrets.md`
// in vgi-python). A requirement the SDK does not understand must never be
// dropped: the client would not be asked for the secret and the function would
// run without credentials. Anything unrecognised is a definition-time error.

/**
 * A secret requirement in object form. `scope` asks the client for the secret of
 * `secretType` whose scope is the longest prefix of it; `secretName` asks for one
 * secret by its DuckDB name (and takes precedence over `scope`, as it does on
 * the client). Neither means "a secret of this type with an empty scope".
 *
 * The snake_case spelling of the wire entry (`secret_type` / `secret_name`) is
 * accepted too.
 */
export interface SecretRequirementSpec {
  secretType?: string;
  scope?: string | null;
  secretName?: string | null;
  secret_type?: string;
  secret_name?: string | null;
}

/**
 * One `requiredSecrets` entry: a bare secret type (the simplest form — "a secret
 * of this type"), or a {@link SecretRequirementSpec} with a scope or a name.
 */
export type RequiredSecret = string | SecretRequirementSpec;

/** A validated, normalized requirement — the shape sent on the wire. */
export interface SecretRequirement {
  secretType: string;
  scope: string | null;
  secretName: string | null;
}

const KNOWN_KEYS = new Set(["secretType", "secret_type", "scope", "secretName", "secret_name"]);

function optionalString(
  value: unknown,
  what: string,
  where: string,
): string | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== "string") {
    throw new TypeError(`${where}: ${what} must be a string, got ${typeof value}`);
  }
  return value === "" ? null : value;
}

/**
 * Validate and normalize a `requiredSecrets` declaration. Throws on anything
 * that is not a list of bare type strings / requirement objects, on an empty
 * type, on unknown object keys (a typo such as `{ type: "s3" }` would otherwise
 * declare nothing), and on conflicting camelCase/snake_case spellings.
 * Duplicate requirements (same type, scope and name) are collapsed.
 *
 * @param value The declared `requiredSecrets` (undefined → no requirements).
 * @param functionName Used in error messages.
 */
export function normalizeRequiredSecrets(
  value: unknown,
  functionName = "<function>",
): SecretRequirement[] {
  if (value === undefined || value === null) return [];
  const where = `requiredSecrets of '${functionName}'`;
  if (!Array.isArray(value)) {
    throw new TypeError(
      `${where} must be an array of secret types or { secretType, scope?, secretName? } objects, got ${typeof value}`,
    );
  }
  const out: SecretRequirement[] = [];
  const seen = new Set<string>();
  value.forEach((entry: unknown, i: number) => {
    const at = `${where}[${i}]`;
    let req: SecretRequirement;
    if (typeof entry === "string") {
      if (entry === "") throw new TypeError(`${at}: secret type must not be empty`);
      req = { secretType: entry, scope: null, secretName: null };
    } else if (entry !== null && typeof entry === "object" && !Array.isArray(entry)) {
      const obj = entry as Record<string, unknown>;
      const unknown = Object.keys(obj).filter((k) => !KNOWN_KEYS.has(k));
      if (unknown.length > 0) {
        throw new TypeError(
          `${at}: unknown key(s) ${unknown.map((k) => `'${k}'`).join(", ")}; ` +
          `expected secretType, scope, secretName`,
        );
      }
      if (obj.secretType !== undefined && obj.secret_type !== undefined && obj.secretType !== obj.secret_type) {
        throw new TypeError(`${at}: secretType and secret_type disagree`);
      }
      if (obj.secretName !== undefined && obj.secret_name !== undefined && obj.secretName !== obj.secret_name) {
        throw new TypeError(`${at}: secretName and secret_name disagree`);
      }
      const secretType = obj.secretType ?? obj.secret_type;
      if (typeof secretType !== "string" || secretType === "") {
        throw new TypeError(`${at}: secretType must be a non-empty string`);
      }
      req = {
        secretType,
        scope: optionalString(obj.scope, "scope", at),
        secretName: optionalString(obj.secretName ?? obj.secret_name, "secretName", at),
      };
    } else {
      throw new TypeError(
        `${at}: expected a secret type string or a { secretType, scope?, secretName? } object, got ` +
        (entry === null ? "null" : Array.isArray(entry) ? "array" : typeof entry),
      );
    }
    const key = JSON.stringify([req.secretType, req.scope, req.secretName]);
    if (seen.has(key)) return;
    seen.add(key);
    out.push(req);
  });
  return out;
}
