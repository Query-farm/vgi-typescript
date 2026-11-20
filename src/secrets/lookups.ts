// Copyright 2025, 2026 Query Farm LLC - https://query.farm

/**
 * The secret-lookup fields an `onBind` may return to ask DuckDB for secrets.
 *
 * The three lists are parallel: entry `i` requests a secret of type
 * `lookupSecretTypes[i]`, optionally narrowed by `lookupScopes[i]` and
 * `lookupNames[i]` (`""` or a missing entry = unset). A lookup without a type
 * cannot be expressed, so `lookupSecretTypes` drives the request.
 */
export interface SecretLookupFields {
  lookupSecretTypes?: string[];
  lookupScopes?: string[];
  lookupNames?: string[];
}

/**
 * Check an `onBind` result's secret-lookup lists before they go on the wire.
 *
 * The extension reads one lookup per `lookupSecretTypes` entry, so a scope or
 * name with no type at the same index was silently dropped: returning
 * `lookupNames: ["prod"]` alone requested nothing and the secret arrived
 * missing. That is now a bind error naming the fix.
 *
 * @param fnName function name, for the error message.
 * @param result the `onBind` return value.
 * @returns the three lists for `BindResponse`.
 * @throws Error when `lookupScopes` or `lookupNames` is longer than `lookupSecretTypes`.
 */
export function secretLookupWire(
  fnName: string,
  result: SecretLookupFields,
): { lookup_secret_types: string[] | undefined; lookup_scopes: string[] | undefined; lookup_names: string[] | undefined } {
  const types = result.lookupSecretTypes ?? [];
  for (const [key, list] of [
    ["lookupScopes", result.lookupScopes],
    ["lookupNames", result.lookupNames],
  ] as const) {
    if (list && list.length > types.length) {
      throw new Error(
        `${fnName}: onBind returned ${list.length} ${key} but ${types.length} lookupSecretTypes. ` +
          "The lists are parallel (entry i is lookupSecretTypes[i] narrowed by lookupScopes[i] / " +
          "lookupNames[i]); every lookup needs a secret type, so the extra entries would request nothing. " +
          'Return e.g. { lookupSecretTypes: ["my_type"], lookupScopes: [""], lookupNames: ["prod"] }.',
      );
    }
  }
  return {
    lookup_secret_types: result.lookupSecretTypes,
    lookup_scopes: result.lookupScopes,
    lookup_names: result.lookupNames,
  };
}
