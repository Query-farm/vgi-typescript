// Read data out of Arrow batches as plain JS values.

import { RecordBatch } from "@query-farm/apache-arrow";

/**
 * Iterate rows of a RecordBatch as plain objects.
 */
export function* iterRows(
  batch: RecordBatch
): Generator<Record<string, any>> {
  for (let i = 0; i < batch.numRows; i++) {
    const row: Record<string, any> = {};
    for (const field of batch.schema.fields) {
      const col = batch.getChild(field.name);
      row[field.name] = col ? col.get(i) : null;
    }
    yield row;
  }
}

/**
 * Extract single-row batch to a scalar dict.
 */
export function batchToScalarDict(
  batch: RecordBatch | null
): Record<string, any> {
  if (!batch || batch.numRows === 0) return {};
  const result: Record<string, any> = {};
  for (const field of batch.schema.fields) {
    const col = batch.getChild(field.name);
    if (col) {
      result[field.name] = col.get(0);
    }
  }
  return result;
}

/**
 * Extract single-row batch to a secret dict (column per secret, each value is a struct).
 * Handles both named secrets (column name = secret type) and scoped secrets
 * (column name = "secret_N" with secret_type in field metadata).
 */
export function batchToSecretDict(
  batch: RecordBatch | null
): Record<string, Record<string, any>> {
  if (!batch || batch.numRows === 0) return {};
  const result: Record<string, Record<string, any>> = {};
  for (const field of batch.schema.fields) {
    const col = batch.getChild(field.name);
    if (col) {
      const val = col.get(0);

      // Determine the key: for scoped secrets (secret_N), use secret_type from metadata
      let key = field.name;
      let scope: string | undefined;
      if (field.name.startsWith("secret_")) {
        const secretType = field.metadata?.get?.("secret_type");
        if (secretType) {
          key = secretType;
          scope = field.metadata?.get?.("scope") ?? undefined;
        }
      }

      if (val && typeof val === "object" && !ArrayBuffer.isView(val)) {
        // Struct scalar -> convert to plain object
        const dict: Record<string, any> = {};
        if (val.toJSON) {
          Object.assign(dict, val.toJSON());
        } else {
          Object.assign(dict, val);
        }
        if (key in result) {
          throw new Error(
            `batchToSecretDict: duplicate secret_type '${key}' (scope=${scope ?? "none"}). ` +
            `Use scoped secrets with distinct scopes to avoid collisions.`
          );
        }
        result[key] = dict;
        // Store scope-qualified key for disambiguation
        if (scope) {
          result[`${key}:${scope}`] = dict;
        }
      } else if (val === null || val === undefined) {
        // Skip null struct values (secret not found)
      } else {
        result[key] = {};
      }
    }
  }
  return result;
}

/**
 * Safe number coercion for BigInt values.
 */
export function safeNumber(value: any): number {
  if (typeof value === "bigint") {
    return Number(value);
  }
  return value as number;
}
