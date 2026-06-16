// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Read data out of Arrow batches as plain JS values.

import { RecordBatch } from "@query-farm/apache-arrow";
import type { VgiBatch, VgiDataType } from "../types.js";
import { codecFor } from "../codec/registry.js";
import { readCanonicalValue } from "./canonical.js";

/**
 * Iterate rows of a RecordBatch as plain objects, in the RICH representation
 * (Date for date32/date64; canonical otherwise). Reads via the per-backend
 * canonical reader (lossless, backend-agnostic) then maps canonical -> rich
 * through the codec — symmetric with the build path. Accepts arrow-js
 * `RecordBatch` or facade `VgiBatch`.
 */
export function* iterRows(
  batch: RecordBatch | VgiBatch
): Generator<Record<string, any>> {
  const a = batch as RecordBatch;
  const codecs = a.schema.fields.map((f) => codecFor(f.type as unknown as VgiDataType));
  for (let i = 0; i < a.numRows; i++) {
    const row: Record<string, any> = {};
    for (let fi = 0; fi < a.schema.fields.length; fi++) {
      const field = a.schema.fields[fi];
      const col = a.getChild(field.name);
      if (!col) { row[field.name] = null; continue; }
      const canonical = readCanonicalValue(field.type as unknown as VgiDataType, col, i);
      row[field.name] = codecs[fi].canonicalToRich(canonical);
    }
    yield row;
  }
}

/**
 * Extract single-row batch to a scalar dict, in the RICH representation. Routes
 * through the canonical reader + codec (same as iterRows) so a temporal/decimal
 * setting is represented identically to column data and across backends, and so
 * Dictionary-encoded columns (DuckDB sends these for enum-shaped fields like
 * SchemaObjectType) are decoded — readCanonicalValue handles dictionary decode.
 */
export function batchToScalarDict(
  batch: RecordBatch | VgiBatch | null
): Record<string, any> {
  if (!batch) return {};
  const a = batch as RecordBatch;
  if (a.numRows === 0) return {};
  const result: Record<string, any> = {};
  for (const field of a.schema.fields) {
    const col = a.getChild(field.name);
    if (col) {
      const type = field.type as unknown as VgiDataType;
      const canonical = readCanonicalValue(type, col, 0);
      result[field.name] = codecFor(type).canonicalToRich(canonical);
    }
  }
  return result;
}

/**
 * If `value` looks like a Dictionary-encoded Arrow scalar (Vector.get on a
 * dict column on the apache-arrow fork returns the underlying Data, not the
 * decoded string), pull out the decoded value at row `index`. Returns
 * `value` unchanged when it isn't dict-shaped.
 *
 * Used at handler call sites where the incoming `params` came from the RPC
 * layer's row extractor (which doesn't auto-decode dictionaries) and the
 * handler needs the plain string.
 */
export function decodeDictValue(value: any, index = 0): any {
  if (
    value && typeof value === "object" &&
    "dictionary" in value && "values" in value &&
    typeof (value as any).dictionary?.get === "function"
  ) {
    // Respect the null bitmap — for nullable Dictionary columns DuckDB emits
    // a "null" value as bitmap-bit=0 + index=0 (or whatever uninitialized
    // memory holds). Without this check we'd return dictionary[0], turning
    // "no ORDER BY pushdown" into a spurious "ASC NULLS_FIRST".
    const data = value as any;
    const nullBitmap = data.nullBitmap;
    if (nullBitmap && nullBitmap.length > 0) {
      const byte = nullBitmap[index >> 3];
      if (((byte >> (index & 7)) & 1) === 0) return null;
    }
    const idx = Number(data.values[index]);
    return (value as any).dictionary.get(idx);
  }
  return value;
}

/**
 * Extract single-row batch to a secret dict (column per secret, each value is a struct).
 * Handles both named secrets (column name = secret type) and scoped secrets
 * (column name = "secret_N" with secret_type in field metadata).
 */
export function batchToSecretDict(
  batch: RecordBatch | VgiBatch | null
): Record<string, Record<string, any>> {
  if (!batch) return {};
  const a = batch as RecordBatch;
  if (a.numRows === 0) return {};
  const result: Record<string, Record<string, any>> = {};
  for (const field of a.schema.fields) {
    const col = a.getChild(field.name);
    if (col) {
      // Read in RICH form via the canonical reader + codec — a secret column is
      // a struct scalar, which surfaces as a plain { field: value } object
      // (same path as iterRows), so no Arrow-scalar toJSON() shimming is needed.
      const type = field.type as unknown as VgiDataType;
      const val = codecFor(type).canonicalToRich(readCanonicalValue(type, col, 0));

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
        // Struct scalar -> already a plain object from the canonical reader.
        const dict: Record<string, any> = { ...(val as Record<string, any>) };
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
