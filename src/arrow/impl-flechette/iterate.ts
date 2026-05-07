// Read data out of flechette tables as plain JS values.

import type { VgiBatch } from "../types.js";

/**
 * Iterate rows of a batch (flechette Table) as plain objects. Mirrors
 * impl-arrowjs/iterate.ts so callers see identical row shapes regardless
 * of backend.
 */
export function* iterRows(batch: VgiBatch): Generator<Record<string, any>> {
  const t = batch as any;
  for (let i = 0; i < t.numRows; i++) {
    const row: Record<string, any> = {};
    for (const f of t.schema.fields) {
      const col = t.getChild(f.name);
      row[f.name] = col ? col.at(i) : null;
    }
    yield row;
  }
}

/**
 * Extract a single-row batch as a flat dict { col: value }.
 */
export function batchToScalarDict(
  batch: VgiBatch | null,
): Record<string, any> {
  if (!batch) return {};
  const t = batch as any;
  if (t.numRows === 0) return {};
  const result: Record<string, any> = {};
  for (const f of t.schema.fields) {
    const col = t.getChild(f.name);
    if (col) result[f.name] = col.at(0);
  }
  return result;
}

/**
 * No-op for flechette: dictionary columns auto-decode at extraction time.
 * Provided for parity with impl-arrowjs (where the apache-arrow fork doesn't
 * auto-decode in some paths).
 */
export function decodeDictValue(value: any, _index = 0): any {
  return value;
}

/**
 * Single-row batch → secret dict. Mirrors impl-arrowjs's same-named function.
 */
export function batchToSecretDict(
  batch: VgiBatch | null,
): Record<string, Record<string, any>> {
  if (!batch) return {};
  const t = batch as any;
  if (t.numRows === 0) return {};
  const result: Record<string, Record<string, any>> = {};
  for (const f of t.schema.fields) {
    const col = t.getChild(f.name);
    if (!col) continue;
    const val = col.at(0);

    let key = f.name;
    let scope: string | undefined;
    if (f.name.startsWith("secret_")) {
      const secretType = f.metadata?.get?.("secret_type");
      if (secretType) {
        key = secretType;
        scope = f.metadata?.get?.("scope") ?? undefined;
      }
    }

    if (val && typeof val === "object" && !ArrayBuffer.isView(val)) {
      const dict: Record<string, any> = {};
      Object.assign(dict, val);
      if (key in result) {
        throw new Error(
          `batchToSecretDict: duplicate secret_type '${key}' (scope=${scope ?? "none"}).`,
        );
      }
      result[key] = dict;
      if (scope) result[`${key}:${scope}`] = dict;
    } else if (val === null || val === undefined) {
      // skip
    } else {
      result[key] = {};
    }
  }
  return result;
}

/**
 * BigInt → number coercion (lossy for values past 2^53).
 */
export function safeNumber(value: any): number {
  if (typeof value === "bigint") return Number(value);
  return value as number;
}
