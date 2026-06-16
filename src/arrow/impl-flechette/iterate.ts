// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Read data out of flechette tables as plain JS values.

import type { VgiBatch, VgiDataType } from "../types.js";
import { codecFor } from "../codec/registry.js";
import { readCanonicalValue } from "./canonical.js";

/**
 * Iterate rows of a batch (flechette Table) as plain objects, in the RICH
 * representation (Date for date32/date64; canonical otherwise). Reads via the
 * per-backend canonical reader then maps canonical -> rich through the codec —
 * symmetric with the build path and identical to the arrow-js backend.
 */
export function* iterRows(
  batch: VgiBatch,
  repr: "rich" | "raw" = "rich",
): Generator<Record<string, any>> {
  const t = batch as any;
  const fields = t.schema.fields;
  const codecs = fields.map((f: any) => codecFor(f.type as VgiDataType));
  for (let i = 0; i < t.numRows; i++) {
    const row: Record<string, any> = {};
    for (let fi = 0; fi < fields.length; fi++) {
      const f = fields[fi];
      const col = t.getChild(f.name);
      if (!col) { row[f.name] = null; continue; }
      const canonical = readCanonicalValue(f.type as VgiDataType, col, i);
      const codec = codecs[fi];
      row[f.name] = repr === "raw"
        ? codec.canonicalToRaw(canonical)
        : codec.canonicalToRich(canonical);
    }
    yield row;
  }
}

/**
 * Extract a single-row batch as a flat dict { col: value }, in the RICH
 * representation. Routes through the canonical reader + codec (same as iterRows)
 * so a temporal/decimal setting is represented identically to column data and
 * identically to the arrow-js backend.
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
    if (!col) continue;
    const type = f.type as VgiDataType;
    result[f.name] = codecFor(type).canonicalToRich(readCanonicalValue(type, col, 0));
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
    // Read in RICH form via the canonical reader + codec — a secret column is a
    // struct scalar, surfacing as a plain { field: value } object (same path as
    // iterRows), identical to the arrow-js backend.
    const type = f.type as VgiDataType;
    const val = codecFor(type).canonicalToRich(readCanonicalValue(type, col, 0));

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
      const dict: Record<string, any> = { ...(val as Record<string, any>) };
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
