// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Build batches from row objects or column arrays for the flechette backend.
//
// The public build path routes RICH author values through the codec registry
// (rich -> canonical) and then through the per-backend canonical writer
// (canonical -> flechette Column). All flechette-specific assembly now lives in
// ./canonical.ts; this file is just the public shape.

import {
  columnFromArray as f_columnFromArray,
} from "@query-farm/flechette";
import type { VgiSchema, VgiBatch, VgiDataType, VgiColumnData } from "../types.js";
import { emptyBatch } from "./empty.js";
import { codecFor } from "../codec/registry.js";
import { writeCanonicalBatch } from "./canonical.js";

/**
 * Build an opaque column-data handle from a JS array. Low-level pass-through
 * for callers that already hold backend-native values; does NOT run the codec.
 */
export function columnFromArray(values: any[], type: VgiDataType): VgiColumnData {
  return f_columnFromArray(values, type as any, {
    useBigInt: true,
    useBigIntTimestamp: true,
    useDecimalInt: true,
  }) as VgiColumnData;
}

/** Build a batch from row objects (values are RICH). */
export function batchFromRows(
  rows: Record<string, any>[],
  schema: VgiSchema,
): VgiBatch {
  if (rows.length === 0) return emptyBatch(schema);
  const columns: Record<string, any[]> = {};
  for (const f of schema.fields) {
    columns[f.name] = rows.map((r) => r[f.name] ?? null);
  }
  return batchFromColumns(columns, schema);
}

/**
 * Build a batch from column arrays (values are RICH). Each column is converted
 * rich -> canonical via the codec, then the canonical batch writer assembles
 * the flechette Table (preserving per-field nullable + metadata).
 */
export function batchFromColumns(
  columns: Record<string, any[]>,
  schema: VgiSchema,
  repr: "rich" | "raw" = "rich",
): VgiBatch {
  let numRows = 0;
  for (const f of schema.fields) {
    if (columns[f.name]) { numRows = columns[f.name].length; break; }
  }
  const canonicalColumns: Record<string, unknown[]> = {};
  for (const f of schema.fields) {
    const values = columns[f.name];
    if (!values) continue;
    const codec = codecFor(f.type as VgiDataType);
    const toCanon = repr === "raw" ? codec.rawToCanonical : codec.richToCanonical;
    canonicalColumns[f.name] = values.map((v) => toCanon.call(codec, v));
  }
  return writeCanonicalBatch(canonicalColumns, schema, numRows);
}
