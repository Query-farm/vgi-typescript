// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Build Arrow RecordBatches from row objects or column arrays (arrow-js).
//
// The public build path routes RICH author values through the codec registry
// (rich -> canonical) and then through the per-backend canonical writer
// (canonical -> arrow-js column data). All the arrow-js-specific assembly that
// used to live here now lives in ./canonical.ts; this file is just the public
// shape (batchFromRows / batchFromColumns / columnFromArray).

import {
  RecordBatch,
  Schema,
  Field,
  Struct,
  DataType,
  makeData,
  vectorFromArray,
} from "@query-farm/apache-arrow";
import type { VgiSchema, VgiDataType, VgiColumnData } from "../types.js";
import { emptyBatch } from "./empty.js";
import { codecFor } from "../codec/registry.js";
import { writeCanonicalColumn } from "./canonical.js";

/**
 * Build an opaque column-data handle from a JS array. arrow-js wraps
 * `vectorFromArray` and exposes its first `Data` node. Unlike the public
 * build path, this is a low-level pass-through used by callers that already
 * hold backend-native values, so it does NOT run the codec.
 */
export function columnFromArray(values: any[], type: VgiDataType): VgiColumnData {
  return vectorFromArray(values, type as DataType).data[0] as VgiColumnData;
}

/**
 * Build a RecordBatch from row objects (values are RICH).
 */
export function batchFromRows(
  rows: Record<string, any>[],
  schema: Schema | VgiSchema,
): RecordBatch {
  const a = schema as Schema;
  if (rows.length === 0) {
    return emptyBatch(a);
  }
  const columns: Record<string, any[]> = {};
  for (const field of a.fields) {
    columns[field.name] = rows.map((r) => r[field.name] ?? null);
  }
  return batchFromColumns(columns, a);
}

/**
 * Build a RecordBatch from column arrays (values are RICH). Each column is
 * converted rich -> canonical via the codec, then canonical -> arrow-js column
 * data via the canonical writer.
 */
export function batchFromColumns(
  columns: Record<string, any[]>,
  schema: Schema | VgiSchema,
  repr: "rich" | "raw" = "rich",
): RecordBatch {
  const a = schema as Schema;
  const numRows =
    a.fields.length > 0
      ? columns[a.fields[0].name]?.length ?? 0
      : 0;

  const children = a.fields.map((f: Field) => {
    const values = columns[f.name];
    if (!values) {
      return makeData({ type: f.type, length: numRows, nullCount: numRows });
    }
    const codec = codecFor(f.type as VgiDataType);
    const toCanon = repr === "raw" ? codec.rawToCanonical : codec.richToCanonical;
    const canonical = values.map((v) => toCanon.call(codec, v));
    return writeCanonicalColumn(f.type as VgiDataType, canonical);
  });

  const structType = new Struct(a.fields);
  const data = makeData({
    type: structType,
    length: numRows,
    children: children as any,
    nullCount: 0,
  });

  return new RecordBatch(a, data);
}
