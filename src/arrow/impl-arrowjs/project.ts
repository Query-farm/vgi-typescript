// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Schema/batch projection by column index.

import {
  RecordBatch,
  Schema,
  Struct,
  makeData,
} from "@query-farm/apache-arrow";
import type { VgiSchema, VgiBatch } from "../types.js";

/**
 * Validate and filter projection IDs. Returns only valid column indices.
 * -1 is DuckDB's row_id sentinel (filtered out). IDs < -1 throw.
 * IDs >= fieldCount are out-of-bounds and logged as warnings.
 */
function validateProjectionIds(
  caller: string,
  projectionIds: number[],
  fieldCount: number,
): number[] {
  for (const id of projectionIds) {
    if (id < -1) {
      throw new Error(`${caller}: unexpected negative projection ID ${id} (only -1 for row_id is allowed)`);
    }
    if (id >= fieldCount) {
      console.warn(`${caller}: projection ID ${id} is out of bounds (schema has ${fieldCount} fields), ignoring`);
    }
  }
  return projectionIds.filter((i) => i >= 0 && i < fieldCount);
}

/**
 * Project a schema by column indices, preserving only selected fields.
 */
export function projectSchema(
  projectionIds: number[] | null,
  schema: Schema | VgiSchema,
): Schema {
  const a = schema as Schema;
  if (!projectionIds) return a;
  const validIds = validateProjectionIds("projectSchema", projectionIds, a.fields.length);
  if (validIds.length === 0) return a;
  return new Schema(validIds.map((i) => a.fields[i]));
}

/**
 * Project a RecordBatch by column indices.
 */
export function projectBatch(
  projectionIds: number[] | null,
  batch: RecordBatch | VgiBatch,
): RecordBatch {
  const a = batch as RecordBatch;
  if (!projectionIds) return a;
  const validIds = validateProjectionIds("projectBatch", projectionIds, a.schema.fields.length);
  if (validIds.length === 0) return a;
  const projectedSchema = projectSchema(projectionIds, a.schema);
  const children = validIds.map((i) => {
    const col = a.getChildAt(i);
    return col!.data[0];
  });

  const structType = new Struct(projectedSchema.fields);
  const data = makeData({
    type: structType,
    length: a.numRows,
    children,
    nullCount: 0,
  });

  return new RecordBatch(projectedSchema, data);
}
