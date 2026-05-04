// Schema/batch projection by column index.

import {
  RecordBatch,
  Schema,
  Struct,
  makeData,
} from "@query-farm/apache-arrow";

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
  schema: Schema
): Schema {
  if (!projectionIds) return schema;
  const validIds = validateProjectionIds("projectSchema", projectionIds, schema.fields.length);
  // If no valid projections, return full schema so functions still produce
  // rows with data (DuckDB only needs the row count for COUNT(*) etc.)
  if (validIds.length === 0) return schema;
  return new Schema(validIds.map((i) => schema.fields[i]));
}

/**
 * Project a RecordBatch by column indices.
 */
export function projectBatch(
  projectionIds: number[] | null,
  batch: RecordBatch
): RecordBatch {
  if (!projectionIds) return batch;
  const validIds = validateProjectionIds("projectBatch", projectionIds, batch.schema.fields.length);
  if (validIds.length === 0) return batch;
  const projectedSchema = projectSchema(projectionIds, batch.schema);
  const children = validIds.map((i) => {
    const col = batch.getChildAt(i);
    return col!.data[0];
  });

  const structType = new Struct(projectedSchema.fields);
  const data = makeData({
    type: structType,
    length: batch.numRows,
    children,
    nullCount: 0,
  });

  return new RecordBatch(projectedSchema, data);
}
