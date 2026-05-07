// Schema/batch projection by column index for the flechette backend.

import { tableFromColumns } from "@uwdata/flechette";
import type { VgiSchema, VgiBatch } from "../types.js";

function validateProjectionIds(
  caller: string,
  ids: number[],
  fieldCount: number,
): number[] {
  for (const id of ids) {
    if (id < -1) {
      throw new Error(
        `${caller}: unexpected negative projection ID ${id} (only -1 for row_id is allowed)`,
      );
    }
    if (id >= fieldCount) {
      console.warn(
        `${caller}: projection ID ${id} is out of bounds (schema has ${fieldCount} fields), ignoring`,
      );
    }
  }
  return ids.filter((i) => i >= 0 && i < fieldCount);
}

export function projectSchema(
  projectionIds: number[] | null,
  schema: VgiSchema,
): VgiSchema {
  if (!projectionIds) return schema;
  const valid = validateProjectionIds(
    "projectSchema",
    projectionIds,
    schema.fields.length,
  );
  if (valid.length === 0) return schema;
  return {
    fields: valid.map((i) => schema.fields[i]),
    metadata: schema.metadata,
  } as VgiSchema;
}

export function projectBatch(
  projectionIds: number[] | null,
  batch: VgiBatch,
): VgiBatch {
  if (!projectionIds) return batch;
  const t = batch as any;
  const valid = validateProjectionIds(
    "projectBatch",
    projectionIds,
    t.schema.fields.length,
  );
  if (valid.length === 0) return batch;

  // Rebuild a Table with only the kept columns, in the requested order.
  const cols: Record<string, any> = {};
  for (const i of valid) {
    const f = t.schema.fields[i];
    cols[f.name] = t.getChild(f.name);
  }
  return tableFromColumns(cols) as unknown as VgiBatch;
}
