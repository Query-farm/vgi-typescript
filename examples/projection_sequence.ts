// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import { schema, field, int32, batchFromColumns } from "../src/arrow/index.js";
import { defineTableFunction } from "../src/functions/table.js";
import { SCALAR_PROJECTION_PROFILE } from "../src/functions/projection.js";
import { decodeSqlExpression } from "../src/generated/vgi-protocol-types.js";
export const projectionSequence = defineTableFunction<
  Record<string, never>,
  { done: boolean }
>({
  name: "projection_sequence",
  args: {},
  projectionPushdown: true,
  onBind: () => ({
    outputSchema: schema([field("n", int32(), true)]),
    projectionExpressionProfiles: [SCALAR_PROJECTION_PROFILE],
    repeatability: "STABLE_WITHIN_QUERY",
  }),
  projection: (_, p) => {
    const acceptedIndices = p.expressions.flatMap((raw, i) => {
      const e = decodeSqlExpression(raw),
        c = e.evaluation_context;
      return e.sql === `CAST("n" AS BIGINT)` &&
        c.timezone === "UTC" &&
        c.collation === "binary" &&
        c.default_order === "asc" &&
        c.default_null_order === "nulls_last" &&
        c.preserve_insertion_order &&
        String(p.resultSchema.fields[i].type) === "Int64"
        ? [i]
        : [];
    });
    return {
      acceptedIndices,
      bindResult: acceptedIndices.length
        ? {
            output_schema: schema(
              [
                ...p.bindResult.output_schema.fields,
                ...acceptedIndices.map((i) => p.resultSchema.fields[i]),
              ],
              p.bindResult.output_schema.metadata,
            ),
            opaque_data: new TextEncoder().encode(
              String(acceptedIndices.length),
            ),
            projection_expression_profiles:
              p.bindResult.projection_expression_profiles,
            repeatability: p.bindResult.repeatability,
          }
        : null,
    };
  },
  initialState: () => ({ done: false }),
  process: (p, s, out) => {
    if (s.done) {
      out.finish();
      return;
    }
    s.done = true;
    const values = Array.from({ length: 10 }, (_, n) => n);
    if (
      p.outputSchema.fields.some((f) => f.name !== "n") &&
      !p.initCall.bind_opaque_data?.length
    )
      throw new Error("projection state missing");
    out.emit(
      batchFromColumns(
        Object.fromEntries(
          p.outputSchema.fields.map((f) => [
            f.name,
            f.name === "n" ? values : values.map(BigInt),
          ]),
        ),
        p.outputSchema,
      ),
    );
  },
});
