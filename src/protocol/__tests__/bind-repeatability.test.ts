// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import { test, expect } from "bun:test";
import {
  schema,
  field,
  int32,
  batchFromColumns,
  serializeBatch,
  deserializeBatch,
} from "../../arrow/index.js";
import { batchToScalarDict } from "../../util/arrow/index.js";
import { BindResultSchema } from "../../generated/vgi-protocol-schemas.js";
import {
  serializeBindResponse,
  deserializeBindResponse,
} from "../serializers/bind.js";
import type { BindRepeatability, BindResponse } from "../types.js";

const output = schema([field("n", int32(), true)]);
function roundTrip(value: BindResponse): BindResponse {
  const row = serializeBindResponse(value);
  const batch = batchFromColumns(
    Object.fromEntries(
      BindResultSchema.fields.map((f) => [f.name, [row[f.name]]]),
    ),
    BindResultSchema,
  );
  return deserializeBindResponse(
    batchToScalarDict(deserializeBatch(serializeBatch(batch))),
  );
}

test("bind repeatability defaults to UNKNOWN and round trips canonical enum IPC", () => {
  for (const repeatability of [
    undefined,
    "UNKNOWN",
    "STABLE_WITHIN_QUERY",
  ] as const) {
    const decoded = roundTrip({
      output_schema: output,
      opaque_data: null,
      repeatability,
    });
    expect(decoded.repeatability).toBe(repeatability ?? "UNKNOWN");
  }
});

test("bind repeatability rejects invalid provider declarations and required wire values", () => {
  for (const repeatability of [null, "", "FUTURE", "stable_within_query"]) {
    expect(() =>
      serializeBindResponse({
        output_schema: output,
        opaque_data: null,
        repeatability: repeatability as BindRepeatability,
      }),
    ).toThrow("repeatability");
  }
  const valid = serializeBindResponse({
    output_schema: output,
    opaque_data: null,
  });
  for (const repeatability of [
    undefined,
    null,
    "",
    "FUTURE",
    "stable_within_query",
  ]) {
    expect(() => deserializeBindResponse({ ...valid, repeatability })).toThrow(
      "repeatability",
    );
  }
});
