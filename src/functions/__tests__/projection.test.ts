// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import { test, expect } from "bun:test";
import {
  schema, field, int32, int64, batchFromColumns, serializeBatch,
} from "../../arrow/index.js";
import { generatedExpressionMetadata } from "../../catalog/column-expressions.js";
import {
  validateProjectionAcceptance,
  validateProjectionOffer,
  projectionRecord,
  SCALAR_PROJECTION_PROFILE,
  type ProjectionProposal,
  type ProjectionAcceptance,
} from "../projection.js";
import type { BindRequest } from "../../protocol/types.js";
const source = schema(
  [field("n", int32(), true)],
  new Map([["source", "keep"]]),
);
const candidate = field("wide", int64(), true, new Map([["result", "keep"]]));
const proposal: ProjectionProposal = {
  bindCall: {
    function_type: "table",
    input_schema: null,
    schema_path: ["main"],
  } as BindRequest,
  bindResult: {
    output_schema: source,
    opaque_data: null,
    projection_expression_profiles: [SCALAR_PROJECTION_PROFILE],
    repeatability: "STABLE_WITHIN_QUERY",
  },
  expressions: [
    new Uint8Array(
      Buffer.from(
        generatedExpressionMetadata(`CAST("n" AS BIGINT)`, ["main"]),
        "base64",
      ),
    ),
  ],
  resultSchema: schema([candidate]),
};
const accepted = (): ProjectionAcceptance => ({
  acceptedIndices: [0],
  bindResult: {
    output_schema: schema([...source.fields, candidate], source.metadata),
    opaque_data: new Uint8Array([7]),
    projection_expression_profiles: [SCALAR_PROJECTION_PROFILE],
    repeatability: "STABLE_WITHIN_QUERY",
  },
});
test("projection acceptance preserves schema metadata and replacement state", () => {
  const result = accepted();
  validateProjectionAcceptance(proposal, result);
  expect(result.bindResult!.opaque_data).toEqual(new Uint8Array([7]));
  validateProjectionAcceptance(proposal, {
    acceptedIndices: [],
    bindResult: null,
  });
});
for (const kind of [
  "duplicate",
  "negative",
  "range",
  "schema",
  "metadata",
  "offer",
  "secret",
  "repeatability",
  "missing",
]) {
  test(`reject malformed provider acceptance ${kind}`, () => {
    const result = accepted();
    switch (kind) {
      case "duplicate":
        result.acceptedIndices = [0, 0];
        break;
      case "negative":
        result.acceptedIndices = [-1];
        break;
      case "range":
        result.acceptedIndices = [1];
        break;
      case "schema":
        result.bindResult!.output_schema = source;
        break;
      case "metadata":
        result.bindResult!.output_schema = schema([
          ...source.fields,
          candidate,
        ]);
        break;
      case "offer":
        result.bindResult!.projection_expression_profiles = [];
        break;
      case "repeatability":
        result.bindResult!.repeatability = "UNKNOWN";
        break;
      case "secret":
        result.bindResult!.lookup_names = ["secret"];
        break;
      case "missing":
        result.bindResult = null;
    }
    expect(() => validateProjectionAcceptance(proposal, result)).toThrow();
  });
}
test("unknown well-formed profiles remain valid offers", () => {
  validateProjectionOffer({
    ...proposal.bindResult,
    projection_expression_profiles: [
      SCALAR_PROJECTION_PROFILE,
      "vendor.scalar.v7",
    ],
  });
  expect(() =>
    validateProjectionOffer({
      ...proposal.bindResult,
      projection_expression_profiles: [
        SCALAR_PROJECTION_PROFILE,
        SCALAR_PROJECTION_PROFILE,
      ],
    }),
  ).toThrow();
});

test("projection records permit complete EOF and reject partial or trailing framing", () => {
  const bytes = serializeBatch(batchFromColumns({ n: [7] }, source));
  const eos = new Uint8Array([255, 255, 255, 255, 0, 0, 0, 0]);
  expect(bytes.slice(-8)).toEqual(eos);
  const body = bytes.slice(0, -8);
  const join = (...parts: Uint8Array[]) => new Uint8Array(parts.flatMap((p) => [...p]));
  for (const complete of [bytes, body, join(body, new Uint8Array(4))])
    expect(projectionRecord(complete, source).n).toBe(7);
  for (const invalid of [
    join(bytes, eos),
    join(bytes, new Uint8Array([1])),
    join(body, eos.slice(0, 4)),
    join(body, eos.slice(0, 7)),
    body.slice(0, -1),
  ]) expect(() => projectionRecord(invalid, source)).toThrow();
});
