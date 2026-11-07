// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Every accepted `requiredSecrets` form must reach FunctionInfo.required_secrets
// (and so the client's bind-time resolution); anything else must be a clear
// error at definition time, never a silently dropped requirement.
import { describe, test, expect } from "bun:test";
import { int64, schema, field, batchFromColumns } from "../../arrow/index.js";
import { normalizeRequiredSecrets } from "../requirements.js";
import { defineScalarFunction } from "../../functions/scalar.js";
import { defineTableFunction } from "../../functions/table.js";
import { defineAggregate } from "../../functions/aggregate.js";
import { defineTableInOutFunction, defineRowTransformFunction } from "../../functions/table-in-out.js";
import { defineTableBufferingFunction } from "../../functions/table-buffering.js";
import { defineCopyFromFunction } from "../../functions/copy-from.js";
import { defineCopyToFunction } from "../../functions/copy-to.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { encodeFunctionInfo, decodeFunctionInfo } from "../../generated/vgi-client.js";
import { metadatasToArrow, arrowToMetadatas } from "../../metadata/serialize.js";
import { resolveMetadata } from "../../metadata/resolve.js";
import type { VgiFunction } from "../../functions/types.js";

const ALL_FORMS = [
  "vgi_example",
  { secretType: "vgi_example", scope: "vgi-conformance://a/" },
  { secret_type: "s3", scope: "s3://bucket/" },
  { secretType: "http", secretName: "my_http" },
  { secret_type: "gcs", secret_name: "my_gcs" },
];

const EXPECTED_WIRE = [
  { secret_type: "vgi_example", scope: null, secret_name: null },
  { secret_type: "vgi_example", scope: "vgi-conformance://a/", secret_name: null },
  { secret_type: "s3", scope: "s3://bucket/", secret_name: null },
  { secret_type: "http", scope: null, secret_name: "my_http" },
  { secret_type: "gcs", scope: null, secret_name: "my_gcs" },
];

describe("normalizeRequiredSecrets", () => {
  test("bare type string is the simplest form", () => {
    expect(normalizeRequiredSecrets(["brave"])).toEqual([
      { secretType: "brave", scope: null, secretName: null },
    ]);
  });

  test("object forms (camelCase and snake_case) carry scope and name", () => {
    expect(normalizeRequiredSecrets(ALL_FORMS)).toEqual(
      EXPECTED_WIRE.map((e) => ({ secretType: e.secret_type, scope: e.scope, secretName: e.secret_name })),
    );
  });

  test("undefined / null / [] declare nothing", () => {
    expect(normalizeRequiredSecrets(undefined)).toEqual([]);
    expect(normalizeRequiredSecrets(null)).toEqual([]);
    expect(normalizeRequiredSecrets([])).toEqual([]);
  });

  test("empty scope/name mean none; exact duplicates collapse, distinct scopes do not", () => {
    expect(
      normalizeRequiredSecrets([
        "s3",
        { secretType: "s3", scope: "", secretName: "" },
        { secretType: "s3", scope: "s3://a/" },
        { secretType: "s3", scope: "s3://a/b/" },
      ]),
    ).toEqual([
      { secretType: "s3", scope: null, secretName: null },
      { secretType: "s3", scope: "s3://a/", secretName: null },
      { secretType: "s3", scope: "s3://a/b/", secretName: null },
    ]);
  });

  test("normalization is idempotent", () => {
    const once = normalizeRequiredSecrets(ALL_FORMS);
    expect(normalizeRequiredSecrets(once)).toEqual(once);
  });

  test.each([
    ["a bare string instead of an array", "brave", /must be an array/],
    ["a number entry", [42], /\[0\]: expected a secret type string/],
    ["a null entry", [null], /\[0\]: expected .* got null/],
    ["a nested array", [["s3"]], /got array/],
    ["an empty type", [""], /must not be empty/],
    ["an object without a type", [{ scope: "s3://x/" }], /secretType must be a non-empty string/],
    ["a misspelled key", [{ type: "s3" }], /unknown key\(s\) 'type'/],
    ["a non-string scope", [{ secretType: "s3", scope: 5 }], /scope must be a string/],
    ["conflicting spellings", [{ secretType: "s3", secret_type: "gcs" }], /disagree/],
  ])("rejects %s", (_label, value, pattern) => {
    expect(() => normalizeRequiredSecrets(value, "f")).toThrow(pattern as RegExp);
  });

  test("error names the function", () => {
    expect(() => normalizeRequiredSecrets([{ type: "s3" }], "my_fn")).toThrow(/'my_fn'/);
  });
});

function wireOf(fn: VgiFunction, type: string): unknown {
  const catalog = new ReadOnlyCatalogInterface(
    { name: "test", schemas: [{ name: "main", functions: [fn] }] },
    new FunctionRegistry(),
  );
  const info = catalog
    .schemaContentsFunctions(new Uint8Array([1]), ["main"], type)
    .find((i) => i.name === fn.meta.name);
  expect(info, `function ${fn.meta.name} not listed`).toBeDefined();
  // Through the wire codec, as the extension receives it.
  return decodeFunctionInfo(encodeFunctionInfo(info!)).required_secrets.map((e: any) => ({
    secret_type: e.secret_type,
    scope: e.scope ?? null,
    secret_name: e.secret_name ?? null,
  }));
}

const outSchema = schema([field("n", int64(), true)]);
const forms = ALL_FORMS as any;

describe("every function kind sends every declared requirement", () => {
  test("table", () => {
    const fn = defineTableFunction({
      name: "t",
      requiredSecrets: forms,
      onBind: () => ({ outputSchema: outSchema }),
      initialState: () => ({}),
      process: (_p: any, _s: any, out: any) => out.finish(),
    });
    expect(wireOf(fn, "table_function")).toEqual(EXPECTED_WIRE);
  });

  test("scalar", () => {
    const fn = defineScalarFunction({
      name: "s",
      params: { x: int64() },
      outputType: () => int64(),
      compute: () => [],
      requiredSecrets: forms,
    });
    expect(wireOf(fn, "scalar_function")).toEqual(EXPECTED_WIRE);
  });

  test("aggregate", () => {
    const fn = defineAggregate({
      name: "a",
      args: { x: int64() },
      outputType: int64(),
      initialState: () => 0n,
      update: () => {},
      combine: (_s: any, t: any) => t,
      finalize: (p: any) => batchFromColumns({ result: p.groupIds.map(() => 0n) }, schema([field("result", int64(), true)])),
      requiredSecrets: forms,
    });
    expect(wireOf(fn, "aggregate_function")).toEqual(EXPECTED_WIRE);
  });

  test("table_in_out", () => {
    const fn = defineTableInOutFunction({
      name: "tio",
      requiredSecrets: forms,
      onBind: () => ({ outputSchema: outSchema }),
      process: () => {},
    });
    expect(wireOf(fn, "table_function")).toEqual(EXPECTED_WIRE);
  });

  test("row transform", () => {
    const fn = defineRowTransformFunction({
      name: "rt",
      args: { x: int64() },
      requiredSecrets: forms,
      onBind: () => ({ outputSchema: outSchema }),
      process: () => {},
    } as any);
    expect(wireOf(fn, "table_function")).toEqual(EXPECTED_WIRE);
  });

  test("table_buffering", () => {
    const fn = defineTableBufferingFunction({
      name: "tb",
      requiredSecrets: forms,
      onBind: () => ({ outputSchema: outSchema }),
      process: () => {},
      combine: () => {},
      finalize: () => {},
    } as any);
    expect(wireOf(fn, "table_function")).toEqual(EXPECTED_WIRE);
  });

  test("copy from / copy to", () => {
    const from = defineCopyFromFunction({
      name: "cf",
      format: "cfmt",
      requiredSecrets: forms,
      read: () => {},
    } as any);
    expect(resolveMetadata(from).requiredSecrets).toEqual(normalizeRequiredSecrets(ALL_FORMS));
    const to = defineCopyToFunction({
      name: "ct",
      format: "ctfmt",
      requiredSecrets: forms,
      write: () => {},
      close: () => {},
    } as any);
    expect(resolveMetadata(to).requiredSecrets).toEqual(normalizeRequiredSecrets(ALL_FORMS));
  });

  test("an unrecognised declaration fails at definition, not silently", () => {
    expect(() =>
      defineTableFunction({
        name: "bad",
        requiredSecrets: [{ type: "brave" }] as any,
        onBind: () => ({ outputSchema: outSchema }),
        initialState: () => ({}),
        process: (_p: any, _s: any, out: any) => out.finish(),
      }),
    ).toThrow(/requiredSecrets of 'bad'.*unknown key/);
    expect(() =>
      defineScalarFunction({
        name: "bad2",
        params: { x: int64() },
        outputType: () => int64(),
        compute: () => [],
        requiredSecrets: [7] as any,
      }),
    ).toThrow(/requiredSecrets of 'bad2'/);
  });

  test("metadata Arrow round-trip keeps scope and name", () => {
    const fn = defineTableFunction({
      name: "rt_meta",
      requiredSecrets: forms,
      onBind: () => ({ outputSchema: outSchema }),
      initialState: () => ({}),
      process: (_p: any, _s: any, out: any) => out.finish(),
    });
    const meta = resolveMetadata(fn);
    expect(arrowToMetadatas(metadatasToArrow([meta]))[0]!.requiredSecrets).toEqual(meta.requiredSecrets);
  });
});
