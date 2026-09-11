// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Regression test: the FunctionInfo objects ReadOnlyCatalogInterface emits
// must populate every non-nullable field of the generated FunctionInfoSchema.
// `tsc` does not catch a missing field here because the generated FunctionInfo
// type marks optional fields with `?` even when the wire schema is
// non-nullable — so a hand-written builder can silently drop one (as happened
// with supports_batch_index / partition_kind after a schema regen).

import { describe, test, expect } from "bun:test";
import { int64, field, schema, batchFromColumns, deserializeBatch } from "../../arrow/index.js";
import { defineScalarFunction } from "../../functions/scalar.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { ReadOnlyCatalogInterface } from "../read-only.js";
import { FunctionInfoSchema } from "../../generated/vgi-protocol-schemas.js";
import { encodeFunctionInfo, decodeFunctionInfo } from "../../generated/vgi-client.js";
import { ArgumentMonotonicity } from "../../types.js";

const fn = defineScalarFunction({
  name: "noop",
  params: { x: int64() },
  outputType: () => int64(),
  compute: () => [],
});

const catalog = new ReadOnlyCatalogInterface(
  { name: "test", schemas: [{ name: "main", functions: [fn] }] },
  new FunctionRegistry(),
);

describe("ReadOnlyCatalogInterface FunctionInfo ↔ generated schema", () => {
  test("populates every non-nullable FunctionInfoSchema field", () => {
    const infos = catalog.schemaContentsFunctions(new Uint8Array([1]), "main", "scalar_function");
    expect(infos).toHaveLength(1);
    const info = infos[0] as unknown as Record<string, unknown>;
    for (const f of FunctionInfoSchema.fields) {
      if (f.nullable) continue;
      expect(info[f.name], `non-nullable FunctionInfo field "${f.name}" must be set`).not.toBeUndefined();
    }
    // The two fields a schema regen added — explicitly pinned.
    expect(info.supports_batch_index).toBe(false);
    expect(info.partition_kind).toBe("NOT_PARTITIONED");
  });

  test("the FunctionInfo round-trips through encode/decode intact", () => {
    const info = catalog.schemaContentsFunctions(new Uint8Array([1]), "main", "scalar_function")[0];
    const decoded = decodeFunctionInfo(encodeFunctionInfo(info)) as unknown as Record<string, unknown>;
    expect(decoded.name).toBe("noop");
    expect(decoded.supports_batch_index).toBe(false);
    expect(decoded.partition_kind).toBe("NOT_PARTITIONED");
  });

  test("round-trips scalar argument monotonicity in declaration order", () => {
    const monotone = defineScalarFunction({
      name: "shift",
      parameters: [
        { name: "value", type: int64() },
        { name: "offset", type: int64(), const: true },
      ],
      argumentMonotonicity: [
        ArgumentMonotonicity.STRICTLY_INCREASING,
        ArgumentMonotonicity.NON_DECREASING,
      ],
      returns: int64(),
      compute: () => [],
    });
    const monotoneCatalog = new ReadOnlyCatalogInterface(
      { name: "test", schemas: [{ name: "main", functions: [monotone] }] },
      new FunctionRegistry(),
    );
    const info = monotoneCatalog.schemaContentsFunctions(new Uint8Array([1]), "main", "scalar_function")[0];
    expect(decodeFunctionInfo(encodeFunctionInfo(info)).argument_monotonicity).toEqual([
      "STRICTLY_INCREASING",
      "NON_DECREASING",
    ]);
  });

  test("rejects a monotonicity list that does not match declaration slots", () => {
    expect(() => defineScalarFunction({
      name: "bad",
      params: { value: int64() },
      argumentMonotonicity: [],
      returns: int64(),
      compute: () => [],
    })).toThrow("expected 1 declaration slots");
  });

  test("emits authoritative typed parameter defaults", () => {
    const defaults = batchFromColumns({ x: [7n] }, schema([field("x", int64(), true)]));
    const withDefaults = defineScalarFunction({
      name: "with_default",
      params: { x: int64() },
      parameterDefaultValues: defaults,
      returns: int64(),
      compute: () => [],
    });
    const defaultCatalog = new ReadOnlyCatalogInterface(
      { name: "test", schemas: [{ name: "main", functions: [withDefaults] }] },
      new FunctionRegistry(),
    );
    const info = defaultCatalog.schemaContentsFunctions(new Uint8Array([1]), "main", "scalar_function")[0];
    expect(info.parameter_default_values).not.toBeNull();
    const decoded = deserializeBatch(info.parameter_default_values!);
    expect(decoded.numRows).toBe(1);
    expect(decoded.schema.fields.map((f) => f.name)).toEqual(["x"]);
  });

  test("rejects parameter defaults outside signature order", () => {
    const defaults = batchFromColumns(
      { y: [1n], x: [2n] },
      schema([field("y", int64(), true), field("x", int64(), true)]),
    );
    const invalid = defineScalarFunction({
      name: "invalid_defaults",
      params: { x: int64(), y: int64() },
      parameterDefaultValues: defaults,
      returns: int64(),
      compute: () => [],
    });
    const invalidCatalog = new ReadOnlyCatalogInterface(
      { name: "test", schemas: [{ name: "main", functions: [invalid] }] },
      new FunctionRegistry(),
    );
    expect(() => invalidCatalog.schemaContentsFunctions(new Uint8Array([1]), "main", "scalar_function"))
      .toThrow("not in argument signature order");
  });
});
