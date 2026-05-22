// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

import { describe, test, expect } from "bun:test";
import {
  CatalogInfoSchema,
  SchemaInfoSchema,
  TableInfoSchema,
  ViewInfoSchema,
} from "../../generated/vgi-protocol-schemas.js";
import { encodeASD, decodeASD } from "../asd.js";
import type {
  CatalogInfo,
  SchemaInfo,
  TableInfo,
  ViewInfo,
} from "../../generated/vgi-client.js";

describe("ASD codec", () => {
  test("CatalogInfo round-trip", () => {
    const v: CatalogInfo = {
      name: "my_cat",
      implementation_version: "v1.2",
      data_version_spec: null,
      attach_option_specs: [],
      releases: [],
    };
    const bytes = encodeASD(CatalogInfoSchema, v);
    const back = decodeASD<CatalogInfo>(CatalogInfoSchema, bytes);
    expect(back.name).toBe("my_cat");
    expect(back.implementation_version).toBe("v1.2");
    expect(back.data_version_spec).toBeNull();
  });

  test("SchemaInfo round-trip with tags Map", () => {
    const v: SchemaInfo = {
      attach_opaque_data: new Uint8Array([1, 2, 3, 4]),
      name: "public",
      comment: "hello",
      tags: { env: "prod", owner: "rusty" },
    };
    const bytes = encodeASD(SchemaInfoSchema, v);
    const back = decodeASD<SchemaInfo>(SchemaInfoSchema, bytes);
    expect(Array.from(back.attach_opaque_data)).toEqual([1, 2, 3, 4]);
    expect(back.name).toBe("public");
    expect(back.comment).toBe("hello");
    expect(back.tags).toEqual({ env: "prod", owner: "rusty" });
  });

  test("SchemaInfo with empty tags", () => {
    const v: SchemaInfo = {
      attach_opaque_data: new Uint8Array([5]),
      name: "main",
      comment: null,
      tags: {},
    };
    const bytes = encodeASD(SchemaInfoSchema, v);
    const back = decodeASD<SchemaInfo>(SchemaInfoSchema, bytes);
    expect(back.comment).toBeNull();
    expect(back.tags).toEqual({});
  });

  test("TableInfo round-trip with nested int lists", () => {
    const v: TableInfo = {
      comment: null,
      tags: {},
      name: "orders",
      schema_name: "main",
      columns: new Uint8Array([9, 9, 9]),
      not_null_constraints: [0, 1, 2],
      unique_constraints: [[0], [1, 2]],
      check_constraints: ["a > 0", "b < 10"],
      primary_key_constraints: [[0]],
      foreign_key_constraints: [new Uint8Array([7])],
      supports_insert: true,
      supports_update: false,
      supports_delete: true,
      supports_returning: false,
      supports_column_statistics: false,
    };
    const bytes = encodeASD(TableInfoSchema, v);
    const back = decodeASD<TableInfo>(TableInfoSchema, bytes);
    expect(back.name).toBe("orders");
    expect(back.not_null_constraints).toEqual([0, 1, 2]);
    expect(back.unique_constraints).toEqual([[0], [1, 2]]);
    expect(back.check_constraints).toEqual(["a > 0", "b < 10"]);
    expect(back.primary_key_constraints).toEqual([[0]]);
    expect(back.foreign_key_constraints?.map((u) => Array.from(u))).toEqual([[7]]);
    expect(back.supports_insert).toBe(true);
    expect(back.supports_delete).toBe(true);
  });

  test("ViewInfo round-trip", () => {
    const v: ViewInfo = {
      comment: "a view",
      tags: { x: "y" },
      name: "v1",
      schema_name: "s1",
      definition: "SELECT 1",
    };
    const bytes = encodeASD(ViewInfoSchema, v);
    const back = decodeASD<ViewInfo>(ViewInfoSchema, bytes);
    expect(back.definition).toBe("SELECT 1");
    expect(back.tags).toEqual({ x: "y" });
  });
});
