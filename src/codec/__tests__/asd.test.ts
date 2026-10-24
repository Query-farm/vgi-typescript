// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

import { describe, test, expect } from "bun:test";
import {
  CatalogInfoSchema,
  SchemaInfoSchema,
  TableInfoSchema,
  ViewInfoSchema,
} from "../../generated/vgi-protocol-schemas.js";
import { encodeASD, decodeASD } from "../asd.js";
import { batchFromRows, serializeBatch } from "../../arrow/index.js";
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
      source_url: "https://example.com/cat",
    };
    const bytes = encodeASD(CatalogInfoSchema, v);
    const back = decodeASD<CatalogInfo>(CatalogInfoSchema, bytes);
    expect(back.name).toBe("my_cat");
    expect(back.implementation_version).toBe("v1.2");
    expect(back.data_version_spec).toBeNull();
    expect(back.source_url).toBe("https://example.com/cat");
  });

  test("SchemaInfo round-trip with tags Map", () => {
    const v: SchemaInfo = {
      attach_opaque_data: new Uint8Array([1, 2, 3, 4]),
      path: ["analytics", "public"],
      comment: "hello",
      tags: { env: "prod", owner: "rusty" },
    };
    const bytes = encodeASD(SchemaInfoSchema, v);
    const back = decodeASD<SchemaInfo>(SchemaInfoSchema, bytes);
    expect("attach_opaque_data" in back).toBe(false);
    expect(back.path).toEqual(["analytics", "public"]);
    expect(back.comment).toBe("hello");
    expect(back.tags).toEqual({ env: "prod", owner: "rusty" });
  });

  test("SchemaInfo estimated_object_count decodes int64 map values as numbers", () => {
    // map<utf8, int64 nullable>: values must come back typed, not stringified.
    const v: SchemaInfo = {
      attach_opaque_data: new Uint8Array([1]),
      path: ["main"],
      comment: null,
      tags: {},
      estimated_object_count: { table: 2, view: 0 },
    };
    const back = decodeASD<SchemaInfo>(SchemaInfoSchema, encodeASD(SchemaInfoSchema, v));
    expect(back.estimated_object_count).toEqual({ table: 2, view: 0 });
    expect(typeof back.estimated_object_count!.view).toBe("number");
    expect(back.estimated_object_count!.view === 0).toBe(true);
  });

  test("int64 map values beyond Number.MAX_SAFE_INTEGER stay bigint", () => {
    const big = 2n ** 60n;
    const v = {
      attach_opaque_data: new Uint8Array([1]),
      path: ["main"],
      comment: null,
      tags: {},
      estimated_object_count: { table: big },
    };
    const back = decodeASD<any>(SchemaInfoSchema, encodeASD(SchemaInfoSchema, v as any));
    expect(back.estimated_object_count.table).toBe(big);
  });

  test("null map values decode as null, not an empty string", () => {
    const v = {
      attach_opaque_data: new Uint8Array([1]),
      path: ["main"],
      comment: null,
      tags: { owner: null },
      estimated_object_count: { table: null },
    };
    // A decoder test: the bytes are built raw, because encodeASD never writes
    // a null map value (vgi-python's maps have no optional values; it writes
    // the value type's zero value instead -- see conform.ts).
    const raw = serializeBatch(batchFromRows([{ ...v, tags: [["owner", null]], estimated_object_count: [["table", null]] }], SchemaInfoSchema));
    const back = decodeASD<any>(SchemaInfoSchema, raw);
    expect(back.tags).toEqual({ owner: null });
    expect(back.estimated_object_count).toEqual({ table: null });
  });

  test("SchemaInfo with empty tags", () => {
    const v: SchemaInfo = {
      attach_opaque_data: new Uint8Array([5]),
      path: ["main"],
      comment: null,
      tags: {},
    };
    const bytes = encodeASD(SchemaInfoSchema, v);
    const back = decodeASD<SchemaInfo>(SchemaInfoSchema, bytes);
    expect(back.comment).toBeNull();
    expect(back.tags).toEqual({});
  });

  test("TableInfo round-trip with canonical constraint IPC and revision", () => {
    const v: TableInfo = {
      comment: null,
      tags: {},
      name: "orders",
      schema_path: ["main"],
      columns: new Uint8Array([9, 9, 9]),
      constraints: [new Uint8Array([7])],
      definition_revision: new Uint8Array([8]),
      write_result_modes: { insert: "changes", delete: "count" },
      supports_column_statistics: false,
      required_filters: [],
    };
    const bytes = encodeASD(TableInfoSchema, v);
    const back = decodeASD<TableInfo>(TableInfoSchema, bytes);
    expect(back.name).toBe("orders");
    expect(back.constraints.map((u) => Array.from(u))).toEqual([[7]]);
    expect(Array.from(back.definition_revision!)).toEqual([8]);
    expect(back.write_result_modes).toEqual({ insert: "changes", delete: "count" });
  });

  test("ViewInfo round-trip", () => {
    const v: ViewInfo = {
      comment: "a view",
      tags: { x: "y" },
      name: "v1",
      schema_path: ["s1"],
      definition: "SELECT 1",
      aliases: ["a.b"],
      column_comments: { n: "the only column" },
    };
    const bytes = encodeASD(ViewInfoSchema, v);
    const back = decodeASD<ViewInfo>(ViewInfoSchema, bytes);
    expect(back.definition).toBe("SELECT 1");
    expect(back.tags).toEqual({ x: "y" });
    expect(back.column_comments).toEqual({ n: "the only column" });
  });
});
