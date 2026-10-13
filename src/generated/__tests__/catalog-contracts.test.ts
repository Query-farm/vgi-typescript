// Copyright 2025, 2026 Query Farm LLC - https://query.farm

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import {
  decodeCatalogCapabilities, encodeCatalogCapabilities,
  decodeSqlExpression, encodeSqlExpression,
  decodeSortKey, encodeSortKey,
  decodeTableLayout, encodeTableLayout,
  decodeTableLayoutChange, encodeTableLayoutChange,
  decodeOptionDefinitions, encodeOptionDefinitions,
  decodeOptionChanges, encodeOptionChanges,
  decodeOptionState, encodeOptionState,
  decodeRequiredFilterPaths, encodeRequiredFilterPaths,
} from "../vgi-protocol-types.js";

import { decodeViewInfo, encodeViewInfo, decodeTableInfo, encodeTableInfo } from "../vgi-client.js";
import { encodeASD, decodeASD } from "../../codec/asd.js";
import { TableCreateRequestSchema } from "../vgi-protocol-schemas.js";
import * as schemas from "../vgi-protocol-schemas.js";

test("column request codecs retain literal and nested targets", () => {
  for (const [operation, paramsSchema] of [
    ["column_add", schemas.CatalogTableColumnAddParamsSchema],
    ["column_drop", schemas.CatalogTableColumnDropParamsSchema],
    ["column_rename", schemas.CatalogTableColumnRenameParamsSchema],
    ["column_type_change", schemas.CatalogTableColumnTypeChangeParamsSchema],
    ["column_default_set", schemas.CatalogTableColumnDefaultSetParamsSchema],
    ["column_default_drop", schemas.CatalogTableColumnDefaultDropParamsSchema],
    ["column_comment_set", schemas.CatalogTableColumnCommentSetParamsSchema],
    ["not_null_set", schemas.CatalogTableNotNullSetParamsSchema],
    ["not_null_drop", schemas.CatalogTableNotNullDropParamsSchema],
  ] as const) {
    for (const variant of ["literal", "nested"]) {
      const name = operation + "_" + variant;
      const original = decodeASD<Record<string, any>>(paramsSchema, fixture(name));
      const restored = decodeASD<Record<string, any>>(paramsSchema, encodeASD(paramsSchema, original));
      expect(restored).toEqual(original);
      expect(restored.column_path).toEqual(variant === "literal" ? ["a.b"] : ["a", 'b."quoted']);
      expect(restored.schema_path).toEqual(["one.two", "nested"]);
      expect(restored.transaction_opaque_data === null).toBe(variant === "literal");
    }
  }
});

import type { TableCreateRequest } from "../vgi-client.js";
const encodeTableCreateRequest = (value: TableCreateRequest) => encodeASD(TableCreateRequestSchema, value);
const decodeTableCreateRequest = (value: Uint8Array) => decodeASD<TableCreateRequest>(TableCreateRequestSchema, value);

const corpus = JSON.parse(readFileSync(new URL("../../../conformance/catalog-contracts.json", import.meta.url), "utf8"));
const fixture = (name: string): Uint8Array => new Uint8Array(Buffer.from(corpus.records[name], "base64"));

describe("Python catalog contract corpus", () => {
  test("option changes preserve opaque Arrow metadata and distinguish absence", () => {
    for (const name of ["changes", "reset_only"]) {
      const first = decodeOptionChanges(fixture(name));
      const restored = decodeOptionChanges(encodeOptionChanges(first));
      expect(restored.reset_names).toEqual(["reset.me"]);
      expect(restored.set_values).toEqual(name === "changes" ? fixture("values") : null);
    }
  });

  test("readback retains explicit NULL overrides and empty one-row payloads", () => {
    for (const name of ["state", "empty_state"]) {
      const first = decodeOptionState(fixture(name));
      const restored = decodeOptionState(encodeOptionState(first));
      expect(restored.values).toEqual(fixture(name === "state" ? "values" : "empty_values"));
      expect(restored.explicit_names).toEqual(name === "state" ? ["optional"] : []);
    }
  });

  test("three list levels retain dotted names, nested paths, and OR groups", () => {
    for (const name of ["paths", "empty_paths"]) {
      const first = decodeRequiredFilterPaths(fixture(name));
      const restored = decodeRequiredFilterPaths(encodeRequiredFilterPaths(first));
      expect(restored.groups).toEqual(name === "paths"
        ? [[["a.b"], ["a", "b"]], [["record", 'literal."name']]] : []);
    }
  });
});

test("capability profiles and option definitions preserve populated and empty fields", () => {
  const capabilities = decodeCatalogCapabilities(encodeCatalogCapabilities(decodeCatalogCapabilities(fixture("capabilities"))));
  expect(capabilities.read).toEqual(["vgi.schema.options.read.v1"]);
  expect(capabilities.ddl).toEqual(["vgi.schema.options.create.v1"]);
  expect(decodeCatalogCapabilities(fixture("empty_capabilities"))).toEqual({ read: [], ddl: [], write: [], transaction: [] });
  const definitions = decodeOptionDefinitions(encodeOptionDefinitions(decodeOptionDefinitions(fixture("definitions"))));
  expect(definitions.defaults).toEqual(fixture("values"));
  expect(definitions.create_names).toEqual(["optional", "record"]);
  expect(definitions.alter_names).toEqual(["optional"]);
});

test("layouts retain explicit ordering and distinguish clear from unchanged", () => {
  const expression = decodeSqlExpression(encodeSqlExpression(decodeSqlExpression(fixture("expression"))));
  expect(expression.schema_path).toEqual(["one.two", "nested"]);
  const key = decodeSortKey(encodeSortKey(decodeSortKey(fixture("sort_key"))));
  expect(key.expression).toEqual(fixture("expression"));
  expect(key.direction).toBe("DESC");
  expect(key.null_order).toBe("FIRST");
  const layout = decodeTableLayout(encodeTableLayout(decodeTableLayout(fixture("layout"))));
  expect(layout.partition_by).toEqual([fixture("expression")]);
  expect(layout.sorted_by).toEqual([fixture("sort_key")]);
  expect(layout.coverage).toBe("ALL_ROWS");
  expect(decodeTableLayout(fixture("empty_layout")).sorted_by).toEqual([]);
  const change = decodeTableLayoutChange(encodeTableLayoutChange(decodeTableLayoutChange(fixture("layout_change"))));
  expect(change.mode).toBe("REWRITE");
  expect(change.partition_by).toEqual([]);
  expect(change.sorted_by).toEqual([fixture("sort_key")]);
  const clear = decodeTableLayoutChange(encodeTableLayoutChange(decodeTableLayoutChange(fixture("layout_clear"))));
  expect(clear.mode).toBe("FUTURE_WRITES");
  expect(clear.partition_by).toBeNull();
  expect(clear.sorted_by).toEqual([]);
  const create = decodeTableCreateRequest(encodeTableCreateRequest(decodeTableCreateRequest(fixture("table_create"))));
  expect(create.options).toEqual(fixture("values"));
  expect(create.layout).toEqual(fixture("layout"));
  expect(create.comment).toBe("kept");
  const old = decodeTableCreateRequest(encodeTableCreateRequest(decodeTableCreateRequest(fixture("table_create_legacy"))));
  expect([old.comment, old.tags, old.options, old.layout]).toEqual([null, null, null, null]);
  const info = decodeTableInfo(encodeTableInfo(decodeTableInfo(fixture("table_info"))));
  expect(info.options).toEqual(fixture("state"));
  expect(info.layout).toEqual(fixture("layout"));
});

test("view aliases retain raw dots, quotes and order", () => {
  const info = decodeViewInfo(encodeViewInfo(decodeViewInfo(fixture("view_info"))));
  expect(info.aliases).toEqual(["a.b", 'with"quote']);
  expect(decodeViewInfo(fixture("view_info_implicit")).aliases).toEqual([]);
});

test("view security survives codec round trips", () => {
  expect(decodeViewInfo(encodeViewInfo(decodeViewInfo(fixture("view_info")))).security).toBe("regular");
  expect(decodeViewInfo(encodeViewInfo(decodeViewInfo(fixture("view_info_barrier")))).security).toBe("barrier");
});
