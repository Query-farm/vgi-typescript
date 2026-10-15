// Copyright 2025, 2026 Query Farm LLC - https://query.farm

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import {
  decodeCatalogCapabilities, encodeCatalogCapabilities,
  decodeCatalogObjectRef, encodeCatalogObjectRef, decodeObjectDependency, encodeObjectDependency, decodeDependencySnapshot, encodeDependencySnapshot,
  decodeCatalogMetadataChanges, encodeCatalogMetadataChanges,
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
import { deserializeBatch, serializeBatch, typeSignature } from "../../arrow/index.js";

test("typed indexes preserve raw paths, contextual keys and typed option state", () => {
  for (const [name, schema] of [
    ["index_key_column", schemas.IndexKeySchema], ["index_key_nested", schemas.IndexKeySchema],
    ["index_key_expression", schemas.IndexKeySchema], ["index_definition", schemas.IndexDefinitionSchema],
    ["index_definition_primary", schemas.IndexDefinitionSchema], ["index_info", schemas.IndexInfoSchema],
    ["index_create", schemas.IndexCreateRequestSchema], ["index_create_cross_schema", schemas.IndexCreateRequestSchema],
  ] as const) {
    const original = decodeASD<Record<string, any>>(schema, fixture(name));
    expect(decodeASD(schema, encodeASD(schema, original))).toEqual(original);
  }
  const definition = queryRecords.decodeIndexDefinition(fixture("index_definition"));
  expect(definition.keys).toEqual([fixture("index_key_column"), fixture("index_key_expression")]);
  expect(definition.options).toEqual(fixture("values"));
  expect(queryRecords.decodeIndexKey(fixture("index_key_column")).column_path).toEqual(["a.b"]);
  expect(queryRecords.decodeIndexKey(fixture("index_key_nested")).column_path).toEqual(["record", "a.b"]);
  const info = decodeASD<Record<string, any>>(schemas.IndexInfoSchema, fixture("index_info"));
  expect(info.definition).toEqual(fixture("index_definition"));
  expect(info.options).toEqual(fixture("state"));
  const create = decodeASD<Record<string, any>>(schemas.IndexCreateRequestSchema, fixture("index_create"));
  expect(create.definition).toEqual(info.definition);
  expect(create.transaction_opaque_data).toEqual(new Uint8Array([116, 120, 110, 0, 255]));
});

test("column expression metadata retains exact IPC bytes through Arrow backends", () => {
  const source = deserializeBatch(fixture("column_expression_metadata"));
  const batch = deserializeBatch(serializeBatch(source));
  for (const [index, key, name] of [[0, "vgi.column.default.v1", "column_default_expression"],
                                  [1, "vgi.column.generated.v1", "column_generated_expression"]] as const) {
    const value = batch.schema.fields[index]!.metadata!.get(key)!;
    expect(new Uint8Array(Buffer.from(value, "base64"))).toEqual(fixture(name));
  }
  expect(batch.schema.fields[1]!.metadata!.get("vgi.column.generation.v1")).toBe("virtual");
});

test("canonical constraint records preserve every field through typed codecs", () => {
  { const original = queryRecords.decodeSqlStatement(fixture("sql_statement"));
    expect(queryRecords.decodeSqlStatement(queryRecords.encodeSqlStatement(original))).toEqual(original); }
  { const original = queryRecords.decodeCatalogObjectRef(fixture("constraint_ref"));
    expect(queryRecords.decodeCatalogObjectRef(queryRecords.encodeCatalogObjectRef(original))).toEqual(original); }
  { const original = queryRecords.decodeForeignKeyDefinition(fixture("foreign_key_definition"));
    expect(queryRecords.decodeForeignKeyDefinition(queryRecords.encodeForeignKeyDefinition(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintDefinition(fixture("constraint_definition_not_null"));
    expect(queryRecords.decodeConstraintDefinition(queryRecords.encodeConstraintDefinition(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintDefinition(fixture("constraint_definition_unique"));
    expect(queryRecords.decodeConstraintDefinition(queryRecords.encodeConstraintDefinition(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintDefinition(fixture("constraint_definition_primary"));
    expect(queryRecords.decodeConstraintDefinition(queryRecords.encodeConstraintDefinition(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintDefinition(fixture("constraint_definition_check"));
    expect(queryRecords.decodeConstraintDefinition(queryRecords.encodeConstraintDefinition(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintDefinition(fixture("constraint_definition_foreign"));
    expect(queryRecords.decodeConstraintDefinition(queryRecords.encodeConstraintDefinition(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintInfo(fixture("constraint_info"));
    expect(queryRecords.decodeConstraintInfo(queryRecords.encodeConstraintInfo(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintInfo(fixture("constraint_info_untrusted"));
    expect(queryRecords.decodeConstraintInfo(queryRecords.encodeConstraintInfo(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintChange(fixture("constraint_change_add"));
    expect(queryRecords.decodeConstraintChange(queryRecords.encodeConstraintChange(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintChange(fixture("constraint_change_drop"));
    expect(queryRecords.decodeConstraintChange(queryRecords.encodeConstraintChange(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintChange(fixture("constraint_change_rename"));
    expect(queryRecords.decodeConstraintChange(queryRecords.encodeConstraintChange(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintChange(fixture("constraint_change_unname"));
    expect(queryRecords.decodeConstraintChange(queryRecords.encodeConstraintChange(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintChange(fixture("constraint_change_replace"));
    expect(queryRecords.decodeConstraintChange(queryRecords.encodeConstraintChange(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintSnapshot(fixture("constraint_snapshot"));
    expect(queryRecords.decodeConstraintSnapshot(queryRecords.encodeConstraintSnapshot(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintSnapshot(fixture("constraint_snapshot_empty"));
    expect(queryRecords.decodeConstraintSnapshot(queryRecords.encodeConstraintSnapshot(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintsApplyRequest(fixture("constraints_apply"));
    expect(queryRecords.decodeConstraintsApplyRequest(queryRecords.encodeConstraintsApplyRequest(original))).toEqual(original); }
  { const original = queryRecords.decodeConstraintsApplyRequest(fixture("constraints_apply_autocommit"));
    expect(queryRecords.decodeConstraintsApplyRequest(queryRecords.encodeConstraintsApplyRequest(original))).toEqual(original); }
  const statement = queryRecords.decodeSqlStatement(fixture("sql_statement"));
  expect(statement.evaluation_context.default_order).toBe("desc");
  expect(statement.evaluation_context.default_null_order).toBe("nulls_first");
  expect(statement.evaluation_context.preserve_insertion_order).toBe(false);
  const snapshot = queryRecords.decodeConstraintSnapshot(fixture("constraint_snapshot"));
  expect(snapshot.definition_revision).toEqual(new Uint8Array([0, 114, 101, 118, 105, 115, 105, 111, 110, 255, 128]));
  expect(snapshot.constraints).toEqual([fixture("constraint_info")]);
  expect(queryRecords.decodeConstraintInfo(fixture("constraint_info")).ref).toEqual(fixture("constraint_ref"));
});

test("query parameter batches retain mapping, typed NULLs, nested metadata and empty rows", () => {
  for (const name of ["query_parameters", "query_empty_parameters"]) {
    const original = deserializeBatch(fixture(name));
    const restored = deserializeBatch(serializeBatch(original));
    expect(restored.numRows).toBe(1);
    expect(restored.schema.metadata).toEqual(original.schema.metadata);
    expect(restored.schema.metadata?.get("vgi.query.parameters.v1")).toBe(
      name === "query_empty_parameters" ? "positional" : "named");
    if (name === "query_empty_parameters") expect(restored.schema.fields).toHaveLength(0);
    else {
      expect(restored.getChildAt(0)!.get(0)).toBeNull();
      expect(typeSignature(restored.schema.fields[0]!.type)).toBe(typeSignature(original.schema.fields[0]!.type));
      expect(restored.schema.fields[0]!.metadata?.get("logical")).toBe("example.optional");
      const child = restored.schema.fields[1]!.type.children![0]!;
      expect(child.metadata?.get("nested")).toBe("retained");
      expect(child.nullable).toBe(false);
    }
  }
});

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
  expect(create.constraint_definitions).toHaveLength(1);
  expect(old.constraint_definitions).toEqual([]);
  expect(info.constraints).toHaveLength(1);
  expect(info.definition_revision).toEqual(new Uint8Array([114, 101, 118, 105, 115, 105, 111, 110, 0, 255]));
  const constraint = queryRecords.decodeConstraintInfo(info.constraints[0]!);
  expect(constraint.definition).toEqual(create.constraint_definitions[0]);
  expect(queryRecords.decodeCatalogObjectRef(constraint.ref).name).toBe("fixture-stable-key");
  expect(queryRecords.decodeConstraintDefinition(constraint.definition).column_paths).toEqual([["record", "a.b"]]);
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

test("metadata patches retain comment presence and literal tag keys", () => {
  const patch = decodeCatalogMetadataChanges(encodeCatalogMetadataChanges(decodeCatalogMetadataChanges(fixture("metadata_patch"))));
  expect(patch.set_comment).toBe(true);
  expect(patch.comment).toBe("");
  expect(patch.set_tags).toEqual({ "a.b": "", ["__proto__"]: "literal", "": "empty-key" });
  expect(patch.remove_tags).toEqual(["old", "OLD"]);
  for (const name of ["metadata_clear_comment", "metadata_noop"]) {
    const value = decodeCatalogMetadataChanges(encodeCatalogMetadataChanges(decodeCatalogMetadataChanges(fixture(name))));
    expect(value.set_comment).toBe(name === "metadata_clear_comment");
    expect(value.comment).toBeNull();
    expect(value.set_tags).toEqual({});
    expect(value.remove_tags).toEqual([]);
  }
});

test("dependency records retain raw identities and nested graph payloads", () => {
  const table = decodeCatalogObjectRef(encodeCatalogObjectRef(decodeCatalogObjectRef(fixture("object_ref"))));
  expect(table.schema_path).toEqual(["one.two", "nested"]);
  expect(table.owner_table).toBeNull();
  const trigger = decodeCatalogObjectRef(encodeCatalogObjectRef(decodeCatalogObjectRef(fixture("trigger_ref"))));
  expect(trigger.owner_table).toBe(table.name);
  const edge = decodeObjectDependency(encodeObjectDependency(decodeObjectDependency(fixture("object_dependency"))));
  expect(edge.kind).toBe("owned");
  expect(edge.blocks_alter).toBe(true);
  expect(edge.subject_column_paths).toBeNull();
  expect(edge.dependent_column_path).toBeNull();
  const dependency = (name: string) => {
    const original = decodeObjectDependency(fixture(name));
    const restored = decodeObjectDependency(encodeObjectDependency(original));
    expect(restored).toEqual(original);
    return restored;
  };
  const membership = dependency("dependency_membership");
  expect(membership.blocks_alter).toBe(false);
  expect(membership.subject_column_paths).toBeNull();
  expect(membership.dependent_column_path).toBeNull();
  const literal = dependency("dependency_column_literal");
  expect(literal.blocks_alter).toBe(true);
  expect(literal.subject_column_paths).toEqual([["a.b"]]);
  expect(literal.dependent_column_path).toBeNull();
  expect(dependency("dependency_column_nested").subject_column_paths).toEqual([["record", "a.b"]]);
  const generated = dependency("dependency_generated");
  expect(generated.dependent).toEqual(generated.subject);
  expect(generated.dependent_column_path).toEqual(["generated"]);
  expect(generated.subject_column_paths).toEqual([["a.b"], ["record", "a.b"]]);
  const chain = dependency("dependency_generated_chain");
  expect(chain.dependent_column_path).toEqual(["second.generated"]);
  expect(chain.subject_column_paths).toEqual([["generated"]]);
  const columns = decodeDependencySnapshot(encodeDependencySnapshot(decodeDependencySnapshot(fixture("dependency_column_snapshot"))));
  expect(columns.dependencies).toEqual([fixture("dependency_column_literal"), fixture("dependency_generated"), fixture("dependency_generated_chain")]);

  expect(Array.from(edge.subject)).toEqual(Array.from(fixture("object_ref")));
  expect(Array.from(edge.dependent)).toEqual(Array.from(fixture("trigger_ref")));
  for (const name of ["dependency_snapshot", "empty_dependency_snapshot"]) {
    const graph = decodeDependencySnapshot(encodeDependencySnapshot(decodeDependencySnapshot(fixture(name))));
    expect(graph.objects.length).toBe(name === "dependency_snapshot" ? 4 : 0);
    expect(graph.dependencies.length).toBe(name === "dependency_snapshot" ? 3 : 0);
  }
});

import * as queryRecords from "../vgi-protocol-types.js";
test("query codecs preserve inline contexts, object identity and nullable results", () => {
  { const original = queryRecords.decodeClientQueryCapabilities(fixture("query_offer"));
    expect(queryRecords.decodeClientQueryCapabilities(queryRecords.encodeClientQueryCapabilities(original))).toEqual(original); }
  { const original = queryRecords.decodeQueryCapabilities(fixture("query_capabilities"));
    expect(queryRecords.decodeQueryCapabilities(queryRecords.encodeQueryCapabilities(original))).toEqual(original); }
  { const original = queryRecords.decodeQueryEvaluationContext(fixture("query_context"));
    expect(queryRecords.decodeQueryEvaluationContext(queryRecords.encodeQueryEvaluationContext(original))).toEqual(original); }
  { const original = queryRecords.decodeQueryObjectReference(fixture("query_object_reference"));
    expect(queryRecords.decodeQueryObjectReference(queryRecords.encodeQueryObjectReference(original))).toEqual(original); }
  { const original = queryRecords.decodeCatalogQueryPrepareRequest(fixture("query_prepare"));
    expect(queryRecords.decodeCatalogQueryPrepareRequest(queryRecords.encodeCatalogQueryPrepareRequest(original))).toEqual(original); }
  { const original = queryRecords.decodeCatalogQueryPrepareResult(fixture("query_accepted"));
    expect(queryRecords.decodeCatalogQueryPrepareResult(queryRecords.encodeCatalogQueryPrepareResult(original))).toEqual(original); }
  { const original = queryRecords.decodeCatalogQueryPrepareResult(fixture("query_declined"));
    expect(queryRecords.decodeCatalogQueryPrepareResult(queryRecords.encodeCatalogQueryPrepareResult(original))).toEqual(original); }
  const request = queryRecords.decodeCatalogQueryPrepareRequest(fixture("query_prepare"));
  expect(request.evaluation_context.timezone).toBe("UTC");
  expect(request.referenced_objects[0].object.schema_path).toEqual(["one.two", "nested"]);
  expect(request.catalog_version).toBe(0);
  expect(queryRecords.decodeCatalogQueryPrepareResult(fixture("query_declined")).prepared_query_token).toBeNull();
});

test("macro codecs preserve complete families, typed NULL and expression defaults", () => {
  for (const name of ["macro_parameter", "macro_parameter_expression", "macro_parameter_untyped"]) {
    const original = queryRecords.decodeMacroParameter(fixture(name));
    expect(queryRecords.decodeMacroParameter(queryRecords.encodeMacroParameter(original))).toEqual(original);
  }
  const overload = queryRecords.decodeMacroOverload(fixture("macro_overload"));
  expect(queryRecords.decodeMacroOverload(queryRecords.encodeMacroOverload(overload))).toEqual(overload);
  expect(overload.parameters.map(parameter => parameter.name)).toEqual(["x", "fallback", "scale"]);
  for (const [name, schema] of [
    ["macro_create", schemas.MacroCreateRequestSchema], ["macro_create_legacy", schemas.MacroCreateRequestSchema],
    ["macro_info", schemas.MacroInfoSchema], ["macro_info_legacy", schemas.MacroInfoSchema],
  ] as const) {
    const original = decodeASD<Record<string, any>>(schema, fixture(name));
    const restored = decodeASD<Record<string, any>>(schema, encodeASD(schema, original));
    expect(restored).toEqual(original);
    if (name.endsWith("legacy")) expect(restored.overloads).toBeNull();
    else expect(restored.overloads).toHaveLength(2);
  }
});

test("logical type batches preserve tuple identity, SQL NULL and variant absence", () => {
  for (const name of ["logical_tuple", "logical_struct", "logical_variant_sql_null", "logical_variant_null", "logical_variant_absent", "logical_variant_null_member"]) {
    const original = deserializeBatch(fixture(name));
    const restored = deserializeBatch(serializeBatch(original));
    expect(restored.schema.fields).toEqual(original.schema.fields);
    expect(restored.schema.metadata).toEqual(original.schema.metadata);
    const field = restored.schema.fields[0]!;
    const value = restored.getChildAt(0)!.get(0);
    if (name === "logical_tuple") expect(field.metadata.get("vgi.logical_type.v1")).toBe("tuple");
    if (name === "logical_struct") expect(field.metadata.has("vgi.logical_type.v1")).toBe(false);
    if (name.startsWith("logical_variant_")) {
      expect(field.metadata.has("vgi.logical_type.v1")).toBe(false);
      expect(field.metadata.get("ARROW:extension:name")).toBe("arrow.parquet.variant");
      expect(field.metadata.get("ARROW:extension:metadata")).toBe("");
      expect(field.type.children!.map(child => [child.name, child.nullable])).toEqual([["metadata", false], ["value", true]]);
      if (name === "logical_variant_sql_null") expect(value).toBeNull();
      else {
        const metadata = name === "logical_variant_null_member" ? [1, 1, 0, 7, 112, 114, 101, 115, 101, 110, 116] : [1, 0, 0];
        const payload = name === "logical_variant_null" ? [0] : name === "logical_variant_absent" ? [2, 0, 0] : [2, 1, 0, 0, 1, 0];
        expect(Array.from(value["metadata"])).toEqual(metadata);
        expect(Array.from(value["value"])).toEqual(payload);
        expect(value["metadata"]).toEqual(original.getChildAt(0)!.get(0)["metadata"]);
        expect(value["value"]).toEqual(original.getChildAt(0)!.get(0)["value"]);
      }
    } else if (name === "logical_tuple" || name === "logical_struct") {
      const valueBefore = original.getChildAt(0)!.get(0);
      for (const child of field.type.children!) {
        expect(value[child.name]).toEqual(valueBefore[child.name]);
      }
    }
  }
});
