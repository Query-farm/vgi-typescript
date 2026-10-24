// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Pins the wire shape of protocol records against vgi-python's container
// rules, which a strict client (the DuckDB extension's generated record
// codecs) enforces:
//
//  - a non-nullable list / map column is never null — an absent one is `[]`;
//  - list items and map values are never null, except `argument_names` items
//    (the protocol's only `list[str | None]`);
//  - dictionary-encoded enum columns carry the Python enum member NAME.
//
// A cross-SDK conformance run caught this SDK sending null for
// `CatalogInfo.releases` / `CatalogInfo.attach_option_specs` (a catalog whose
// `catalogsInfo()` left them out) and null items in `BindResponse.lookup_names`
// (an `onBind` that requested a secret lookup without a name).

import { describe, expect, test } from "bun:test";
import { deserializeBatch, field, int64, schema, utf8 } from "../../arrow/index.js";
import { conformRecord } from "../conform.js";
import { encodeASD } from "../asd.js";
import { wireViolations } from "../../__tests__/support/wire-check.js";
import {
  encodeCatalogInfo,
  encodeSchemaInfo,
  encodeTableInfo,
} from "../../generated/vgi-client.js";
import {
  BindResultSchema,
  CatalogCatalogsResultSchema,
  CatalogInfoSchema,
  CatalogSchemaContentsFunctionsResultSchema,
  CatalogSchemaContentsMacrosResultSchema,
  SchemaInfoSchema,
} from "../../generated/vgi-protocol-schemas.js";
import { wrapResult } from "../../protocol/handlers/shared.js";
import { serializeBindRequest, serializeBindResponse } from "../../protocol/serializers/bind.js";
import { decodeASD } from "../asd.js";
import { buildVgiProtocol } from "../../protocol/dispatch.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { defineTableFunction } from "../../functions/table.js";
import { Arguments } from "../../arguments/arguments.js";
import { FunctionType } from "../../types.js";
import { allFunctions, catalog, createExampleCatalog } from "../../../examples/common.js";
import { serializeBatch } from "../../arrow/index.js";
import { wrapRequest } from "../../client/protocol.js";
import { batchFromColumns } from "../../util/arrow/index.js";
import { binary } from "../../arrow/index.js";

/** Row 0 of an encoded single-row record, column by column. */
function row(bytes: Uint8Array): Record<string, any> {
  const batch = deserializeBatch(bytes);
  const out: Record<string, any> = {};
  for (const f of batch.schema.fields) out[f.name] = batch.getChild(f.name)?.get(0) ?? null;
  return out;
}

function list(v: any): any[] {
  if (v == null) return v;
  return Array.from(v as Iterable<any>);
}

/** Every IPC-encoded record nested in `bytes` (item lists of binary records). */
function nestedViolations(bytes: Uint8Array, record: string): string[] {
  const out = wireViolations(bytes, record);
  const batch = deserializeBatch(bytes);
  for (const f of batch.schema.fields) {
    const col = batch.getChild(f.name);
    for (let r = 0; r < batch.numRows; r++) {
      const v = col?.get(r);
      const items = v instanceof Uint8Array ? [v] : v != null && typeof v === "object" && typeof v[Symbol.iterator] === "function" ? Array.from(v as Iterable<any>) : [];
      items.forEach((item, i) => {
        if (item instanceof Uint8Array && item.length > 8 && item[0] === 0xff && item[1] === 0xff && item[2] === 0xff && item[3] === 0xff) {
          try {
            out.push(...nestedViolations(item, `${record}.${f.name}[${i}]`));
          } catch {
            // Not a record batch stream (e.g. a bare serialized schema).
          }
        }
      });
    }
  }
  return out;
}

describe("CatalogInfo: non-nullable lists are never null", () => {
  test("a CatalogInfo with no releases / attach_option_specs encodes both as non-null empty lists", () => {
    const bytes = encodeCatalogInfo({ name: "c", implementation_version: null, data_version_spec: null } as any);
    const r = row(bytes);
    expect(r.releases).not.toBeNull();
    expect(list(r.releases)).toEqual([]);
    expect(r.attach_option_specs).not.toBeNull();
    expect(list(r.attach_option_specs)).toEqual([]);
    const batch = deserializeBatch(bytes);
    expect(batch.getChild("releases")!.nullCount).toBe(0);
    expect(batch.getChild("attach_option_specs")!.nullCount).toBe(0);
    expect(wireViolations(bytes, "CatalogInfo")).toEqual([]);
  });

  test("explicit null lists are written as empty too", () => {
    const bytes = encodeCatalogInfo({
      name: "c", implementation_version: null, data_version_spec: null,
      attach_option_specs: null, releases: null, source_url: null,
    } as any);
    expect(wireViolations(bytes, "CatalogInfo")).toEqual([]);
  });

  test("populated releases survive unchanged", () => {
    const bytes = encodeCatalogInfo({
      name: "c", implementation_version: "1", data_version_spec: ">=1",
      attach_option_specs: [],
      releases: [{ version: "1.0.0", released_at: null, summary: "first", notes_url: null }],
      source_url: null,
    });
    const releases = list(row(bytes).releases);
    expect(releases).toHaveLength(1);
    expect(releases[0].version).toBe("1.0.0");
    expect(releases[0].summary).toBe("first");
    expect(wireViolations(bytes, "CatalogInfo")).toEqual([]);
  });

  test("catalog_catalogs serves a conforming CatalogInfo for a catalogsInfo() that omits the lists", async () => {
    // The shape examples/versioned-worker.ts returns.
    class Versioned extends ReadOnlyCatalogInterface {
      override catalogsInfo(): any[] {
        return [{ name: "v", implementation_version: "1.0.0", data_version_spec: ">=1" }];
      }
    }
    const cat = new Versioned({ name: "v", schemas: [{ name: "main" }] }, new FunctionRegistry());
    const protocol = buildVgiProtocol({ registry: new FunctionRegistry(), catalogInterface: cat });
    const m = (protocol as any)._methods?.get?.("catalog_catalogs") ?? (protocol as any).methods?.get?.("catalog_catalogs");
    const res = await m.handler({}, {} as any);
    const items = decodeASD<{ items: Uint8Array[] }>(CatalogCatalogsResultSchema, res.result).items;
    expect(items).toHaveLength(1);
    expect(nestedViolations(res.result, "CatalogCatalogsResult")).toEqual([]);
    expect(list(row(items[0]).releases)).toEqual([]);
  });
});

describe("BindResponse: list items are never null", () => {
  test("a null lookup_names item (no secret name) is sent as an empty string, like vgi-python", () => {
    const wire = wrapResult(
      serializeBindResponse({
        output_schema: schema([field("x", int64(), true)]),
        opaque_data: null,
        lookup_secret_types: ["s3", "s3"],
        lookup_scopes: ["s3://a/", null as any],
        lookup_names: [null as any, "named"],
      }),
      BindResultSchema,
    );
    const r = row(wire.result);
    expect(list(r.lookup_names)).toEqual(["", "named"]);
    expect(list(r.lookup_scopes)).toEqual(["s3://a/", ""]);
    expect(deserializeBatch(wire.result).getChild("lookup_names")!.data[0].children[0].nullCount).toBe(0);
    expect(wireViolations(wire.result, "BindResult")).toEqual([]);
  });

  test("an onBind secret-lookup request with unnamed lookups goes out conforming through the bind handler", async () => {
    const fn = defineTableFunction<{ path: string }, null>({
      name: "lookup_demo",
      args: { path: utf8() },
      onBind: (p: any) =>
        p.resolvedSecretsProvided
          ? { outputSchema: schema([field("x", int64(), true)]) }
          : {
              outputSchema: schema([field("x", int64(), true)]),
              lookupSecretTypes: ["vgi_example"],
              lookupScopes: [p.args.path],
              lookupNames: [null as any],
            },
      process: () => null,
    } as any);
    const registry = new FunctionRegistry();
    registry.register(fn);
    const protocol = buildVgiProtocol({ registry });
    const m = (protocol as any)._methods?.get?.("bind") ?? (protocol as any).methods?.get?.("bind");
    const args = new Arguments();
    args.positional.push({ value: "s3://bucket/", type: utf8() } as any);
    const req = serializeBindRequest({
      function_name: "lookup_demo",
      arguments: args,
      function_type: FunctionType.TABLE,
      input_schema: null,
      settings: null,
      secrets: null,
      attach_opaque_data: null,
      transaction_opaque_data: null,
      resolved_secrets_provided: false,
    } as any);
    const res = await m.handler({ request: serializeBatch(req) }, {} as any);
    expect(list(row(res.result).lookup_names)).toEqual([""]);
    expect(wireViolations(res.result, "BindResult")).toEqual([]);
  });
});

describe("BindRequest: argument_names keeps its null items", () => {
  test("a positional (unnamed) argument stays null; the function type is the enum NAME", () => {
    const batch = serializeBindRequest({
      function_name: "f",
      arguments: new Arguments(),
      function_type: FunctionType.TABLE_BUFFERING,
      input_schema: null,
      settings: null,
      secrets: null,
      attach_opaque_data: null,
      transaction_opaque_data: null,
      resolved_secrets_provided: false,
      argument_names: [null, "named"],
    } as any);
    const bytes = serializeBatch(batch);
    const r = row(bytes);
    expect(list(r.argument_names)).toEqual([null, "named"]);
    expect(String(r.function_type)).toBe("TABLE_BUFFERING");
    expect(wireViolations(bytes, "BindRequest")).toEqual([]);
  });
});

describe("maps and nested records", () => {
  test("a missing non-nullable map is an empty map, and a null map value its zero value", () => {
    const empty = encodeSchemaInfo({ comment: null, attach_opaque_data: new Uint8Array(0), path: ["main"] } as any);
    expect(wireViolations(empty, "SchemaInfo")).toEqual([]);
    expect(deserializeBatch(empty).getChild("tags")!.nullCount).toBe(0);

    const nullValue = encodeASD(SchemaInfoSchema, {
      comment: null, tags: { a: null, b: "x" }, attach_opaque_data: new Uint8Array(0), path: ["main"],
      estimated_object_count: null,
    });
    expect(wireViolations(nullValue, "SchemaInfo")).toEqual([]);
  });

  test("null items in nested raw column paths become their zero values", () => {
    const bytes = encodeTableInfo({
      comment: null, tags: {}, name: "t", schema_path: ["main"], columns: new Uint8Array(0),
      constraints: [], write_result_modes: {}, supports_column_statistics: false,
      required_filters: [null, [null, [null, "column"]]],
    } as any);
    expect(wireViolations(bytes, "TableInfo")).toEqual([]);
    const r = row(bytes);
    expect(list(r.required_filters).map((group) => list(group).map(list)))
      .toEqual([[], [[], ["", "column"]]]);
  });

  test("conformRecord leaves a conforming record (and frozen input) untouched", () => {
    const rec = Object.freeze({
      name: "c", implementation_version: null, data_version_spec: null,
      attach_option_specs: Object.freeze([]), releases: Object.freeze([]), source_url: null,
    });
    expect(conformRecord(CatalogInfoSchema, rec)).toBe(rec);
  });
});

describe("the example catalog serves conforming records", () => {
  // Every record the example catalog lists, decoded down through its nested
  // item records, checked against the strict-client rules — including the
  // enum NAME columns (function_type, macro_type, stability, ...).
  function handlers() {
    const r = new FunctionRegistry();
    for (const fn of allFunctions) r.register(fn);
    const cat = createExampleCatalog(new ReadOnlyCatalogInterface(catalog, r));
    const protocol = buildVgiProtocol({ registry: r, catalogInterface: cat });
    const call = async (name: string, params: Record<string, unknown>): Promise<any> => {
      const m = (protocol as any)._methods?.get?.(name) ?? (protocol as any).methods?.get?.(name);
      return m.handler(params, {} as any);
    };
    return { call };
  }

  test("catalog_catalogs, attach, contents and per-schema listings", async () => {
    const { call } = handlers();
    const violations: string[] = [];
    const cats = await call("catalog_catalogs", {});
    violations.push(...nestedViolations(cats.result, "CatalogCatalogsResult"));

    const attachSchema = schema([
      field("name", utf8(), false),
      field("options", binary(), true),
      field("data_version_spec", utf8(), true),
      field("implementation_version", utf8(), true),
      field("client_capabilities", binary(), true),
    ]);
    const attach = await call(
      "catalog_attach",
      wrapRequest(batchFromColumns(
        { name: ["example"], options: [null], data_version_spec: [null], implementation_version: [null], client_capabilities: [null] },
        attachSchema,
      )),
    );
    violations.push(...nestedViolations(attach.result, "CatalogAttachResult"));
    const attachOpaque = row(attach.result).attach_opaque_data as Uint8Array;

    const contents = await call("catalog_contents", { attach_opaque_data: attachOpaque, if_none_match: null });
    if (contents.result) violations.push(...nestedViolations(contents.result, "CatalogContentsResult"));

    const schemas = await call("catalog_schemas", { attach_opaque_data: attachOpaque, transaction_opaque_data: null });
    violations.push(...nestedViolations(schemas.result, "CatalogSchemasResult"));
    const paths = (decodeASD<{ items: Uint8Array[] }>(CatalogSchemaContentsFunctionsResultSchema, schemas.result).items)
      .map((b) => list(row(b).path));
    expect(paths.length).toBeGreaterThan(0);

    let functions = 0;
    let macros = 0;
    for (const path of paths) {
      const base = { attach_opaque_data: attachOpaque, path, transaction_opaque_data: null };
      for (const type of ["SCALAR_FUNCTION", "TABLE_FUNCTION", "AGGREGATE_FUNCTION"]) {
        const fr = await call("catalog_schema_contents_functions", { ...base, type });
        functions += decodeASD<{ items: Uint8Array[] }>(CatalogSchemaContentsFunctionsResultSchema, fr.result).items.length;
        violations.push(...nestedViolations(fr.result, `functions(${path.join(".")}, ${type})`));
      }
      for (const type of ["SCALAR_MACRO", "TABLE_MACRO"]) {
        const mr = await call("catalog_schema_contents_macros", { ...base, type });
        macros += decodeASD<{ items: Uint8Array[] }>(CatalogSchemaContentsMacrosResultSchema, mr.result).items.length;
        violations.push(...nestedViolations(mr.result, `macros(${path.join(".")}, ${type})`));
      }
      for (const name of ["catalog_schema_contents_tables", "catalog_schema_contents_views", "catalog_schema_contents_indexes"]) {
        const tr = await call(name, base);
        violations.push(...nestedViolations(tr.result, `${name}(${path.join(".")})`));
      }
    }
    expect(functions).toBeGreaterThan(10);
    expect(macros).toBeGreaterThan(0);
    expect(violations).toEqual([]);
  }, 60_000);
});
