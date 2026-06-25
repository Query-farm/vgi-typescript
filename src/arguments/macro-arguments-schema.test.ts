// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

import { describe, test, expect } from "bun:test";
import { int64, utf8, schema, field, batchFromColumns, serializeBatch, deserializeSchema, TypeId } from "../arrow/index.js";
import { VGI_DOC_KEY } from "../types.js";
import { macroArgumentsSchema, macroParameterDocsFromSchema } from "./argument-spec.js";
import { ReadOnlyCatalogInterface } from "../catalog/read-only.js";
import { FunctionRegistry } from "../functions/registry.js";

describe("macro arguments_schema vgi_doc", () => {
  test("documented params carry vgi_doc per field; undocumented omit the key", () => {
    // Unicode doc exercises UTF-8 metadata encoding.
    const docA = "µ ≥ first parameter — note";
    const parameters = ["a", "b", "c"];

    const argSchema = macroArgumentsSchema(parameters, null, { a: docA, c: "" });

    // One field per parameter, in order, all nullable.
    expect(argSchema.fields.map((f) => f.name)).toEqual(parameters);
    for (const f of argSchema.fields) {
      expect(f.nullable).toBe(true);
    }

    const byName = new Map(argSchema.fields.map((f) => [f.name, f]));

    // Documented param "a" carries vgi_doc; "b" (absent) and "c" (empty) do not.
    expect(byName.get("a")?.metadata.has(VGI_DOC_KEY)).toBe(true);
    expect(byName.get("a")?.metadata.get(VGI_DOC_KEY)).toBe(docA);
    expect(byName.get("b")?.metadata.has(VGI_DOC_KEY)).toBe(false);
    expect(byName.get("c")?.metadata.has(VGI_DOC_KEY)).toBe(false);

    // Round-trip: only documented params come back.
    expect(macroParameterDocsFromSchema(argSchema)).toEqual({ a: docA });
  });

  test("field type comes from the default value type when known, else null", () => {
    const parameters = ["typed", "untyped"];
    // One-row RecordBatch with a typed default for "typed" only.
    const defaults = serializeBatch(
      batchFromColumns(
        { typed: [5n] },
        schema([field("typed", int64(), true)]),
      ),
    );

    const argSchema = macroArgumentsSchema(parameters, defaults, {
      typed: "the typed one",
    });
    const byName = new Map(argSchema.fields.map((f) => [f.name, f]));

    // "typed" inherits int64 from the default; "untyped" falls back to null type.
    expect(byName.get("typed")?.type.typeId).toBe(int64().typeId);
    expect(byName.get("untyped")?.type.typeId).toBe(TypeId.Null);

    // Docs still ride along independent of type inference.
    expect(macroParameterDocsFromSchema(argSchema)).toEqual({
      typed: "the typed one",
    });
  });

  test("no parameters yields an empty schema and no docs", () => {
    const argSchema = macroArgumentsSchema([], null, {});
    expect(argSchema.fields).toHaveLength(0);
    expect(macroParameterDocsFromSchema(argSchema)).toEqual({});
    // utf8 import kept meaningful: confirm a string-typed default flows through.
    const defaults = serializeBatch(
      batchFromColumns({ s: ["x"] }, schema([field("s", utf8(), true)])),
    );
    const one = macroArgumentsSchema(["s"], defaults);
    expect(one.fields[0]?.type.typeId).toBe(utf8().typeId);
  });
});

describe("ReadOnlyCatalogInterface macro arguments_schema", () => {
  test("descriptor parameterDocs flow into MacroInfo.arguments_schema vgi_doc", () => {
    const catalog = new ReadOnlyCatalogInterface(
      {
        name: "test",
        schemas: [
          {
            name: "main",
            macros: [
              {
                name: "doc_macro",
                macroType: "scalar",
                parameters: ["x", "y"],
                parameterDocs: { x: "the documented x" },
                definition: "x + y",
              },
            ],
          },
        ],
      },
      new FunctionRegistry(),
    );

    const macros = catalog.schemaContentsMacros(new Uint8Array([1]), "main", "SCALAR_MACRO");
    expect(macros).toHaveLength(1);

    const info = macros[0]!;
    // arguments_schema is always populated (non-nullable wire slot, IPC bytes).
    expect(info.arguments_schema).toBeInstanceOf(Uint8Array);
    expect((info.arguments_schema as Uint8Array).length).toBeGreaterThan(0);

    const argSchema = deserializeSchema(info.arguments_schema as Uint8Array);
    expect(argSchema.fields.map((f) => f.name)).toEqual(["x", "y"]);

    // Documented -> vgi_doc present; undocumented -> key omitted.
    expect(macroParameterDocsFromSchema(argSchema)).toEqual({ x: "the documented x" });
  });
});
