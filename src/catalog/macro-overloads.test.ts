// Copyright 2026 Query Farm LLC - https://query.farm
import { expect, test } from "bun:test";
import { macroOverloads } from "./macro-overloads.js";
import { int64, field, schema, batchFromColumns, serializeBatch, deserializeBatch, iterRows } from "../arrow/index.js";
import { encodeMacroInfo, decodeMacroInfo } from "../generated/vgi-client.js";

test("canonical default preserves typed NULL, metadata and empty documentation", () => {
  const defaults = serializeBatch(batchFromColumns({ x: [null] }, schema([
    field("x", int64(), true, new Map([["logical", "field annotation"]])),
  ], new Map([["logical", "schema annotation"]]))));
  const family = macroOverloads({ name: "m", macroType: "scalar", parameters: ["x"], definition: "x",
    parameterDefaultValues: defaults, parameterDocs: { x: "" } }, ["sales.archive"]);
  const p = family[0].parameters[0];
  expect(p.declared_type).toBeNull();
  expect(p.default_expression).toBeNull();
  expect(p.documentation).toBe("");
  const value = deserializeBatch(p.default_value!);
  expect([...iterRows(value)]).toEqual([{ x: null }]);
  expect(value.schema.metadata.get("logical")).toBe("schema annotation");
  expect(value.schema.fields[0].metadata.get("logical")).toBe("field annotation");
  expect(value.schema.fields[0].type.typeId).toBe(int64().typeId);
  const two = macroOverloads({ name: "m", macroType: "scalar", parameters: [], definition: "", overloads: [family[0], { ...family[0], parameters: [], definition: "1" }] }, ["sales.archive"]);
  const decoded = decodeMacroInfo(encodeMacroInfo({ name:"m", schema_path:["sales.archive"], comment:null, tags:{}, macro_type:"scalar", overloads:two }));
  expect(decoded.overloads).toHaveLength(2);
  expect(decoded.overloads[0].parameters[0].default_value).toEqual(p.default_value);
  expect(decoded.overloads[1].parameters).toEqual([]);
  expect(Object.keys(decoded).sort()).toEqual(["comment", "macro_type", "name", "overloads", "schema_path", "tags"]);
});

test("simple descriptor rejects unknown or repeated defaults and discovery expressions", () => {
  const descriptor = { name: "m", macroType: "scalar" as const, parameters: ["x"], definition: "x" };
  expect(() => macroOverloads({ ...descriptor, parameterDocs: { missing: "doc" } }, ["main"])).toThrow("unknown");
  const overload = macroOverloads(descriptor, ["main"])[0];
  overload.parameters[0].default_expression = new Uint8Array([1]);
  expect(() => macroOverloads({ ...descriptor, parameters:[], definition:"", overloads:[overload] }, ["main"])).toThrow("resolved");
});
