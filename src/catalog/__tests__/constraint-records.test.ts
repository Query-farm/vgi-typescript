import { expect, test } from "bun:test";
import { encodeASD, decodeASD } from "../../codec/asd.js";
import { CatalogObjectRefSchema, ClientCapabilitiesSchema } from "../../generated/vgi-protocol-schemas.js";
import {
  decodeCatalogCapabilities, decodeConstraintDefinition, decodeConstraintInfo,
  decodeForeignKeyDefinition, encodeCatalogCapabilities,
} from "../../generated/vgi-protocol-types.js";
import {
  constraintRecord, readConstraintRecord, CONSTRAINT_RECORDS_READ, FunctionRegistry, ReadOnlyCatalogInterface,
  ROW_SCALAR_EXPRESSION, type CatalogDescriptor,
} from "../../index.js";
import { field, int64, schema, struct, utf8 } from "../../arrow/index.js";

const ATTESTED = { enforcement: "ENFORCED", validation: "ALL_ROWS" } as const;

function catalogWith(constraints: Uint8Array[] | undefined): ReadOnlyCatalogInterface {
  const descriptor: CatalogDescriptor = {
    name: "c",
    schemas: [{
      name: "main",
      tables: [
        { name: "parent", columns: schema([field("id", int64(), false)]), constraints:
          constraints === undefined ? undefined : [constraintRecord({ schemaPath: ["main"], table: "parent",
            id: "parent.pk", kind: "PRIMARY_KEY", columns: [["id"]], ...ATTESTED })] },
        { name: "child", columns: schema([field("id", int64(), false), field("parent_id", int64(), true),
          field("name", utf8(), true)]), constraints },
      ],
    }],
  };
  return new ReadOnlyCatalogInterface(descriptor, new FunctionRegistry());
}

function offer(read: string[]): Uint8Array {
  return encodeASD(ClientCapabilitiesSchema, { engine: "test", native_formats: [], catalogs: [], can_stream: true,
    filter_encodings: [], query_pushdown: null,
    catalog: encodeCatalogCapabilities({ read, ddl: [], write: [], transaction: [] }) });
}

test("constraintRecord encodes the provider's identity, definition and attestation", () => {
  const fk = decodeConstraintInfo(constraintRecord({
    schemaPath: ["main"], table: "child", id: "child.parent", kind: "FOREIGN_KEY", columns: [["parent_id"]],
    references: { schemaPath: ["main"], table: "parent", columns: [["id"]], constraintId: "parent.pk" },
    ...ATTESTED,
  }));
  expect(fk.enforcement).toBe("ENFORCED");
  expect(fk.validation).toBe("ALL_ROWS");
  expect(decodeASD<any>(CatalogObjectRefSchema, fk.ref)).toMatchObject(
    { kind: "constraint", schema_path: ["main"], name: "child.parent", owner_table: "child" });
  expect(decodeASD<any>(CatalogObjectRefSchema, fk.referenced_constraint!)).toMatchObject(
    { kind: "constraint", name: "parent.pk", owner_table: "parent" });
  const def = decodeConstraintDefinition(fk.definition);
  expect(def.kind).toBe("FOREIGN_KEY");
  expect(def.column_paths).toEqual([["parent_id"]]);
  expect(decodeForeignKeyDefinition(def.foreign_key!).referenced_column_paths).toEqual([["id"]]);

  const check = decodeConstraintDefinition(decodeConstraintInfo(constraintRecord({
    schemaPath: ["main"], table: "child", id: "child.name", kind: "CHECK", columns: [], checkSql: "name <> ''",
    enforcement: "METADATA_ONLY", validation: "UNVALIDATED",
  })).definition);
  expect(check.check_expression).not.toBeNull();
});

test("constraintRecord rejects shapes the protocol rejects", () => {
  const base = { schemaPath: ["main"], table: "t", id: "x", ...ATTESTED };
  expect(() => constraintRecord({ ...base, kind: "NOT_NULL", columns: [["a"], ["b"]] })).toThrow();
  expect(() => constraintRecord({ ...base, kind: "PRIMARY_KEY", columns: [] })).toThrow();
  expect(() => constraintRecord({ ...base, kind: "CHECK", columns: [["a"]], checkSql: "a > 0" })).toThrow();
  expect(() => constraintRecord({ ...base, kind: "UNIQUE", columns: [["a"]], checkSql: "a > 0" })).toThrow();
  expect(() => constraintRecord({ ...base, kind: "FOREIGN_KEY", columns: [["a"]] })).toThrow();
  expect(() => constraintRecord({ ...base, id: "", kind: "UNIQUE", columns: [["a"]] })).toThrow();
});

test("read-only attach negotiates constraint records only for catalogs that serve them", async () => {
  const both = [CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION].sort();
  const served = catalogWith([
    constraintRecord({ schemaPath: ["main"], table: "child", id: "child.id", kind: "NOT_NULL", columns: [["id"]], ...ATTESTED }),
  ]);
  for (const [read, expected] of [[both, both], [[CONSTRAINT_RECORDS_READ], [CONSTRAINT_RECORDS_READ]],
    [[ROW_SCALAR_EXPRESSION], []], [[], []]] as const) {
    const result = await served.attach("c", {}, null, null, offer([...read]));
    expect(decodeCatalogCapabilities(result.catalog_capabilities!).read).toEqual(both);
    expect(decodeCatalogCapabilities(result.selected_capabilities!).read).toEqual([...expected]);
  }
  const plain = await catalogWith(undefined).attach("c", {}, null, null, offer(both));
  expect(plain.catalog_capabilities ?? null).toBeNull();
  expect(plain.selected_capabilities ?? null).toBeNull();
});

test("served TableInfo carries the records unchanged", async () => {
  const record = constraintRecord({ schemaPath: ["main"], table: "child", id: "child.id", kind: "NOT_NULL",
    columns: [["id"]], ...ATTESTED });
  const cat = catalogWith([record]);
  const { attach_opaque_data } = await cat.attach("c", {}, null, null, offer([CONSTRAINT_RECORDS_READ]));
  const child = (await cat.schemaContentsTables(attach_opaque_data, ["main"])).find((t) => t.name === "child")!;
  expect(child.constraints).toEqual([record]);
});

test("legacy descriptor constraint fields name the replacement", async () => {
  const cat = new ReadOnlyCatalogInterface({ name: "c", schemas: [{ name: "main", tables: [
    { name: "t", columns: schema([field("id", int64(), false)]), primaryKey: [["id"]] }] }] }, new FunctionRegistry());
  const { attach_opaque_data } = await cat.attach("c");
  expect(() => cat.schemaContentsTables(attach_opaque_data, ["main"])).toThrow(/constraintRecord\(\)/);
});

test("readConstraintRecord inverts constraintRecord", () => {
  const spec = {
    schemaPath: ["main"], table: "child", id: "child.parent", kind: "FOREIGN_KEY" as const, columns: [["parent_id"]],
    references: { schemaPath: ["other"], table: "parent", columns: [["id"]], constraintId: "parent.pk" },
    ...ATTESTED,
  };
  expect(readConstraintRecord(constraintRecord(spec))).toEqual({ ...spec, declaredName: null, checkSql: null });
  const check = readConstraintRecord(constraintRecord({ schemaPath: ["main"], table: "t", id: "t.c", kind: "CHECK",
    columns: [], checkSql: "a > 0", declaredName: "positive", enforcement: "METADATA_ONLY", validation: "UNVALIDATED" }));
  expect(check).toMatchObject({ kind: "CHECK", columns: [], checkSql: "a > 0", declaredName: "positive", references: null });
});

test("Arrow nullability must agree with NOT_NULL/PRIMARY_KEY records", async () => {
  const tables = async (columns: ReturnType<typeof schema>, constraints?: Uint8Array[]) => {
    const cat = new ReadOnlyCatalogInterface({ name: "c", schemas: [{ name: "main",
      tables: [{ name: "t", columns, constraints }] }] }, new FunctionRegistry());
    const { attach_opaque_data } = await cat.attach("c");
    return () => cat.schemaContentsTables(attach_opaque_data, ["main"]);
  };
  const notNull = (...path: string[]) => constraintRecord({ schemaPath: ["main"], table: "t",
    id: `t.${path.join(".")}`, kind: "NOT_NULL", columns: [path], ...ATTESTED });

  // Non-nullable without a record, and a record on a nullable field, both fail.
  expect(await tables(schema([field("a", utf8(), false)]))).toThrow(/non-nullable in Arrow but no NOT_NULL/);
  expect(await tables(schema([field("a", utf8(), true)]), [notNull("a")])).toThrow(/Arrow field is nullable/);
  // Agreement passes, including a PRIMARY_KEY standing in for NOT_NULL.
  expect(await tables(schema([field("a", utf8(), false)]), [notNull("a")])).not.toThrow();
  expect(await tables(schema([field("a", utf8(), false)]), [constraintRecord({ schemaPath: ["main"], table: "t",
    id: "t.pk", kind: "PRIMARY_KEY", columns: [["a"]], ...ATTESTED })])).not.toThrow();
  // The rule descends through ordinary STRUCTs.
  const nested = schema([field("s", struct([field("x", utf8(), false)]), true)]);
  expect(await tables(nested)).toThrow(/column s\.x is non-nullable/);
  expect(await tables(nested, [notNull("s", "x")])).not.toThrow();
});

test("only ENFORCED + ALL_ROWS records are served", async () => {
  const cat = new ReadOnlyCatalogInterface({ name: "c", schemas: [{ name: "main", tables: [{
    name: "t", columns: schema([field("a", utf8(), true)]),
    constraints: [constraintRecord({ schemaPath: ["main"], table: "t", id: "t.a.unique", kind: "UNIQUE",
      columns: [["a"]], enforcement: "METADATA_ONLY", validation: "UNVALIDATED" })],
  }] }] }, new FunctionRegistry());
  const { attach_opaque_data } = await cat.attach("c");
  expect(() => cat.schemaContentsTables(attach_opaque_data, ["main"])).toThrow(/accepts only ENFORCED \+ ALL_ROWS/);
});
