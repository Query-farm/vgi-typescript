import { expect, test } from "bun:test";
import { encodeASD, decodeASD } from "../../codec/asd.js";
import {
  CatalogObjectRefSchema, ClientCapabilitiesSchema, ConstraintDefinitionSchema, ConstraintInfoSchema,
  ForeignKeyDefinitionSchema,
} from "../../generated/vgi-protocol-schemas.js";
import {
  decodeCatalogCapabilities, decodeConstraintDefinition, decodeConstraintInfo,
  decodeForeignKeyDefinition, decodeSqlExpression, encodeCatalogCapabilities,
} from "../../generated/vgi-protocol-types.js";
import {
  constraintRecord, readConstraintRecord, CONSTRAINT_RECORDS_READ, FunctionRegistry, ReadOnlyCatalogInterface,
  ROW_SCALAR_EXPRESSION, type CatalogDescriptor, defineTableFunction, servedTableSchema, selectCatalogCapabilities,
  catalogCapabilities, CONSTRAINT_CREATE, CONSTRAINT_ENFORCE, CONSTRAINT_MUTATE, TRANSACTION_DDL,
  generatedExpressionMetadata, DEFAULT_EXPRESSION_KEY,
} from "../../index.js";
import { deserializeSchema, field, float64, int64, schema, struct, timestampMicros, utf8 } from "../../arrow/index.js";

const ATTESTED = { enforcement: "ENFORCED", validation: "ALL_ROWS" } as const;
const NN = (table: string, ...path: string[]) => constraintRecord({ schemaPath: ["main"], table,
  id: `${table}.${path.join(".")}.nn`, kind: "NOT_NULL", columns: [path], ...ATTESTED });
const PK = (table: string, ...cols: string[]) => constraintRecord({ schemaPath: ["main"], table,
  id: `${table}.pk`, kind: "PRIMARY_KEY", columns: cols.map((c) => [c]), ...ATTESTED });

function catalogWith(constraints: Uint8Array[] | undefined): ReadOnlyCatalogInterface {
  const descriptor: CatalogDescriptor = {
    name: "c",
    schemas: [{
      name: "main",
      tables: [
        { name: "parent", columns: schema([field("id", int64(), false)]),
          constraints: constraints === undefined ? undefined : [PK("parent", "id")] },
        { name: "child", columns: schema([field("id", int64(), false), field("parent_id", int64(), true),
          field("name", utf8(), true)]), constraints },
      ],
    }],
  };
  return new ReadOnlyCatalogInterface(descriptor, new FunctionRegistry());
}

function offer(read: string[], more: { ddl?: string[]; write?: string[]; transaction?: string[] } = {}): Uint8Array {
  return encodeASD(ClientCapabilitiesSchema, { engine: "test", native_formats: [], catalogs: [], can_stream: true,
    filter_encodings: [], query_pushdown: null,
    catalog: encodeCatalogCapabilities({ read, ddl: more.ddl ?? [], write: more.write ?? [], transaction: more.transaction ?? [] }) });
}

/** Serve one descriptor table and return its served Arrow schema (or the thrown error via expect). */
async function serve(columns: ReturnType<typeof schema>, constraints?: Uint8Array[], extra: Record<string, unknown> = {}) {
  const cat = new ReadOnlyCatalogInterface({ name: "c", schemas: [{ name: "main",
    tables: [{ name: "t", columns, constraints, ...extra }] }] }, new FunctionRegistry());
  const { attach_opaque_data } = await cat.attach("c");
  const [info] = await cat.schemaContentsTables(attach_opaque_data, ["main"]);
  return { info, fields: deserializeSchema(info.columns).fields };
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
    schemaPath: ["main"], table: "child", id: "child.id", kind: "CHECK", columns: [], checkSql: "id >= 0", ...ATTESTED,
  })).definition);
  expect(decodeSqlExpression(check.check_expression!).sql).toBe("id >= 0");
});

test("constraintRecord rejects what DuckDB would refuse at ATTACH", () => {
  const base = { schemaPath: ["main"], table: "t", id: "x", ...ATTESTED };
  expect(() => constraintRecord({ ...base, kind: "NOT_NULL", columns: [["a"], ["b"]] })).toThrow();
  expect(() => constraintRecord({ ...base, kind: "PRIMARY_KEY", columns: [] })).toThrow();
  expect(() => constraintRecord({ ...base, kind: "CHECK", columns: [["a"]], checkSql: "a > 0" })).toThrow();
  expect(() => constraintRecord({ ...base, kind: "UNIQUE", columns: [["a"]], checkSql: "a > 0" })).toThrow();
  expect(() => constraintRecord({ ...base, kind: "FOREIGN_KEY", columns: [["a"]] })).toThrow();
  expect(() => constraintRecord({ ...base, id: "", kind: "UNIQUE", columns: [["a"]] })).toThrow();
  // Attestation, schema shape, nested key paths, duplicates, cross-schema FKs, CHECK grammar.
  expect(() => constraintRecord({ ...base, kind: "UNIQUE", columns: [["a"]], enforcement: "METADATA_ONLY",
    validation: "UNVALIDATED" })).toThrow(/ENFORCED \+ ALL_ROWS/);
  expect(() => constraintRecord({ ...base, schemaPath: ["db", "main"], kind: "UNIQUE", columns: [["a"]] })).toThrow(/one-component/);
  expect(() => constraintRecord({ ...base, kind: "UNIQUE", columns: [["s", "a"]] })).toThrow(/only for NOT_NULL/);
  expect(() => constraintRecord({ ...base, kind: "UNIQUE", columns: [["a"], ["a"]] })).toThrow(/duplicate column path/);
  expect(() => constraintRecord({ ...base, kind: "FOREIGN_KEY", columns: [["a"]],
    references: { schemaPath: ["other"], table: "p", columns: [["id"]], constraintId: "p.pk" } })).toThrow(/same schema/);
  expect(() => constraintRecord({ ...base, kind: "CHECK", columns: [], checkSql: "name <> ''" })).toThrow(/numeric/);
  expect(() => constraintRecord({ ...base, kind: "CHECK", columns: [], checkSql: "a > 0 AND b > 0" })).toThrow(/comparisons/);
  expect(() => constraintRecord({ ...base, kind: "CHECK", columns: [], checkSql: "abs(a) > 0" })).toThrow();
  expect(constraintRecord({ ...base, kind: "CHECK", columns: [], checkSql: "lo < hi" })).toBeInstanceOf(Uint8Array);
  expect(constraintRecord({ ...base, kind: "CHECK", columns: [], checkSql: "(price >= -1.5)" })).toBeInstanceOf(Uint8Array);
  expect(constraintRecord({ ...base, kind: "NOT_NULL", columns: [["s", "a"]] })).toBeInstanceOf(Uint8Array);
});

test("read-only attach negotiates constraint records only for catalogs that serve them", async () => {
  const both = [CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION].sort();
  const served = catalogWith([NN("child", "id")]);
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

test("selectCatalogCapabilities keeps a profile only with its prerequisites", () => {
  const all = catalogCapabilities({ read: [CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION], ddl: [CONSTRAINT_CREATE, CONSTRAINT_MUTATE],
    write: [CONSTRAINT_ENFORCE], transaction: [TRANSACTION_DDL] });
  const full = offer([CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION], { ddl: [CONSTRAINT_CREATE, CONSTRAINT_MUTATE],
    write: [CONSTRAINT_ENFORCE], transaction: [TRANSACTION_DDL] });
  expect(selectCatalogCapabilities(all, full, true)).toEqual(all);
  // No transactions: the DDL transaction profile is dropped.
  expect(selectCatalogCapabilities(all, full, false).transaction).toEqual([]);
  // Create needs enforcement; mutate does not.
  const noEnforce = selectCatalogCapabilities(all, offer([CONSTRAINT_RECORDS_READ], { ddl: [CONSTRAINT_CREATE, CONSTRAINT_MUTATE] }));
  expect(noEnforce).toEqual(catalogCapabilities({ read: [CONSTRAINT_RECORDS_READ], ddl: [CONSTRAINT_MUTATE] }));
  // Nothing without records read.
  expect(selectCatalogCapabilities(all, offer([ROW_SCALAR_EXPRESSION], { ddl: [CONSTRAINT_MUTATE], write: [CONSTRAINT_ENFORCE] })))
    .toEqual(catalogCapabilities());
  expect(selectCatalogCapabilities(all, null)).toEqual(catalogCapabilities());
});

test("served TableInfo carries the records unchanged", async () => {
  const record = NN("child", "id");
  const cat = catalogWith([record]);
  const { attach_opaque_data } = await cat.attach("c", {}, null, null, offer([CONSTRAINT_RECORDS_READ]));
  const child = (await cat.schemaContentsTables(attach_opaque_data, ["main"])).find((t) => t.name === "child")!;
  expect(child.constraints).toEqual([record]);
});

test("legacy descriptor constraint fields name the replacement before any other check", async () => {
  // A non-nullable field plus a legacy field: the legacy error wins.
  const cat = new ReadOnlyCatalogInterface({ name: "c", schemas: [{ name: "main", tables: [
    { name: "t", columns: schema([field("id", int64(), false)]), primaryKey: [["id"]],
      constraints: [NN("t", "missing")] }] }] }, new FunctionRegistry());
  const { attach_opaque_data } = await cat.attach("c");
  expect(() => cat.schemaContentsTables(attach_opaque_data, ["main"])).toThrow(/constraintRecord\(\)/);
});

test("readConstraintRecord inverts constraintRecord", () => {
  const spec = {
    schemaPath: ["main"], table: "child", id: "child.parent", kind: "FOREIGN_KEY" as const, columns: [["parent_id"]],
    references: { schemaPath: ["main"], table: "parent", columns: [["id"]], constraintId: "parent.pk" },
    ...ATTESTED,
  };
  expect(readConstraintRecord(constraintRecord(spec))).toEqual({ ...spec, declaredName: null, checkSql: null,
    references: { ...spec.references, constraintTable: "parent", constraintSchemaPath: ["main"], match: "SIMPLE",
      onUpdate: "NO_ACTION", onDelete: "NO_ACTION" } });
  const check = readConstraintRecord(constraintRecord({ schemaPath: ["main"], table: "t", id: "t.c", kind: "CHECK",
    columns: [], checkSql: "a > 0", declaredName: "positive", ...ATTESTED }));
  expect(check).toMatchObject({ kind: "CHECK", columns: [], checkSql: "a > 0", declaredName: "positive", references: null });
});

test("served nullability follows the records: unnamed fields are relaxed, named fields made non-nullable", async () => {
  // Non-nullable without a record is served nullable (top level and nested).
  let served = await serve(schema([field("a", utf8(), false), field("s", struct([field("x", utf8(), false)]), false)]));
  expect(served.fields[0].nullable).toBe(true);
  expect(served.fields[1].nullable).toBe(true);
  expect((served.fields[1].type as any).children[0].nullable).toBe(true);
  // A record on a nullable field serves it non-nullable; PRIMARY_KEY stands in for NOT_NULL.
  served = await serve(schema([field("a", utf8(), true), field("b", int64(), true)]), [NN("t", "a"), PK("t", "b")]);
  expect(served.fields.map((f) => f.nullable)).toEqual([false, false]);
  // Nested NOT_NULL descends through ordinary STRUCTs; the parent stays as named.
  served = await serve(schema([field("s", struct([field("x", utf8(), true), field("y", utf8(), false)]), true)]), [NN("t", "s", "x")]);
  const kids = (served.fields[0].type as any).children;
  expect([served.fields[0].nullable, kids[0].nullable, kids[1].nullable]).toEqual([true, false, true]);
  // Agreement leaves the original bytes untouched.
  const agreed = schema([field("a", utf8(), false)]);
  const { info } = await serve(agreed, [NN("t", "a")]);
  expect(deserializeSchema(info.columns).fields[0].nullable).toBe(false);
});

test("the row-id column is never rewritten nor nameable", async () => {
  const rowId = field("rowid", int64(), false, new Map([["is_row_id", ""]]));
  const served = await serve(schema([rowId, field("a", utf8(), false)]));
  expect(served.fields.map((f) => f.nullable)).toEqual([false, true]);
  await expect(serve(schema([rowId, field("a", utf8(), true)]), [NN("t", "rowid")])).rejects.toThrow(/row-id/);
});

test("function-backed table output schemas are relaxed too; a failed bind lists the table bare", async () => {
  const make = (fail: boolean) => defineTableFunction({
    name: fail ? "broken_scan" : "scan",
    args: {},
    onBind: () => { if (fail) throw new Error("needs credentials"); return { outputSchema: schema([field("id", int64(), false)]) }; },
    initialState: () => ({}),
    process: (_p: any, _s: any, out: any) => out.finish(),
  });
  const cat = new ReadOnlyCatalogInterface({ name: "c", schemas: [{ name: "main", tables: [
    { name: "ok", function: make(false) },
    { name: "bad", function: make(true), columnComments: { id: "the id" }, generatedColumns: { g: "id + 1" },
      constraints: [NN("bad", "id")] },
  ] }] }, new FunctionRegistry());
  const { attach_opaque_data } = await cat.attach("c");
  const [ok, bad] = await cat.schemaContentsTables(attach_opaque_data, ["main"]);
  expect(deserializeSchema(ok.columns).fields[0].nullable).toBe(true);
  expect(deserializeSchema(bad.columns).fields).toEqual([]);
  expect(bad.constraints).toEqual([]);
});

test("unknown columnComments/generatedColumns keys name the table", async () => {
  await expect(serve(schema([field("a", utf8(), true)]), undefined, { columnComments: { nope: "x" } }))
    .rejects.toThrow(/Table 't': columnComments names unknown column nope/);
  await expect(serve(schema([field("a", utf8(), true)]), undefined, { generatedColumns: { nope: "a" } }))
    .rejects.toThrow(/Table 't': generatedColumns names unknown column nope/);
});

test("ATTACH-time record checks name the table, constraint and path", async () => {
  const cols = schema([field("a", int64(), true), field("b", int64(), true), field("s", utf8(), true)]);
  const raw = (over: { ref?: any; kind?: string; paths?: string[][]; label?: string | null; fk?: any; target?: any }) =>
    encodeASD(ConstraintInfoSchema, {
      ref: encodeASD(CatalogObjectRefSchema, { kind: "constraint", schema_path: ["main"], name: "x", owner_table: "t", ...over.ref }),
      definition: encodeASD(ConstraintDefinitionSchema, { kind: over.kind ?? "UNIQUE", column_paths: over.paths ?? [["a"]],
        declared_name: over.label ?? null, check_expression: null, foreign_key: over.fk ?? null }),
      enforcement: "ENFORCED", validation: "ALL_ROWS", referenced_constraint: over.target ?? null,
    });
  const fails = async (constraints: Uint8Array[], pattern: RegExp) =>
    await expect(serve(cols, constraints)).rejects.toThrow(pattern);
  await fails([raw({ ref: { owner_table: "other" } })], /Table 't': constraint 'x': belongs to/);
  await fails([raw({ ref: { schema_path: ["other"] } })], /belongs to/);
  await fails([raw({}), raw({ paths: [["b"]] })], /duplicate constraint id/);
  await fails([raw({ label: "K" }), raw({ ref: { name: "y" }, paths: [["b"]], label: "k" })], /duplicate declared name/);
  await fails([PK("t", "a"), constraintRecord({ schemaPath: ["main"], table: "t", id: "t.pk2", kind: "PRIMARY_KEY",
    columns: [["b"]], ...ATTESTED })], /only one PRIMARY KEY/);
  await fails([raw({ paths: [["missing"]] })], /column path missing names a missing/);
  await fails([NN("t", "s", "x")], /must traverse ordinary STRUCT/);
  const fk = (action: string) => raw({ kind: "FOREIGN_KEY", fk: encodeASD(ForeignKeyDefinitionSchema, {
    referenced_table: encodeASD(CatalogObjectRefSchema, { kind: "table", schema_path: ["main"], name: "t", owner_table: null }),
    referenced_column_paths: [["b"]], match: "SIMPLE", on_update: action, on_delete: "NO_ACTION" }),
    target: encodeASD(CatalogObjectRefSchema, { kind: "constraint", schema_path: ["main"], name: "t.u", owner_table: "t" }) });
  const unique = constraintRecord({ schemaPath: ["main"], table: "t", id: "t.u", kind: "UNIQUE", columns: [["b"]], ...ATTESTED });
  await fails([fk("CASCADE"), unique], /referential action CASCADE/);
  expect((await serve(cols, [fk("RESTRICT"), unique])).info.constraints.length).toBe(2);
  await fails([fk("NO_ACTION")], /referenced constraint 't.u' is not a PRIMARY_KEY or UNIQUE record/);
  // CHECK operands must be signed numeric, non-generated columns.
  const check = (sql: string) => constraintRecord({ schemaPath: ["main"], table: "t", id: "t.c", kind: "CHECK",
    columns: [], checkSql: sql, ...ATTESTED });
  expect((await serve(cols, [check("A >= 0")])).info.constraints.length).toBe(1);
  await fails([check("s > 0")], /signed numeric columns; s/);
  await fails([check("zz > 0")], /missing or row-id column zz/);
});

test("foreign keys must reference a served key of a table in the same schema", async () => {
  const fk = constraintRecord({ schemaPath: ["main"], table: "child", id: "child.fk", kind: "FOREIGN_KEY",
    columns: [["parent_id"]], references: { schemaPath: ["main"], table: "parent", columns: [["id"]], constraintId: "parent.pk" },
    ...ATTESTED });
  const ok = catalogWith([fk]);
  const { attach_opaque_data } = await ok.attach("c");
  expect((await ok.schemaContentsTables(attach_opaque_data, ["main"])).length).toBe(2);
  const missingTarget = new ReadOnlyCatalogInterface({ name: "c", schemas: [{ name: "main", tables: [
    { name: "child", columns: schema([field("parent_id", int64(), true)]), constraints: [fk] }] }] }, new FunctionRegistry());
  await expect(missingTarget.schemaContentsTables(attach_opaque_data, ["main"])).rejects.toThrow(/does not serve/);
});

test("generated columns: legacy metadata translated, DuckDB's type and grammar rules applied", async () => {
  const legacy = await serve(schema([field("a", int64(), true),
    field("g", int64(), false, new Map([["generated_expression", "a + 1"]]))]));
  expect(legacy.fields[1].metadata.get("vgi.column.generation.v1")).toBe("virtual");
  expect(legacy.fields[1].metadata.has("generated_expression")).toBe(false);
  expect(legacy.fields[1].nullable).toBe(true);
  await expect(serve(schema([field("a", int64(), true), field("g", float64(), true)]), undefined,
    { generatedColumns: { g: "a + 1" } })).rejects.toThrow(/boolean, signed integer or utf8/);
  expect(() => generatedExpressionMetadata("now()", ["main"])).toThrow(/unsupported scalar function/);
  expect(() => generatedExpressionMetadata("a + 1.5", ["main"])).toThrow(/literal type DECIMAL/);
  await expect(serve(schema([field("a", int64(), true), field("g", int64(), true)]), [NN("t", "g")],
    { generatedColumns: { g: "a + 1" } })).rejects.toThrow(/generated column/);
});

test("defaults are canonical where DuckDB reads them and legacy otherwise", async () => {
  const served = await serve(schema([field("s", utf8(), true), field("n", int64(), true),
    field("ts", timestampMicros(), true), field("price", float64(), true)]), undefined,
    { defaults: { s: "x", n: 5, ts: "2020-01-01", price: 9.99 } });
  expect(served.fields[0].metadata.has(DEFAULT_EXPRESSION_KEY)).toBe(true);
  expect(served.fields[0].metadata.has("default")).toBe(false);
  expect(served.fields[1].metadata.get("default")).toBe("5");
  expect(served.fields[2].metadata.get("default")).toBe("'2020-01-01'");
  expect(served.fields[3].metadata.get("default")).toBe("9.99");
});

test("servedTableSchema is a no-op for a schema that already agrees", () => {
  const s = schema([field("a", utf8(), false)]);
  expect(servedTableSchema("t", ["main"], s, [NN("t", "a")])).toBe(s);
});
