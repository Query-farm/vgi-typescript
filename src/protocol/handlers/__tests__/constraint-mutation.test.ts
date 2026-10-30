import { expect, test } from "bun:test";
import { buildVgiProtocol } from "../../dispatch.js";
import { wrapRequest } from "../../../client/protocol.js";
import { batchFromColumns } from "../../../util/arrow/index.js";
import { binary, deserializeSchema, field, int64, schema, serializeSchema, utf8 } from "../../../arrow/index.js";
import { decodeASD, encodeASD } from "../../../codec/asd.js";
import {
  CatalogAttachResultSchema, CatalogObjectRefSchema, ClientCapabilitiesSchema, ConstraintsApplyRequestSchema,
  TableCreateRequestSchema,
} from "../../../generated/vgi-protocol-schemas.js";
import {
  buildConstraintChange, decodeCatalogCapabilities, decodeConstraintSnapshot, encodeCatalogCapabilities,
  encodeConstraintChange, encodeConstraintDefinition, type ConstraintSnapshot, type ConstraintsApplyRequest,
} from "../../../generated/vgi-protocol-types.js";
import { FunctionRegistry } from "../../../functions/registry.js";
import { CatalogInterface, type CatalogAttachResult, type TableInfo } from "../../../catalog/interface.js";
import {
  constraintCapabilities, constraintRecord, readConstraintRecord, CONSTRAINT_CREATE, CONSTRAINT_ENFORCE,
  CONSTRAINT_MUTATE, CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION,
} from "../../../catalog/constraints.js";
import { catalogTableHandlers } from "../catalog/table.js";
import * as clientEntry from "../../../client-entry.js";

const ATTACH = new Uint8Array([7, 7, 7]);

/** One table `t(a)` whose NOT NULL state is changed through the deprecated hooks. */
class LegacyNotNullCatalog extends CatalogInterface {
  notNull = false;
  revision = 1;
  catalogs() { return ["c"]; }
  attach(): CatalogAttachResult {
    return { attach_opaque_data: ATTACH, supports_transactions: false, supports_time_travel: false,
      catalog_version_frozen: false, catalog_version: 1, attach_opaque_data_required: true, default_schema: "main" } as any;
  }
  detach() {}
  version() { return 1; }
  schemas() { return [{ path: ["main"], comment: null, tags: {} }]; }
  override tableGet(_a: Uint8Array, schemaPath: string[], name: string): TableInfo | null {
    if (name !== "t") return null;
    return {
      comment: null, tags: {}, name, schema_path: schemaPath,
      columns: serializeSchema(schema([field("a", int64(), true), field("b", utf8(), false)])),
      constraints: this.notNull ? [constraintRecord({ schemaPath: ["main"], table: "t", id: "t.a.nn", kind: "NOT_NULL",
        columns: [["a"]], enforcement: "ENFORCED", validation: "ALL_ROWS" })] : [],
      definition_revision: new Uint8Array([this.revision]),
      write_result_modes: {}, supports_column_statistics: false, scan_function: null, insert_function: null,
      update_function: null, delete_function: null, cardinality_estimate: null, cardinality_max: null, required_filters: [],
    } as unknown as TableInfo;
  }
  override tableNotNullSet(_a: Uint8Array, _s: string[], _n: string, path: string[]) {
    expect(path).toEqual(["a"]);
    this.notNull = true;
    this.revision++;
  }
  override tableNotNullDrop(_a: Uint8Array, _s: string[], _n: string, path: string[]) {
    expect(path).toEqual(["a"]);
    this.notNull = false;
    this.revision++;
  }
}

const tableRef = encodeASD(CatalogObjectRefSchema, { kind: "table", schema_path: ["main"], name: "t", owner_table: null });

function applyRequest(expected: number, change: Uint8Array): Uint8Array {
  return encodeASD(ConstraintsApplyRequestSchema, { attach_opaque_data: ATTACH, table: tableRef,
    expected_revision: new Uint8Array([expected]), changes: [change], transaction_opaque_data: null });
}

const addNotNull = encodeConstraintChange(buildConstraintChange({ operation: "ADD",
  definition: encodeConstraintDefinition({ kind: "NOT_NULL", column_paths: [["a"]], declared_name: null,
    check_expression: null, foreign_key: null }), enforcement: "ENFORCED", validation: "ALL_ROWS" }));
const dropNotNull = encodeConstraintChange(buildConstraintChange({ operation: "DROP", cascade: false,
  ref: encodeASD(CatalogObjectRefSchema, { kind: "constraint", schema_path: ["main"], name: "t.a.nn", owner_table: "t" }) }));

test("ALTER SET/DROP NOT NULL route to the deprecated hooks through catalog_table_constraints_apply", async () => {
  const cat = new LegacyNotNullCatalog();
  expect(await cat.catalogCapabilities(ATTACH)).toEqual(constraintCapabilities({ mutate: true, enforce: true }));
  const handlers = catalogTableHandlers(() => cat) as any;
  const added = decodeConstraintSnapshot((await handlers.catalogTableConstraintsApply({ request: applyRequest(1, addNotNull) }, {})).result);
  expect([...added.definition_revision]).toEqual([2]);
  expect(added.constraints.map((c) => readConstraintRecord(c).id)).toEqual(["t.a.nn"]);
  // A stale revision is refused before any hook runs.
  await expect(handlers.catalogTableConstraintsApply({ request: applyRequest(1, dropNotNull) }, {})).rejects.toThrow(/definition_revision/);
  const dropped = decodeConstraintSnapshot((await handlers.catalogTableConstraintsApply({ request: applyRequest(2, dropNotNull) }, {})).result);
  expect(dropped.constraints).toEqual([]);
  expect(cat.notNull).toBe(false);
});

test("tableConstraintsApply is the hook a catalog implements; the snapshot must rotate the revision", async () => {
  class Custom extends LegacyNotNullCatalog {
    seen: ConstraintsApplyRequest | null = null;
    rotate = true;
    override async tableConstraintsApply(request: ConstraintsApplyRequest): Promise<ConstraintSnapshot> {
      this.seen = request;
      return { table: request.table, definition_revision: new Uint8Array([this.rotate ? 9 : 1]), constraints: [] };
    }
  }
  const cat = new Custom();
  const handlers = catalogTableHandlers(() => cat) as any;
  await handlers.catalogTableConstraintsApply({ request: applyRequest(1, addNotNull) }, {});
  expect([...cat.seen!.attach_opaque_data]).toEqual([...ATTACH]);
  expect(cat.seen!.changes.length).toBe(1);
  cat.rotate = false;
  await expect(handlers.catalogTableConstraintsApply({ request: applyRequest(1, addNotNull) }, {})).rejects.toThrow(/new, nonempty/);
});

test("a catalog without constraint hooks refuses constraints_apply", async () => {
  class Plain extends LegacyNotNullCatalog {}
  // Remove the overrides by restoring the base hooks.
  (Plain.prototype as any).tableNotNullSet = CatalogInterface.prototype.tableNotNullSet;
  (Plain.prototype as any).tableNotNullDrop = CatalogInterface.prototype.tableNotNullDrop;
  const cat = new Plain();
  expect(await cat.catalogCapabilities(ATTACH)).toBeNull();
  const handlers = catalogTableHandlers(() => cat) as any;
  await expect(handlers.catalogTableConstraintsApply({ request: applyRequest(1, addNotNull) }, {})).rejects.toThrow(/constraints_apply/);
});

test("catalog_table_create passes constraint definitions through when the catalog enforces them", async () => {
  const seen: Uint8Array[][] = [];
  class Enforcing extends LegacyNotNullCatalog {
    override catalogCapabilities() { return constraintCapabilities({ create: true }); }
    override tableCreate(_a: Uint8Array, _s: string[], _n: string, _c: Uint8Array, _o: string, defs?: Uint8Array[]) {
      seen.push(defs ?? []);
    }
  }
  class NonEnforcing extends Enforcing {
    override catalogCapabilities() { return constraintCapabilities({ mutate: true }); }
  }
  const definition = encodeConstraintDefinition({ kind: "NOT_NULL", column_paths: [["a"]], declared_name: null,
    check_expression: null, foreign_key: null });
  const request = (columns: ReturnType<typeof schema>, defs: Uint8Array[]) => ({ request: encodeASD(TableCreateRequestSchema, {
    attach_opaque_data: ATTACH, schema_path: ["main"], name: "n", columns: serializeSchema(columns), on_conflict: "ERROR",
    constraint_definitions: defs, transaction_opaque_data: null, comment: null, tags: null, options: null, layout: null }) });
  const nonNull = schema([field("a", int64(), false)]);
  await (catalogTableHandlers(() => new Enforcing()) as any).catalogTableCreate(request(nonNull, [definition]), {});
  expect(seen[0].length).toBe(1);
  // A column made non-nullable without a definition, and a catalog that does not enforce, are refused.
  await expect((catalogTableHandlers(() => new Enforcing()) as any).catalogTableCreate(request(nonNull, []), {}))
    .rejects.toThrow(/non-nullable/);
  await expect((catalogTableHandlers(() => new NonEnforcing()) as any).catalogTableCreate(request(nonNull, [definition]), {}))
    .rejects.toThrow(/does not enforce constraints/);
});

test("catalog_attach advertises catalogCapabilities() for catalogs that do not set it themselves", async () => {
  const cat = new LegacyNotNullCatalog();
  const protocol = buildVgiProtocol({ registry: new FunctionRegistry(), catalogInterface: cat });
  const method = ((protocol as any)._methods?.get?.("catalog_attach") ?? (protocol as any).methods?.get?.("catalog_attach"));
  const offer = encodeASD(ClientCapabilitiesSchema, { engine: "duckdb", native_formats: [], catalogs: [], can_stream: true,
    filter_encodings: [], query_pushdown: null, catalog: encodeCatalogCapabilities({
      read: [CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION], ddl: [CONSTRAINT_CREATE, CONSTRAINT_MUTATE],
      write: [CONSTRAINT_ENFORCE], transaction: [] }) });
  const request = wrapRequest(batchFromColumns(
    { name: ["c"], options: [null], data_version_spec: [null], implementation_version: [null], client_capabilities: [offer] },
    schema([field("name", utf8(), false), field("options", binary(), true), field("data_version_spec", utf8(), true),
      field("implementation_version", utf8(), true), field("client_capabilities", binary(), true)]),
  ));
  const result = decodeASD<CatalogAttachResult>(CatalogAttachResultSchema, (await method.handler(request, {} as any)).result);
  const selected = decodeCatalogCapabilities(result.selected_capabilities!);
  expect(selected.ddl).toEqual([CONSTRAINT_MUTATE]);
  expect(selected.write).toEqual([CONSTRAINT_ENFORCE]);
  expect(selected.read).toEqual([CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION].sort());
});

test("served table_get relaxes unnamed non-nullable fields for handler-served catalogs", async () => {
  const cat = new LegacyNotNullCatalog();
  const handlers = catalogTableHandlers(() => cat) as any;
  const wire = { attach_opaque_data: ATTACH, schema_path: ["main"], name: "t", at_unit: null, at_value: null, transaction_opaque_data: null };
  const decodeTable = async () => {
    const res = await handlers.catalogTableGet(wire, {});
    const items = decodeASD<any>((await import("../../../generated/vgi-protocol-schemas.js")).CatalogTableGetResultSchema, res.result).items;
    const info = decodeASD<any>((await import("../../../generated/vgi-protocol-schemas.js")).TableInfoSchema, items[0]);
    return deserializeSchema(info.columns).fields.map((f) => f.nullable);
  };
  expect(await decodeTable()).toEqual([true, true]);
  cat.notNull = true;
  expect(await decodeTable()).toEqual([false, true]);
});

test("the client entry exports the record codecs and sends a default capability offer", () => {
  for (const name of ["readConstraintRecord", "encodeCatalogCapabilities", "decodeCatalogCapabilities", "sqlExpression",
    "encodeSqlExpression", "decodeSqlExpression", "generatedExpressionMetadata", "canonicalIndex", "defaultClientCapabilities"])
    expect(typeof (clientEntry as any)[name]).toBe("function");
  const offer = clientEntry.defaultClientCapabilities();
  expect(clientEntry.decodeCatalogCapabilities(offer.catalog!).read).toEqual([CONSTRAINT_RECORDS_READ, ROW_SCALAR_EXPRESSION].sort());
});
