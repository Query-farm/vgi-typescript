import { encodeASD } from "../../codec/asd.js";
import { ClientCapabilitiesSchema } from "../../generated/vgi-protocol-schemas.js";
import { expect, test } from "bun:test";
import { fixtureConstraint, validateFixtureConstraint, versionedFixtureConstraints } from "../../../examples/canonical_constraints.js";
import { IMMUTABLE_CONSTRAINT_ROWS, IMMUTABLE_VERSIONED_ROWS } from "../../../examples/immutable_constraint_rows.js";
import { decodeConstraintInfo, decodeConstraintDefinition, decodeCatalogCapabilities, encodeCatalogCapabilities } from "../../generated/vgi-protocol-types.js";
import { createExampleCatalog, catalog, allFunctions } from "../../../examples/common.js";
import { ReadOnlyCatalogInterface, CompositeCatalogInterface, FunctionRegistry } from "../../index.js";

test("immutable scan rows are the authority for every supported fixture constraint", () => {
  const valid = decodeConstraintInfo(fixtureConstraint("departments", "id", "PRIMARY_KEY", [["id"]]));
  expect(valid.enforcement).toBe("ENFORCED");
  expect(valid.validation).toBe("ALL_ROWS");
  expect(Object.isFrozen(IMMUTABLE_CONSTRAINT_ROWS.departments!.id)).toBe(true);
  expect(() => validateFixtureConstraint({ id: [null] }, "NOT_NULL", [["id"]])).toThrow();
  expect(() => validateFixtureConstraint({ id: [1n, 1n] }, "PRIMARY_KEY", [["id"]])).toThrow();
  expect(() => validateFixtureConstraint({ a: [1n, 1n], b: ["x", "x"] }, "UNIQUE", [["a"], ["b"]])).toThrow();
  expect(() => validateFixtureConstraint({ id: [null, null] }, "UNIQUE", [["id"]])).not.toThrow();
  expect(() => validateFixtureConstraint({ budget: [-1] }, "CHECK", [], { checkSql: "budget >= 0" })).toThrow();
  const foreign = { targetTable: "departments", targetPaths: [["id"]], targetKey: "fixture.departments.primary" };
  expect(() => validateFixtureConstraint({ id: [99n] }, "FOREIGN_KEY", [["id"]], foreign)).toThrow();
  expect(() => validateFixtureConstraint({ id: [1n] }, "FOREIGN_KEY", [["id"]], foreign, { id: [1n, 1n] })).toThrow();
  expect(() => validateFixtureConstraint({ id: [1n] }, "NOT_NULL", [["missing"]])).toThrow();
});

test("historical facts are validated against version-owned rows and columns", () => {
  for (const version of [1, 2, 3]) {
    const constraints = versionedFixtureConstraints(version).map(decodeConstraintInfo);
    expect(constraints.length).toBe([1, 4, 5][version - 1]);
    for (const info of constraints) {
      expect(info.validation).toBe("ALL_ROWS");
      for (const path of decodeConstraintDefinition(info.definition).column_paths)
        expect(path[0]! in IMMUTABLE_VERSIONED_ROWS[version]!).toBe(true);
    }
  }
});

test("example selects only offered read profiles through composite attach", async () => {
  const registry = new FunctionRegistry();
  for (const fn of allFunctions) registry.register(fn);
  const example = createExampleCatalog(new ReadOnlyCatalogInterface(catalog, registry));
  const composite = new CompositeCatalogInterface([example]);
  for (const read of [[], ["vgi.expression.duckdb.scalar.v1"], ["vgi.constraint.records.read.v1"],
    ["vgi.constraint.records.read.v1", "vgi.expression.duckdb.scalar.v1"]]) {
    const capabilities = encodeASD(ClientCapabilitiesSchema, { engine: "test", native_formats: [], catalogs: [], can_stream: true,
      filter_encodings: [], query_pushdown: null,
      catalog: encodeCatalogCapabilities({ read, ddl: ["unknown"], write: ["vgi.constraint.enforce.v1"], transaction: [] }) });
    const result = await composite.attach("example", {}, null, null, capabilities);
    const selected = decodeCatalogCapabilities(result.selected_capabilities!);
    expect(selected.read).toEqual(read.includes("vgi.constraint.records.read.v1") ? read : []);
    expect(selected.write).toEqual([]);
    expect(selected.ddl).toEqual([]);
    expect(selected.transaction).toEqual([]);
  }
  const result = await composite.attach("example");
  expect(decodeCatalogCapabilities(result.selected_capabilities!).read).toEqual([]);
});
