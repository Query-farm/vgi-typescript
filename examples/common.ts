// Shared function and catalog registration for example workers.
// Used by both the IPC worker (worker.ts) and HTTP worker (http-worker.ts).

import { Schema, Field, Int32, Int64, Float64, Bool, Utf8, Struct } from "@query-farm/apache-arrow";
import { List } from "@query-farm/apache-arrow";
import {
  type CatalogDescriptor, type MacroDescriptor, Arguments,
  serializeBatch, batchFromColumns,
  ReadOnlyCatalogInterface, TableInfo,
  type AttachId, type TransactionId,
} from "../src/index.js";
import { serializeSchema } from "../src/util/arrow.js";
import { argumentSpecsToSchema } from "../src/arguments/argument-spec.js";
import { scalarFunctions } from "./scalar.js";
import {
  tableFunctions, resolveVersion, getVersionedSchema,
  resolveVersionedConstraintsVersion, getVersionedConstraintsSchema,
} from "./table.js";
import { tableInOutFunctions } from "./table_in_out.js";

// Find functions for table-backed catalog entries
const sequenceFunction = tableFunctions.find((f) => f.meta.name === "sequence");
const rowIdSequenceFunction = tableFunctions.find((f) => f.meta.name === "rowid_sequence");

export const allFunctions = [
  ...scalarFunctions,
  ...tableFunctions,
  ...tableInOutFunctions,
];

// Serialize parameter default values for vgi_clamp: lo=0, hi=100
const clampDefaultsSchema = new Schema([
  new Field("lo", new Int64(), true),
  new Field("hi", new Int64(), true),
]);
const clampDefaults = serializeBatch(
  batchFromColumns({ lo: [0n], hi: [100n] }, clampDefaultsSchema)
);

export const catalog: CatalogDescriptor = {
  name: "example",
  defaultSchema: "main",
  comment: "Example VGI catalog for testing",
  tags: { source: "vgi-example-worker", version: "1" },
  secretTypes: [
    {
      name: "vgi_example",
      description: "Example VGI secret for testing",
      schema: new Schema([
        new Field("secret_string", new Utf8(), true, new Map([["redact", "true"]])),
        new Field("api_key", new Utf8(), true, new Map([["redact", "true"]])),
        new Field("port", new Int32(), true),
        new Field("use_ssl", new Bool(), true),
        new Field("timeout", new Float64(), true),
      ]),
    },
  ],
  settings: [
    { name: "vgi_verbose_mode", description: "Enable verbose output", type: new Bool(), defaultValue: false },
    { name: "greeting", description: "Custom greeting message", type: new Utf8(), defaultValue: "Hello" },
    { name: "multiplier", description: "Value multiplier", type: new Int64(), defaultValue: 1 },
    { name: "threshold", description: "Filter threshold", type: new Int64(), defaultValue: 0 },
    {
      name: "config",
      description: "Sequence configuration struct",
      type: new Struct([
        new Field("start", new Int64(), true),
        new Field("step", new Int64(), true),
        new Field("label", new Utf8(), true),
      ]),
    },
  ],
  schemas: [
    {
      name: "main",
      comment: "Example functions for testing VGI",
      functions: allFunctions,
      views: [
        {
          name: "first_ten",
          definition: "SELECT * FROM sequence(10)",
          comment: "First 10 integers",
        },
        {
          name: "even_numbers",
          definition: "SELECT * FROM sequence(100) WHERE n % 2 = 0",
          comment: "Even numbers from 0 to 98",
        },
      ],
      macros: [
        {
          name: "vgi_multiply",
          macroType: "scalar",
          parameters: ["x", "y"],
          definition: "x * y",
          comment: "Multiply two values",
        },
        {
          name: "vgi_clamp",
          macroType: "scalar",
          parameters: ["val", "lo", "hi"],
          parameterDefaultValues: clampDefaults,
          definition: "GREATEST(lo, LEAST(hi, val))",
          comment: "Clamp a value between lo and hi (defaults: 0..100)",
        },
        {
          name: "vgi_range_table",
          macroType: "table",
          parameters: ["n"],
          definition: "SELECT * FROM range(n)",
          comment: "Table macro returning range of values",
        },
      ],
    },
    {
      name: "data",
      comment: "Example tables backed by functions",
      tables: [
        {
          name: "large_sequence",
          function: sequenceFunction,
          arguments: new Arguments([1_000_000]),
          comment: "A large sequence of integers from 0 to 1,000,000",
        },
        {
          name: "versioned_data",
          columns: new Schema([
            new Field("id", new Int64(), true),
            new Field("score", new Float64(), true),
          ]),
          supportsTimeTravel: true,
          comment: "Versioned data table demonstrating time travel with schema evolution",
        },
        {
          name: "numbers",
          function: sequenceFunction,
          arguments: new Arguments([100]),
          comment: "First 100 integers (demonstrates explicit columns)",
        },
        {
          name: "colors",
          columns: new Schema([
            new Field("id", new Int64(), true),
            new Field("color", new Utf8(), true),
            new Field("hex_code", new Utf8(), true),
          ]),
          comment: "Colors table with ENUM-derived statistics",
        },
        {
          name: "funny_numbers",
          columns: new Schema([
            new Field("n", new Int64(), true),
          ]),
          comment: "123456 integers; stats served by the sequence function, not the table",
        },
        {
          name: "volatile_numbers",
          columns: new Schema([
            new Field("value", new Int64(), true),
          ]),
          comment: "Numbers with volatile stats (TTL=0, always re-fetched)",
        },
        {
          name: "generated_sequence",
          columns: new Schema([
            new Field("n", new Int64(), true),
            new Field("doubled", new Int64(), true),
            new Field("label", new Utf8(), true),
          ]),
          function: sequenceFunction,
          arguments: new Arguments([10]),
          generatedColumns: {
            doubled: "n * 2",
            label: "'item_' || CAST(n AS VARCHAR)",
          },
          comment: "Table with generated columns backed by sequence(10)",
        },
        // Row ID position tests (int64 row_id)
        {
          name: "rowid_first",
          columns: new Schema([
            new Field("row_id", new Int64(), true, new Map([["is_row_id", ""]])),
            new Field("name", new Utf8(), true),
            new Field("value", new Utf8(), true),
          ]),
          function: rowIdSequenceFunction,
          arguments: new Arguments([20], new Map<string, any>([["layout", "first"], ["row_id_type", "int64"]])),
          comment: "Table with row_id at column index 0",
        },
        {
          name: "rowid_middle",
          columns: new Schema([
            new Field("name", new Utf8(), true),
            new Field("row_id", new Int64(), true, new Map([["is_row_id", ""]])),
            new Field("value", new Utf8(), true),
          ]),
          function: rowIdSequenceFunction,
          arguments: new Arguments([20], new Map<string, any>([["layout", "middle"], ["row_id_type", "int64"]])),
          comment: "Table with row_id at column index 1",
        },
        {
          name: "rowid_last",
          columns: new Schema([
            new Field("name", new Utf8(), true),
            new Field("value", new Utf8(), true),
            new Field("row_id", new Int64(), true, new Map([["is_row_id", ""]])),
          ]),
          function: rowIdSequenceFunction,
          arguments: new Arguments([20], new Map<string, any>([["layout", "last"], ["row_id_type", "int64"]])),
          comment: "Table with row_id at column index 2",
        },
        // Row ID type tests
        {
          name: "rowid_string",
          columns: new Schema([
            new Field("row_id", new Utf8(), true, new Map([["is_row_id", ""]])),
            new Field("value", new Int64(), true),
          ]),
          function: rowIdSequenceFunction,
          arguments: new Arguments([20], new Map<string, any>([["layout", "first"], ["row_id_type", "string"]])),
          comment: "Table with string row_id",
        },
        {
          name: "rowid_struct",
          columns: new Schema([
            new Field("row_id", new Struct([new Field("a", new Int64(), true), new Field("b", new Utf8(), true)]), true, new Map([["is_row_id", ""]])),
            new Field("value", new Utf8(), true),
          ]),
          function: rowIdSequenceFunction,
          arguments: new Arguments([20], new Map<string, any>([["layout", "first"], ["row_id_type", "struct"]])),
          comment: "Table with struct row_id",
        },
        // ----- Constraint example tables -----
        {
          name: "departments",
          columns: new Schema([
            new Field("id", new Int64(), true),
            new Field("name", new Utf8(), true),
            new Field("budget", new Float64(), true),
          ]),
          primaryKey: [["id"]],
          notNull: ["id", "name"],
          unique: [["name"]],
          check: ["budget >= 0"],
          defaults: { budget: 0 },
          comment: "Department reference table",
        },
        {
          name: "products",
          columns: new Schema([
            new Field("id", new Int64(), true),
            new Field("name", new Utf8(), true),
            new Field("quantity", new Int64(), true),
            new Field("price", new Float64(), true),
          ]),
          notNull: ["id"],
          primaryKey: [["id"]],
          defaults: { quantity: 0, name: "unknown", price: 9.99 },
          columnComments: {
            id: "Unique product identifier",
            name: "Product display name",
            price: "Unit price in USD",
          },
          comment: "Product table with column defaults",
        },
        {
          name: "employees",
          columns: new Schema([
            new Field("id", new Int64(), true),
            new Field("name", new Utf8(), true),
            new Field("email", new Utf8(), true),
            new Field("department_id", new Int64(), true),
          ]),
          primaryKey: [["id"]],
          notNull: ["id", "name", "email"],
          unique: [["email"]],
          foreignKey: [{
            columns: ["department_id"],
            referencedTable: "departments",
            referencedColumns: ["id"],
          }],
          comment: "Employee table with FK to departments",
        },
        {
          name: "projects",
          columns: new Schema([
            new Field("department_id", new Int64(), true),
            new Field("project_code", new Utf8(), true),
            new Field("title", new Utf8(), true),
          ]),
          primaryKey: [["department_id", "project_code"]],
          notNull: ["department_id", "project_code", "title"],
          foreignKey: [{
            columns: ["department_id"],
            referencedTable: "departments",
            referencedColumns: ["id"],
          }],
          comment: "Projects with composite PK and FK to departments",
        },
        {
          name: "versioned_constraints",
          columns: new Schema([
            new Field("id", new Int64(), true),
            new Field("name", new Utf8(), true),
            new Field("email", new Utf8(), true),
            new Field("department_id", new Int64(), true),
          ]),
          supportsTimeTravel: true,
          notNull: ["id", "name"],
          primaryKey: [["id"]],
          unique: [["email"]],
          foreignKey: [{
            columns: ["department_id"],
            referencedTable: "departments",
            referencedColumns: ["id"],
          }],
          comment: "Table with constraints that evolve across versions",
        },
      ],
      views: [
        {
          name: "small_numbers",
          definition: "SELECT n AS value FROM numbers WHERE n < 10",
          comment: "Numbers less than 10",
        },
      ],
    },
  ],
};

// ============================================================================
// ExampleCatalog: custom catalog with time travel routing
// ============================================================================

const versionedDataScanFunction = tableFunctions.find((f) => f.meta.name === "versioned_data_scan");

// FK serialization schema (matches Python wire format)
const FK_BATCH_SCHEMA = new Schema([
  new Field("fk_columns", new List(new Field("item", new Utf8(), true)), false),
  new Field("pk_columns", new List(new Field("item", new Utf8(), true)), false),
  new Field("referenced_table", new Utf8(), false),
  new Field("referenced_schema", new Utf8(), false),
]);

function buildFkBytes(fkCols: string[], pkCols: string[], refTable: string, refSchema: string): Uint8Array {
  return serializeBatch(batchFromColumns({
    fk_columns: [fkCols],
    pk_columns: [pkCols],
    referenced_table: [refTable],
    referenced_schema: [refSchema],
  }, FK_BATCH_SCHEMA));
}

function buildVersionArgBytes(version: number): Uint8Array {
  const argBatchSchema = new Schema([new Field("version", new Int64(), true)]);
  return serializeBatch(batchFromColumns({ version: [BigInt(version)] }, argBatchSchema));
}

export function createExampleCatalog(base: ReadOnlyCatalogInterface): ReadOnlyCatalogInterface {
  const origTableGet = base.tableGet.bind(base);
  const origTableScanFunctionGet = base.tableScanFunctionGet.bind(base);

  base.tableGet = (
    attachId: AttachId,
    schemaName: string,
    name: string,
    atUnit?: string,
    atValue?: string,
    transactionId?: TransactionId,
  ): TableInfo | null => {
    if (schemaName.toLowerCase() === "data" && name.toLowerCase() === "versioned_data" && atUnit) {
      const version = resolveVersion(atUnit, atValue);
      const cols = getVersionedSchema(version);
      return new TableInfo(
        name, schemaName, serializeSchema(cols),
        [], [], [], [], [],
        "Versioned data table demonstrating time travel with schema evolution", {},
      );
    }
    if (schemaName.toLowerCase() === "data" && name.toLowerCase() === "versioned_constraints" && atUnit) {
      const version = resolveVersionedConstraintsVersion(atUnit, atValue);
      const cols = getVersionedConstraintsSchema(version);
      // Constraints evolve: V1: NOT NULL(id), V2: +NOT NULL(name),PK(id),UNIQUE(email), V3: +FK
      const colNames = cols.fields.map((f) => f.name);
      const notNull: number[] = [];
      const pk: number[][] = [];
      const unique: number[][] = [];
      const fk: Uint8Array[] = [];
      if (version >= 1) notNull.push(colNames.indexOf("id"));
      if (version >= 2) {
        notNull.push(colNames.indexOf("name"));
        pk.push([colNames.indexOf("id")]);
        unique.push([colNames.indexOf("email")]);
      }
      if (version >= 3) {
        fk.push(buildFkBytes(["department_id"], ["id"], "departments", schemaName));
      }
      return new TableInfo(
        name, schemaName, serializeSchema(cols),
        notNull, unique, [], pk, fk,
        "Table with constraints that evolve across versions", {},
      );
    }
    return origTableGet(attachId, schemaName, name, atUnit, atValue, transactionId);
  };

  base.tableScanFunctionGet = (
    attachId: AttachId,
    schemaName: string,
    name: string,
    atUnit?: string,
    atValue?: string,
    transactionId?: TransactionId,
  ): any => {
    // Time-travel tables
    if (schemaName.toLowerCase() === "data" && name.toLowerCase() === "versioned_data") {
      const version = resolveVersion(atUnit, atValue);
      return { function_name: "versioned_data_scan", arguments: buildVersionArgBytes(version), required_extensions: [] };
    }
    if (schemaName.toLowerCase() === "data" && name.toLowerCase() === "versioned_constraints") {
      const version = resolveVersionedConstraintsVersion(atUnit, atValue);
      return { function_name: "versioned_constraints_scan", arguments: buildVersionArgBytes(version), required_extensions: [] };
    }

    // Static constraint tables
    const staticTables: Record<string, string> = {
      departments: "departments_scan",
      employees: "employees_scan",
      products: "products_scan",
      projects: "projects_scan",
    };
    if (schemaName.toLowerCase() === "data" && name.toLowerCase() in staticTables) {
      return { function_name: staticTables[name.toLowerCase()], arguments: serializeBatch(batchFromColumns({}, new Schema([]))), required_extensions: [] };
    }

    return origTableScanFunctionGet(attachId, schemaName, name, atUnit, atValue, transactionId);
  };

  return base;
}
