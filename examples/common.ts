// Shared function and catalog registration for example workers.
// Used by both the IPC worker (worker.ts) and HTTP worker (http-worker.ts).

import { Schema, Field, Int32, Int64, Float64, Bool, Utf8, Struct } from "@query-farm/apache-arrow";
import {
  type CatalogDescriptor, type MacroDescriptor, Arguments,
  serializeBatch, batchFromColumns,
  ReadOnlyCatalogInterface, TableInfo,
  type AttachId, type TransactionId,
} from "../src/index.js";
import { serializeSchema } from "../src/util/arrow.js";
import { argumentSpecsToSchema } from "../src/arguments/argument-spec.js";
import { scalarFunctions } from "./scalar.js";
import { tableFunctions, resolveVersion, getVersionedSchema } from "./table.js";
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
          comment: "First 100 integers",
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

export function createExampleCatalog(base: ReadOnlyCatalogInterface): ReadOnlyCatalogInterface {
  // Override tableGet and tableScanFunctionGet for time travel support
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
        name,
        schemaName,
        serializeSchema(cols),
        [],
        [],
        [],
        "Versioned data table demonstrating time travel with schema evolution",
        {},
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
    // Handle versioned_data with time travel
    if (schemaName.toLowerCase() === "data" && name.toLowerCase() === "versioned_data") {
      const version = resolveVersion(atUnit, atValue);
      // Build arguments batch with the version
      const func = versionedDataScanFunction!;
      const argSchema = argumentSpecsToSchema(func.argumentSpecs);
      const args = new Arguments([version]);
      const argFields = [new Field("version", new Int64(), true)];
      const argBatchSchema = new Schema(argFields);
      const argBytes = serializeBatch(batchFromColumns({ version: [BigInt(version)] }, argBatchSchema));
      return {
        function_name: "versioned_data_scan",
        arguments: argBytes,
        required_extensions: [],
      };
    }

    return origTableScanFunctionGet(attachId, schemaName, name, atUnit, atValue, transactionId);
  };

  return base;
}
