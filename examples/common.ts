// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Shared function and catalog registration for example workers.
// Used by both the IPC worker (worker.ts) and HTTP worker (http-worker.ts).

import { Schema, Field, Int32, Int64, Float64, Float32, Bool, Utf8, Struct } from "@query-farm/apache-arrow";
import { List } from "@query-farm/apache-arrow";
import {
  type CatalogDescriptor, type MacroDescriptor, Arguments,
  serializeBatch, batchFromColumns,
  ReadOnlyCatalogInterface, TableInfo,
  type AttachOpaqueData, type TransactionOpaqueData,
  buildScanBranchesResult, type ScanBranchInput,
  functionStorage,
} from "../src/index.js";
import { serializeSchema } from "../src/util/arrow/index.js";
import { argumentSpecsToSchema } from "../src/arguments/argument-spec.js";
import { scalarFunctions } from "./scalar.js";
import {
  tableFunctions, resolveVersion, getVersionedSchema,
  resolveVersionedConstraintsVersion, getVersionedConstraintsSchema,
  resolveTtVersion, TT_SCHEMA,
  RFF_SIMPLE_SCHEMA, RFF_STRUCT_SCHEMA, RFF_NESTED_SCHEMA, RFF_MULTI_SCHEMA, RFF_NONE_SCHEMA, RFF_ROWID_SCHEMA,
} from "./table.js";
import { tableInOutFunctions } from "./table_in_out.js";
import { tableBufferingFunctions } from "./table_buffering.js";
import { aggregateFunctions } from "./aggregate.js";
import { partitionTableFunctions } from "./table_partition.js";
import { copyFromFunctions } from "./copy_from.js";
import { copyToFunctions } from "./copy_to.js";

// Find functions for table-backed catalog entries
const sequenceFunction = tableFunctions.find((f) => f.meta.name === "sequence");
const rowIdSequenceFunction = tableFunctions.find((f) => f.meta.name === "rowid_sequence");
const lateMaterializationFunction = tableFunctions.find((f) => f.meta.name === "late_materialization");
const tenThousandFunction = tableFunctions.find((f) => f.meta.name === "ten_thousand");
const secretDemoFunction = tableFunctions.find((f) => f.meta.name === "secret_demo");
const ttPushdownScanFunction = tableFunctions.find((f) => f.meta.name === "tt_pushdown_scan");

// Precompute stats for tables whose data is known at worker startup. The
// statisticsFromDuckDB helper spins up an in-process DuckDB, builds the demo
// dataset, and extracts typed ColumnStatistics so DuckDB's optimizer can do
// plan-time filter elimination on these tables.
import { statisticsFromDuckDB } from "./duckdb-stats.js";
import { Binary } from "@query-farm/apache-arrow";

const NUMBERS_STATS = await statisticsFromDuckDB(
  "numbers",
  async (c) => {
    await c.run("CREATE TABLE numbers AS SELECT i AS value FROM range(0, 100) t(i)");
  },
);

// Colors: ENUM column + plain VARCHAR hex_code. Proves the stats path
// unwraps dictionary-encoded values back to strings.
const COLORS_STATS = await statisticsFromDuckDB(
  "colors",
  async (c) => {
    await c.run("CREATE TYPE color AS ENUM ('red', 'green', 'blue')");
    await c.run(
      "CREATE TABLE colors AS " +
        "SELECT id::BIGINT AS id, color, hex_code FROM (VALUES " +
        "(1, 'red'::color, '#FF0000'), (2, 'green'::color, '#00FF00'), (3, 'blue'::color, '#0000FF')" +
        ") AS t(id, color, hex_code)",
    );
  },
);

// Geometry statistics via ST_Extent (requires the spatial extension).
const GEO_POINTS_SCHEMA = new Schema([
  new Field("id", new Int64(), true),
  new Field("geom", new Binary(), true, new Map<string, string>([
    ["ARROW:extension:name", "geoarrow.wkb"],
    ["ARROW:extension:metadata", "{}"],
  ])),
]);
const GEO_POINTS_STATS = await statisticsFromDuckDB(
  "geo_points",
  async (c) => {
    await c.run(
      "CREATE TABLE geo_points AS " +
        "SELECT row_number() OVER () AS id, " +
        "ST_Point(x::DOUBLE, y::DOUBLE)::GEOMETRY AS geom " +
        "FROM range(5) t1(x), range(5) t2(y)",
    );
  },
  { loadExtensions: ["spatial"] },
);

// colors_scan and geo_points_scan: minimal generator functions that emit the
// fixed dataset for these catalog tables. The optimizer-side statistics above
// come from the catalog, not the scan function, but the scan still needs to
// return correct rows at query time.
const colorsScanFunction = tableFunctions.find((f) => f.meta.name === "colors_scan");

export const allFunctions = [
  ...scalarFunctions,
  ...tableFunctions,
  ...partitionTableFunctions,
  ...tableInOutFunctions,
  ...tableBufferingFunctions,
  ...aggregateFunctions,
  ...copyFromFunctions,
  ...copyToFunctions,
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
  sourceUrl: "https://github.com/Query-farm/vgi-typescript",
  tags: { source: "vgi-fixture-worker", version: "1" },
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
    { name: "scale_factor", description: "Float scale factor", type: new Float64(), defaultValue: 1.0 },
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
      tags: {
        "vgi.description_llm": "Example functions schema for the VGI fixture worker.",
        "vgi.description_md": "Example functions for testing VGI.",
      },
      functions: allFunctions,
      views: [
        {
          name: "first_ten",
          definition: "SELECT * FROM sequence(10)",
          comment: "First 10 integers",
          columnComments: { n: "Sequence index 0..9" },
          tags: { layer: "demo", origin: "sequence" },
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
          parameterDocs: { x: "First factor", y: "Second factor" },
          definition: "x * y",
          comment: "Multiply two values",
        },
        {
          name: "vgi_clamp",
          macroType: "scalar",
          parameters: ["val", "lo", "hi"],
          parameterDefaultValues: clampDefaults,
          parameterDocs: {
            val: "Value to clamp",
            lo: "Lower bound (inclusive)",
            hi: "Upper bound (inclusive)",
          },
          definition: "GREATEST(lo, LEAST(hi, val))",
          comment: "Clamp a value between lo and hi (defaults: 0..100)",
        },
        {
          name: "vgi_range_table",
          macroType: "table",
          parameters: ["n"],
          parameterDocs: { n: "Number of rows to generate" },
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
        // Function-backed table over the no-arg ten_thousand function. Used
        // by integration/table/inlined_scan_function.test to verify the C++
        // extension reads the inlined scan_function from TableInfo and
        // skips catalog_table_scan_function_get.
        {
          name: "ten_thousand_table",
          function: tenThousandFunction,
          comment: "Function-backed table over the no-arg ten_thousand function",
        },
        // Function-backed table whose backing function (secret_demo) resolves a
        // secret. The catalog schema-derivation does a single, non-retrying bind,
        // and secret_demo declares a static requiredSecrets so it returns its full
        // 3-column key/value/arrow_type schema in that one bind. Backs
        // integration/secret/secret_function_backed_table.test. The backing
        // function name (secret_demo) intentionally differs from the table name.
        {
          name: "secret_demo_table",
          function: secretDemoFunction,
          comment: "Function-backed table over the secret-using secret_demo function",
        },
        // Function-backed table with inlined cardinality. Used by
        // integration/table/inlined_cardinality.test to verify the C++
        // extension uses Table.cardinality_estimate / cardinality_max from
        // TableInfo and skips the per-bind table_function_cardinality RPC.
        {
          name: "cardinality_inlined_table",
          function: tenThousandFunction,
          inlinedCardinality: { estimate: 10000n, max: 10000n },
          comment: "Function-backed table with inlined cardinality (10000 rows)",
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
        // Time travel + filter pushdown together. tt_pushdown_fn is
        // function-backed (reads AT at init); tt_pushdown_cols is columns-based
        // (AT -> version arg via tableScanFunctionGet). Back time_travel_pushdown.test.
        {
          name: "tt_pushdown_fn",
          function: ttPushdownScanFunction,
          supportsTimeTravel: true,
          comment: "Function-backed: prunes by filter AND time-travels (AT read at init).",
        },
        {
          name: "tt_pushdown_cols",
          columns: TT_SCHEMA,
          supportsTimeTravel: true,
          comment: "Columns-based: prunes by filter AND time-travels (AT → version arg).",
        },
        {
          name: "numbers",
          columns: new Schema([
            new Field("value", new Int64(), true),
          ]),
          function: sequenceFunction,
          arguments: new Arguments([100]),
          statistics: NUMBERS_STATS,
          statisticsCacheMaxAgeSeconds: 3600,
          comment: "First 100 integers (demonstrates explicit columns)",
        },
        {
          name: "colors",
          columns: new Schema([
            new Field("id", new Int64(), true),
            new Field("color", new Utf8(), true),
            new Field("hex_code", new Utf8(), true),
          ]),
          function: colorsScanFunction,
          statistics: COLORS_STATS,
          statisticsCacheMaxAgeSeconds: 3600,
          comment: "Colors table with ENUM-derived statistics",
        },
        {
          name: "geo_points",
          columns: GEO_POINTS_SCHEMA,
          statistics: GEO_POINTS_STATS,
          statisticsCacheMaxAgeSeconds: 3600,
          comment: "5x5 grid of points with spatial statistics",
        },
        {
          name: "funny_numbers",
          columns: new Schema([
            new Field("n", new Int64(), true),
          ]),
          function: sequenceFunction,
          arguments: new Arguments([123456]),
          comment: "123456 integers; stats served by the sequence function, not the table",
        },
        {
          name: "volatile_numbers",
          columns: new Schema([
            new Field("value", new Int64(), true),
          ]),
          function: sequenceFunction,
          arguments: new Arguments([100]),
          statistics: {
            value: {
              columnName: "value",
              arrowType: new Int64(),
              min: 0n,
              max: 99n,
              hasNull: false,
              hasNotNull: true,
              distinctCount: 100n,
              containsUnicode: null,
              maxStringLength: null,
            },
          },
          statisticsCacheMaxAgeSeconds: 0,
          comment: "Numbers with volatile stats (TTL=0, always re-fetched)",
        },
        // ----- Multi-branch scan fixtures -----
        // These tables declare explicit `columns` only (no `function`), so
        // they appear as catalog tables but route through the
        // tableScanBranchesGet override below for their scan plans.
        {
          name: "multi_branch_numbers",
          columns: new Schema([new Field("n", new Int64(), true)]),
          comment: "Multi-branch: UNION of sequence(50) + sequence(50) — used by multi_branch_scan.test",
        },
        {
          name: "multi_branch_filtered_numbers",
          columns: new Schema([new Field("n", new Int64(), true)]),
          comment: "Multi-branch with complementary branch_filters — exercises pruning",
        },
        {
          name: "multi_branch_hetero",
          columns: new Schema([new Field("n", new Int64(), true)]),
          comment: "Multi-branch: sequence(50) + read_parquet — used by multi_branch_heterogeneous.test",
        },
        {
          name: "multi_branch_recon",
          columns: new Schema([
            new Field("a", new Int64(), true),
            new Field("b", new Int64(), true),
          ]),
          comment: "Multi-branch: column reconciliation — used by multi_branch_reconciliation.test",
        },
        {
          name: "multi_branch_nopushdown",
          columns: new Schema([new Field("n", new Int64(), true)]),
          comment: "Multi-branch: VGI + read_csv — used by multi_branch_pushdown_incapable.test",
        },
        {
          name: "multi_branch_empty",
          columns: new Schema([new Field("n", new Int64(), true)]),
          comment: "Multi-branch: empty branches list — used by multi_branch_empty_branches.test",
        },
        {
          name: "multi_branch_two_writable",
          columns: new Schema([new Field("n", new Int64(), true)]),
          comment: "Multi-branch with two writable=True arms — used by multi_branch_two_writable.test",
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
        // ----- Late-materialization tables (rowid + scrambled ord) -----
        // Backed by the late_materialization scan function, which advertises
        // lateMaterialization. row_id is the row index (unique/deterministic/
        // snapshot-stable); ord is scrambled so a Top-N on ord yields scattered
        // survivor rowids. pushed echoes the rowid filter the worker received.
        // 1000 rows so LIMIT k << count makes the rewrite a real win and
        // LIMIT 200 exceeds dynamic_or_filter_threshold (50). See
        // late_materialization.test.
        {
          name: "late_mat",
          columns: new Schema([
            new Field("row_id", new Int64(), true, new Map([["is_row_id", ""]])),
            new Field("ord", new Int64(), true),
            new Field("payload", new Utf8(), true),
            new Field("pushed", new Utf8(), true),
          ]),
          function: lateMaterializationFunction,
          arguments: new Arguments([1000], new Map<string, any>()),
          comment: "Late-materialization table (1000 rows, unique rowid)",
        },
        {
          name: "late_mat_dup",
          columns: new Schema([
            new Field("row_id", new Int64(), true, new Map([["is_row_id", ""]])),
            new Field("ord", new Int64(), true),
            new Field("payload", new Utf8(), true),
            new Field("pushed", new Utf8(), true),
          ]),
          function: lateMaterializationFunction,
          arguments: new Arguments([1000], new Map<string, any>([["dup_row_id", true]])),
          comment: "Late-materialization table with deliberately non-unique rowid (contract violation)",
        },
        {
          name: "late_mat_nulls",
          columns: new Schema([
            new Field("row_id", new Int64(), true, new Map([["is_row_id", ""]])),
            new Field("ord", new Int64(), true),
            new Field("payload", new Utf8(), true),
            new Field("pushed", new Utf8(), true),
          ]),
          function: lateMaterializationFunction,
          arguments: new Arguments([1000], new Map<string, any>([["null_ord_stride", 7]])),
          comment: "Late-materialization table with NULLs in the ord column",
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
          statistics: {
            id: {
              columnName: "id", arrowType: new Int64(),
              min: 1n, max: 10n, hasNull: false, hasNotNull: true,
              distinctCount: 10n, containsUnicode: null, maxStringLength: null,
            },
            name: {
              columnName: "name", arrowType: new Utf8(),
              min: "Accounting", max: "Sales", hasNull: false, hasNotNull: true,
              distinctCount: 10n, containsUnicode: false, maxStringLength: 20n,
            },
            budget: {
              columnName: "budget", arrowType: new Float64(),
              min: 50000.0, max: 500000.0, hasNull: false, hasNotNull: true,
              distinctCount: 10n, containsUnicode: null, maxStringLength: null,
            },
          },
          statisticsCacheMaxAgeSeconds: 3600,
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
          statistics: {
            id: {
              columnName: "id", arrowType: new Int64(),
              min: 1n, max: 100n, hasNull: false, hasNotNull: true,
              distinctCount: 100n, containsUnicode: null, maxStringLength: null,
            },
            name: {
              columnName: "name", arrowType: new Utf8(),
              min: "Anvil", max: "Zebra Tape", hasNull: false, hasNotNull: true,
              distinctCount: 100n, containsUnicode: false, maxStringLength: 30n,
            },
            quantity: {
              columnName: "quantity", arrowType: new Int64(),
              min: 0n, max: 10000n, hasNull: true, hasNotNull: true,
              distinctCount: 100n, containsUnicode: null, maxStringLength: null,
            },
            price: {
              columnName: "price", arrowType: new Float64(),
              min: 0.99, max: 999.99, hasNull: false, hasNotNull: true,
              distinctCount: 100n, containsUnicode: null, maxStringLength: null,
            },
          },
          statisticsCacheMaxAgeSeconds: 3600,
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
        // ----- required_field_filter_paths fixtures -----
        // Exercised by ~/Development/vgi/test/sql/integration/table/
        // required_field_filter_paths_*.test to verify the C++ optimizer
        // extension that enforces Table.requiredFieldFilterPaths.
        {
          name: "rff_simple",
          columns: RFF_SIMPLE_SCHEMA,
          requiredFieldFilterPaths: ["a"],
          comment: "rff_simple — requires a filter referencing column 'a'.",
        },
        {
          name: "rff_struct",
          columns: RFF_STRUCT_SCHEMA,
          requiredFieldFilterPaths: ["s.a", "s.b"],
          comment: "rff_struct — requires filters on both struct subfields s.a and s.b.",
        },
        {
          name: "rff_nested",
          columns: RFF_NESTED_SCHEMA,
          requiredFieldFilterPaths: ["wrapper.mid.leaf"],
          comment: "rff_nested — requires a filter on the 3-deep nested path wrapper.mid.leaf.",
        },
        {
          name: "rff_multi",
          columns: RFF_MULTI_SCHEMA,
          requiredFieldFilterPaths: ["top", "s.a"],
          comment: "rff_multi — mixed top-level + struct subfield requirements.",
        },
        {
          name: "rff_none",
          columns: RFF_NONE_SCHEMA,
          comment: "rff_none — control table with no required_field_filter_paths (opt-out fast path).",
        },
        {
          name: "rff_rowid",
          columns: RFF_ROWID_SCHEMA,
          requiredFieldFilterPaths: ["bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax"],
          comment: "rff_rowid — row_id virtual column + required bbox.* filters.",
        },
        // Native read_parquet delegation (scan dispatched in tableScanFunctionGet
        // below). The *.test writes the parquet/hive files to /tmp first.
        {
          name: "rff_parquet",
          columns: new Schema([
            new Field("bbox", new Struct([
              new Field("xmin", new Float32(), true),
              new Field("ymin", new Float32(), true),
              new Field("xmax", new Float32(), true),
              new Field("ymax", new Float32(), true),
            ]), true),
            new Field("other", new Int64(), true),
          ]),
          requiredFieldFilterPaths: ["bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax"],
          comment: "rff_parquet — native read_parquet delegation with bbox.* required filters.",
        },
        {
          name: "rff_hive",
          columns: new Schema([
            new Field("id", new Utf8(), true),
            new Field("bbox", new Struct([
              new Field("xmin", new Float32(), true),
              new Field("ymin", new Float32(), true),
              new Field("xmax", new Float32(), true),
              new Field("ymax", new Float32(), true),
            ]), true),
            new Field("name", new Utf8(), true),
            new Field("num", new Int64(), true),
            new Field("theme", new Utf8(), true),
            new Field("type", new Utf8(), true),
          ]),
          requiredFieldFilterPaths: ["bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax"],
          comment: "rff_hive — native read_parquet over Hive glob with bbox.* required filters.",
        },
        {
          name: "rff_hive_mixed",
          columns: new Schema([
            new Field("id", new Utf8(), true),
            new Field("bbox", new Struct([
              new Field("xmin", new Float32(), true),
              new Field("ymin", new Float32(), true),
              new Field("xmax", new Float32(), true),
              new Field("ymax", new Float32(), true),
            ]), true),
            new Field("name", new Utf8(), true),
            new Field("num", new Int64(), true),
            new Field("theme", new Utf8(), true),
            new Field("type", new Utf8(), true),
          ]),
          requiredFieldFilterPaths: ["id", "bbox.xmin", "bbox.xmax", "bbox.ymin", "bbox.ymax"],
          comment: "rff_hive_mixed — native read_parquet, top-level 'id' + bbox.* required filters.",
        },
        {
          name: "filter_echo_table",
          columns: new Schema([
            new Field("n", new Int64(), true),
            new Field("s", new Utf8(), true),
            new Field("pushed_filters", new Utf8(), true),
          ]),
          comment: "Catalog table echoing pushed-down filters (filter-pushdown-through-view tests).",
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
          definition: "SELECT value FROM numbers WHERE value < 10",
          comment: "Numbers less than 10",
          columnComments: { value: "Single-digit value 0..9" },
        },
      ],
      indexes: [
        {
          name: "idx_numbers_value",
          tableName: "numbers",
          expressions: ["value"],
          comment: "Index on numbers.value",
        },
        {
          name: "idx_numbers_value_unique",
          tableName: "numbers",
          expressions: ["value"],
          constraintType: "UNIQUE",
          comment: "Unique index on numbers.value",
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
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    atUnit?: string,
    atValue?: string,
    transactionOpaqueData?: TransactionOpaqueData,
  ): TableInfo | null => {
    if (schemaName.toLowerCase() === "data" && name.toLowerCase() === "versioned_data" && atUnit) {
      const version = resolveVersion(atUnit, atValue);
      const cols = getVersionedSchema(version);
      return {
        comment: "Versioned data table demonstrating time travel with schema evolution",
        tags: {},
        name,
        schema_name: schemaName,
        columns: serializeSchema(cols),
        not_null_constraints: [],
        unique_constraints: [],
        check_constraints: [],
        primary_key_constraints: [],
        foreign_key_constraints: [],
        supports_insert: false,
        supports_update: false,
        supports_delete: false,
        supports_returning: false,
        supports_column_statistics: false,
        scan_function: new Uint8Array(0),
        insert_function: new Uint8Array(0),
        update_function: new Uint8Array(0),
        delete_function: new Uint8Array(0),
        cardinality_estimate: 0,
        cardinality_max: 0,
        required_field_filter_paths: [],
      };
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
      return {
        comment: "Table with constraints that evolve across versions",
        tags: {},
        name,
        schema_name: schemaName,
        columns: serializeSchema(cols),
        not_null_constraints: notNull,
        unique_constraints: unique,
        check_constraints: [],
        primary_key_constraints: pk,
        foreign_key_constraints: fk,
        supports_insert: false,
        supports_update: false,
        supports_delete: false,
        supports_returning: false,
        supports_column_statistics: false,
        scan_function: new Uint8Array(0),
        insert_function: new Uint8Array(0),
        update_function: new Uint8Array(0),
        delete_function: new Uint8Array(0),
        cardinality_estimate: 0,
        cardinality_max: 0,
        required_field_filter_paths: [],
      };
    }
    // Multi-branch tables: accept AT at table_get and pass it through with AT
    // stripped, so the time-travel guard in the read-only base doesn't fire.
    // The C++ side's B2 guard in VgiTableEntry::GetScanFunctionImpl detects
    // branches.size() > 1 and throws BinderException with the documented
    // message before any scan. Mirrors vgi-python's fixture table_get.
    if (
      schemaName.toLowerCase() === "data" &&
      ["multi_branch_numbers", "multi_branch_filtered_numbers"].includes(name.toLowerCase())
    ) {
      return origTableGet(attachOpaqueData, schemaName, name, undefined, undefined, transactionOpaqueData);
    }
    return origTableGet(attachOpaqueData, schemaName, name, atUnit, atValue, transactionOpaqueData);
  };

  base.tableScanFunctionGet = (
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    atUnit?: string,
    atValue?: string,
    transactionOpaqueData?: TransactionOpaqueData,
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
    // Columns-based time-travel + pushdown: resolve AT -> version and pass it as
    // a scan-function argument (the native columns-based AT mechanism).
    if (schemaName.toLowerCase() === "data" && name.toLowerCase() === "tt_pushdown_cols") {
      const version = resolveTtVersion(atUnit, atValue);
      return { function_name: "tt_pushdown_cols_scan", arguments: buildVersionArgBytes(version), required_extensions: [] };
    }

    // rff_parquet — single-file native read_parquet delegation.
    if (schemaName.toLowerCase() === "data" && name.toLowerCase() === "rff_parquet") {
      const args = serializeBatch(batchFromColumns(
        { arg_0: ["/tmp/rff_seg.parquet"] },
        new Schema([new Field("arg_0", new Utf8(), true)]),
      ));
      return { function_name: "read_parquet", arguments: args, required_extensions: [] };
    }
    // rff_hive / rff_hive_mixed — native read_parquet over a Hive glob.
    if (schemaName.toLowerCase() === "data" && ["rff_hive", "rff_hive_mixed"].includes(name.toLowerCase())) {
      const args = serializeBatch(batchFromColumns(
        { arg_0: ["/tmp/rff_hive/*/*/*.parquet"], hive_partitioning: [true] },
        new Schema([new Field("arg_0", new Utf8(), true), new Field("hive_partitioning", new Bool(), true)]),
      ));
      return { function_name: "read_parquet", arguments: args, required_extensions: [] };
    }

    // Static constraint tables
    const staticTables: Record<string, string> = {
      departments: "departments_scan",
      employees: "employees_scan",
      products: "products_scan",
      projects: "projects_scan",
      rff_simple: "rff_simple_scan",
      rff_struct: "rff_struct_scan",
      rff_nested: "rff_nested_scan",
      rff_multi: "rff_multi_scan",
      rff_none: "rff_none_scan",
      rff_rowid: "rff_rowid_scan",
      filter_echo_table: "filter_echo_table_scan",
    };
    if (schemaName.toLowerCase() === "data" && name.toLowerCase() in staticTables) {
      return { function_name: staticTables[name.toLowerCase()], arguments: serializeBatch(batchFromColumns({}, new Schema([]))), required_extensions: [] };
    }

    return origTableScanFunctionGet(attachOpaqueData, schemaName, name, atUnit, atValue, transactionOpaqueData);
  };

  // Multi-branch scan plans for the multi_branch_* fixtures. Falls through to
  // the default-impl shim (one-branch wrap of tableScanFunctionGet) for every
  // other table. Mirrors vgi-python's ExampleCatalog.table_scan_branches_get.
  const origTableScanBranchesGet = base.tableScanBranchesGet.bind(base);
  base.tableScanBranchesGet = (
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    atUnit?: string,
    atValue?: string,
    transactionOpaqueData?: TransactionOpaqueData,
  ): any => {
    validateAtParams(atUnit, atValue);

    const i64 = (value: number | bigint) => ({ value: BigInt(value), type: new Int64() });
    const str = (value: string) => ({ value, type: new Utf8() });

    const seq = (count: number, extra: Partial<ScanBranchInput> = {}): ScanBranchInput => ({
      functionName: "sequence",
      positionalArguments: [i64(count)],
      ...extra,
    });

    if (schemaName.toLowerCase() === "data") {
      switch (name.toLowerCase()) {
        case "multi_branch_numbers":
          return buildScanBranchesResult([seq(50), seq(50)]);
        case "multi_branch_filtered_numbers":
          return buildScanBranchesResult([
            seq(100, { branchFilter: "n < 50" }),
            seq(100, { branchFilter: "n >= 50" }),
          ]);
        case "multi_branch_hetero":
          return buildScanBranchesResult([
            seq(50),
            { functionName: "read_parquet", positionalArguments: [str("/tmp/vgi_hetero_branch.parquet")] },
          ]);
        case "multi_branch_empty":
          return buildScanBranchesResult([]);
        case "multi_branch_two_writable":
          return buildScanBranchesResult([
            seq(10, { writable: true }),
            seq(10, { writable: true }),
          ]);
        case "multi_branch_nopushdown":
          return buildScanBranchesResult([
            seq(50),
            { functionName: "read_csv_auto", positionalArguments: [str("/tmp/vgi_nopushdown_branch.csv")] },
          ]);
        case "multi_branch_recon":
          return buildScanBranchesResult([
            { functionName: "read_parquet", positionalArguments: [str("/tmp/vgi_recon_a_b.parquet")] },
            { functionName: "read_parquet", positionalArguments: [str("/tmp/vgi_recon_b_a.parquet")] },
            { functionName: "read_parquet", positionalArguments: [str("/tmp/vgi_recon_a_only.parquet")] },
          ]);
      }
    }

    return origTableScanBranchesGet(attachOpaqueData, schemaName, name, atUnit, atValue, transactionOpaqueData);
  };

  // Transaction support — required by tx_cached_value (transaction_storage).
  // attach() must advertise supports_transactions=true so the C++ extension
  // populates BindRequest.transaction_opaque_data inside BEGIN/COMMIT blocks.
  const origAttach = base.attach.bind(base);
  base.attach = async (
    name: string,
    options?: Record<string, unknown>,
    dataVersionSpec?: string | null,
    implementationVersion?: string | null,
  ) => {
    const result = await origAttach(name, options, dataVersionSpec, implementationVersion);
    return { ...result, supports_transactions: true };
  };

  // Each BEGIN mints a fresh random transaction token; the scope it implies
  // (used by tx_cached_value as the FunctionStorage state scope) is therefore
  // empty for every new transaction. commit/rollback wipe the scope so a
  // re-used token (should one ever recur) starts clean.
  base.transactionBegin = (): Uint8Array => {
    const tok = new Uint8Array(16);
    crypto.getRandomValues(tok);
    return tok;
  };
  // Commit/rollback are intentionally cheap no-ops. The C++ extension wraps
  // EVERY example-catalog statement in BEGIN/COMMIT once supports_transactions
  // is advertised, and these handlers run on the single-threaded worker event
  // loop that the launcher shares across all parallel unittest processes.
  // Doing synchronous SQLite DELETEs here (executionClear) blocked that loop
  // under -j8 load and produced "VGI catalog operation timed out" failures in
  // filter_echo / column_statistics / constant_columns. Cleanup isn't needed
  // for correctness: transactionBegin mints a fresh random token every time,
  // so a transaction's storage scope is always empty at BEGIN regardless of
  // whether the prior scope was cleared; orphaned rows are reaped by
  // FunctionStorage.cleanupOldEntries.
  base.transactionCommit = (): void => {};
  base.transactionRollback = (): void => {};

  return base;
}

function validateAtParams(atUnit?: string, atValue?: string): void {
  if (Boolean(atUnit) !== Boolean(atValue)) {
    throw new Error("at_unit and at_value must both be provided or both be absent");
  }
}
