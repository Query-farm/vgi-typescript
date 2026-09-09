// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Round-trip coverage for `schema_path`, the nullable path field in protocol 2.0.0
// added to ScanFunctionResult and ScanBranch.
//
// wire-schema-completeness.test.ts already pins that the builders write EVERY
// column the generated schema declares; what it cannot see is whether the value
// that comes back is the one that went in. That matters more here than for a
// typical field: `schema_path` exists to disambiguate one function name
// declared in several schemas, so a value that survives serialization as the
// wrong string — or as null — reroutes a scan to the other schema's
// implementation rather than failing. Both halves are covered: a set value must
// come back intact, and an unset one must come back null (a pre-1.5.0 peer, a
// natively-delegated function, or a FORMAT branch, none of which have a
// VGI-side schema to report).

import { describe, test, expect } from "bun:test";
import {
  batchFromColumns,
  serializeBatch,
  deserializeBatch,
  batchToScalarDict,
  schema as makeSchema,
  field as makeField,
  utf8,
} from "../arrow/index.js";
import {
  encodeScanFunctionResult,
  decodeScanFunctionResult,
  buildScanBranchesResult,
  singleBranchResult,
} from "../catalog/interface.js";

/** A serialized inner arguments batch, as ScanBranch/ScanFunctionResult carry. */
const ARGUMENTS_BYTES = serializeBatch(
  batchFromColumns({ arg_0: ["s3://bucket/x.parquet"] }, makeSchema([makeField("arg_0", utf8(), true)])),
);

/** Read one serialized branch back as a flat column dict. */
function branchRow(bytes: Uint8Array): Record<string, unknown> {
  return batchToScalarDict(deserializeBatch(bytes));
}

describe("ScanFunctionResult.schema_path", () => {
  test("round-trips when set", () => {
    const bytes = encodeScanFunctionResult("rowid_sequence", ARGUMENTS_BYTES, [], ["main"]);
    const decoded = decodeScanFunctionResult(batchToScalarDict(deserializeBatch(bytes)));
    expect(decoded.schemaPath).toEqual(["main"]);
    expect(decoded.functionName).toBe("rowid_sequence");
  });

  test("is null when the caller doesn't set it", () => {
    // read_parquet is a native DuckDB function delegated straight through — it
    // has no VGI-side schema, which is the permanent case that keeps the field
    // optional rather than required.
    const bytes = encodeScanFunctionResult("read_parquet", ARGUMENTS_BYTES);
    const decoded = decodeScanFunctionResult(batchToScalarDict(deserializeBatch(bytes)));
    expect(decoded.schemaPath).toBeNull();
  });

  test("decodes as null from a pre-1.5.0 peer, whose payload has no such column", () => {
    // Not "column present but null" — the column is absent from the wire
    // schema entirely, which is what an older worker actually sends.
    const decoded = decodeScanFunctionResult({
      function_name: "sequence",
      arguments: ARGUMENTS_BYTES,
      required_extensions: [],
    });
    expect(decoded.schemaPath).toBeNull();
  });
});

describe("ScanBranch.schema_path", () => {
  test("a function branch round-trips its schema", () => {
    const result = buildScanBranchesResult([{ functionName: "sequence", schemaPath: ["main"] }]);
    expect(branchRow(result.branches[0]!).schema_path).toEqual(["main"]);
  });

  test("a catalog-table branch leaves it null", () => {
    // A catalog-table branch names a source table, not a function; its schema
    // travels in source_schema_path, a different (and older) field.
    const result = buildScanBranchesResult([
      {
        functionName: "",
        sourceCatalog: "lakehouse",
        sourceSchemaPath: ["bronze"],
        sourceTable: "orders",
      },
    ]);
    const row = branchRow(result.branches[0]!);
    expect(row.schema_path).toBeNull();
    expect(row.source_schema_path).toEqual(["bronze"]);
  });

  test("singleBranchResult carries the legacy result's schema through", () => {
    // The shim synthesises a branch from a legacy scan-function result; it has
    // no schema of its own to contribute, so dropping the one it was handed
    // would silently downgrade a 1.5.0 worker to the client's old heuristic.
    const result = singleBranchResult({
      function_name: "sequence",
      arguments: ARGUMENTS_BYTES,
      schema_path: ["main"],
    });
    expect(branchRow(result.branches[0]!).schema_path).toEqual(["main"]);
  });

  test("singleBranchResult leaves it null when the legacy result had none", () => {
    const result = singleBranchResult({
      function_name: "read_parquet",
      arguments: ARGUMENTS_BYTES,
    });
    expect(branchRow(result.branches[0]!).schema_path).toBeNull();
  });
});
