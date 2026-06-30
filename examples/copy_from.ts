// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Fixture `COPY ... FROM` format reader for VGI integration tests.
//
// `exampleLinesCopyFrom` registers the SQL format `example_lines` — a toy
// delimited-text reader. It exercises the full COPY-FROM path plus the option
// machinery: a defaulted option (`delimiter`), a BIGINT option with a range
// constraint (`skip_rows`), a required option (`null_string`), and an
// enum/`choices` option (`on_error`).
//
// TypeScript port of vgi-python/vgi/_test_fixtures/copy_from.py
// (ExampleLinesCopyFromFunction).
//
// Usage:
//   CREATE TABLE t (a INTEGER, b VARCHAR);
//   COPY t FROM '/path/data.txt' (FORMAT 'acme.example_lines', null_string 'NA');

import { readFileSync } from "node:fs";
import {
  defineCopyFromFunction,
  batchFromColumns,
  secretForScopeOfType,
  utf8,
  int64,
  isInt,
  isFloat,
  isBool,
  type VgiFunction,
  type VgiDataType,
} from "../src/index.js";

/** Options for the `example_lines` COPY format. */
interface ExampleLinesArgs {
  null_string: string;
  delimiter: string;
  skip_rows: number;
  on_error: string;
}

// Coerce a raw string cell to the target column's Arrow type, matching what
// DuckDB will INSERT (no cast happens between the scan and the INSERT).
function coerceCell(raw: string, type: VgiDataType): unknown {
  if (isInt(type)) {
    return (type as any).bitWidth === 64 ? BigInt(raw) : Number.parseInt(raw, 10);
  }
  if (isFloat(type)) return Number.parseFloat(raw);
  if (isBool(type)) {
    const l = raw.trim().toLowerCase();
    return l === "true" || l === "t" || l === "1";
  }
  return raw; // VARCHAR and everything else: leave as string
}

export const exampleLinesCopyFrom: VgiFunction = defineCopyFromFunction<ExampleLinesArgs>({
  name: "example_lines_copy_reader",
  format: "example_lines",
  description: "Read a delimited text file into the COPY target table",
  comment: "Toy delimited-text reader for tests",
  categories: ["copy", "test"],
  tags: { category: "copy_from", stability: "test" },
  options: {
    null_string: { type: utf8(), doc: "Token parsed as SQL NULL" }, // required (no default)
    delimiter: { type: utf8(), default: ",", doc: "Field separator" },
    skip_rows: { type: int64(), default: 0, doc: "Leading lines to skip before data" },
    on_error: {
      type: utf8(),
      default: "fail",
      choices: ["fail", "skip"],
      doc: "Behavior on a row whose column count does not match the target",
    },
  },
  read: ({ path, options, expectedSchema, out }) => {
    let lines = readFileSync(path, "utf-8").split(/\r?\n/);
    lines = lines.slice(options.skip_rows);

    const ncols = expectedSchema.fields.length;
    const rows: string[][] = [];
    for (const line of lines) {
      if (line === "") continue;
      const cells = line.split(options.delimiter);
      if (cells.length !== ncols) {
        if (options.on_error === "skip") continue;
        throw new Error(
          `example_lines: row has ${cells.length} fields, expected ${ncols}: ${JSON.stringify(line)}`,
        );
      }
      rows.push(cells);
    }

    // Column-major: NULL where the cell equals null_string, else coerced to the
    // target column type.
    const columns: Record<string, unknown[]> = {};
    expectedSchema.fields.forEach((field, idx) => {
      columns[field.name] = rows.map((r) =>
        r[idx] === options.null_string ? null : coerceCell(r[idx], field.type),
      );
    });

    out.emit(batchFromColumns(columns, expectedSchema));
  },
});

// ============================================================================
// secret_lines_in — forwards a CREATE SECRET credential to the reader.
//
// Exercises the COPY-FROM secret-bind hook (`onSecrets`): it requests the
// `secret_type` secret scoped to the source path during bind, and `read` emits a
// single VARCHAR row holding the resolved secret's api_key (or NONE) — so a test
// can assert the caller's secret reached the reader. TypeScript port of
// vgi-python's SecretLinesCopyFromFunction.
// ============================================================================

interface SecretLinesInArgs {
  secret_type: string;
}

export const secretLinesCopyFrom: VgiFunction = defineCopyFromFunction<SecretLinesInArgs>({
  name: "secret_lines_reader",
  format: "secret_lines_in",
  description: "Emit the resolved secret's api_key as a single VARCHAR row",
  comment: "Reader that forwards a CREATE SECRET credential (test fixture)",
  categories: ["copy", "test", "secret"],
  tags: { category: "copy_from", stability: "test" },
  options: {
    secret_type: {
      type: utf8(),
      default: "vgi_example",
      doc: "Secret type to fetch, scoped by the source path",
    },
  },
  // Request the source-scoped secret; the framework's two-phase secret bind
  // resolves it and surfaces it on processParams.secrets at read time.
  onSecrets: ({ options, path }) => [
    { secretType: options.secret_type, scope: path },
  ],
  read: ({ path, options, expectedSchema, processParams, out }) => {
    const secret = secretForScopeOfType(processParams.secrets, path, options.secret_type);
    const apiKey = secret && secret.api_key != null ? String(secret.api_key) : "NONE";
    const firstCol = expectedSchema.fields[0]?.name ?? "k";
    out.emit(batchFromColumns({ [firstCol]: [apiKey] }, expectedSchema));
  },
});

export const copyFromFunctions: VgiFunction[] = [exampleLinesCopyFrom, secretLinesCopyFrom];
