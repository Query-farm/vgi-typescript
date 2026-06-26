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

export const copyFromFunctions: VgiFunction[] = [exampleLinesCopyFrom];
