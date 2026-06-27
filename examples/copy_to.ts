// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Fixture `COPY ... TO` format writer for VGI integration tests.
//
// `exampleLinesCopyTo` registers the SQL format `example_lines_out` — a toy
// delimited-text writer, the symmetric counterpart of the `example_lines`
// reader. It exercises the COPY-TO Sink+Combine path plus the option machinery:
// a required option (`null_string`), a defaulted option (`delimiter`), a BOOLEAN
// option (`header`), and an enum/`choices` option (`on_exists`).
//
// Shards are buffered in `params.storage` (execution_id-scoped) by `write()` and
// concatenated to the destination by `close()` — the cross-process-safe pattern,
// so it works under pool rotation / HTTP.
//
// TypeScript port of vgi-python/vgi/_test_fixtures/copy_to.py
// (ExampleLinesCopyToFunction + ExampleLinesOrderedCopyToFunction).
//
// Usage:
//   COPY (SELECT * FROM t) TO '/path/out.txt'
//     (FORMAT 'acme.example_lines_out', null_string 'NA');

import { existsSync, writeFileSync } from "node:fs";
import {
  defineCopyToFunction,
  type VgiFunction,
  type CopyToWriteParams,
  type CopyToCloseParams,
} from "../src/index.js";
// The Arrow type factories `bool`/`utf8` and the batch IPC helpers live on the
// arrow facade — the package root shadows `bool` with the vgi-rpc arg builder of
// the same name (see src/index.core.ts note), so import them from here.
import { utf8, bool, serializeBatch, deserializeBatch, iterRows } from "../src/arrow/index.js";

const SHARD_NS = new TextEncoder().encode("copy_to_shard");
const EMPTY_KEY = new Uint8Array(0);

/** Options for the `example_lines_out` COPY format. */
interface ExampleLinesOutArgs {
  null_string: string;
  delimiter: string;
  header: boolean;
  on_exists: string;
}

function fmtCell(value: unknown, nullString: string): string {
  return value === null || value === undefined ? nullString : String(value);
}

// Buffer one input batch as an IPC blob in execution-scoped storage. state_append
// is atomic + race-safe across parallel sink threads / workers.
async function write(p: CopyToWriteParams<ExampleLinesOutArgs>): Promise<void> {
  await p.params.storage.stateAppend(SHARD_NS, EMPTY_KEY, serializeBatch(p.batch));
}

// Concatenate every shard and write the delimited destination file (once).
async function close(p: CopyToCloseParams<ExampleLinesOutArgs>): Promise<number> {
  const { options, filePath, params } = p;

  if (options.on_exists === "error" && existsSync(filePath)) {
    throw new Error(`example_lines_out: destination already exists: ${filePath}`);
  }

  const shards = await params.storage.stateLogScan(SHARD_NS, EMPTY_KEY, -1);

  const lines: string[] = [];
  let wroteHeader = false;
  let rowsWritten = 0;
  for (const [, blob] of shards) {
    const batch = deserializeBatch(blob);
    const names = batch.schema.fields.map((f) => f.name);
    if (options.header && !wroteHeader) {
      lines.push(names.join(options.delimiter));
      wroteHeader = true;
    }
    for (const row of iterRows(batch)) {
      lines.push(names.map((n) => fmtCell(row[n], options.null_string)).join(options.delimiter));
      rowsWritten++;
    }
  }
  // Empty COPY with header=true still emits the header row. The source column
  // names ride the bind's input_schema.
  if (options.header && !wroteHeader) {
    const inSchema = params.initCall.bind_call.input_schema;
    if (inSchema) {
      lines.push(inSchema.fields.map((f) => f.name).join(options.delimiter));
    }
  }

  writeFileSync(filePath, lines.length > 0 ? lines.join("\n") + "\n" : "", "utf-8");
  return rowsWritten;
}

const sharedOptions = {
  null_string: { type: utf8(), doc: "Token written for SQL NULL" }, // required
  delimiter: { type: utf8(), default: ",", doc: "Field separator" },
  header: { type: bool(), default: false, doc: "Write a header row of column names" },
  on_exists: {
    type: utf8(),
    default: "overwrite",
    choices: ["overwrite", "error"],
    doc: "Behavior when the destination file already exists",
  },
} as const;

const sharedTags = { category: "copy_to", stability: "test" };

/** Toy delimited-text `COPY ... TO` writer (parallel sharded sink). */
export const exampleLinesCopyTo: VgiFunction = defineCopyToFunction<ExampleLinesOutArgs>({
  name: "example_lines_writer",
  format: "example_lines_out",
  description: "Write the COPY source to a delimited text file",
  comment: "Toy delimited-text writer for tests",
  categories: ["copy", "test"],
  tags: sharedTags,
  options: sharedOptions,
  write,
  close,
});

/**
 * Ordered variant: `ordered: true` makes the extension use a single-threaded
 * sink, so the worker receives every batch in source order and writes the file
 * in order. Mirrors vgi-python's ExampleLinesOrderedCopyToFunction.
 */
export const exampleLinesOrderedCopyTo: VgiFunction = defineCopyToFunction<ExampleLinesOutArgs>({
  name: "example_lines_ordered_writer",
  format: "example_lines_ordered_out",
  description: "Write the COPY source to a delimited file, preserving source order",
  comment: "Toy delimited-text writer (ordered, single-thread sink)",
  categories: ["copy", "test"],
  tags: sharedTags,
  ordered: true,
  options: sharedOptions,
  write,
  close,
});

export const copyToFunctions: VgiFunction[] = [exampleLinesCopyTo, exampleLinesOrderedCopyTo];
