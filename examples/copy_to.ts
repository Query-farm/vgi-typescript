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
  secretForScopeOfType,
  type VgiFunction,
  type CopyToWriteParams,
  type CopyToCloseParams,
} from "../src/index.js";
// The Arrow type factories `bool`/`utf8` and the batch IPC helpers live on the
// arrow facade — the package root shadows `bool` with the vgi-rpc arg builder of
// the same name (see src/index.core.ts note), so import them from here.
import { utf8, bool, int64, serializeBatch, deserializeBatch, iterRows } from "../src/arrow/index.js";

const SHARD_NS = new TextEncoder().encode("copy_to_shard");
const EMPTY_KEY = new Uint8Array(0);

/** Options for the `example_lines_out` COPY format. */
interface ExampleLinesOutArgs {
  null_string: string;
  delimiter: string;
  header: boolean;
  header_repeat: number;
  on_exists: string;
  fail_on_value: string;
}

function fmtCell(value: unknown, nullString: string): string {
  return value === null || value === undefined ? nullString : String(value);
}

// Buffer one input batch as an IPC blob in execution-scoped storage. state_append
// is atomic + race-safe across parallel sink threads / workers.
async function write(p: CopyToWriteParams<ExampleLinesOutArgs>): Promise<void> {
  const { batch, options } = p;
  // Mid-sink failure trigger: raise during a process() call when a cell matches
  // fail_on_value. Exercises the in-flight teardown/recovery path.
  if (options.fail_on_value) {
    for (const row of iterRows(batch)) {
      for (const value of Object.values(row)) {
        if (value !== null && value !== undefined && String(value) === options.fail_on_value) {
          throw new Error(`example_lines_out: fail_on_value hit: ${JSON.stringify(options.fail_on_value)}`);
        }
      }
    }
  }
  await p.params.storage.stateAppend(SHARD_NS, EMPTY_KEY, serializeBatch(batch));
}

// Concatenate every shard and write the delimited destination file (once).
async function close(p: CopyToCloseParams<ExampleLinesOutArgs>): Promise<number> {
  const { options, filePath, params } = p;

  if (options.on_exists === "error" && existsSync(filePath)) {
    throw new Error(`example_lines_out: destination already exists: ${filePath}`);
  }

  const shards = await params.storage.stateLogScan(SHARD_NS, EMPTY_KEY, -1);

  // header=true writes the column-name line `header_repeat` times (default 1).
  const writeHeader = (names: string[]): void => {
    if (!options.header) return;
    const headerLine = names.join(options.delimiter);
    for (let i = 0; i < options.header_repeat; i++) lines.push(headerLine);
  };

  const lines: string[] = [];
  let wroteHeader = false;
  let rowsWritten = 0;
  for (const [, blob] of shards) {
    const batch = deserializeBatch(blob);
    const names = batch.schema.fields.map((f) => f.name);
    if (!wroteHeader) {
      writeHeader(names);
      wroteHeader = true;
    }
    for (const row of iterRows(batch)) {
      lines.push(names.map((n) => fmtCell(row[n], options.null_string)).join(options.delimiter));
      rowsWritten++;
    }
  }
  // Empty COPY with header=true still emits the header row(s). The source column
  // names ride the bind's input_schema.
  if (!wroteHeader) {
    const inSchema = params.initCall.bind_call.input_schema;
    if (inSchema) {
      writeHeader(inSchema.fields.map((f) => f.name));
    }
  }

  writeFileSync(filePath, lines.length > 0 ? lines.join("\n") + "\n" : "", "utf-8");
  return rowsWritten;
}

const sharedOptions = {
  null_string: { type: utf8(), doc: "Token written for SQL NULL" }, // required
  delimiter: { type: utf8(), default: ",", doc: "Field separator" },
  header: { type: bool(), default: false, doc: "Write a header row of column names" },
  header_repeat: {
    type: int64(),
    default: 1,
    ge: 0,
    le: 3,
    doc: "When header=true, write the header line this many times",
  },
  on_exists: {
    type: utf8(),
    default: "overwrite",
    choices: ["overwrite", "error"],
    doc: "Behavior when the destination file already exists",
  },
  fail_on_value: {
    type: utf8(),
    default: "",
    doc: "If non-empty, fail mid-write when a cell equals this value",
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

// ============================================================================
// secret_lines_out — forwards a CREATE SECRET credential to the writer.
//
// Exercises the COPY-TO secret-bind hook (`onSecrets`): it requests the
// `secret_type` secret scoped to the destination path during bind, and `close`
// writes the resolved secret's api_key (or NONE) plus the row count — so a test
// can assert the caller's secret reached the writer for a secret-backed cloud
// write. TypeScript port of vgi-python's SecretLinesCopyToFunction.
// ============================================================================

const SECRET_SHARD_NS = new TextEncoder().encode("copy_to_secret_shard");

interface SecretLinesOutArgs {
  secret_type: string;
}

export const secretLinesCopyTo: VgiFunction = defineCopyToFunction<SecretLinesOutArgs>({
  name: "secret_lines_writer",
  format: "secret_lines_out",
  description: "Write the resolved secret's api_key + row count to the destination",
  comment: "Writer that forwards a CREATE SECRET credential (test fixture)",
  categories: ["copy", "test", "secret"],
  tags: { category: "copy_to", stability: "test" },
  options: {
    secret_type: {
      type: utf8(),
      default: "vgi_example",
      doc: "Secret type to fetch, scoped by the destination path",
    },
  },
  // Request the destination-scoped secret; the framework's two-phase secret bind
  // resolves it and surfaces it on params.secrets at close time.
  onSecrets: ({ options, filePath }) => [
    { secretType: options.secret_type, scope: filePath },
  ],
  // Record this shard's row count (cross-process-safe append).
  write: async (p) => {
    await p.params.storage.stateAppend(
      SECRET_SHARD_NS,
      EMPTY_KEY,
      new TextEncoder().encode(String(p.batch.numRows)),
    );
  },
  // Write the forwarded secret's api_key + total row count, once.
  close: async (p) => {
    const secret = secretForScopeOfType(p.params.secrets, p.filePath, p.options.secret_type);
    const apiKey = secret && secret.api_key != null ? String(secret.api_key) : "NONE";
    const shards = await p.params.storage.stateLogScan(SECRET_SHARD_NS, EMPTY_KEY, -1);
    let total = 0;
    for (const [, blob] of shards) {
      total += Number(new TextDecoder().decode(blob));
    }
    writeFileSync(p.filePath, `api_key=${apiKey}\nrows=${total}\n`, "utf-8");
    return total;
  },
});

export const copyToFunctions: VgiFunction[] = [
  exampleLinesCopyTo,
  exampleLinesOrderedCopyTo,
  secretLinesCopyTo,
];
