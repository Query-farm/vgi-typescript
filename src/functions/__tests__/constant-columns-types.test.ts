// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// `constant_columns` repeats each argument as a column of that argument's type.
//
// The integration file for it (vgi test/sql/integration/table/
// constant_columns_types.test) was excluded on every lane for "arrow-js has no
// TIMESTAMP_NS" -- which was not true -- and the exclusion hid real wrong data:
//
//   * DATE: the argument arrives as a JS Date (its rich value); the fixture
//     "unwrapped" it with valueOf() to epoch-milliseconds, and a date32 column
//     reads a number as DAYS. '2024-01-15' came back as 480510-12-09 (arrow-js)
//     or 1970-01-01 (flechette).
//   * Nested DATEs: MAP {DATE: DATE} came back as {1970-01-01=1970-01-01} on
//     flechette, and a fixed-size DATE[2] as [1970-01-01, 1969-12-31] on
//     arrow-js -- each backend's writer skipped date preparation in one
//     composite (flechette: map keys/values; arrow-js: fixed_size_list).
//   * Extension types: DuckDB sends UUID, HUGEINT, UHUGEINT, TIMETZ, BIT and
//     BIGNUM as a storage type plus `ARROW:extension:*` field metadata. The
//     fixture dropped the metadata, so UUID/TIMETZ/BIT/BIGNUM came back as
//     BLOB; it hand-mapped HUGEINT/UHUGEINT to a signed decimal128, so
//     UHUGEINT's max came back as -1; and that mapping tested `instanceof` an
//     arrow-js class, so on flechette HUGEINT came back as raw bytes.
//
// Driven over HTTP against the real `constant_columns` fixture served
// in-process. The requests are hand-built -- this test plays DuckDB, and the
// argument shapes are the ones DuckDB sends under arrow_lossless_conversion
// (captured from a live bind) -- and every response is the worker's own.
// Run under both `bun test` and `bun --conditions=flechette test`.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { httpConnect, type RpcClient } from "@query-farm/vgi-rpc";
import { tableFunctions } from "../../../examples/table.js";
import {
  batchFromColumns,
  binary,
  codecFor,
  dateDay,
  fixedSizeBinary,
  fixedSizeList,
  field,
  int8,
  int64,
  map,
  readCanonicalValue,
  schema,
  serializeBatch,
  struct,
  type VgiBatch,
  type VgiDataType,
  type VgiField,
} from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import { ReadOnlyCatalogInterface } from "../../catalog/read-only.js";
import { unwrapResult, wrapRequest } from "../../client/protocol.js";
import { deserializeBindResponse, serializeBindRequest, serializeInitRequest } from "../../protocol/serialize.js";
import type { BindRequest, BindResponse, InitRequest } from "../../protocol/types.js";
import { SIGNING_KEY_BYTES, serveVgiWorker, type VgiHttpServer } from "../../serve-entry.js";
import { FunctionType } from "../../types.js";
import { FunctionRegistry } from "../registry.js";
import { FunctionStorageSqlite, resolveStorageFromEnv, setStorage } from "../storage.js";

const CONSTANT_COLUMNS = tableFunctions.find((f) => f.meta.name === "constant_columns")!;
const ROWS = 3;

const EXT_NAME = "ARROW:extension:name";
const EXT_META = "ARROW:extension:metadata";

/** Field metadata for a DuckDB `arrow.opaque` extension type. */
function opaque(typeName: string): Map<string, string> {
  return new Map([
    [EXT_META, JSON.stringify({ type_name: typeName, vendor_name: "DuckDB" })],
    [EXT_NAME, "arrow.opaque"],
  ]);
}

/** One constant argument: its Arrow field (as DuckDB types it) and its value. */
interface Arg {
  type: VgiDataType;
  metadata?: Map<string, string>;
  value: unknown;
}

// '2024-01-15'::DATE and '1900-03-01'::DATE as date32 day-numbers.
const DAY_2024_01_15 = 19737;
const DAY_1900_03_01 = -25508;
const UUID_BYTES = Uint8Array.from([
  0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
]);
// 2^127 - 1 (HUGEINT max) and 2^128 - 1 (UHUGEINT max), little-endian.
const HUGEINT_MAX = Uint8Array.from([...new Array(15).fill(0xff), 0x7f]);
const UHUGEINT_MAX = new Uint8Array(16).fill(0xff);

let dir: string;
let server: VgiHttpServer;
let rpc: RpcClient;

beforeAll(() => {
  dir = mkdtempSync(join(tmpdir(), "vgi-constant-columns-"));
  setStorage(new FunctionStorageSqlite(join(dir, "state.db")));
  const registry = new FunctionRegistry();
  registry.register(CONSTANT_COLUMNS);
  server = serveVgiWorker({
    name: "demo",
    doc: "constant_columns types test worker.",
    version: "0.0.1",
    registry,
    catalogInterface: new ReadOnlyCatalogInterface(
      { name: "demo", schemas: [{ name: "main", functions: [CONSTANT_COLUMNS] }] },
      registry,
    ),
    port: 0,
    signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(7),
    quiet: true,
    env: {},
  });
  rpc = httpConnect(`http://localhost:${server.port}`, { prefix: "" });
});

afterAll(() => {
  server?.stop(true);
  setStorage(resolveStorageFromEnv());
  rmSync(dir, { recursive: true, force: true });
});

/** A one-row request record with one column's value replaced. */
function withColumn(batch: VgiBatch, name: string, value: unknown): VgiBatch {
  const columns: Record<string, unknown[]> = {};
  for (const f of batch.schema.fields) {
    const type = f.type as unknown as VgiDataType;
    columns[f.name] = [
      f.name === name ? value : codecFor(type).canonicalToRich(readCanonicalValue(type, batch.getChild(f.name), 0)),
    ];
  }
  return batchFromColumns(columns as Record<string, any[]>, batch.schema);
}

/**
 * The bind record DuckDB sends for `constant_columns(ROWS, ...args)`: the
 * arguments are one `args` struct whose `positional_<i>` children carry each
 * value's own Arrow type and extension metadata. (`serializeArguments` infers a
 * type from each JS value, so it cannot say "date32" or "arrow.uuid"; the
 * arguments bytes are built here and spliced in.)
 */
function bindRecord(args: Arg[]): { request: BindRequest; batch: VgiBatch } {
  const children: VgiField[] = [field("positional_0", int64(), true)];
  const row: Record<string, unknown> = { positional_0: BigInt(ROWS) };
  args.forEach((a, i) => {
    children.push(field(`positional_${i + 1}`, a.type, true, a.metadata));
    row[`positional_${i + 1}`] = a.value;
  });
  const argsBytes = serializeBatch(
    batchFromColumns({ args: [row] }, schema([field("args", struct(children as any), true)])),
  );
  const request: BindRequest = {
    function_name: "constant_columns",
    arguments: new Arguments(),
    function_type: FunctionType.TABLE,
    input_schema: null,
    settings: null,
    secrets: null,
    attach_opaque_data: null,
    transaction_opaque_data: null,
    resolved_secrets_provided: false,
  };
  return { request, batch: withColumn(serializeBindRequest(request), "arguments", argsBytes) };
}

function initRecord(bind: { request: BindRequest; batch: VgiBatch }, bound: BindResponse): VgiBatch {
  const init: InitRequest = {
    bind_call: bind.request,
    output_schema: bound.output_schema,
    bind_opaque_data: bound.opaque_data,
    split_tokens: null,
    row_limit: null,
    projection_ids: null,
    pushdown_filters: null,
    join_keys: [],
    phase: null,
    finalize_state_id: null,
    order_by_column_name: null,
    order_by_direction: null,
    order_by_null_order: null,
    order_by_limit: null,
    tablesample_percentage: null,
    tablesample_seed: null,
    execution_id: null,
    init_opaque_data: null,
    substream_id: null,
  };
  return withColumn(serializeInitRequest(init), "bind_call", serializeBatch(bind.batch));
}

/** Bind + init `constant_columns(ROWS, ...args)` and return the worker's first data batch. */
async function scan(args: Arg[]): Promise<VgiBatch> {
  const bind = bindRecord(args);
  const bound = deserializeBindResponse(
    unwrapResult((await rpc.call("bind", wrapRequest(bind.batch))) as Record<string, any>),
  );
  const session = await rpc.stream("init", wrapRequest(initRecord(bind, bound)));
  try {
    const reply = await (session as any).tickRaw();
    return reply.batch as VgiBatch;
  } finally {
    session.close();
  }
}

/** Every row of column `i`, read off the wire losslessly (bytes stay bytes). */
function column(batch: VgiBatch, i: number): unknown[] {
  const f = batch.schema.fields[i];
  const col = batch.getChild(f.name);
  return Array.from({ length: batch.numRows }, (_, r) =>
    readCanonicalValue(f.type as unknown as VgiDataType, col, r),
  );
}

/** A field's metadata as a plain object, so `toEqual` compares contents. */
function meta(f: { metadata?: Map<string, string> | null }): Record<string, string> {
  return Object.fromEntries(f.metadata ?? []);
}

describe("constant_columns repeats a DATE as that DATE", () => {
  test("'2024-01-15'::DATE", async () => {
    const batch = await scan([{ type: dateDay(), value: DAY_2024_01_15 }]);
    expect(batch.schema.fields[0].type.typeId).toBe(dateDay().typeId);
    expect((batch.schema.fields[0].type as any).unit).toBe(0); // DAY
    // Before: 174783488 on arrow-js (480510-12-09), 0 on flechette (1970-01-01).
    expect(column(batch, 0)).toEqual([DAY_2024_01_15, DAY_2024_01_15, DAY_2024_01_15]);
  });

  test("a date before the epoch", async () => {
    const batch = await scan([{ type: dateDay(), value: DAY_1900_03_01 }]);
    // Before: 1566790-11-04 (BC) on arrow-js.
    expect(column(batch, 0)).toEqual([DAY_1900_03_01, DAY_1900_03_01, DAY_1900_03_01]);
  });
});

describe("constant_columns repeats DATEs nested in a MAP and an ARRAY", () => {
  test("MAP {DATE: DATE}", async () => {
    const t = map(field("key", dateDay(), false), field("value", dateDay(), true));
    const batch = await scan([{ type: t, value: [[DAY_2024_01_15, DAY_1900_03_01]] }]);
    // Before (flechette): [[0, 0]] -- {1970-01-01=1970-01-01}.
    const want = [[DAY_2024_01_15, DAY_1900_03_01]];
    expect(column(batch, 0)).toEqual([want, want, want]);
  });

  test("fixed-size DATE[2]", async () => {
    const t = fixedSizeList(field("item", dateDay(), true), 2);
    const batch = await scan([{ type: t, value: [DAY_2024_01_15, DAY_1900_03_01] }]);
    // Before (arrow-js): [0, -1] -- [1970-01-01, 1969-12-31].
    const want = [DAY_2024_01_15, DAY_1900_03_01];
    expect(column(batch, 0)).toEqual([want, want, want]);
  });
});

describe("constant_columns keeps the argument's extension type", () => {
  const cases: { name: string; arg: Arg }[] = [
    {
      name: "UUID",
      arg: {
        type: fixedSizeBinary(16),
        metadata: new Map([[EXT_META, ""], [EXT_NAME, "arrow.uuid"]]),
        value: UUID_BYTES,
      },
    },
    { name: "HUGEINT max", arg: { type: fixedSizeBinary(16), metadata: opaque("hugeint"), value: HUGEINT_MAX } },
    { name: "UHUGEINT max", arg: { type: fixedSizeBinary(16), metadata: opaque("uhugeint"), value: UHUGEINT_MAX } },
    {
      name: "TIMETZ",
      arg: {
        type: fixedSizeBinary(8),
        metadata: opaque("time_tz"),
        value: Uint8Array.from([0xaf, 0x9a, 0x00, 0x40, 0x27, 0xe4, 0x7c, 0x0a]),
      },
    },
    { name: "BIT", arg: { type: binary(), metadata: opaque("bit"), value: Uint8Array.from([0x05, 0xfd]) } },
    {
      name: "BIGNUM",
      arg: {
        type: binary(),
        metadata: opaque("bignum"),
        value: Uint8Array.from([0x80, 0x00, 0x0d, 0x01, 0x8e, 0xe9, 0x0f, 0xf6, 0xc3, 0x73, 0xe0, 0xee, 0x4e, 0x3f, 0x0a, 0xd2]),
      },
    },
  ];

  for (const { name, arg } of cases) {
    test(`${name}: same storage type, same extension metadata, same bytes`, async () => {
      const batch = await scan([arg]);
      const out = batch.schema.fields[0];
      // Before: UUID/TIMETZ/BIT/BIGNUM lost the metadata (DuckDB read BLOB);
      // HUGEINT/UHUGEINT became decimal128 (UHUGEINT max read back as -1) on
      // arrow-js, and bare FixedSizeBinary (BLOB) on flechette.
      expect(out.type.typeId).toBe(arg.type.typeId);
      expect(meta(out)).toEqual(Object.fromEntries(arg.metadata!));
      const want = Array.from(arg.value as Uint8Array);
      expect(column(batch, 0).map((v) => Array.from(v as Uint8Array))).toEqual([want, want, want]);
    });
  }

  test("BOOLEAN keeps DuckDB's arrow.bool8 field", async () => {
    // DuckDB sends BOOLEAN as arrow.bool8 (int8 storage) and reads it back as
    // BOOLEAN, so the column is repeated as-is -- no bool8 -> Bool mapping to
    // drift out of step with the value.
    const bool8 = new Map([[EXT_META, ""], [EXT_NAME, "arrow.bool8"]]);
    const batch = await scan([{ type: int8(), metadata: bool8, value: 1 }]);
    expect(batch.schema.fields[0].type.typeId).toBe(int8().typeId);
    expect(meta(batch.schema.fields[0])).toEqual(Object.fromEntries(bool8));
    expect(column(batch, 0)).toEqual([1, 1, 1]);
  });

  test("several columns at once, each its own argument's field", async () => {
    const batch = await scan([
      { type: dateDay(), value: DAY_2024_01_15 },
      { type: fixedSizeBinary(16), metadata: new Map([[EXT_META, ""], [EXT_NAME, "arrow.uuid"]]), value: UUID_BYTES },
      { type: int64(), value: 42n },
    ]);
    expect(batch.schema.fields.map((f) => f.name)).toEqual(["col_0", "col_1", "col_2"]);
    expect(column(batch, 0)).toEqual([DAY_2024_01_15, DAY_2024_01_15, DAY_2024_01_15]);
    expect(meta(batch.schema.fields[1])[EXT_NAME]).toBe("arrow.uuid");
    expect(column(batch, 2)).toEqual([42n, 42n, 42n]);
  });
});
