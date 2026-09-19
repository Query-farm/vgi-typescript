// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// A DATE argument read through Arguments.get() is a JS Date.
//
// Arguments are decoded to their RICH values, and the rich value of
// date32/date64 is a `Date`. `get()` used to "unwrap" it with valueOf(), which
// hands back epoch MILLISECONDS -- and a bare number in a date32 column means
// DAYS. So `get()` of '2024-01-15'::DATE (every typed `params.args.x` and every
// scalar const goes through it), written back out as a DATE, landed 86,400,000x
// too far out: 480510-12-09 on arrow-js, an invalid Date on flechette.
//
// The arguments batch is hand-built the way DuckDB sends it -- one `args`
// struct column whose children are `positional_<i>` / `named_<name>`, a DATE as
// a date32 day-number -- and decoded by the real deserializer. Run under both
// `bun test` and `bun --conditions=flechette test`.

import { describe, expect, test } from "bun:test";
import {
  batchFromColumns,
  dateDay,
  dateMillisecond,
  field,
  int64,
  readCanonicalValue,
  schema,
  serializeBatch,
  struct,
  type VgiField,
} from "../arrow/index.js";
import { defineTableFunction } from "../functions/table.js";
import { deserializeArguments } from "../protocol/serializers/arguments.js";
import type { BindRequest } from "../protocol/types.js";
import { FunctionType } from "../types.js";

const DAY_2024_01_15 = 19737; // '2024-01-15'::DATE as a date32 day-number
const MS_2024_01_15 = DAY_2024_01_15 * 86_400_000;

/** A single-row DuckDB-shaped arguments batch (IPC bytes). */
function argumentsBytes(children: VgiField[], values: Record<string, unknown>): Uint8Array {
  const sch = schema([field("args", struct(children as any), true)]);
  return serializeBatch(batchFromColumns({ args: [values] }, sch));
}

/** Write `value` into a one-row date32 column and read the day-number back off it. */
function asDate32Column(value: unknown): unknown {
  const sch = schema([field("d", dateDay(), true)]);
  const batch = batchFromColumns({ d: [value] }, sch);
  return readCanonicalValue(dateDay(), batch.getChild("d"), 0);
}

describe("Arguments.get() keeps a DATE a Date", () => {
  const bytes = argumentsBytes(
    [
      field("positional_0", int64(), true),
      field("positional_1", dateDay(), true),
      field("positional_2", dateMillisecond(), true),
      field("named_d", dateDay(), true),
    ],
    { positional_0: 3n, positional_1: DAY_2024_01_15, positional_2: BigInt(MS_2024_01_15), named_d: DAY_2024_01_15 },
  );

  test("positional date32 and date64 read back as that Date", () => {
    const args = deserializeArguments(bytes);
    for (const position of [1, 2]) {
      const v = args.get(position);
      expect(v).toBeInstanceOf(Date);
      expect((v as Date).toISOString()).toBe("2024-01-15T00:00:00.000Z");
    }
  });

  test("a named date32 reads back as that Date", () => {
    const v = deserializeArguments(bytes).get("d");
    expect(v).toBeInstanceOf(Date);
    expect((v as Date).toISOString()).toBe("2024-01-15T00:00:00.000Z");
  });

  test("get() agrees with the decoded positional value", () => {
    const args = deserializeArguments(bytes);
    expect(args.get(1)).toEqual(args.positional[1]);
  });

  test("a DATE read through get() writes back out as the same day", () => {
    // Before: 1705276800000 went into a column of days -- 174783488 after the
    // int32 wrap on arrow-js (480510-12-09), an invalid Date on flechette.
    expect(asDate32Column(deserializeArguments(bytes).get(1))).toBe(DAY_2024_01_15);
  });

  test("a table function's typed DATE argument is a Date", async () => {
    let seen: unknown;
    const fn = defineTableFunction<{ d: Date }, null>({
      name: "date_arg_probe",
      description: "captures its typed DATE argument",
      args: { d: dateDay() },
      onBind: (params) => {
        seen = params.args.d;
        return { outputSchema: schema([field("d", dateDay(), true)]) };
      },
      initialState: () => null,
      process: (_params, _state, out) => out.finish(),
    });
    const request: BindRequest = {
      function_name: "date_arg_probe",
      arguments: deserializeArguments(
        argumentsBytes([field("positional_0", dateDay(), true)], { positional_0: DAY_2024_01_15 }),
      ),
      function_type: FunctionType.TABLE,
      input_schema: null,
      settings: null,
      secrets: null,
      attach_opaque_data: null,
      transaction_opaque_data: null,
      resolved_secrets_provided: false,
    };
    await (fn as any).bind(request);
    expect(seen).toBeInstanceOf(Date);
    expect(asDate32Column(seen)).toBe(DAY_2024_01_15);
  });
});
