// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// batchToScalarDict / batchToSecretDict single-row reads, on BOTH Arrow
// backends. Phase 2 routes these through the codec/canonical path, so a
// temporal/decimal setting surfaces in the RICH representation (Date for dates)
// identically across arrow-js and flechette, and secret structs come back as
// plain objects. Run both:
//
//   bun test src/arrow/__tests__/scalar-secret-dict.test.ts
//   bun --conditions=worker test src/arrow/__tests__/scalar-secret-dict.test.ts

import { describe, test, expect } from "bun:test";
import {
  schema,
  field,
  utf8,
  int64,
  bool,
  binary,
  dateDay,
  timestamp,
  decimal128,
  struct,
  TimeUnit,
  batchFromColumns,
  batchToScalarDict,
  batchToSecretDict,
  serializeBatch,
  deserializeBatch,
  backend,
} from "../index.js";

function roundTrip(batch: ReturnType<typeof batchFromColumns>) {
  return deserializeBatch(serializeBatch(batch));
}

describe(`scalar/secret dict reads (backend=${backend.name})`, () => {
  test("batchToScalarDict returns rich values for representative types", () => {
    const sch = schema([
      field("name", utf8(), true),
      field("big", int64(), true),
      field("flag", bool(), true),
      field("blob", binary(), true),
      field("created", dateDay(), true),
      field("ts", timestamp(TimeUnit.MICROSECOND), true),
      field("amount", decimal128(38, 4), true),
    ]);
    const created = new Date("2021-03-04T00:00:00Z");
    const batch = roundTrip(
      batchFromColumns(
        {
          name: ["alpha"],
          big: [9_000_000_000n],
          flag: [true],
          blob: [new Uint8Array([1, 2, 3])],
          created: [created],
          ts: [1_700_000_000_000_123n],
          amount: [123_456n],
        },
        sch,
      ),
    );

    const dict = batchToScalarDict(batch);
    expect(dict.name).toBe("alpha");
    expect(dict.big).toBe(9_000_000_000n);
    expect(dict.flag).toBe(true);
    expect(dict.blob).toBeInstanceOf(Uint8Array);
    expect(Array.from(dict.blob as Uint8Array)).toEqual([1, 2, 3]);
    // date32 -> rich Date
    expect(dict.created).toBeInstanceOf(Date);
    expect((dict.created as Date).getTime()).toBe(created.getTime());
    // timestamp[us] -> bigint, full precision
    expect(dict.ts).toBe(1_700_000_000_000_123n);
    // decimal -> unscaled bigint
    expect(dict.amount).toBe(123_456n);
  });

  test("batchToScalarDict empty / null inputs", () => {
    expect(batchToScalarDict(null)).toEqual({});
    const sch = schema([field("x", int64(), true)]);
    const batch = roundTrip(batchFromColumns({ x: [null] }, sch));
    expect(batchToScalarDict(batch)).toEqual({ x: null });
  });

  test("batchToSecretDict decodes a named secret struct to a plain object", () => {
    const secretStruct = struct([
      field("key_id", utf8(), true),
      field("secret", utf8(), true),
      field("region", utf8(), true),
    ]);
    const sch = schema([field("s3", secretStruct, true)]);
    const batch = roundTrip(
      batchFromColumns(
        { s3: [{ key_id: "AKIA", secret: "shh", region: "us-east-1" }] },
        sch,
      ),
    );

    const dict = batchToSecretDict(batch);
    expect(dict.s3).toEqual({ key_id: "AKIA", secret: "shh", region: "us-east-1" });
  });

  test("batchToSecretDict uses secret_type metadata for scoped secrets", () => {
    const secretStruct = struct([field("token", utf8(), true)]);
    const sch = schema([
      field(
        "secret_0",
        secretStruct,
        true,
        new Map([
          ["secret_type", "http"],
          ["scope", "https://example.com"],
        ]),
      ),
    ]);
    const batch = roundTrip(
      batchFromColumns({ secret_0: [{ token: "abc" }] }, sch),
    );

    const dict = batchToSecretDict(batch);
    expect(dict.http).toEqual({ token: "abc" });
    expect(dict["http:https://example.com"]).toEqual({ token: "abc" });
  });
});
