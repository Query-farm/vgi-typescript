// Type fixtures for the flechette feasibility spike. Each case represents one
// of the gnarly type paths in src/util/arrow/build.ts that currently bypasses
// arrow-js's Builder API.
//
// Cases are described in a flechette-agnostic way: a logical schema (name +
// flechette DataType factory + nullable flag) and a column-oriented values
// dictionary. The harness translates these into both an arrow-js batch (via
// the existing batchFromColumns) and a flechette table (via tableFromColumns)
// and round-trips them through both encoders/decoders.

import {
  utf8 as f_utf8,
  int32 as f_int32,
  int64 as f_int64,
  float64 as f_float64,
  bool as f_bool,
  binary as f_binary,
  decimal128 as f_decimal128,
  timestamp as f_timestamp,
  duration as f_duration,
  dictionary as f_dictionary,
  list as f_list,
  struct as f_struct,
  map as f_map,
  union as f_union,
  field as f_field,
  TimeUnit,
  UnionMode,
} from "@uwdata/flechette";

import {
  Schema,
  Field,
  Utf8,
  Int32,
  Int64,
  Float64,
  Bool,
  Binary,
  Decimal,
  Timestamp,
  Duration,
  Dictionary,
  List,
  Struct,
  Map_,
  TimeUnit as A_TimeUnit,
} from "@query-farm/apache-arrow";

export interface SpikeCase {
  name: string;
  // arrow-js schema, used to build a ground-truth batch via batchFromColumns
  arrowSchema: Schema;
  // flechette field list, used to build a flechette Table
  flechetteFields: ReturnType<typeof f_field>[];
  // column-oriented values, indexed by field name
  columns: Record<string, any[]>;
  // optional comparator override; if absent the default deep equality is used
  // (BigInts compared by value, Uint8Arrays element-wise).
  notes?: string;
}

const decimal128Type = new Decimal(38, 4, 128);

export const CASES: SpikeCase[] = [
  {
    name: "primitives",
    arrowSchema: new Schema([
      new Field("id", new Int32(), true),
      new Field("name", new Utf8(), true),
      new Field("score", new Float64(), true),
      new Field("active", new Bool(), true),
    ]),
    flechetteFields: [
      f_field("id", f_int32(), true),
      f_field("name", f_utf8(), true),
      f_field("score", f_float64(), true),
      f_field("active", f_bool(), true),
    ],
    columns: {
      id: [1, 2, null, 4],
      name: ["a", null, "c", "d"],
      score: [1.5, 2.5, null, 4.5],
      active: [true, false, null, true],
    },
  },

  {
    name: "int64-bigint",
    arrowSchema: new Schema([new Field("v", new Int64(), true)]),
    flechetteFields: [f_field("v", f_int64(), true)],
    columns: {
      v: [1n, 2n, null, 9223372036854775807n],
    },
    notes: "BigInt round-trip",
  },

  {
    name: "binary",
    arrowSchema: new Schema([new Field("b", new Binary(), true)]),
    flechetteFields: [f_field("b", f_binary(), true)],
    columns: {
      b: [
        new Uint8Array([1, 2, 3]),
        null,
        new Uint8Array([255, 254, 253, 252]),
        new Uint8Array([]),
      ],
    },
  },

  {
    name: "decimal128",
    arrowSchema: new Schema([new Field("d", decimal128Type, true)]),
    flechetteFields: [f_field("d", f_decimal128(38, 4), true)],
    columns: {
      // mix of bigint (already scaled) and null. We avoid Number inputs because
      // build.ts treats them as already-scaled too — so the wire bytes match.
      d: [12345n, -99999n, null, 1n],
    },
    notes: "Decimal128: BigInt unscaled-integer values",
  },

  {
    name: "timestamp-ns",
    arrowSchema: new Schema([
      new Field("ts", new Timestamp(A_TimeUnit.NANOSECOND), true),
    ]),
    flechetteFields: [
      f_field("ts", f_timestamp(TimeUnit.NANOSECOND), true),
    ],
    columns: {
      ts: [1700000000000000000n, null, 1700000000000000001n, 0n],
    },
  },

  {
    name: "duration-ns",
    arrowSchema: new Schema([
      new Field("dur", new Duration(A_TimeUnit.NANOSECOND), true),
    ]),
    flechetteFields: [
      f_field("dur", f_duration(TimeUnit.NANOSECOND), true),
    ],
    columns: {
      dur: [1n, null, 999999999n, -1n],
    },
  },

  {
    name: "dictionary-utf8",
    arrowSchema: new Schema([
      new Field(
        "color",
        new Dictionary(new Utf8(), new Int32(), 0, false),
        true
      ),
    ]),
    flechetteFields: [
      f_field("color", f_dictionary(f_utf8(), f_int32(), false, 0), true),
    ],
    columns: {
      // values are the decoded strings; the builders dictionary-encode for us
      color: ["red", "green", "red", null, "blue", "green"],
    },
  },

  {
    name: "list-int32",
    arrowSchema: new Schema([
      new Field(
        "tags",
        new List(new Field("item", new Int32(), true)),
        true
      ),
    ]),
    flechetteFields: [f_field("tags", f_list(f_field("item", f_int32(), true)), true)],
    columns: {
      tags: [[1, 2, 3], [], null, [42]],
    },
  },

  {
    name: "list-struct",
    arrowSchema: new Schema([
      new Field(
        "rows",
        new List(
          new Field(
            "item",
            new Struct([
              new Field("k", new Utf8(), true),
              new Field("v", new Int32(), true),
            ]),
            true
          )
        ),
        true
      ),
    ]),
    flechetteFields: [
      f_field(
        "rows",
        f_list(
          f_field(
            "item",
            f_struct([
              f_field("k", f_utf8(), true),
              f_field("v", f_int32(), true),
            ]),
            true
          )
        ),
        true
      ),
    ],
    columns: {
      rows: [
        [
          { k: "a", v: 1 },
          { k: "b", v: 2 },
        ],
        null,
        [],
        [{ k: "z", v: 99 }],
      ],
    },
  },

  {
    name: "map-utf8-int64",
    arrowSchema: new Schema([
      new Field(
        "attrs",
        new Map_(
          new Field(
            "entries",
            new Struct([
              new Field("key", new Utf8(), false),
              new Field("value", new Int64(), true),
            ]),
            false
          ),
          false
        ),
        true
      ),
    ]),
    flechetteFields: [
      f_field(
        "attrs",
        f_map(f_field("key", f_utf8(), false), f_field("value", f_int64(), true), false),
        true
      ),
    ],
    columns: {
      attrs: [
        [
          ["a", 1n],
          ["b", 2n],
        ],
        null,
        [],
      ],
    },
  },

  {
    name: "struct-mixed-null",
    arrowSchema: new Schema([
      new Field(
        "obj",
        new Struct([
          new Field("x", new Int32(), true),
          new Field("y", new Utf8(), true),
        ]),
        true
      ),
    ]),
    flechetteFields: [
      f_field(
        "obj",
        f_struct([f_field("x", f_int32(), true), f_field("y", f_utf8(), true)]),
        true
      ),
    ],
    columns: {
      obj: [{ x: 1, y: "a" }, null, { x: null, y: "c" }, { x: 4, y: null }],
    },
  },

  {
    name: "sparse-union",
    arrowSchema: new Schema([
      // intentionally omit — we'll skip the arrow-js side for union since
      // statistics.ts uses a custom path. See harness for special-case.
    ]),
    flechetteFields: [
      f_field(
        "u",
        f_union(
          UnionMode.Sparse,
          [f_field("i", f_int32(), true), f_field("s", f_utf8(), true)],
          [0, 1],
          (v) => (typeof v === "number" ? 0 : 1)
        ),
        true
      ),
    ],
    columns: {
      u: [1, "two", 3, null],
    },
    notes: "SparseUnion: flechette-only construction (statistics.ts is the sole arrow-js consumer)",
  },
];
