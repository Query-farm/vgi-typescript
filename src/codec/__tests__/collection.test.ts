// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import { test, expect } from "bun:test";
import { encodeASD, decodeASD, encodeASDCollection, decodeASDCollection } from "../asd.js";
import { schema, field, utf8, int16, int32, binary, dictionary, list, deserializeBatches, serializeSchema } from "../../arrow/index.js";
import * as arrow from "@query-farm/apache-arrow";
import { FunctionInfoSchema } from "../../generated/vgi-protocol-schemas.js";

test("collection codecs preserve later batches and different dictionary values", () => {
  const metadata = schema([field("name", utf8(), false), field("kind", dictionary(utf8(), int16()), false)]);
  const rows = Array.from({ length: 2300 }, (_, i) => ({ name: `object-${i}`, kind: i < 1024 ? "SCALAR" : i < 2048 ? "TABLE" : "AGGREGATE" }));
  const bytes = encodeASDCollection(metadata, rows)!;
  expect(decodeASDCollection(metadata, bytes)).toEqual(rows);
  expect(deserializeBatches(bytes).reduce((count, b) => count + b.numRows, 0)).toBe(2300);
});

test("empty collection bytes still require the declared metadata schema", () => {
  const metadata = schema([field("name", utf8(), false)]);
  expect(decodeASDCollection(metadata, null)).toEqual([]);
  expect(decodeASDCollection(metadata, serializeSchema(metadata))).toEqual([]);
  expect(() => decodeASDCollection(metadata, new Uint8Array())).toThrow();
  expect(() => decodeASDCollection(metadata, serializeSchema(schema([field("wrong", int16(), false)])))).toThrow("incompatible metadata schema");
});

test("collection and singleton encoders apply identical container and binary dictionary defaults", () => {
  const metadata = schema([
    field("names", list(field("item", utf8(), true)), false),
    field("payloads", list(field("item", dictionary(binary(), int32()), true)), false),
  ]);
  const rows = [{ names: null, payloads: [null, new Uint8Array([255])] }, {}];
  const expected = [{ names: [], payloads: [new Uint8Array(), new Uint8Array([255])] }, { names: [], payloads: [] }];
  expect(decodeASDCollection(metadata, encodeASDCollection(metadata, rows))).toEqual(expected);
  expect(rows.map((row) => decodeASD(metadata, encodeASD(metadata, row)))).toEqual(expected);
  expect(rows[0].names).toBeNull();
  expect(rows[0].payloads![0]).toBeNull();
});

test("binary dictionaries deduplicate equal bytes and preserve null, empty and non-UTF8 values", () => {
  const metadata = schema([field("payload", dictionary(binary(), int32()), true)]);
  const values = [new Uint8Array([255, 0, 128]), new Uint8Array([255, 0, 128]), null, new Uint8Array(), new Uint8Array([254, 0, 128])];
  const rows = Array.from({ length: 2300 }, (_, i) => ({ payload: values[i % values.length]?.slice() ?? null }));
  const bytes = encodeASDCollection(metadata, rows)!;
  expect(decodeASDCollection(metadata, bytes)).toEqual(rows);
  const batches = [...arrow.RecordBatchReader.from(bytes)];
  expect(batches.length).toBe(3);
  for (const batch of batches) {
    const data = batch.getChild("payload")!.data[0];
    expect(data.dictionary!.length).toBe(3);
    expect((data.type as arrow.Dictionary).indices.bitWidth).toBe(32);
  }
  for (const payload of values) expect(decodeASD(metadata, encodeASD(metadata, { payload }))).toEqual({ payload });
});

test("binary dictionary readers apply replacement dictionaries at each batch", () => {
  const type = new arrow.Dictionary(new arrow.Binary(), new arrow.Int32(), 700);
  const wireSchema = new arrow.Schema([new arrow.Field("payload", type, true)]);
  const writer = new arrow.RecordBatchStreamWriter();
  writer.reset(undefined, wireSchema);
  const rows = [new Uint8Array([255, 0]), new Uint8Array([128, 1]), new Uint8Array()];
  for (const payload of rows) {
    const vector = arrow.vectorFromArray([payload, null], type);
    const batch = new arrow.RecordBatch(wireSchema, arrow.makeData({ type: new arrow.Struct(wireSchema.fields), length: 2, children: [vector.data[0]] }));
    (writer as any)._writeRecordBatch(batch);
  }
  writer.close();
  const metadata = schema([field("payload", dictionary(binary(), int32()), true)]);
  expect(decodeASDCollection(metadata, writer.toUint8Array(true))).toEqual(rows.flatMap((payload) => [{ payload }, { payload: null }]));
});

test("required binary dictionaries reject indexes pointing to a null dictionary value", () => {
  const type = new arrow.Dictionary(new arrow.Binary(), new arrow.Int32(), 701);
  const wireSchema = new arrow.Schema([new arrow.Field("payload", type, false)]);
  const column = arrow.makeData({ type, length: 1, data: new Int32Array([0]), dictionary: arrow.vectorFromArray([null], new arrow.Binary()) });
  const batch = new arrow.RecordBatch(wireSchema, arrow.makeData({ type: new arrow.Struct(wireSchema.fields), length: 1, children: [column] }));
  const writer = new arrow.RecordBatchStreamWriter();
  writer.reset(undefined, wireSchema);
  (writer as any)._writeRecordBatch(batch);
  writer.close();
  expect(() => decodeASDCollection(schema([field("payload", dictionary(binary(), int32()), false)]), writer.toUint8Array(true))).toThrow("null payload");
});

test("FunctionInfo dictionaries retain the public byte fields and reject old binary schemas", () => {
  const row = Object.fromEntries(FunctionInfoSchema.fields.map((f) => [f.name, f.nullable ? null : false]));
  Object.assign(row, {
    tags: {}, name: "binary_metadata", schema_path: ["main"], function_type: "SCALAR",
    arguments: new Uint8Array([255, 0, 128]), output_schema: new Uint8Array(), parameter_default_values: new Uint8Array([254, 0]),
    description: "", examples: [], categories: [], filter_semantic_profiles: [], additional_filter_functions: [],
    runtime_filter_algorithms: [], filter_evaluation_contexts: [], partition_kind: "NONE", order_dependent: "NOT_ORDER_DEPENDENT",
    distinct_dependent: "NOT_DISTINCT_DEPENDENT", required_settings: [], required_secrets: [],
  });
  expect(decodeASD(FunctionInfoSchema, encodeASD(FunctionInfoSchema, row))).toEqual(row);
  expect(decodeASDCollection(FunctionInfoSchema, encodeASDCollection(FunctionInfoSchema, [row, { ...row, name: "second", parameter_default_values: null }]))).toEqual([row, { ...row, name: "second", parameter_default_values: null }]);
  const old = schema(FunctionInfoSchema.fields.map((f) => ["arguments", "output_schema", "parameter_default_values"].includes(f.name) ? field(f.name, binary(), f.nullable) : f));
  expect(() => decodeASDCollection(FunctionInfoSchema, encodeASDCollection(old, [row]))).toThrow("incompatible metadata schema");
});
