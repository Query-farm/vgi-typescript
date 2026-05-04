// Arguments wire format — single-row Arrow batch with one "args" Struct
// column whose children are named `positional_<i>` / `named_<name>`.
// Matches Python's Arguments.serialize_to_bytes().

import {
  Schema,
  Field,
  RecordBatch,
  DataType,
  Utf8,
  Binary,
  Int64,
  Bool,
  Null,
  Struct,
  vectorFromArray,
  makeData,
  RecordBatchReader,
} from "@query-farm/apache-arrow";
import { Arguments } from "../../arguments/arguments.js";
import { serializeBatch } from "../../util/arrow/index.js";

export function serializeArguments(args: Arguments): Uint8Array {
  // Arguments are serialized as a single-row batch with one "args" Struct column.
  // The struct has fields: "positional_0", "positional_1", ... and "named_<name>".

  const structFields: Field[] = [];
  const structValues: Record<string, any> = {};

  // Positional args
  for (let i = 0; i < args.positional.length; i++) {
    const val = args.positional[i];
    const fieldName = `positional_${i}`;
    structFields.push(new Field(fieldName, inferScalarType(val), true));
    structValues[fieldName] = val;
  }

  // Named args
  for (const [name, val] of args.named) {
    const fieldName = `named_${name}`;
    structFields.push(new Field(fieldName, inferScalarType(val), true));
    structValues[fieldName] = val;
  }

  // Build the "args" struct column
  const structType = new Struct(structFields);
  const argsField = new Field("args", structType, true);
  const schema = new Schema([argsField]);

  if (structFields.length === 0) {
    // Empty struct: create batch with empty struct column
    const structData = makeData({ type: structType, length: 1, children: [], nullCount: 0 });
    const outerStructType = new Struct(schema.fields);
    const data = makeData({ type: outerStructType, length: 1, children: [structData], nullCount: 0 });
    const batch = new RecordBatch(schema, data);
    return serializeBatch(batch);
  }

  // Build struct children
  const children = structFields.map((f) => {
    const val = structValues[f.name];
    let coerced = [val];
    if (DataType.isInt(f.type) && (f.type as any).bitWidth === 64) {
      coerced = [typeof val === "number" ? BigInt(val) : val];
    }
    return vectorFromArray(coerced, f.type).data[0];
  });

  const structData = makeData({ type: structType, length: 1, children, nullCount: 0 });
  const outerStructType = new Struct(schema.fields);
  const data = makeData({ type: outerStructType, length: 1, children: [structData], nullCount: 0 });
  const batch = new RecordBatch(schema, data);
  return serializeBatch(batch);
}

function inferScalarType(val: any): DataType {
  if (val === null || val === undefined) return new Null();
  if (typeof val === "string") return new Utf8();
  if (typeof val === "boolean") return new Bool();
  if (typeof val === "number") return new Int64();
  if (typeof val === "bigint") return new Int64();
  if (val instanceof Uint8Array || val instanceof ArrayBuffer) return new Binary();
  return new Utf8(); // fallback
}

export function deserializeArguments(bytes: Uint8Array): Arguments {
  if (!bytes || bytes.length === 0) return new Arguments();

  // Ensure we have a clean copy (not a view into a larger buffer)
  const cleanBytes = bytes.byteOffset !== 0 || bytes.byteLength !== bytes.buffer.byteLength
    ? new Uint8Array(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength))
    : bytes;

  const reader = RecordBatchReader.from(cleanBytes);
  const batches = [...reader];

  const positional: any[] = [];
  const named = new Map<string, any>();

  if (batches.length === 0 || batches[0].numRows === 0) {
    return new Arguments(positional, named);
  }

  const batch = batches[0];
  // Arguments are serialized as a single "args" Struct column.
  // The struct has fields named "positional_0", "positional_1", etc.
  // for positional args, and "named_<name>" for named args.
  const argsCol = batch.getChild("args");
  if (argsCol) {
    // Extract per-positional types from the struct's children
    const argsStructType = argsCol.type as any;
    const structSchema = argsStructType.children
      ? new Schema(argsStructType.children)
      : batch.schema;

    const structVal = argsCol.get(0);
    if (structVal && typeof structVal === "object") {
      // Convert struct scalar to a plain object
      const dict: Record<string, any> = structVal.toJSON
        ? structVal.toJSON()
        : Object.assign({}, structVal);
      for (const [key, value] of Object.entries(dict)) {
        if (key.startsWith("positional_")) {
          const idx = parseInt(key.slice("positional_".length), 10);
          while (positional.length <= idx) positional.push(null);
          positional[idx] = value;
        } else if (key.startsWith("named_")) {
          const name = key.slice("named_".length);
          named.set(name, value);
        }
      }
    }
    return new Arguments(positional, named, structSchema);
  }

  // Fallback: flat columns (legacy format - each field is a column)
  const schema = batch.schema;
  if (!schema || schema.fields.length === 0) {
    return new Arguments(positional, named);
  }

  for (const field of schema.fields) {
    const col = batch.getChild(field.name);
    const val = col ? col.get(0) : null;

    if (field.name.startsWith("positional_")) {
      const idx = parseInt(field.name.slice("positional_".length), 10);
      while (positional.length <= idx) positional.push(null);
      positional[idx] = val;
    } else if (field.name.startsWith("named_")) {
      const name = field.name.slice("named_".length);
      named.set(name, val);
    } else {
      // Try numeric field name (old format)
      const metadata = field.metadata;
      const isNamed = metadata.get("vgi_arg") === "named";
      if (isNamed) {
        named.set(field.name, val);
      } else {
        const idx = parseInt(field.name, 10);
        if (!isNaN(idx)) {
          while (positional.length <= idx) positional.push(null);
          positional[idx] = val;
        }
      }
    }
  }

  return new Arguments(positional, named, schema);
}
