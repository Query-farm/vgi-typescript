// Arguments wire format — single-row Arrow batch with one "args" Struct
// column whose children are named `positional_<i>` / `named_<name>`.
// Matches Python's Arguments.serialize_to_bytes().

import {
  type VgiDataType,
  type VgiField,
  schema as makeSchema,
  field,
  struct as makeStruct,
  utf8,
  binary,
  int64,
  bool,
  nullType,
  isInt,
  batchFromColumns,
  serializeBatch,
  deserializeBatch,
} from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";

export function serializeArguments(args: Arguments): Uint8Array {
  // Arguments are serialized as a single-row batch with one "args" Struct column.
  // The struct has fields: "positional_0", "positional_1", ... and "named_<name>".

  const structFields: VgiField[] = [];
  const structValues: Record<string, any> = {};

  for (let i = 0; i < args.positional.length; i++) {
    const val = args.positional[i];
    const fieldName = `positional_${i}`;
    structFields.push(field(fieldName, inferScalarType(val), true));
    structValues[fieldName] = val;
  }

  for (const [name, val] of args.named) {
    const fieldName = `named_${name}`;
    structFields.push(field(fieldName, inferScalarType(val), true));
    structValues[fieldName] = val;
  }

  const structType = makeStruct(structFields);
  const sch = makeSchema([field("args", structType, true)]);

  // Coerce JS numbers to BigInt for Int64 children — both backends require
  // BigInt at 64-bit width.
  const structRow: Record<string, any> = {};
  for (const f of structFields) {
    let val = structValues[f.name];
    if (isInt(f.type) && (f.type as any).bitWidth === 64 && typeof val === "number") {
      val = BigInt(val);
    }
    structRow[f.name] = val;
  }

  const batch = batchFromColumns({ args: [structRow] }, sch);
  return serializeBatch(batch);
}

function inferScalarType(val: any): VgiDataType {
  if (val === null || val === undefined) return nullType();
  if (typeof val === "string") return utf8();
  if (typeof val === "boolean") return bool();
  if (typeof val === "number") return int64();
  if (typeof val === "bigint") return int64();
  if (val instanceof Uint8Array || val instanceof ArrayBuffer) return binary();
  return utf8(); // fallback
}

export function deserializeArguments(bytes: Uint8Array): Arguments {
  if (!bytes || bytes.length === 0) return new Arguments();

  // Ensure we have a clean copy (not a view into a larger buffer)
  const cleanBytes = bytes.byteOffset !== 0 || bytes.byteLength !== bytes.buffer.byteLength
    ? new Uint8Array(bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength))
    : bytes;

  const batch = deserializeBatch(cleanBytes);
  const positional: any[] = [];
  const named = new Map<string, any>();

  if (batch.numRows === 0) {
    return new Arguments(positional, named);
  }

  // Arguments are serialized as a single "args" Struct column.
  // The struct has fields named "positional_0", "positional_1", etc.
  // for positional args, and "named_<name>" for named args.
  const argsCol = batch.getChild("args");
  if (argsCol) {
    // Extract per-positional types from the struct's children
    const argsStructType = argsCol.type as any;
    const structSchema = argsStructType.children
      ? makeSchema(argsStructType.children)
      : batch.schema;

    const structVal = argsCol.get(0) as any;
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
  const sch = batch.schema;
  if (!sch || sch.fields.length === 0) {
    return new Arguments(positional, named);
  }

  for (const f of sch.fields) {
    const col = batch.getChild(f.name);
    const val = col ? col.get(0) : null;

    if (f.name.startsWith("positional_")) {
      const idx = parseInt(f.name.slice("positional_".length), 10);
      while (positional.length <= idx) positional.push(null);
      positional[idx] = val;
    } else if (f.name.startsWith("named_")) {
      const name = f.name.slice("named_".length);
      named.set(name, val);
    } else {
      // Try numeric field name (old format)
      const metadata = f.metadata;
      const isNamed = metadata?.get?.("vgi_arg") === "named";
      if (isNamed) {
        named.set(f.name, val);
      } else {
        const idx = parseInt(f.name, 10);
        if (!isNaN(idx)) {
          while (positional.length <= idx) positional.push(null);
          positional[idx] = val;
        }
      }
    }
  }

  return new Arguments(positional, named, sch);
}
