// Empty (0-row) Arrow batch construction. Handles complex types (List, Map,
// Struct) that need child data to round-trip through Arrow IPC serialization.

import {
  RecordBatch,
  Schema,
  Field,
  Struct,
  List,
  DataType,
  makeData,
  Map_,
} from "@query-farm/apache-arrow";

/**
 * Create empty (0-length) data for any Arrow type, handling complex types
 * that need child data (List, Map, Struct) to be serializable.
 */
function emptyData(field: Field): any {
  const type = field.type;

  if (DataType.isList(type)) {
    const childField = (type as List).children[0];
    return makeData({
      type,
      length: 0,
      nullCount: 0,
      child: emptyData(childField),
      valueOffsets: new Int32Array([0]),
    });
  }

  if (DataType.isMap(type)) {
    const entriesField = (type as Map_).children[0];
    return makeData({
      type,
      length: 0,
      nullCount: 0,
      child: emptyData(entriesField),
      valueOffsets: new Int32Array([0]),
    });
  }

  if (DataType.isStruct(type)) {
    const children = type.children.map((child: Field) => emptyData(child));
    return makeData({ type, length: 0, nullCount: 0, children });
  }

  return makeData({ type, length: 0, nullCount: 0 });
}

/**
 * Create an empty (0-row) batch with the given schema.
 */
export function emptyBatch(schema: Schema): RecordBatch {
  const children = schema.fields.map((f: Field) => emptyData(f));

  const structType = new Struct(schema.fields);
  const data = makeData({
    type: structType,
    length: 0,
    children,
    nullCount: 0,
  });

  return new RecordBatch(schema, data);
}
