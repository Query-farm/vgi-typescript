// Test if the nested apache-arrow in vgi-rpc produces different serialization
import { Schema, Field, Utf8, RecordBatchStreamWriter } from 'apache-arrow';

// Our serialization
function serialize(schema: Schema): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.close();
  return writer.toUint8Array(true);
}

const idSchema = new Schema([new Field("id", new Utf8(), false)]);
const ourBytes = serialize(idSchema);
console.log(`Our serialization: ${ourBytes.length} bytes`);

// Now try to load from vgi-rpc's nested apache-arrow
// The vgi-rpc dist/index.js imports from "apache-arrow" which resolves
// through node_modules/vgi-rpc/node_modules/apache-arrow/
const nestedArrow = await import('/Users/rusty/Development/vgi-typescript/node_modules/vgi-rpc/node_modules/apache-arrow/index.ts');

const NestedSchema = nestedArrow.Schema;
const NestedField = nestedArrow.Field;
const NestedUtf8 = nestedArrow.Utf8;
const NestedWriter = nestedArrow.RecordBatchStreamWriter;

console.log(`\nSchema class same? ${Schema === NestedSchema}`);
console.log(`Field class same? ${Field === NestedField}`);
console.log(`Utf8 class same? ${Utf8 === NestedUtf8}`);
console.log(`RecordBatchStreamWriter same? ${RecordBatchStreamWriter === NestedWriter}`);

// Create schema using nested classes
const nestedSchema = new NestedSchema([new NestedField("id", new NestedUtf8(), false)]);
const nestedWriter = new NestedWriter();
nestedWriter.reset(undefined, nestedSchema);
nestedWriter.close();
const nestedBytes = nestedWriter.toUint8Array(true);
console.log(`\nNested serialization: ${nestedBytes.length} bytes`);

const same = Buffer.from(ourBytes).equals(Buffer.from(nestedBytes));
console.log(`Bytes match: ${same}`);

if (!same) {
  console.log(`Our:    ${Buffer.from(ourBytes).toString('hex')}`);
  console.log(`Nested: ${Buffer.from(nestedBytes).toString('hex')}`);
}

// Cross-module test: serialize OUR schema with NESTED writer
const crossWriter = new NestedWriter();
crossWriter.reset(undefined, idSchema);
crossWriter.close();
const crossBytes = crossWriter.toUint8Array(true);
console.log(`\nCross-module serialization: ${crossBytes.length} bytes`);
console.log(`Matches our: ${Buffer.from(ourBytes).equals(Buffer.from(crossBytes))}`);
console.log(`Matches nested: ${Buffer.from(nestedBytes).equals(Buffer.from(crossBytes))}`);
