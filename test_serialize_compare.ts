// Test serializeSchema directly - compare our local vs what the dist produces
import { Schema, Field, Utf8, RecordBatchStreamWriter } from 'apache-arrow';

// Our local serializeSchema
function localSerialize(schema: Schema): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.close();
  return writer.toUint8Array(true);
}

// Import the dist index to get its internal serializeSchema
// We can't import it directly, but we CAN test by building a protocol and checking describe output
import { Protocol } from 'vgi-rpc';

const schema = new Schema([new Field("id", new Utf8(), false)]);

console.log("=== Local serialization ===");
const localBytes = localSerialize(schema);
console.log(`Length: ${localBytes.length}`);
console.log(`Hex: ${Buffer.from(localBytes).toString('hex')}`);

// Now create a minimal protocol with headerSchema and extract describe
console.log("\n=== Protocol with headerSchema ===");
const protocol = new Protocol("test");
protocol.exchange("test_method", {
  params: new Schema([new Field("x", new Utf8(), false)]),
  inputSchema: new Schema([]),
  outputSchema: new Schema([]),
  init: () => ({}),
  exchange: async () => {},
  headerSchema: schema,
  headerInit: () => ({ id: "test" }),
});

// Access methods directly
const methods = protocol.getMethods();
const method = methods.get("test_method")!;
console.log("headerSchema identity same?", method.headerSchema === schema);
console.log("headerSchema fields:", method.headerSchema?.fields.map(f => `${f.name}: ${f.type}`));

// Serialize the headerSchema using our local function
const headerBytes = localSerialize(method.headerSchema!);
console.log(`Header serialized locally: ${headerBytes.length} bytes`);
console.log(`Same as direct? ${Buffer.from(headerBytes).equals(Buffer.from(localBytes))}`);

// Now let's also serialize the params schema
const paramsSchema = method.paramsSchema;
const paramsBytes = localSerialize(paramsSchema);
console.log(`\nParams schema local: ${paramsBytes.length} bytes`);
console.log(`Hex: ${Buffer.from(paramsBytes).toString('hex')}`);

// Compare with what we know the describe response produces
// describe_with_header.bin has params=128 bytes and header=120 bytes
// Our local serialization gives: params=136 and header=128
// So there's a consistent 8-byte difference

// Let me check if dist uses a different RecordBatchStreamWriter
import fs from 'fs';
const distCode = fs.readFileSync('/Users/rusty/Development/vgi-rpc-typescript/dist/index.js', 'utf-8');
const serializeMatch = distCode.match(/function serializeSchema[\s\S]*?^}/m);
console.log("\n=== dist serializeSchema ===");

// Extract and show the relevant section
const startIdx = distCode.indexOf('function serializeSchema');
const endIdx = distCode.indexOf('}', startIdx) + 1;
console.log(distCode.slice(startIdx, endIdx));

// Check which RecordBatchStreamWriter it uses
const importsBefore = distCode.slice(Math.max(0, startIdx - 200), startIdx);
console.log("\nImports before serializeSchema:");
console.log(importsBefore);
