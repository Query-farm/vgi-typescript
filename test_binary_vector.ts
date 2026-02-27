// Test if vectorFromArray with Binary type corrupts data when nulls are present
import { vectorFromArray, Binary, RecordBatch, Schema, Field, makeData, Struct, RecordBatchStreamWriter, RecordBatchReader } from 'apache-arrow';

// Create test binary data
const data1 = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]);
const data2 = new Uint8Array(128);  // 128 bytes
for (let i = 0; i < 128; i++) data2[i] = i % 256;

// Test 1: All non-null
console.log("=== Test 1: All non-null ===");
const vec1 = vectorFromArray([data1, data2], new Binary());
console.log(`Vec1[0]: ${vec1.get(0)?.length} bytes`);
console.log(`Vec1[1]: ${vec1.get(1)?.length} bytes`);

// Test 2: First null, second non-null
console.log("\n=== Test 2: [null, data] ===");
const vec2 = vectorFromArray([null, data2], new Binary());
console.log(`Vec2[0]: ${vec2.get(0)}`);
console.log(`Vec2[1]: ${vec2.get(1)?.length} bytes`);
if (vec2.get(1)) {
  const retrieved = new Uint8Array(vec2.get(1));
  const original = data2;
  const same = Buffer.from(retrieved).equals(Buffer.from(original));
  console.log(`Data matches: ${same}`);
  if (!same) {
    console.log(`  Retrieved length: ${retrieved.length}`);
    console.log(`  Original length: ${original.length}`);
    // Find first difference
    for (let i = 0; i < Math.min(retrieved.length, original.length); i++) {
      if (retrieved[i] !== original[i]) {
        console.log(`  First diff at byte ${i}: got ${retrieved[i]}, expected ${original[i]}`);
        break;
      }
    }
  }
}

// Test 3: What about with a real serialized schema
console.log("\n=== Test 3: Real schema bytes ===");
import { Schema as S, Field as F, Utf8 } from 'apache-arrow';
function serializeSchema(schema: S): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.close();
  return writer.toUint8Array(true);
}

const idSchema = new S([new F("id", new Utf8(), false)]);
const idBytes = serializeSchema(idSchema);
console.log(`Schema bytes: ${idBytes.length}`);

const vec3 = vectorFromArray([null, idBytes], new Binary());
console.log(`Vec3[0]: ${vec3.get(0)}`);
const vec3val = vec3.get(1);
console.log(`Vec3[1]: ${vec3val?.length} bytes`);
if (vec3val) {
  const same = Buffer.from(new Uint8Array(vec3val)).equals(Buffer.from(idBytes));
  console.log(`Data matches: ${same}`);
  if (!same) {
    console.log(`  Retrieved: ${Buffer.from(new Uint8Array(vec3val)).toString('hex').slice(0, 80)}`);
    console.log(`  Original:  ${Buffer.from(idBytes).toString('hex').slice(0, 80)}`);
    console.log(`  Retrieved length: ${vec3val.length}`);
  }
}

// Test 4: Round-trip through IPC
console.log("\n=== Test 4: Round-trip through IPC ===");
const batchSchema = new Schema([
  new F("has_header", new (await import('apache-arrow')).Bool(), false),
  new F("header_schema_ipc", new Binary(), true),
]);

const hasHeaderVec = vectorFromArray([false, true], new (await import('apache-arrow')).Bool());
const headerSchemaVec = vectorFromArray([null, idBytes], new Binary());

const structType = new Struct(batchSchema.fields);
const batchData = makeData({
  type: structType,
  length: 2,
  children: [hasHeaderVec.data[0], headerSchemaVec.data[0]],
  nullCount: 0,
});

const batch = new RecordBatch(batchSchema, batchData);

// Serialize to IPC
const writer = new RecordBatchStreamWriter();
writer.reset(undefined, batchSchema);
writer.write(batch);
writer.close();
const ipcBytes = writer.toUint8Array(true);
console.log(`IPC bytes: ${ipcBytes.length}`);

// Read back
const reader = RecordBatchReader.from(ipcBytes);
for (const readBatch of reader) {
  const headerIpc = readBatch.getChild('header_schema_ipc');
  console.log(`Row 0 header_ipc: ${headerIpc?.get(0)}`);
  const row1 = headerIpc?.get(1);
  console.log(`Row 1 header_ipc: ${row1?.length} bytes`);
  if (row1) {
    const retrieved = new Uint8Array(row1);
    const same = Buffer.from(retrieved).equals(Buffer.from(idBytes));
    console.log(`Data matches after IPC round-trip: ${same}`);
    if (!same) {
      console.log(`  Retrieved (${retrieved.length}): ${Buffer.from(retrieved).toString('hex')}`);
      console.log(`  Original  (${idBytes.length}): ${Buffer.from(idBytes).toString('hex')}`);
    }
  }
}
