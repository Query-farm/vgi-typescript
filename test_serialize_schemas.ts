// Test serializing schemas directly and compare
import { Schema, Field, Utf8, RecordBatchStreamWriter, RecordBatchReader } from 'apache-arrow';

function serializeSchema(schema: Schema): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.close();
  return writer.toUint8Array(true);
}

// Test schemas
const schemas = {
  "name_utf8": new Schema([new Field("name", new Utf8(), false)]),
  "id_utf8": new Schema([new Field("id", new Utf8(), false)]),
  "data_utf8": new Schema([new Field("data", new Utf8(), false)]),
  "empty": new Schema([]),
};

for (const [label, schema] of Object.entries(schemas)) {
  const bytes = serializeSchema(schema);
  console.log(`\n${label}: ${bytes.length} bytes`);
  console.log(`  Hex: ${Buffer.from(bytes).toString('hex')}`);

  // Verify we can read it back
  try {
    const reader = RecordBatchReader.from(bytes);
    const readSchema = reader.schema;
    console.log(`  Read back: ${readSchema.fields.length} fields: [${readSchema.fields.map(f => `${f.name}: ${f.type}`).join(', ')}]`);
  } catch (e: any) {
    console.log(`  Read back FAILED: ${e.message}`);
  }
}

// Compare our bytes with the header bytes from the describe response
import fs from 'fs';
const describeData = fs.readFileSync('/tmp/describe_with_header.bin');

// Also - the REAL question: try reading back the header schema from describe response
// Read the describe response as Arrow IPC
const describeReader = RecordBatchReader.from(describeData);
for (const batch of describeReader) {
  console.log(`\nDescribe batch: ${batch.numRows} rows`);
  for (let i = 0; i < batch.numRows; i++) {
    const name = batch.getChild('name')?.get(i);
    const headerIpc = batch.getChild('header_schema_ipc')?.get(i);
    const paramsIpc = batch.getChild('params_schema_ipc')?.get(i);
    console.log(`\n  Row ${i}: ${name}`);
    console.log(`    params_ipc: ${paramsIpc ? paramsIpc.length + ' bytes' : 'null'}`);
    console.log(`    header_ipc: ${headerIpc ? headerIpc.length + ' bytes' : 'null'}`);

    if (paramsIpc) {
      const paramsBytes = new Uint8Array(paramsIpc);
      console.log(`    params hex: ${Buffer.from(paramsBytes).toString('hex')}`);
      try {
        const pr = RecordBatchReader.from(paramsBytes);
        console.log(`    params parsed: ${pr.schema.fields.map(f => `${f.name}:${f.type}`).join(', ')}`);
      } catch (e: any) {
        console.log(`    params parse FAILED: ${e.message}`);
      }
    }

    if (headerIpc) {
      const headerBytes = new Uint8Array(headerIpc);
      console.log(`    header hex: ${Buffer.from(headerBytes).toString('hex')}`);
      try {
        const hr = RecordBatchReader.from(headerBytes);
        console.log(`    header parsed: ${hr.schema.fields.map(f => `${f.name}:${f.type}`).join(', ')}`);
      } catch (e: any) {
        console.log(`    header parse FAILED: ${e.message}`);
      }
    }
  }
}
