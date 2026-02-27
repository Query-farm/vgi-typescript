// Test the actual describe batch structure for binary corruption
import {
  Schema, Field, Utf8, Bool, Binary, RecordBatch,
  RecordBatchStreamWriter, RecordBatchReader,
  vectorFromArray, makeData, Struct
} from 'apache-arrow';

function serializeSchema(schema: Schema): Uint8Array {
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  writer.close();
  return writer.toUint8Array(true);
}

// Build exact describe schema matching vgi-rpc
const DESCRIBE_SCHEMA = new Schema([
  new Field("name", new Utf8(), false),
  new Field("method_type", new Utf8(), false),
  new Field("doc", new Utf8(), true),
  new Field("has_return", new Bool(), false),
  new Field("params_schema_ipc", new Binary(), false),
  new Field("result_schema_ipc", new Binary(), false),
  new Field("param_types_json", new Utf8(), true),
  new Field("param_defaults_json", new Utf8(), true),
  new Field("has_header", new Bool(), false),
  new Field("header_schema_ipc", new Binary(), true),
]);

// Build test data for 2 methods (matching test_bare_exchange3)
const idSchema = new Schema([new Field("id", new Utf8(), false)]);
const headerBytes = serializeSchema(idSchema);
console.log(`headerSchema bytes: ${headerBytes.length}`);
console.log(`headerSchema hex: ${Buffer.from(headerBytes).toString('hex')}`);

const nameSchema = new Schema([new Field("name", new Utf8(), false)]);
const nameBytes = serializeSchema(nameSchema);
const msgSchema = new Schema([new Field("message", new Utf8(), false)]);
const msgBytes = serializeSchema(msgSchema);
const dataSchema = new Schema([new Field("data", new Utf8(), false)]);
const dataBytes = serializeSchema(dataSchema);
const emptySchema = new Schema([]);
const emptyBytes = serializeSchema(emptySchema);

const names = ["greet", "process"];
const methodTypes = ["unary", "stream"];
const docs = [null, null];
const hasReturns = [true, false];
const paramsSchemas = [nameBytes, dataBytes];
const resultSchemas = [msgBytes, emptyBytes];
const paramTypesJsons = ['{"name":"str"}', '{"data":"str"}'];
const paramDefaultsJsons = [null, null];
const hasHeaders = [false, true];
const headerSchemas: (Uint8Array | null)[] = [null, headerBytes];

// Build vectors
const nameArr = vectorFromArray(names, new Utf8());
const methodTypeArr = vectorFromArray(methodTypes, new Utf8());
const docArr = vectorFromArray(docs, new Utf8());
const hasReturnArr = vectorFromArray(hasReturns, new Bool());
const paramsSchemasArr = vectorFromArray(paramsSchemas, new Binary());
const resultSchemasArr = vectorFromArray(resultSchemas, new Binary());
const paramTypesArr = vectorFromArray(paramTypesJsons, new Utf8());
const paramDefaultsArr = vectorFromArray(paramDefaultsJsons, new Utf8());
const hasHeaderArr = vectorFromArray(hasHeaders, new Bool());
const headerSchemasArr = vectorFromArray(headerSchemas, new Binary());

// Check in-memory values BEFORE building batch
console.log("\n=== In-memory vector values ===");
const hmem = headerSchemasArr.get(1);
console.log(`headerSchemas[1] length: ${hmem?.length}`);
if (hmem) {
  const same = Buffer.from(new Uint8Array(hmem)).equals(Buffer.from(headerBytes));
  console.log(`Matches original: ${same}`);
}

// Build batch
const children = [
  nameArr.data[0], methodTypeArr.data[0], docArr.data[0],
  hasReturnArr.data[0], paramsSchemasArr.data[0], resultSchemasArr.data[0],
  paramTypesArr.data[0], paramDefaultsArr.data[0],
  hasHeaderArr.data[0], headerSchemasArr.data[0],
];

const structType = new Struct(DESCRIBE_SCHEMA.fields);
const data = makeData({
  type: structType, length: 2, children, nullCount: 0,
});

// Add batch-level metadata
const metadata = new Map<string, string>();
metadata.set("vgi_rpc.protocol_name", "test5");
metadata.set("vgi_rpc.request_version", "1");
metadata.set("vgi_rpc.describe_version", "2");
metadata.set("vgi_rpc.server_id", "test-id");

const batch = new RecordBatch(DESCRIBE_SCHEMA, data, metadata);

// Check batch values
console.log("\n=== Batch values ===");
const hbatch = batch.getChild('header_schema_ipc');
console.log(`header_schema_ipc[0]: ${hbatch?.get(0)}`);
const hval = hbatch?.get(1);
console.log(`header_schema_ipc[1]: ${hval?.length} bytes`);
if (hval) {
  const same = Buffer.from(new Uint8Array(hval)).equals(Buffer.from(headerBytes));
  console.log(`Matches original: ${same}`);
}

// Serialize to IPC stream
const writer = new RecordBatchStreamWriter();
writer.reset(undefined, DESCRIBE_SCHEMA);
writer.write(batch);
writer.close();
const ipcOutput = writer.toUint8Array(true);
console.log(`\nIPC output: ${ipcOutput.length} bytes`);

// Read back
const reader = RecordBatchReader.from(ipcOutput);
for (const readBatch of reader) {
  console.log(`\n=== Read back ===`);
  console.log(`Rows: ${readBatch.numRows}`);
  const hcol = readBatch.getChild('header_schema_ipc');
  console.log(`header_schema_ipc[0]: ${hcol?.get(0)}`);
  const hread = hcol?.get(1);
  console.log(`header_schema_ipc[1]: ${hread?.length} bytes`);
  if (hread) {
    const retrieved = new Uint8Array(hread);
    const same = Buffer.from(retrieved).equals(Buffer.from(headerBytes));
    console.log(`Matches original: ${same}`);
    if (!same) {
      console.log(`  Retrieved: ${Buffer.from(retrieved).toString('hex')}`);
      console.log(`  Original:  ${Buffer.from(headerBytes).toString('hex')}`);
    }
  }
}

// Save the IPC output for Python verification
import fs from 'fs';
fs.writeFileSync('/tmp/test_describe_roundtrip.bin', ipcOutput);
console.log("\nSaved IPC to /tmp/test_describe_roundtrip.bin");
