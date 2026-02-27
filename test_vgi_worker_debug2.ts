// Debug: Find which method registration causes the describe failure
import { Protocol, VgiRpcServer } from "vgi-rpc";
import { Schema, Field, Binary, Utf8, Int64, Bool } from "apache-arrow";

const protocol = new Protocol("vgi");

// Register bind (simple schema, no List types)
protocol.unary("bind", {
  params: new Schema([
    new Field("function_name", new Utf8(), false),
    new Field("arguments", new Binary(), false),
  ]),
  result: new Schema([
    new Field("output_schema", new Binary(), false),
  ]),
  handler: () => ({ output_schema: new Uint8Array(0) }),
});

process.stderr.write("Protocol built with 1 method\n");

const server = new VgiRpcServer(protocol);
server.run().catch((err) => {
  process.stderr.write(`Error: ${err.message}\n`);
  process.exit(1);
});
