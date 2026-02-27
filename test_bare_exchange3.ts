// Test exchange with simple headerSchema
import { Protocol, VgiRpcServer } from "vgi-rpc";
import { Schema, Field, Utf8 } from "apache-arrow";

const protocol = new Protocol("test5");

protocol.unary("greet", {
  params: new Schema([new Field("name", new Utf8(), false)]),
  result: new Schema([new Field("message", new Utf8(), false)]),
  handler: (params) => ({ message: `Hello, ${params.name}!` }),
});

// Exchange with Utf8-only headerSchema
protocol.exchange("process", {
  params: new Schema([new Field("data", new Utf8(), false)]),
  inputSchema: new Schema([]),
  outputSchema: new Schema([]),
  headerSchema: new Schema([new Field("id", new Utf8(), false)]),
  init: () => ({}),
  exchange: async (state, input, out) => {},
  headerInit: () => ({ id: "test" }),
});

const server = new VgiRpcServer(protocol);
server.run().catch((err) => {
  process.stderr.write(`Error: ${err.message}\n`);
  process.exit(1);
});
