// Test if exchange method causes describe failure
import { Protocol, VgiRpcServer } from "vgi-rpc";
import { Schema, Field, Utf8, Binary, Int64 } from "apache-arrow";

const protocol = new Protocol("test3");

// Simple unary
protocol.unary("greet", {
  params: new Schema([new Field("name", new Utf8(), false)]),
  result: new Schema([new Field("message", new Utf8(), false)]),
  handler: (params) => ({ message: `Hello, ${params.name}!` }),
});

// Exchange method (like init)
protocol.exchange("process", {
  params: new Schema([new Field("data", new Binary(), false)]),
  inputSchema: new Schema([]),
  outputSchema: new Schema([]),
  headerSchema: new Schema([
    new Field("execution_id", new Binary(), false),
    new Field("max_workers", new Int64(), false),
  ]),
  init: () => ({}),
  exchange: async (state, input, out) => {},
  headerInit: () => ({
    execution_id: new Uint8Array(16),
    max_workers: 1,
  }),
});

process.stderr.write("Protocol methods: " + [...protocol.getMethods().keys()].join(", ") + "\n");
const server = new VgiRpcServer(protocol);
server.run().catch((err) => {
  process.stderr.write(`Error: ${err.message}\n`);
  process.exit(1);
});
