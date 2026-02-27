// Bare minimum VGI RPC server (no VGI functions)
import { Protocol, VgiRpcServer } from "vgi-rpc";
import { Schema, Field, Utf8, Float64 } from "apache-arrow";

const protocol = new Protocol("test");

// Register one simple unary method
protocol.unary("greet", {
  params: new Schema([new Field("name", new Utf8(), false)]),
  result: new Schema([new Field("message", new Utf8(), false)]),
  handler: (params) => {
    return { message: `Hello, ${params.name}!` };
  },
});

const server = new VgiRpcServer(protocol);
server.run().catch((err) => {
  process.stderr.write(`Error: ${err.message}\n`);
  process.exit(1);
});
