// Bare worker with Binary field
import { Protocol, VgiRpcServer } from "vgi-rpc";
import { Schema, Field, Utf8, Binary } from "apache-arrow";

const protocol = new Protocol("test2");

protocol.unary("store", {
  params: new Schema([
    new Field("name", new Utf8(), false),
    new Field("data", new Binary(), false),
  ]),
  result: new Schema([
    new Field("status", new Utf8(), false),
  ]),
  handler: () => ({ status: "ok" }),
});

const server = new VgiRpcServer(protocol);
server.run().catch((err) => {
  process.stderr.write(`Error: ${err.message}\n`);
  process.exit(1);
});
