// Debug: VGI protocol build + serve
import { VgiRpcServer } from "vgi-rpc";
import { FunctionRegistry } from "./src/functions/registry.js";
import { buildVgiProtocol } from "./src/protocol/dispatch.js";
import { scalarFunctions } from "./examples/scalar.js";

const registry = new FunctionRegistry();
registry.register(scalarFunctions[0]);

process.stderr.write("Building protocol...\n");
try {
  const protocol = buildVgiProtocol({ registry });
  process.stderr.write("Protocol built OK. Methods: " + [...protocol.getMethods().keys()].join(", ") + "\n");

  const server = new VgiRpcServer(protocol);
  process.stderr.write("Server created, running...\n");
  server.run().catch((err) => {
    process.stderr.write(`Server error: ${err.message}\n${err.stack}\n`);
    process.exit(1);
  });
} catch (err: any) {
  process.stderr.write(`Build error: ${err.message}\n${err.stack}\n`);
  process.exit(1);
}
