// Check how imports resolve
console.log("vgi-rpc:", import.meta.resolve('vgi-rpc'));
console.log("apache-arrow:", import.meta.resolve('apache-arrow'));

// Check from vgi-rpc's perspective
const vgiRpcDir = '/Users/rusty/Development/vgi-typescript/node_modules/vgi-rpc/';
console.log("\n=== Checking vgi-rpc exports ===");
import fs from 'fs';
const pkg = JSON.parse(fs.readFileSync(vgiRpcDir + 'package.json', 'utf-8'));
console.log("exports:", JSON.stringify(pkg.exports, null, 2));
