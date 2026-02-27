// Check which apache-arrow modules are being loaded
import { Schema as OurSchema, RecordBatchStreamWriter } from 'apache-arrow';
import { Protocol } from 'vgi-rpc';

// Try to find the actual file paths
console.log("Our Schema:", OurSchema);
console.log("Our RecordBatchStreamWriter:", RecordBatchStreamWriter);

// Check if vgi-rpc is loaded from source or dist
const vgiRpcPath = import.meta.resolve('vgi-rpc');
console.log("vgi-rpc resolves to:", vgiRpcPath);

const arrowPath = import.meta.resolve('apache-arrow');
console.log("apache-arrow resolves to:", arrowPath);

// Try to import apache-arrow from within vgi-rpc's context
import { Schema as VgiSchema } from 'apache-arrow';
console.log("Same Schema class?", OurSchema === VgiSchema);

// Check node_modules structure
import { existsSync } from 'fs';
const paths = [
  '/Users/rusty/Development/vgi-typescript/node_modules/apache-arrow',
  '/Users/rusty/Development/vgi-rpc-typescript/node_modules/apache-arrow',
  '/Users/rusty/Development/vgi-typescript/node_modules/vgi-rpc',
];
for (const p of paths) {
  console.log(`${p}: ${existsSync(p) ? 'EXISTS' : 'not found'}`);
}
