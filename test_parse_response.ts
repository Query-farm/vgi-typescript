import { RecordBatchReader } from 'apache-arrow';
import fs from 'fs';

const data = fs.readFileSync('/tmp/worker_response.bin');
console.log('Response size:', data.length, 'bytes');

const reader = await RecordBatchReader.from(data);
await reader.open();
console.log('Reader opened, closed:', reader.closed);

if (!reader.closed) {
  const schema = reader.schema;
  console.log('Schema fields:', schema?.fields.map(f => f.name));
  console.log('Schema metadata:', schema?.metadata ? Object.fromEntries(schema.metadata) : 'none');

  for await (const batch of reader) {
    console.log('Batch:', batch.numRows, 'rows');
    console.log('Batch metadata:', batch.metadata ? Object.fromEntries(batch.metadata) : 'none');
    if (batch.numRows > 0) {
      for (const field of batch.schema.fields) {
        const col = batch.getChild(field.name);
        console.log(`  ${field.name}: ${col?.get(0)}`);
      }
    }
  }
}
