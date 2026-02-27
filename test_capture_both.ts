// Capture describe responses from both with-header and without-header workers
import { spawn } from 'child_process';
import { Schema, RecordBatch, RecordBatchStreamWriter, Struct, makeData } from 'apache-arrow';
import fs from 'fs';

// Build a __describe__ request
const schema = new Schema([]);
const metadata = new Map<string, string>();
metadata.set('vgi_rpc.method', '__describe__');
metadata.set('vgi_rpc.request_version', '1');

const structType = new Struct([]);
const data = makeData({ type: structType, length: 0, children: [], nullCount: 0 });
const batch = new RecordBatch(schema, data, metadata);

const writer = new RecordBatchStreamWriter();
writer.reset(undefined, schema);
writer.write(batch);
writer.close();
const ipcBytes = writer.toUint8Array(true);

function testWorker(workerPath: string, outputFile: string): Promise<void> {
  return new Promise((resolve) => {
    const proc = spawn('bun', ['run', workerPath], {
      cwd: '/Users/rusty/Development/vgi-typescript',
      stdio: ['pipe', 'pipe', 'pipe']
    });

    const stdoutChunks: Buffer[] = [];
    proc.stdout.on('data', (chunk: Buffer) => stdoutChunks.push(chunk));
    proc.stderr.on('data', (chunk: Buffer) => process.stderr.write(chunk));

    proc.on('exit', (code: number) => {
      const all = Buffer.concat(stdoutChunks);
      console.log(`${workerPath}: Exit=${code}, Response=${all.length} bytes`);
      if (all.length > 0) {
        fs.writeFileSync(outputFile, all);
        console.log(`  Saved to ${outputFile}`);
      }
      resolve();
    });

    proc.stdin.write(ipcBytes, () => {
      setTimeout(() => proc.stdin.end(), 2000);
    });

    setTimeout(() => proc.kill(), 5000);
  });
}

async function main() {
  // Test without header (should work)
  await testWorker('test_bare_exchange2.ts', '/tmp/describe_no_header.bin');

  // Test with header (should fail on Python side)
  await testWorker('test_bare_exchange3.ts', '/tmp/describe_with_header.bin');

  console.log('\nDone. Now compare the binary files.');
}

main();
