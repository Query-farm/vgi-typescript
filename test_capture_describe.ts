// Capture the describe response for the exchange-with-header case
import { spawn } from 'child_process';
import { Schema, Field, Utf8, Binary, RecordBatch, RecordBatchStreamWriter, Struct, makeData } from 'apache-arrow';
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

// Test against the exchange-with-header worker
const proc = spawn('bun', ['run', 'test_bare_exchange3.ts'], {
  cwd: '/Users/rusty/Development/vgi-typescript',
  stdio: ['pipe', 'pipe', 'pipe']
});

const stdoutChunks: Buffer[] = [];
proc.stdout.on('data', (chunk: Buffer) => stdoutChunks.push(chunk));
proc.stderr.on('data', (chunk: Buffer) => process.stderr.write(chunk));

proc.on('exit', (code: number) => {
  const all = Buffer.concat(stdoutChunks);
  console.log('Exit:', code, 'Response:', all.length, 'bytes');
  if (all.length > 0) {
    fs.writeFileSync('/tmp/exchange_describe.bin', all);
    console.log('Saved to /tmp/exchange_describe.bin');
  }
});

proc.stdin.write(ipcBytes, () => {
  setTimeout(() => proc.stdin.end(), 2000);
});

setTimeout(() => proc.kill(), 5000);
