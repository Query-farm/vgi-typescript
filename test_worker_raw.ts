import { spawn } from 'child_process';
import { Schema, Field, Utf8, RecordBatch, RecordBatchStreamWriter, Struct, makeData } from 'apache-arrow';
import fs from 'fs';

// Build a __describe__ request with metadata on the batch
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

console.log('Request size:', ipcBytes.length, 'bytes');

// Spawn the worker
const proc = spawn('bun', ['run', 'examples/worker.ts'], {
  cwd: '/Users/rusty/Development/vgi-typescript',
  stdio: ['pipe', 'pipe', 'pipe']
});

const stdoutChunks: Buffer[] = [];
proc.stdout.on('data', (chunk: Buffer) => {
  stdoutChunks.push(chunk);
  console.log('Got stdout chunk:', chunk.length, 'bytes');
});

proc.stderr.on('data', (chunk: Buffer) => {
  console.log('Worker stderr:', chunk.toString().trim());
});

proc.on('exit', (code: number) => {
  console.log('Worker exit code:', code);
  const all = Buffer.concat(stdoutChunks);
  console.log('Total stdout:', all.length, 'bytes');
  if (all.length > 0) {
    console.log('First 40 bytes:', Array.from(all.slice(0, 40)).map(b => b.toString(16).padStart(2, '0')).join(' '));
    fs.writeFileSync('/tmp/worker_response.bin', all);
    console.log('Wrote response to /tmp/worker_response.bin');
  }
});

// Write the request then close stdin after delay
proc.stdin.write(ipcBytes, () => {
  console.log('Request sent');
  setTimeout(() => {
    proc.stdin.end();
    console.log('stdin closed');
  }, 3000);
});

setTimeout(() => {
  console.log('Timeout - killing');
  proc.kill();
}, 6000);
