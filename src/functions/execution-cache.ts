// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// A small bounded LRU of per-execution framework context, keyed by
// (attach shard, execution id).
//
// Framework context an execution writes once and never changes (the buffered
// init, an aggregate's bind context) is read on every unary RPC of that
// execution. Caching the decoded value in-process saves one storage round trip
// per batch — 30-90 ms each on the Cloudflare DO. Storage stays authoritative:
// a miss (another process wrote it, the entry was evicted, a fresh isolate)
// falls through to a storage read, and the destructor evicts. Entries hold only
// values that were authenticated (opened from a sealed payload) or produced in
// this process. Sized for a Cloudflare isolate's memory, not a server's.

function hex(b: Uint8Array): string {
  let s = "";
  for (let i = 0; i < b.length; i++) s += b[i]!.toString(16).padStart(2, "0");
  return s;
}

export class ExecutionCache<V> {
  private readonly _map = new Map<string, V>();

  constructor(readonly maxEntries: number) {}

  private static _key(shardKey: string, executionId: Uint8Array): string {
    return `${shardKey}\u0000${hex(executionId)}`;
  }

  get(shardKey: string, executionId: Uint8Array): V | undefined {
    const k = ExecutionCache._key(shardKey, executionId);
    const v = this._map.get(k);
    if (v === undefined) return undefined;
    // Refresh recency.
    this._map.delete(k);
    this._map.set(k, v);
    return v;
  }

  set(shardKey: string, executionId: Uint8Array, value: V): void {
    const k = ExecutionCache._key(shardKey, executionId);
    this._map.delete(k);
    this._map.set(k, value);
    while (this._map.size > this.maxEntries) {
      this._map.delete(this._map.keys().next().value!);
    }
  }

  delete(shardKey: string, executionId: Uint8Array): void {
    this._map.delete(ExecutionCache._key(shardKey, executionId));
  }

  clear(): void {
    this._map.clear();
  }

  get size(): number {
    return this._map.size;
  }
}
