// Storage for VGI function state.
// Provides shared state across worker processes for distributed execution.
// Port of vgi-python's vgi/function_storage.py.
//
// The interface is async so HTTP-backed implementations (Cloudflare DO,
// Azure SQL, etc.) can use fetch/network drivers without sync hacks.
// FunctionStorageSqlite wraps bun:sqlite's sync calls in resolved promises.

// FunctionStorageSqlite is the only thing in this module that needs Bun + node:*
// modules. Workers (Cloudflare workerd / browsers) should use
// FunctionStorageCfDo (HTTP-backed) instead — they never reach this class.
// We resolve all node:* and bun:* dependencies through indirect string
// variables that esbuild can't trace statically, so workerd bundles cleanly
// without nodejs_compat polyfills for paths we never run.
type Database = any;
const _BUN_SQLITE_MOD = "bun:sqlite";
const _NODE_OS_MOD = "node:os";
const _NODE_PATH_MOD = "node:path";
const _NODE_FS_MOD = "node:fs";
let _Database: any = null;
function _req(): any {
  const req: any = (import.meta as any).require ?? (globalThis as any).require ?? null;
  if (!req) {
    throw new Error(
      "FunctionStorageSqlite requires Node.js or Bun (bun:sqlite + node:os/path/fs). " +
        "Use FunctionStorageCfDo on non-Bun runtimes.",
    );
  }
  return req;
}
function _loadDatabase(): any {
  if (_Database) return _Database;
  try {
    _Database = _req()(_BUN_SQLITE_MOD).Database;
    return _Database;
  } catch (e: any) {
    throw new Error(
      `FunctionStorageSqlite: failed to load bun:sqlite — ${e?.message ?? e}`,
    );
  }
}
function _loadNodeIo(): {
  homedir: () => string;
  platform: () => string;
  join: (...parts: string[]) => string;
  mkdirSync: (path: string, opts?: any) => void;
} {
  const r = _req();
  return {
    homedir: r(_NODE_OS_MOD).homedir,
    platform: r(_NODE_OS_MOD).platform,
    join: r(_NODE_PATH_MOD).join,
    mkdirSync: r(_NODE_FS_MOD).mkdirSync,
  };
}

// ============================================================================
// Errors
// ============================================================================

export class UnknownInvocationError extends Error {
  constructor(message?: string | Uint8Array) {
    let text: string;
    if (message instanceof Uint8Array) {
      text =
        `Invocation ${hexEncode(message)} is not registered. ` +
        "Call queuePush first to register the invocation.";
    } else {
      text =
        message ??
        "Invocation is not registered. Call queuePush first to register the invocation.";
    }
    super(text);
    this.name = "UnknownInvocationError";
  }
}

function hexEncode(buf: Uint8Array): string {
  return Array.from(buf, (b) => b.toString(16).padStart(2, "0")).join("");
}

// ============================================================================
// FunctionStorage interface
// ============================================================================

export interface FunctionStorage {
  workerPut(executionId: Uint8Array, workerId: number, state: Uint8Array): Promise<void>;
  workerCollect(executionId: Uint8Array): Promise<Uint8Array[]>;
  workerScan(executionId: Uint8Array): Promise<Array<[number, Uint8Array]>>;
  queuePush(executionId: Uint8Array, items: Uint8Array[]): Promise<number>;
  queuePop(executionId: Uint8Array): Promise<Uint8Array | null>;
  queueClear(executionId: Uint8Array): Promise<number>;
  // --- Namespaced key/value state (scoped by execution_id) ---
  stateGet(scopeId: Uint8Array, ns: Uint8Array, key: Uint8Array): Promise<Uint8Array | null>;
  statePut(scopeId: Uint8Array, ns: Uint8Array, key: Uint8Array, value: Uint8Array): Promise<void>;
  // --- Append-only log (scoped by execution_id) ---
  stateAppend(scopeId: Uint8Array, ns: Uint8Array, key: Uint8Array, item: Uint8Array): Promise<number>;
  stateLogScan(
    scopeId: Uint8Array,
    ns: Uint8Array,
    key: Uint8Array,
    afterId?: number,
    limit?: number | null,
  ): Promise<Array<[number, Uint8Array]>>;
  /** Wipe all state + log rows for a scope across every namespace. */
  executionClear(scopeId: Uint8Array): Promise<number>;
  /** Optional cleanup hook. Implementations holding a connection should release it here. */
  close?(): Promise<void> | void;
}

// FrameworkNS — namespaces the framework itself uses (mirrors Python's
// FrameworkNS enum). User code picks its own arbitrary namespace bytes.
export const FrameworkNS = {
  BUFFERING_INIT: textBytes("_vgi/buffering_init"),
  STREAMING_FINALIZE: textBytes("_vgi/streaming_finalize"),
} as const;

function textBytes(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

/** Encode an int as 8-byte little-endian, matching Python's pack_int_key. */
export function packIntKey(i: number | bigint): Uint8Array {
  const buf = new ArrayBuffer(8);
  new DataView(buf).setBigInt64(0, BigInt(i), true);
  return new Uint8Array(buf);
}

// ============================================================================
// FunctionStorageSqlite
// ============================================================================

function getDefaultDbPath(): string {
  // VGI_WORKER_SQLITE_PATH=":memory:" picks an in-process backend used by
  // single-process test fixtures (notably the HTTP server fixture) to avoid
  // per-op WAL fsync cost. Mirrors vgi-python's same env var.
  const override = process.env.VGI_WORKER_SQLITE_PATH;
  if (override) return override;
  const { homedir, platform, join, mkdirSync } = _loadNodeIo();
  const home = homedir();
  const stateDir =
    platform() === "darwin"
      ? join(home, "Library", "Application Support", "vgi")
      : join(home, ".local", "state", "vgi");
  mkdirSync(stateDir, { recursive: true });
  return join(stateDir, "vgi_storage.db");
}

// Worker state rides the unified function_state table under a reserved
// namespace, keyed by the worker/process id (8-byte big-endian) — exactly the
// mapping the Cloudflare DO client uses, so both backends share one schema.
const NS_WORKER = new TextEncoder().encode("worker");

/** Encode an int64 worker id as an 8-byte big-endian state key. */
function int64Key(v: number): Uint8Array {
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigInt64(0, BigInt(v), false);
  return b;
}

function int64FromKey(b: Uint8Array): number {
  if (b.length !== 8) return 0;
  return Number(new DataView(b.buffer, b.byteOffset, 8).getBigInt64(0, false));
}

export class FunctionStorageSqlite implements FunctionStorage {
  readonly dbPath: string;
  private _db: Database;

  constructor(dbPath?: string) {
    this.dbPath = dbPath ?? getDefaultDbPath();
    // Persistent connection for the lifetime of this storage instance.
    // Bun is single-threaded, so one connection is the analogue of
    // Python's per-thread persistent connection. WAL still coordinates
    // writes across processes via file locking.
    const Db = _loadDatabase();
    this._db = new Db(this.dbPath);
    this._db.exec("PRAGMA journal_mode=WAL");
    this._db.exec("PRAGMA synchronous=NORMAL");
    this._db.exec("PRAGMA busy_timeout=30000");
    this._db.exec("PRAGMA temp_store=MEMORY");
    this._db.exec("PRAGMA cache_size=-65536");
    this._ensureTables();
  }

  close(): void {
    this._db.close();
  }

  private _ensureTables(): void {
    const db = this._db;
    // Self-heal an older on-disk DB to the unified minimal schema. The local
    // SQLite tier carries none of the DO's HTTP idempotency machinery and no
    // created_at, so drop any table left over with those columns; all of this
    // is ephemeral in-progress state, so dropping + recreating is safe.
    for (const [table, staleCol] of [
      ["function_state", "created_at"],
      ["function_state_log", "created_at"],
      ["work_queue", "created_at"],
    ] as const) {
      const info = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>;
      if (info.some((row) => row.name === staleCol)) {
        db.exec(`DROP TABLE IF EXISTS ${table}`);
      }
    }
    // Tables eliminated by the unified schema: worker collect now rides
    // function_state (ns=worker), and the queue carries no registration.
    for (const dead of ["global_state_storage", "worker_state", "invocation_registry", "init_storage"]) {
      db.exec(`DROP TABLE IF EXISTS ${dead}`);
    }

    // Unified schema — the same three tables every backend uses (the Durable
    // Object adds an HTTP-idempotency column layer on top).
    db.exec(`
      CREATE TABLE IF NOT EXISTS work_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        execution_id BLOB NOT NULL,
        work_item BLOB NOT NULL
      )
    `);
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_work_queue_execution
      ON work_queue(execution_id, id)
    `);
    // Composite-key K/V over (scope_id, ns, key): the single home for
    // per-execution / per-transaction / per-group / per-worker state.
    db.exec(`
      CREATE TABLE IF NOT EXISTS function_state (
        scope_id BLOB NOT NULL,
        ns BLOB NOT NULL,
        key BLOB NOT NULL,
        value BLOB NOT NULL,
        PRIMARY KEY (scope_id, ns, key)
      )
    `);
    // Append-only log keyed by (scope, ns, key); the AUTOINCREMENT id is the
    // globally-monotonic scan cursor.
    db.exec(`
      CREATE TABLE IF NOT EXISTS function_state_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scope_id BLOB NOT NULL,
        ns BLOB NOT NULL,
        key BLOB NOT NULL,
        value BLOB NOT NULL
      )
    `);
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_function_state_log_lookup
      ON function_state_log(scope_id, ns, key, id)
    `);
  }

  async stateGet(scopeId: Uint8Array, ns: Uint8Array, key: Uint8Array): Promise<Uint8Array | null> {
    const row = this._db
      .prepare("SELECT value FROM function_state WHERE scope_id = ? AND ns = ? AND key = ?")
      .get(bufferArg(scopeId), bufferArg(ns), bufferArg(key)) as { value: Uint8Array } | null;
    return row ? new Uint8Array(row.value) : null;
  }

  async statePut(scopeId: Uint8Array, ns: Uint8Array, key: Uint8Array, value: Uint8Array): Promise<void> {
    this._db
      .prepare(
        "INSERT OR REPLACE INTO function_state (scope_id, ns, key, value) VALUES (?, ?, ?, ?)",
      )
      .run(bufferArg(scopeId), bufferArg(ns), bufferArg(key), bufferArg(value));
  }

  async stateAppend(scopeId: Uint8Array, ns: Uint8Array, key: Uint8Array, item: Uint8Array): Promise<number> {
    const res = this._db
      .prepare("INSERT INTO function_state_log (scope_id, ns, key, value) VALUES (?, ?, ?, ?)")
      .run(bufferArg(scopeId), bufferArg(ns), bufferArg(key), bufferArg(item));
    return Number(res.lastInsertRowid);
  }

  async stateLogScan(
    scopeId: Uint8Array,
    ns: Uint8Array,
    key: Uint8Array,
    afterId: number = -1,
    limit: number | null = null,
  ): Promise<Array<[number, Uint8Array]>> {
    let sql =
      "SELECT id, value FROM function_state_log " +
      "WHERE scope_id = ? AND ns = ? AND key = ? AND id > ? ORDER BY id ASC";
    const args: any[] = [bufferArg(scopeId), bufferArg(ns), bufferArg(key), afterId];
    if (limit != null) {
      sql += " LIMIT ?";
      args.push(limit);
    }
    const rows = this._db.prepare(sql).all(...args) as Array<{ id: number; value: Uint8Array }>;
    return rows.map((r) => [Number(r.id), new Uint8Array(r.value)]);
  }

  async executionClear(scopeId: Uint8Array): Promise<number> {
    const eid = bufferArg(scopeId);
    let total = 0;
    total += (this._db.prepare("DELETE FROM function_state WHERE scope_id = ?").run(eid).changes as number);
    total += (this._db.prepare("DELETE FROM function_state_log WHERE scope_id = ?").run(eid).changes as number);
    return total;
  }

  // Worker state rides function_state under ns=worker, keyed by worker id.

  async workerPut(
    executionId: Uint8Array,
    workerId: number,
    state: Uint8Array
  ): Promise<void> {
    this._db
      .prepare(
        "INSERT OR REPLACE INTO function_state (scope_id, ns, key, value) VALUES (?, ?, ?, ?)"
      )
      .run(bufferArg(executionId), bufferArg(NS_WORKER), bufferArg(int64Key(workerId)), bufferArg(state));
  }

  async workerCollect(executionId: Uint8Array): Promise<Uint8Array[]> {
    const rows = this._db
      .prepare(
        "DELETE FROM function_state WHERE scope_id = ? AND ns = ? RETURNING value"
      )
      .all(bufferArg(executionId), bufferArg(NS_WORKER)) as Array<{ value: Uint8Array }>;
    return rows.map((row) => new Uint8Array(row.value));
  }

  async workerScan(executionId: Uint8Array): Promise<Array<[number, Uint8Array]>> {
    const rows = this._db
      .prepare(
        "SELECT key, value FROM function_state WHERE scope_id = ? AND ns = ?"
      )
      .all(bufferArg(executionId), bufferArg(NS_WORKER)) as Array<{
      key: Uint8Array;
      value: Uint8Array;
    }>;
    return rows.map((row) => [int64FromKey(new Uint8Array(row.key)), new Uint8Array(row.value)]);
  }

  async queuePush(executionId: Uint8Array, items: Uint8Array[]): Promise<number> {
    const eidBuf = bufferArg(executionId);
    this._db.exec("BEGIN");
    try {
      const insert = this._db.prepare(
        "INSERT INTO work_queue (execution_id, work_item) VALUES (?, ?)"
      );
      for (const item of items) {
        insert.run(eidBuf, bufferArg(item));
      }
      this._db.exec("COMMIT");
    } catch (e) {
      this._db.exec("ROLLBACK");
      throw e;
    }
    return items.length;
  }

  async queuePop(executionId: Uint8Array): Promise<Uint8Array | null> {
    // No registration: an empty or never-pushed queue both return null,
    // matching the Durable Object.
    const row = this._db
      .prepare(
        "DELETE FROM work_queue WHERE id = (" +
          "  SELECT id FROM work_queue WHERE execution_id = ? ORDER BY id ASC LIMIT 1" +
          ") RETURNING work_item"
      )
      .get(bufferArg(executionId)) as { work_item: Uint8Array } | null;

    return row ? new Uint8Array(row.work_item) : null;
  }

  async queueClear(executionId: Uint8Array): Promise<number> {
    return this._db
      .prepare("DELETE FROM work_queue WHERE execution_id = ?")
      .run(bufferArg(executionId)).changes;
  }
}

// ============================================================================
// BoundStorage
// ============================================================================

export class BoundStorage {
  private _base: FunctionStorage;
  private _executionId: Uint8Array;

  constructor(storage: FunctionStorage, executionId: Uint8Array) {
    this._base = storage;
    this._executionId = executionId;
  }

  put(state: Uint8Array): Promise<void> {
    return this._base.workerPut(this._executionId, process.pid, state);
  }

  collect(): Promise<Uint8Array[]> {
    return this._base.workerCollect(this._executionId);
  }

  workerScan(): Promise<Array<[number, Uint8Array]>> {
    return this._base.workerScan(this._executionId);
  }

  queuePush(items: Uint8Array[]): Promise<number> {
    return this._base.queuePush(this._executionId, items);
  }

  queuePop(): Promise<Uint8Array | null> {
    return this._base.queuePop(this._executionId);
  }

  queueClear(): Promise<number> {
    return this._base.queueClear(this._executionId);
  }

  stateGet(ns: Uint8Array, key: Uint8Array): Promise<Uint8Array | null> {
    return this._base.stateGet(this._executionId, ns, key);
  }

  statePut(ns: Uint8Array, key: Uint8Array, value: Uint8Array): Promise<void> {
    return this._base.statePut(this._executionId, ns, key, value);
  }

  stateAppend(ns: Uint8Array, key: Uint8Array, item: Uint8Array): Promise<number> {
    return this._base.stateAppend(this._executionId, ns, key, item);
  }

  stateLogScan(
    ns: Uint8Array,
    key: Uint8Array,
    afterId: number = -1,
    limit: number | null = null,
  ): Promise<Array<[number, Uint8Array]>> {
    return this._base.stateLogScan(this._executionId, ns, key, afterId, limit);
  }

  executionClear(): Promise<number> {
    return this._base.executionClear(this._executionId);
  }

  static packIntKey(i: number | bigint): Uint8Array {
    return packIntKey(i);
  }
}

// ============================================================================
// Backend factory + lazy default storage
// ============================================================================

/**
 * Resolve the FunctionStorage backend from environment variables.
 *
 * Mirrors vgi-python's `_resolve_storage` in `vgi/function.py`.
 *
 * `VGI_WORKER_SHARED_STORAGE` selects the backend (default: `sqlite`):
 *   - `memory`        — `FunctionStorageSqlite` at `:memory:`. Process-local
 *                       with no cross-process coordination — single-process
 *                       deployments only. Ignores `VGI_WORKER_SQLITE_PATH`.
 *   - `sqlite`        — `FunctionStorageSqlite`. Honors `VGI_WORKER_SQLITE_PATH`
 *                       (including `:memory:`).
 *   - `cloudflare-do` — `FunctionStorageCfDo`. Requires `VGI_CF_DO_URL`;
 *                       optional `VGI_CF_DO_TOKEN` for bearer auth.
 */
export function resolveStorageFromEnv(): FunctionStorage {
  const backend = (process.env.VGI_WORKER_SHARED_STORAGE ?? "sqlite").toLowerCase();
  if (backend === "memory") {
    return new FunctionStorageSqlite(":memory:");
  }
  if (backend === "sqlite") {
    return new FunctionStorageSqlite();
  }
  if (backend === "cloudflare-do") {
    // Lazy require to avoid loading the CF client when sqlite is in use.
    const { FunctionStorageCfDo } = require("./storage-cf-do.js") as typeof import("./storage-cf-do.js");
    return FunctionStorageCfDo.fromEnv();
  }
  throw new Error(
    `Unknown VGI_WORKER_SHARED_STORAGE backend: '${backend}'. Supported: 'memory', 'sqlite', 'cloudflare-do'.`,
  );
}

// Module-level proxy that lazy-resolves the configured backend on first use.
// Importing this module no longer opens a SQLite connection — that happens
// the first time anyone touches a storage method. Matches Python's
// _DefaultStorageDescriptor lazy attribute pattern.
let _resolved: FunctionStorage | null = null;
function getDefaultStorage(): FunctionStorage {
  if (_resolved == null) _resolved = resolveStorageFromEnv();
  return _resolved;
}

/**
 * Override the default backend used by the `storage` singleton. Call once at
 * worker startup to inject a pre-built backend that env-driven selection can't
 * construct — notably a `FunctionStorageCfDo` wired to a Cloudflare
 * service-binding Fetcher (the binding lives on `env`, not `process.env`, and a
 * same-zone public-URL fetch is rejected with CF error 1042). Takes precedence
 * over `VGI_WORKER_SHARED_STORAGE`.
 */
export function setStorage(impl: FunctionStorage): void {
  _resolved = impl;
}

/** Default FunctionStorage instance. Backend is selected via env on first access. */
export const storage: FunctionStorage = new Proxy(
  {} as FunctionStorage,
  {
    get(_t, prop, _r) {
      const inst = getDefaultStorage() as any;
      const v = inst[prop];
      return typeof v === "function" ? v.bind(inst) : v;
    },
  },
);

// ============================================================================
// Helpers
// ============================================================================

/** bun:sqlite needs Buffer for blob params */
function bufferArg(data: Uint8Array): Buffer {
  return Buffer.from(data.buffer, data.byteOffset, data.byteLength);
}
