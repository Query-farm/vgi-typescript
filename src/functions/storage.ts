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
  const req: any = (globalThis as any).require ?? null;
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
  /** Optional cleanup hook. Implementations holding a connection should release it here. */
  close?(): Promise<void> | void;
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
    for (const [table, requiredCol] of [
      ["worker_state", "execution_id"],
      ["work_queue", "execution_id"],
      ["invocation_registry", "execution_id"],
    ] as const) {
      const info = db.prepare(`PRAGMA table_info(${table})`).all() as Array<{
        name: string;
      }>;
      const columns = new Set(info.map((row) => row.name));
      if (columns.size > 0 && !columns.has(requiredCol)) {
        db.exec(`DROP TABLE IF EXISTS ${table}`);
      }
    }

    db.exec("DROP TABLE IF EXISTS init_storage");

    db.exec(`
      CREATE TABLE IF NOT EXISTS global_state_storage (
        key BLOB PRIMARY KEY,
        value BLOB NOT NULL,
        created_at REAL DEFAULT (julianday('now'))
      )
    `);
    db.exec(`
      CREATE TABLE IF NOT EXISTS worker_state (
        execution_id BLOB NOT NULL,
        process_id INTEGER NOT NULL,
        state_data BLOB NOT NULL,
        created_at REAL DEFAULT (julianday('now')),
        PRIMARY KEY (execution_id, process_id)
      )
    `);
    db.exec(`
      CREATE TABLE IF NOT EXISTS work_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        execution_id BLOB NOT NULL,
        work_item BLOB NOT NULL,
        created_at REAL DEFAULT (julianday('now'))
      )
    `);
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_work_queue_invocation
      ON work_queue(execution_id)
    `);
    db.exec(`
      CREATE TABLE IF NOT EXISTS invocation_registry (
        execution_id BLOB PRIMARY KEY,
        created_at REAL DEFAULT (julianday('now'))
      )
    `);
  }

  async workerPut(
    executionId: Uint8Array,
    workerId: number,
    state: Uint8Array
  ): Promise<void> {
    if (Math.random() < 0.01) {
      this.cleanupOldEntries(1.0);
    }
    this._db
      .prepare(
        "INSERT OR REPLACE INTO worker_state " +
          "(execution_id, process_id, state_data, created_at) " +
          "VALUES (?, ?, ?, julianday('now'))"
      )
      .run(bufferArg(executionId), workerId, bufferArg(state));
  }

  async workerCollect(executionId: Uint8Array): Promise<Uint8Array[]> {
    const rows = this._db
      .prepare(
        "DELETE FROM worker_state WHERE execution_id = ? RETURNING state_data"
      )
      .all(bufferArg(executionId)) as Array<{ state_data: Uint8Array }>;
    return rows.map((row) => new Uint8Array(row.state_data));
  }

  async workerScan(executionId: Uint8Array): Promise<Array<[number, Uint8Array]>> {
    const rows = this._db
      .prepare(
        "SELECT process_id, state_data FROM worker_state WHERE execution_id = ?"
      )
      .all(bufferArg(executionId)) as Array<{
      process_id: number;
      state_data: Uint8Array;
    }>;
    return rows.map((row) => [row.process_id, new Uint8Array(row.state_data)]);
  }

  async queuePush(executionId: Uint8Array, items: Uint8Array[]): Promise<number> {
    const eidBuf = bufferArg(executionId);
    this._db.exec("BEGIN");
    try {
      this._db
        .prepare(
          "INSERT OR IGNORE INTO invocation_registry (execution_id) VALUES (?)"
        )
        .run(eidBuf);
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
    const eidBuf = bufferArg(executionId);
    const reg = this._db
      .prepare("SELECT 1 FROM invocation_registry WHERE execution_id = ?")
      .get(eidBuf);
    if (reg == null) {
      throw new UnknownInvocationError(executionId);
    }

    const row = this._db
      .prepare(
        "DELETE FROM work_queue WHERE id = (" +
          "  SELECT id FROM work_queue WHERE execution_id = ? ORDER BY id ASC LIMIT 1" +
          ") RETURNING work_item"
      )
      .get(eidBuf) as { work_item: Uint8Array } | null;

    return row ? new Uint8Array(row.work_item) : null;
  }

  async queueClear(executionId: Uint8Array): Promise<number> {
    const eidBuf = bufferArg(executionId);
    this._db.exec("BEGIN");
    try {
      const result = this._db
        .prepare("DELETE FROM work_queue WHERE execution_id = ?")
        .run(eidBuf);
      this._db
        .prepare("DELETE FROM invocation_registry WHERE execution_id = ?")
        .run(eidBuf);
      this._db.exec("COMMIT");
      return result.changes;
    } catch (e) {
      this._db.exec("ROLLBACK");
      throw e;
    }
  }

  cleanupOldEntries(maxAgeDays: number = 1.0): number {
    let total = 0;
    for (const table of [
      "global_state_storage",
      "worker_state",
      "work_queue",
      "invocation_registry",
    ]) {
      const result = this._db
        .prepare(
          `DELETE FROM ${table} WHERE julianday('now') - created_at > ?`
        )
        .run(maxAgeDays);
      total += result.changes;
    }
    return total;
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
 *   - `sqlite`        — `FunctionStorageSqlite`. Honors `VGI_WORKER_SQLITE_PATH`
 *                       (including `:memory:`).
 *   - `cloudflare-do` — `FunctionStorageCfDo`. Requires `VGI_CF_DO_URL`;
 *                       optional `VGI_CF_DO_TOKEN` for bearer auth.
 */
export function resolveStorageFromEnv(): FunctionStorage {
  const backend = (process.env.VGI_WORKER_SHARED_STORAGE ?? "sqlite").toLowerCase();
  if (backend === "sqlite") {
    return new FunctionStorageSqlite();
  }
  if (backend === "cloudflare-do") {
    // Lazy require to avoid loading the CF client when sqlite is in use.
    const { FunctionStorageCfDo } = require("./storage-cf-do.js") as typeof import("./storage-cf-do.js");
    return FunctionStorageCfDo.fromEnv();
  }
  throw new Error(
    `Unknown VGI_WORKER_SHARED_STORAGE backend: '${backend}'. Supported: 'sqlite', 'cloudflare-do'.`,
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
