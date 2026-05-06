// Storage for VGI function state.
// Provides shared state across worker processes for distributed execution.
// Port of vgi-python's vgi/function_storage.py using bun:sqlite.

import { Database } from "bun:sqlite";
import { homedir, platform } from "node:os";
import { join } from "node:path";
import { mkdirSync } from "node:fs";

// ============================================================================
// Errors
// ============================================================================

export class UnknownInvocationError extends Error {
  constructor(executionId: Uint8Array) {
    super(
      `Invocation ${hexEncode(executionId)} is not registered. ` +
        "Call queuePush first to register the invocation."
    );
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
  workerPut(executionId: Uint8Array, workerId: number, state: Uint8Array): void;
  workerCollect(executionId: Uint8Array): Uint8Array[];
  workerScan(executionId: Uint8Array): Array<[number, Uint8Array]>;
  queuePush(executionId: Uint8Array, items: Uint8Array[]): number;
  queuePop(executionId: Uint8Array): Uint8Array | null;
  queueClear(executionId: Uint8Array): number;
}

// ============================================================================
// FunctionStorageSqlite
// ============================================================================

function getDefaultDbPath(): string {
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
    this._db = new Database(this.dbPath);
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

  workerPut(
    executionId: Uint8Array,
    workerId: number,
    state: Uint8Array
  ): void {
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

  workerCollect(executionId: Uint8Array): Uint8Array[] {
    const rows = this._db
      .prepare(
        "DELETE FROM worker_state WHERE execution_id = ? RETURNING state_data"
      )
      .all(bufferArg(executionId)) as Array<{ state_data: Uint8Array }>;
    return rows.map((row) => new Uint8Array(row.state_data));
  }

  workerScan(executionId: Uint8Array): Array<[number, Uint8Array]> {
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

  queuePush(executionId: Uint8Array, items: Uint8Array[]): number {
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

  queuePop(executionId: Uint8Array): Uint8Array | null {
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

  queueClear(executionId: Uint8Array): number {
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

  put(state: Uint8Array): void {
    this._base.workerPut(this._executionId, process.pid, state);
  }

  collect(): Uint8Array[] {
    return this._base.workerCollect(this._executionId);
  }

  workerScan(): Array<[number, Uint8Array]> {
    return this._base.workerScan(this._executionId);
  }

  queuePush(items: Uint8Array[]): number {
    return this._base.queuePush(this._executionId, items);
  }

  queuePop(): Uint8Array | null {
    return this._base.queuePop(this._executionId);
  }

  queueClear(): number {
    return this._base.queueClear(this._executionId);
  }
}

// ============================================================================
// Singleton storage instance
// ============================================================================

export const storage: FunctionStorage = new FunctionStorageSqlite();

// ============================================================================
// Helpers
// ============================================================================

/** bun:sqlite needs Buffer for blob params */
function bufferArg(data: Uint8Array): Buffer {
  return Buffer.from(data.buffer, data.byteOffset, data.byteLength);
}
