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

  constructor(dbPath?: string) {
    this.dbPath = dbPath ?? getDefaultDbPath();
    this._ensureTables();
  }

  private _connect(): Database {
    const db = new Database(this.dbPath);
    db.exec("PRAGMA busy_timeout=30000");
    // WAL mode persists in the DB file; set once in _ensureTables
    return db;
  }

  private _ensureTables(): void {
    const db = new Database(this.dbPath);
    try {
      db.exec("PRAGMA busy_timeout=30000");
      db.exec("PRAGMA journal_mode=WAL");

      // Schema migration: drop tables with stale column names
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
    } finally {
      db.close();
    }
  }

  workerPut(
    executionId: Uint8Array,
    workerId: number,
    state: Uint8Array
  ): void {
    if (Math.random() < 0.01) {
      this.cleanupOldEntries(1.0);
    }
    const db = this._connect();
    try {
      db.prepare(
        "INSERT OR REPLACE INTO worker_state " +
          "(execution_id, process_id, state_data, created_at) " +
          "VALUES (?, ?, ?, julianday('now'))"
      ).run(bufferArg(executionId), workerId, bufferArg(state));
    } finally {
      db.close();
    }
  }

  workerCollect(executionId: Uint8Array): Uint8Array[] {
    const db = this._connect();
    try {
      const rows = db
        .prepare(
          "DELETE FROM worker_state WHERE execution_id = ? RETURNING state_data"
        )
        .all(bufferArg(executionId)) as Array<{ state_data: Uint8Array }>;
      return rows.map((row) => new Uint8Array(row.state_data));
    } finally {
      db.close();
    }
  }

  queuePush(executionId: Uint8Array, items: Uint8Array[]): number {
    const db = this._connect();
    try {
      const eidBuf = bufferArg(executionId);
      // Use exec BEGIN/COMMIT so busy_timeout applies to each statement
      db.exec("BEGIN");
      try {
        db.prepare(
          "INSERT OR IGNORE INTO invocation_registry (execution_id) VALUES (?)"
        ).run(eidBuf);
        const insert = db.prepare(
          "INSERT INTO work_queue (execution_id, work_item) VALUES (?, ?)"
        );
        for (const item of items) {
          insert.run(eidBuf, bufferArg(item));
        }
        db.exec("COMMIT");
      } catch (e) {
        db.exec("ROLLBACK");
        throw e;
      }
      return items.length;
    } finally {
      db.close();
    }
  }

  queuePop(executionId: Uint8Array): Uint8Array | null {
    const db = this._connect();
    try {
      const eidBuf = bufferArg(executionId);
      const reg = db
        .prepare(
          "SELECT 1 FROM invocation_registry WHERE execution_id = ?"
        )
        .get(eidBuf);
      if (reg == null) {
        throw new UnknownInvocationError(executionId);
      }

      const row = db
        .prepare(
          "DELETE FROM work_queue WHERE id = (" +
            "  SELECT id FROM work_queue WHERE execution_id = ? ORDER BY id ASC LIMIT 1" +
            ") RETURNING work_item"
        )
        .get(eidBuf) as { work_item: Uint8Array } | null;

      return row ? new Uint8Array(row.work_item) : null;
    } finally {
      db.close();
    }
  }

  queueClear(executionId: Uint8Array): number {
    const db = this._connect();
    try {
      const eidBuf = bufferArg(executionId);
      db.exec("BEGIN");
      try {
        const result = db
          .prepare("DELETE FROM work_queue WHERE execution_id = ?")
          .run(eidBuf);
        db.prepare(
          "DELETE FROM invocation_registry WHERE execution_id = ?"
        ).run(eidBuf);
        db.exec("COMMIT");
        return result.changes;
      } catch (e) {
        db.exec("ROLLBACK");
        throw e;
      }
    } finally {
      db.close();
    }
  }

  cleanupOldEntries(maxAgeDays: number = 1.0): number {
    const db = this._connect();
    try {
      let total = 0;
      for (const table of [
        "global_state_storage",
        "worker_state",
        "work_queue",
        "invocation_registry",
      ]) {
        const result = db
          .prepare(
            `DELETE FROM ${table} WHERE julianday('now') - created_at > ?`
          )
          .run(maxAgeDays);
        total += result.changes;
      }
      return total;
    } finally {
      db.close();
    }
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
