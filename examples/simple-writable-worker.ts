// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Minimal writable worker: the TypeScript counterpart of vgi-python's
// `vgi/_test_fixtures/simple_writable.py`, serving the C++ extension's
// test/sql/integration/simple_writable/*.test (VGI_SIMPLE_WRITABLE_WORKER).
//
// Unlike the Python fixture (a hand-written catalog), this one is declared
// with the descriptor API -- `TableDescriptor.insertFunction` /
// `updateFunction` / `deleteFunction` / `writeResultModes` -- so the
// extension's DML tests exercise the declarative path end to end.
//
// Rows live in one SQLite file per attach under TMPDIR, so they survive the
// extension routing statements to different worker processes. BEGIN, COMMIT
// and ROLLBACK are no-ops (supports_transactions is false): every write is
// visible immediately. Wire-protocol fixture, not a database.
//
// Tables (schema `main`):
//   items                  INSERT / UPDATE / DELETE, up to `changes`
//   items_no_returning     INSERT / UPDATE / DELETE, `count` only
//   items_insert_only      INSERT only
//   items_broken_returning INSERT that advertises `rows` but always emits a
//                          count batch -- the extension must reject it cleanly

import { Database } from "bun:sqlite";
import { mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  Arguments,
  type CatalogAttachResult,
  type CatalogDescriptor,
  defineTableFunction,
  defineTableInOutFunction,
  ReadOnlyCatalogInterface,
  type TableDescriptor,
  type VgiFunction,
  Worker,
} from "../src/index.js";
import { batchFromColumns, field, int64, schema, utf8, type VgiBatch, type VgiSchema } from "../src/arrow/index.js";
import { FunctionRegistry } from "../src/functions/registry.js";
import {
  rowIdField,
  type WriteResultMode,
  writeCountBatch,
  writeOptions,
  writeResultBatch,
  writeResultSchema,
} from "../src/write-results.js";

const CATALOG_NAME = "simple_writable";

const TABLES: Record<string, VgiSchema> = {
  items: schema([field("id", int64(), true), field("name", utf8(), true), field("qty", int64(), true)]),
  items_no_returning: schema([field("id", int64(), true), field("name", utf8(), true), field("qty", int64(), true)]),
  items_insert_only: schema([field("id", int64(), true), field("name", utf8(), true)]),
  items_broken_returning: schema([field("id", int64(), true), field("name", utf8(), true)]),
};

// --------------------------------------------------------------- storage

const DB_DIR = join(tmpdir(), "vgi-ts-simple-writable");

function attachKey(attach: Uint8Array | null | undefined): string {
  if (!attach || attach.length === 0) throw new Error("simple_writable: attach_opaque_data missing");
  return Buffer.from(attach).toString("hex");
}

/** Open (creating on first use) the SQLite file of one attach. */
function openDb(attach: Uint8Array | null | undefined): Database {
  mkdirSync(DB_DIR, { recursive: true });
  const db = new Database(join(DB_DIR, `${attachKey(attach)}.db`), { create: true, safeIntegers: true });
  db.exec("PRAGMA busy_timeout = 5000");
  for (const [name, cols] of Object.entries(TABLES)) {
    const defs = cols.fields.map((f) => `"${f.name}" ${f.name === "name" ? "TEXT" : "INTEGER"}`).join(", ");
    db.exec(`CREATE TABLE IF NOT EXISTS "${name}" (${defs})`);
  }
  return db;
}

type Row = Record<string, unknown>;

function selectRow(db: Database, table: string, cols: VgiSchema, rowid: unknown): Row | null {
  const list = cols.fields.map((f) => `"${f.name}"`).join(", ");
  return (db.query(`SELECT ${list} FROM "${table}" WHERE rowid = ?`).get(rowid as any) as Row | null) ?? null;
}

const column = (batch: VgiBatch, name: string, i: number) => batch.getChild(name)?.get(i) ?? null;

// ------------------------------------------------------------- functions

function scanFunction(): VgiFunction {
  return defineTableFunction<{ table: string }, { done: boolean }>({
    name: "simple_writable_scan",
    description: "Scan one simple_writable table, row ids included",
    args: { table: utf8() },
    // DuckDB reads `rowid` as a virtual column, which needs projection
    // pushdown: params.outputSchema is then exactly the requested columns.
    projectionPushdown: true,
    onBind: (params) => {
      const cols = TABLES[params.args.table];
      if (!cols) throw new Error(`Unknown table '${params.args.table}'`);
      return { outputSchema: schema([...cols.fields, rowIdField()]) };
    },
    initialState: () => ({ done: false }),
    process: (params, state, out) => {
      if (state.done) return out.finish();
      state.done = true;
      const db = openDb(params.initCall.bind_call.attach_opaque_data);
      try {
        const names = params.outputSchema.fields.map((f) => f.name);
        const rows = db
          .query(`SELECT ${names.map((n) => `"${n}"`).join(", ")} FROM "${params.args.table}" ORDER BY rowid`)
          .all() as Row[];
        out.emit(
          batchFromColumns(Object.fromEntries(names.map((n) => [n, rows.map((r) => r[n] ?? null)])), params.outputSchema),
        );
      } finally {
        db.close();
      }
    },
  });
}

/** INSERT / UPDATE / DELETE functions for one table. */
function writeFunctions(table: string): { insert: VgiFunction; update: VgiFunction; delete: VgiFunction } {
  const cols = TABLES[table]!;
  const bindFor = (params: { bindCall: Parameters<typeof writeOptions>[0] }) => ({
    outputSchema: writeResultSchema(writeOptions(params.bindCall).resultMode, cols),
  });
  const modeOf = (params: { initCall: { bind_call: Parameters<typeof writeOptions>[0] } }): WriteResultMode =>
    writeOptions(params.initCall.bind_call).resultMode;

  const insert = defineTableInOutFunction({
    name: `simple_writable_${table}_insert`,
    description: `INSERT into ${table}`,
    onBind: bindFor,
    process: (params, _state, batch, out) => {
      const db = openDb(params.initCall.bind_call.attach_opaque_data);
      const inserted: Row[] = [];
      try {
        const names = cols.fields.map((f) => f.name);
        const stmt = db.query(
          `INSERT INTO "${table}" (${names.map((n) => `"${n}"`).join(", ")}) VALUES (${names.map(() => "?").join(", ")})`,
        );
        db.transaction(() => {
          for (let i = 0; i < batch.numRows; i++) {
            const row = Object.fromEntries(names.map((n) => [n, column(batch, n, i)]));
            stmt.run(...(names.map((n) => row[n]) as any[]));
            inserted.push(row);
          }
        })();
      } finally {
        db.close();
      }
      out.emit(writeResultBatch(modeOf(params), cols, inserted.map(() => null), inserted));
    },
  });

  const update = defineTableInOutFunction({
    name: `simple_writable_${table}_update`,
    description: `UPDATE ${table} by row id`,
    onBind: bindFor,
    process: (params, _state, batch, out) => {
      const changed = batch.schema.fields.map((f) => f.name).filter((n) => n !== "rowid");
      const db = openDb(params.initCall.bind_call.attach_opaque_data);
      const before: Row[] = [];
      const after: Row[] = [];
      try {
        const set = db.query(`UPDATE "${table}" SET ${changed.map((c) => `"${c}" = ?`).join(", ")} WHERE rowid = ?`);
        db.transaction(() => {
          for (let i = 0; i < batch.numRows; i++) {
            const rowid = column(batch, "rowid", i);
            const old = selectRow(db, table, cols, rowid);
            if (!old) throw new Error(`Update target rowid ${rowid} not in table ${table}`);
            set.run(...(changed.map((c) => column(batch, c, i)) as any[]), rowid as any);
            before.push(old);
            after.push(selectRow(db, table, cols, rowid)!);
          }
        })();
      } finally {
        db.close();
      }
      out.emit(writeResultBatch(modeOf(params), cols, before, after));
    },
  });

  const del = defineTableInOutFunction({
    name: `simple_writable_${table}_delete`,
    description: `DELETE from ${table} by row id`,
    onBind: bindFor,
    process: (params, _state, batch, out) => {
      const db = openDb(params.initCall.bind_call.attach_opaque_data);
      const deleted: Row[] = [];
      try {
        const remove = db.query(`DELETE FROM "${table}" WHERE rowid = ?`);
        db.transaction(() => {
          for (let i = 0; i < batch.numRows; i++) {
            const rowid = column(batch, "rowid", i);
            const old = selectRow(db, table, cols, rowid);
            if (!old) throw new Error(`Delete target rowid ${rowid} not in table ${table}`);
            remove.run(rowid as any);
            deleted.push(old);
          }
        })();
      } finally {
        db.close();
      }
      out.emit(writeResultBatch(modeOf(params), cols, deleted, deleted.map(() => null)));
    },
  });

  return { insert, update, delete: del };
}

/** Advertises `rows` but always binds and emits a count batch: the extension must refuse it cleanly. */
const brokenReturningInsert = defineTableInOutFunction({
  name: "simple_writable_broken_returning_insert",
  description: "INSERT that lies about RETURNING support",
  onBind: () => ({ outputSchema: writeResultSchema("count", schema([])) }),
  process: (params, _state, batch, out) => {
    const db = openDb(params.initCall.bind_call.attach_opaque_data);
    try {
      const stmt = db.query(`INSERT INTO "items_broken_returning" ("id", "name") VALUES (?, ?)`);
      for (let i = 0; i < batch.numRows; i++) stmt.run(column(batch, "id", i) as any, column(batch, "name", i) as any);
    } finally {
      db.close();
    }
    out.emit(writeCountBatch(batch.numRows));
  },
});

// --------------------------------------------------------------- catalog

const scan = scanFunction();

function table(name: string, ops: Partial<TableDescriptor>): TableDescriptor {
  return { name, columns: schema([...TABLES[name]!.fields, rowIdField()]), function: scan, arguments: new Arguments([name]), ...ops };
}

const items = writeFunctions("items");
const noReturning = writeFunctions("items_no_returning");
const insertOnly = writeFunctions("items_insert_only");

export const descriptor: CatalogDescriptor = {
  name: CATALOG_NAME,
  defaultSchema: "main",
  schemas: [
    {
      name: "main",
      // Listed (as the Python fixture lists its functions) so DuckDB knows the
      // scan's capabilities -- projection pushdown, which rowid needs.
      functions: [scan],
      tables: [
        table("items", {
          insertFunction: items.insert,
          updateFunction: items.update,
          deleteFunction: items.delete,
          writeResultModes: { insert: "changes", update: "changes", delete: "changes" },
        }),
        table("items_no_returning", {
          insertFunction: noReturning.insert,
          updateFunction: noReturning.update,
          deleteFunction: noReturning.delete,
          writeResultModes: { insert: "count", update: "count", delete: "count" },
        }),
        table("items_insert_only", { insertFunction: insertOnly.insert, writeResultModes: { insert: "changes" } }),
        table("items_broken_returning", { insertFunction: brokenReturningInsert, writeResultModes: { insert: "rows" } }),
      ],
    },
  ],
};

/** Seeds the attach's SQLite file and, like the Python fixture, loads per schema. */
class SimpleWritableCatalog extends ReadOnlyCatalogInterface {
  override supportsCatalogContents = false;

  override async attach(
    name: string,
    options?: Record<string, unknown>,
    dataVersionSpec?: string | null,
    implementationVersion?: string | null,
    clientCapabilities?: Uint8Array | null,
  ): Promise<CatalogAttachResult> {
    const result = await super.attach(name, options, dataVersionSpec, implementationVersion, clientCapabilities);
    openDb(result.attach_opaque_data).close();
    return result;
  }
}

if (import.meta.main) {
  const registry = new FunctionRegistry();
  new Worker({
    catalog: descriptor,
    registry,
    catalogInterfaceFactory: () => new SimpleWritableCatalog(descriptor, registry),
  }).run();
}
