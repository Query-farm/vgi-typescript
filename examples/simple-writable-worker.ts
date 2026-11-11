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
//   <created>              CREATE TABLE / CTAS / DROP TABLE: INSERT, up to
//                          `changes`. tableCreate reports `created` (protocol
//                          3.3): false when IF NOT EXISTS found the table, so
//                          a CTAS does not append into it.

import { Database } from "bun:sqlite";
import { mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  Arguments,
  type AttachOpaqueData,
  CatalogAlreadyExistsError,
  type CatalogAttachResult,
  type CatalogDescriptor,
  CatalogNotFoundError,
  defineTableFunction,
  defineTableInOutFunction,
  encodeScanFunctionResult,
  FUNCTION_VISIBILITY_TABLE_ONLY,
  type FunctionInfo,
  functionInfoFor,
  ReadOnlyCatalogInterface,
  type ScanFunctionResultRecord,
  type TableDescriptor,
  type TableInfo,
  type TransactionOpaqueData,
  type VgiFunction,
  Worker,
} from "../src/index.js";
import {
  batchFromColumns,
  deserializeSchema,
  field,
  int64,
  schema,
  serializeBatch,
  serializeSchema,
  utf8,
  type VgiBatch,
  type VgiSchema,
  TypeId,
  typeSignature,
} from "../src/arrow/index.js";
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
  // Tables made by CREATE TABLE: their Arrow schema (IPC), and a version that
  // every CREATE / DROP bumps (catalog_version: DuckDB re-lists on a change).
  db.exec('CREATE TABLE IF NOT EXISTS "__created" (name TEXT PRIMARY KEY, columns BLOB NOT NULL)');
  db.exec('CREATE TABLE IF NOT EXISTS "__version" (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL)');
  db.exec('INSERT OR IGNORE INTO "__version" (id, version) VALUES (1, 1)');
  return db;
}

/** Run `fn` against the attach's SQLite file, closing it afterwards. */
function withDb<T>(attach: Uint8Array | null | undefined, fn: (db: Database) => T): T {
  const db = openDb(attach);
  try {
    return fn(db);
  } finally {
    db.close();
  }
}

/** The SQLite table holding created table `name` (prefixed: never a built-in's). */
const createdStore = (name: string) => `created_${name.toLowerCase()}`;

/** The user columns of created table `name` in this attach, or null. */
function createdColumns(db: Database, name: string): VgiSchema | null {
  const row = db.query('SELECT columns FROM "__created" WHERE name = ?').get(name.toLowerCase()) as { columns: Uint8Array } | null;
  return row ? deserializeSchema(new Uint8Array(row.columns)) : null;
}

/** The user columns of any table of this attach: built-in or created. */
function columnsOf(db: Database, table: string): VgiSchema | null {
  return TABLES[table] ?? createdColumns(db, table);
}

/** The SQLite table holding `table`'s rows. */
const storeOf = (table: string) => (TABLES[table] ? table : createdStore(table));

// Arrow types a created table may use, and how a SQLite value reads back.
// Read through typeId (not String(type)): flechette types are plain objects
// that stringify to "[object Object]", so a name check refuses every column.
type ColumnKind = "bool" | "int64" | "number" | "utf8";
function columnKind(f: VgiSchema["fields"][number]): ColumnKind | null {
  const t = f.type as any;
  switch (t.typeId) {
    case TypeId.Bool:
      return "bool";
    case TypeId.Int: {
      const signed = t.isSigned ?? t.signed ?? true;
      if (t.bitWidth === 64) return signed ? "int64" : null;
      return "number";
    }
    case TypeId.Float:
      return "number";
    case TypeId.Utf8:
    case TypeId.LargeUtf8:
      return "utf8";
    default:
      return null;
  }
}

/** A SQLite value as the column's Arrow value (SQLite has no BOOLEAN; small ints read back as bigint). */
function fromSqlite(f: VgiSchema["fields"][number], value: unknown): unknown {
  if (value == null) return null;
  const kind = columnKind(f);
  if (kind === "bool") return Number(value) !== 0;
  if (kind === "int64") return BigInt(value as bigint);
  if (kind === "number") return Number(value);
  return value;
}

/** An Arrow value as SQLite can bind it. */
const toSqlite = (value: unknown): unknown => (typeof value === "boolean" ? (value ? 1 : 0) : value);

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
      const cols = withDb(params.bindCall.attach_opaque_data, (db) => columnsOf(db, params.args.table));
      if (!cols) throw new Error(`Unknown table '${params.args.table}'`);
      return { outputSchema: schema([...cols.fields, rowIdField()]) };
    },
    initialState: () => ({ done: false }),
    process: (params, state, out) => {
      if (state.done) return out.finish();
      state.done = true;
      const db = openDb(params.initCall.bind_call.attach_opaque_data);
      try {
        const fields = params.outputSchema.fields;
        const rows = db
          .query(`SELECT ${fields.map((f) => `"${f.name}"`).join(", ")} FROM "${storeOf(params.args.table)}" ORDER BY rowid`)
          .all() as Row[];
        const created = !TABLES[params.args.table];
        out.emit(
          batchFromColumns(
            Object.fromEntries(
              fields.map((f) => [
                f.name,
                rows.map((r) => (created && f.name !== "rowid" ? fromSqlite(f, r[f.name]) : (r[f.name] ?? null))),
              ]),
            ),
            params.outputSchema,
          ),
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

/**
 * INSERT into a created table: one function for all of them, told which by
 * its `table` argument (DuckDB passes the arguments `tableInsertFunctionGet`
 * returns, then the rows).
 */
const createdInsert = defineTableInOutFunction<{ table: string }>({
  name: "simple_writable_created_insert",
  description: "INSERT into a table made by CREATE TABLE",
  args: { table: utf8() },
  onBind: (params) => {
    const cols = withDb(params.bindCall.attach_opaque_data, (db) => createdColumns(db, params.args.table));
    if (!cols) throw new Error(`Unknown table '${params.args.table}'`);
    return { outputSchema: writeResultSchema(writeOptions(params.bindCall).resultMode, cols) };
  },
  process: (params, _state, batch, out) => {
    const table = params.args.table;
    const inserted = withDb(params.initCall.bind_call.attach_opaque_data, (db) => {
      const cols = createdColumns(db, table);
      if (!cols) throw new Error(`Unknown table '${table}'`);
      const names = cols.fields.map((f) => f.name);
      const stmt = db.query(
        `INSERT INTO "${createdStore(table)}" (${names.map((n) => `"${n}"`).join(", ")}) VALUES (${names.map(() => "?").join(", ")})`,
      );
      const rows: Row[] = [];
      db.transaction(() => {
        for (let i = 0; i < batch.numRows; i++) {
          const row = Object.fromEntries(names.map((n) => [n, column(batch, n, i)]));
          stmt.run(...(names.map((n) => toSqlite(row[n])) as any[]));
          rows.push(row);
        }
      })();
      return { cols, rows };
    });
    out.emit(
      writeResultBatch(writeOptions(params.initCall.bind_call).resultMode, inserted.cols, inserted.rows.map(() => null), inserted.rows),
    );
  },
});

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
      // Listed in `functions`, so the scan is also a public, callable table
      // function. (Protocol 3.3: the SDK would send it anyway, as table_only,
      // so DuckDB knows its projection pushdown, which rowid needs.)
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

/**
 * Seeds the attach's SQLite file and, like the Python fixture, loads per
 * schema. Also serves CREATE TABLE / CTAS / DROP TABLE: a created table lives
 * in the attach's file, so every worker process sees it.
 */
export class SimpleWritableCatalog extends ReadOnlyCatalogInterface {
  override supportsCatalogContents = false;
  // CREATE / DROP TABLE change the contents, per attach.
  override catalogVersionFrozen = false;
  override catalogContentsAttachIndependent = false;

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

  override version(attachOpaqueData: AttachOpaqueData): number {
    return withDb(attachOpaqueData, (db) => Number((db.query('SELECT version FROM "__version"').get() as { version: bigint }).version));
  }

  override tableCreate(
    attachOpaqueData: AttachOpaqueData,
    schemaPath: string[],
    name: string,
    columns: Uint8Array,
    onConflict: string,
    constraintDefinitions?: Uint8Array[],
  ): boolean {
    if (schemaPath.length !== 1 || schemaPath[0]!.toLowerCase() !== "main") throw new CatalogNotFoundError("Schema", schemaPath.join("."));
    if (constraintDefinitions?.length) throw new Error("simple_writable: CREATE TABLE constraints are not supported");
    const cols = deserializeSchema(columns);
    for (const f of cols.fields) {
      if (columnKind(f) == null) throw new Error(`simple_writable: column ${f.name}: type ${typeSignature(f.type)} is not supported`);
      if (f.name.toLowerCase() === "rowid") throw new Error("simple_writable: rowid is reserved");
    }
    const mode = String(onConflict ?? "").toUpperCase();
    return withDb(attachOpaqueData, (db) =>
      db.transaction(() => {
        const builtIn = TABLES[name] != null;
        if (builtIn || createdColumns(db, name)) {
          // IF NOT EXISTS: leave it alone and say so (created = false).
          if (mode === "IGNORE") return false;
          if (mode !== "REPLACE" || builtIn) throw new CatalogAlreadyExistsError("Table", name);
          db.exec(`DROP TABLE IF EXISTS "${createdStore(name)}"`);
        }
        const defs = cols.fields.map((f) => `"${f.name}"`).join(", ");
        db.exec(`CREATE TABLE "${createdStore(name)}" (${defs})`);
        db.query('INSERT OR REPLACE INTO "__created" (name, columns) VALUES (?, ?)').run(name.toLowerCase(), serializeSchema(cols));
        db.exec('UPDATE "__version" SET version = version + 1');
        return true;
      })(),
    );
  }

  override tableDrop(attachOpaqueData: AttachOpaqueData, schemaPath: string[], name: string, ignoreNotFound?: boolean): void {
    if (TABLES[name]) throw new Error(`simple_writable: built-in table ${name} cannot be dropped`);
    withDb(attachOpaqueData, (db) =>
      db.transaction(() => {
        if (!createdColumns(db, name)) {
          if (ignoreNotFound) return;
          throw new CatalogNotFoundError("Table", `${schemaPath.join(".")}.${name}`);
        }
        db.exec(`DROP TABLE IF EXISTS "${createdStore(name)}"`);
        db.query('DELETE FROM "__created" WHERE name = ?').run(name.toLowerCase());
        db.exec('UPDATE "__version" SET version = version + 1');
      })(),
    );
  }

  /** The built-in tables plus this attach's created ones. */
  override async schemaContentsTables(
    attachOpaqueData: AttachOpaqueData,
    path: string[],
    transactionOpaqueData?: TransactionOpaqueData,
  ): Promise<TableInfo[]> {
    const tables = await super.schemaContentsTables(attachOpaqueData, path, transactionOpaqueData);
    if (path.length !== 1 || path[0]!.toLowerCase() !== "main") return tables;
    const created = withDb(attachOpaqueData, (db) =>
      (db.query('SELECT name FROM "__created" ORDER BY name').all() as { name: string }[]).map((r) => ({
        name: r.name,
        cols: createdColumns(db, r.name)!,
      })),
    );
    for (const { name, cols } of created) {
      tables.push({
        comment: null,
        tags: {},
        name,
        schema_path: ["main"],
        columns: serializeSchema(schema([...cols.fields, rowIdField()])),
        constraints: [],
        definition_revision: null,
        write_result_modes: { insert: "changes" },
        supports_column_statistics: false,
        // The scan inlined with its argument; the insert function is asked
        // for (tableInsertFunctionGet), which passes the table name too.
        scan_function: encodeScanFunctionResult(scan.meta.name, tableArgument(name), [], ["main"]),
        insert_function: null,
        update_function: null,
        delete_function: null,
        required_filters: [],
        scan_planning: "bind",
      });
    }
    return tables;
  }

  override tableInsertFunctionGet(
    attachOpaqueData: AttachOpaqueData,
    schemaPath: string[],
    name: string,
    transactionOpaqueData?: TransactionOpaqueData,
    writableBranchFunctionName?: string | null,
  ): ScanFunctionResultRecord {
    if (TABLES[name]) return super.tableInsertFunctionGet(attachOpaqueData, schemaPath, name, transactionOpaqueData, writableBranchFunctionName);
    return { function_name: createdInsert.meta.name, arguments: tableArgument(name, true), required_extensions: [], schema_path: ["main"] };
  }

  /** The created-table insert function is listed table_only: DuckDB needs its capabilities, SQL never calls it. */
  override schemaContentsFunctions(
    attachOpaqueData: AttachOpaqueData,
    path: string[],
    type: string,
    transactionOpaqueData?: TransactionOpaqueData,
  ): FunctionInfo[] {
    const listed = super.schemaContentsFunctions(attachOpaqueData, path, type, transactionOpaqueData);
    const kind = String(type ?? "").toUpperCase();
    if (path.length === 1 && path[0]!.toLowerCase() === "main" && (kind === "TABLE_FUNCTION" || !kind.endsWith("_FUNCTION"))) {
      listed.push(functionInfoFor(createdInsert, ["main"], FUNCTION_VISIBILITY_TABLE_ONLY));
    }
    return listed;
  }
}

/**
 * The serialized arguments batch `(table)` the extension binds a created
 * table's functions with. The scan's takes the argument by name, the shape the
 * descriptor path sends for `arguments: new Arguments([name])`; the insert
 * function's is positional (`arg_0`), as a table in-out function's arguments
 * before its TABLE input are.
 */
function tableArgument(name: string, positional = false): Uint8Array {
  const column = positional ? "arg_0" : "table";
  return serializeBatch(batchFromColumns({ [column]: [name] }, schema([field(column, utf8(), true)])));
}

if (import.meta.main) {
  const registry = new FunctionRegistry();
  registry.register(createdInsert);
  new Worker({
    catalog: descriptor,
    registry,
    catalogInterfaceFactory: () => new SimpleWritableCatalog(descriptor, registry),
  }).run();
}
