// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// The `txfix` fixture worker: the TypeScript implementation of the C++
// extension's writable-table and transaction-hook fixtures
// (Query-farm/vgi docs/writable_tx_fixtures.md), serving
// test/sql/integration/writable_tx/*.test (VGI_WRITABLE_TX_WORKER).
//
// Built only from the SDK's public API: a descriptor catalog whose `items`
// table declares insert, update and delete functions (each with a finalize
// stage) and write result modes, a column default (`qty DEFAULT 0`), ATTACH options
// (one of them secret), a per-attach `supports_transactions`, and the
// catalog's transactionBegin / transactionCommit / transactionRollback hooks.
// Functions and hooks read the options (the secret `token` included) only
// through the SDK's `attachOptions()` accessor: the catalog never puts them in
// its own attach bytes, and the SDK keeps the token encrypted inside
// attach_opaque_data (whoami().token_in_attach_data checks it never shows).
//
//   ATTACH 'txfix' AS t   (TYPE vgi, LOCATION '...', token 'tok-123');
//   ATTACH 'txfix' AS ttx (TYPE vgi, LOCATION '...', token 'tok-123', transactional true);
//
// On a transactional attach INSERT, UPDATE and DELETE stage their changes
// under the plaintext transaction id the function sees; scans on that attach
// read their own writes; the commit hook applies them (or refuses a negative
// qty) and the rollback hook discards them. Every write call is logged
// (`writes()`), and its finalize stage marks the log row finalized. State lives in one SQLite file under
// $VGI_TXFIX_STATE_DIR (default <tmpdir>/vgi-txfix), partitioned by the
// `token` option, because the extension may route the statements of one
// transaction to different worker processes.
//
//   bin/vgi-example-writable-tx-worker                 stdio / --unix (launcher)
//   bin/vgi-example-writable-tx-worker --http [--port N]   HTTP; prints PORT:<n>

import { Database } from "bun:sqlite";
import { mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  type AttachOptionSpec,
  attachOptions,
  type CatalogAttachResult,
  type CatalogDescriptor,
  constraintRecord,
  DEFAULT_EXPRESSION_KEY,
  defaultExpressionMetadata,
  defineTableFunction,
  defineTableInOutFunction,
  FunctionRegistry,
  FunctionStability,
  ReadOnlyCatalogInterface,
  rowIdField,
  type TableDescriptor,
  type VgiFunction,
  Worker,
  writeOptions,
  writeResultBatch,
  writeResultSchema,
} from "../src/index.js";
// The arrow facade: the package root's `bool` / `int32` are vgi-rpc argument builders.
import { batchFromColumns, bool, field, int32, int64, schema, utf8, type VgiBatch, type VgiSchema } from "../src/arrow/index.js";
import { serveVgiWorker } from "../src/serve-entry.js";

const CATALOG_NAME = "txfix";

// ------------------------------------------------------------ ATTACH options

const ATTACH_OPTIONS: AttachOptionSpec[] = [
  {
    name: "transactional",
    description: "Stage UPDATEs in DuckDB transactions (supports_transactions for this attach)",
    type: bool(),
    default: false,
  },
  { name: "token", description: "A credential every function must receive", type: utf8(), secret: true },
];

interface AttachOptions {
  token: string | null;
  transactional: boolean;
}

const encoder = new TextEncoder();

/** The attach's options, from the SDK's accessor (never from the attach bytes). */
function decodeAttach(bytes: Uint8Array | null | undefined): AttachOptions {
  if (!bytes || bytes.length === 0) throw new Error("txfix: call has no attach_opaque_data");
  const o = attachOptions(bytes);
  return { token: typeof o.token === "string" ? o.token : null, transactional: o.transactional === true };
}

/** Whether `needle` occurs in `haystack`. */
function contains(haystack: Uint8Array, needle: Uint8Array): boolean {
  outer: for (let i = 0; i + needle.length <= haystack.length; i++) {
    for (let j = 0; j < needle.length; j++) if (haystack[i + j] !== needle[j]) continue outer;
    return true;
  }
  return false;
}

/**
 * Whether the token's UTF-8 bytes appear in the attach bytes this call got.
 * The SDK hands functions a view of the whole attach value it opened (on HTTP
 * the unsealed inner bytes, on stdio / unix the raw wire bytes): framework
 * uuid, the catalog's bytes, and the SDK's options trailer. Search the entire
 * backing buffer, not just the catalog's slice of it.
 */
function tokenInAttachData(bytes: Uint8Array, token: string | null): boolean {
  if (token == null) return false;
  const needle = encoder.encode(token);
  const whole = new Uint8Array(bytes.buffer, 0, bytes.buffer.byteLength);
  return contains(bytes, needle) || contains(whole, needle);
}

const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString("hex");

/** The plaintext transaction id of a call, or null outside a transaction. */
function txOf(bindCall: { transaction_opaque_data?: Uint8Array | null }): string | null {
  const tx = bindCall.transaction_opaque_data;
  return tx != null && tx.length > 0 ? hex(tx) : null;
}

/** The state partition of an attach: its token (NULL is a partition of its own). */
const partOf = (o: AttachOptions) => (o.token == null ? "null:" : `token:${o.token}`);

// ------------------------------------------------------------------- state

const INITIAL_ITEMS: [number, string, number][] = [
  [1, "a", 10],
  [2, "b", 20],
  [3, "c", 30],
];
const INITIAL_LABELS: [number, string][] = [
  [1, "x"],
  [2, "y"],
];

/** Every per-partition table of the state file. */
const PART_TABLES = ["items", "labels", "staged_rows", "events", "write_calls"];

function openState(): Database {
  const dir = process.env.VGI_TXFIX_STATE_DIR || join(tmpdir(), "vgi-txfix");
  mkdirSync(dir, { recursive: true });
  const db = new Database(join(dir, "txfix.db"), { create: true, safeIntegers: true });
  db.exec("PRAGMA busy_timeout = 10000");
  db.exec("PRAGMA journal_mode = WAL");
  db.exec(
    "CREATE TABLE IF NOT EXISTS parts (part TEXT PRIMARY KEY);" +
      "CREATE TABLE IF NOT EXISTS items (part TEXT NOT NULL, id INTEGER NOT NULL, name TEXT, qty INTEGER, PRIMARY KEY (part, id));" +
      "CREATE TABLE IF NOT EXISTS labels (part TEXT NOT NULL, id INTEGER NOT NULL, label TEXT, PRIMARY KEY (part, id));" +
      // A transaction's staged image of a row: its new values, or `deleted`.
      "CREATE TABLE IF NOT EXISTS staged_rows (part TEXT NOT NULL, tx TEXT NOT NULL, id INTEGER NOT NULL, name TEXT, qty INTEGER, " +
      "deleted INTEGER NOT NULL, PRIMARY KEY (part, tx, id));" +
      "CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, part TEXT NOT NULL, kind TEXT NOT NULL, tx TEXT, ok INTEGER NOT NULL);" +
      // One row per finished write call (written by its finalize stage).
      "CREATE TABLE IF NOT EXISTS write_calls (seq INTEGER PRIMARY KEY AUTOINCREMENT, part TEXT NOT NULL, op TEXT NOT NULL, " +
      "tx TEXT, rows INTEGER NOT NULL, finalized INTEGER NOT NULL);",
  );
  return db;
}

/** Run `fn` in one write transaction (BEGIN IMMEDIATE: the cross-process lock). */
export function withState<T>(fn: (db: Database) => T): T {
  const db = openState();
  try {
    return db.transaction(fn).immediate(db);
  } finally {
    db.close();
  }
}

export function seed(db: Database, part: string, force: boolean): void {
  const fresh = db.query("INSERT OR IGNORE INTO parts (part) VALUES (?)").run(part).changes === 1;
  if (!fresh && !force) return;
  for (const t of PART_TABLES) db.query(`DELETE FROM ${t} WHERE part = ?`).run(part);
  for (const [id, name, qty] of INITIAL_ITEMS) {
    db.query("INSERT INTO items (part, id, name, qty) VALUES (?, ?, ?, ?)").run(part, id, name, qty);
  }
  for (const [id, label] of INITIAL_LABELS) {
    db.query("INSERT INTO labels (part, id, label) VALUES (?, ?, ?)").run(part, id, label);
  }
}

export interface ItemRow {
  id: number;
  name: string | null;
  qty: bigint | null;
  [key: string]: unknown;
}

const toItem = (r: { id: unknown; name: unknown; qty: unknown }): ItemRow => ({
  id: Number(r.id),
  name: (r.name as string | null) ?? null,
  qty: r.qty == null ? null : BigInt(r.qty as bigint),
});

/** The rows `part` sees: committed, with transaction `tx`'s staged changes on top. */
export function visibleItems(db: Database, part: string, tx: string | null): Map<number, ItemRow> {
  seed(db, part, false);
  const rows = new Map<number, ItemRow>();
  for (const r of db.query("SELECT id, name, qty FROM items WHERE part = ? ORDER BY id").all(part) as any[]) {
    rows.set(Number(r.id), toItem(r));
  }
  if (tx) {
    for (const r of db.query("SELECT id, name, qty, deleted FROM staged_rows WHERE part = ? AND tx = ?").all(part, tx) as any[]) {
      if (Number(r.deleted) === 1) rows.delete(Number(r.id));
      else rows.set(Number(r.id), toItem(r));
    }
  }
  return new Map([...rows.entries()].sort((a, b) => a[0] - b[0]));
}

/** Write `row` (or, with `deleted`, its removal): applied now without `tx`, staged under `tx` with one. */
function putItem(db: Database, part: string, tx: string | null, row: ItemRow, deleted = false): void {
  if (tx) {
    db.query("INSERT OR REPLACE INTO staged_rows (part, tx, id, name, qty, deleted) VALUES (?, ?, ?, ?, ?, ?)").run(
      part, tx, row.id, row.name, row.qty, deleted ? 1 : 0,
    );
  } else if (deleted) {
    db.query("DELETE FROM items WHERE part = ? AND id = ?").run(part, row.id);
  } else {
    db.query("INSERT OR REPLACE INTO items (part, id, name, qty) VALUES (?, ?, ?, ?)").run(part, row.id, row.name, row.qty);
  }
}

function logEvent(db: Database, part: string, kind: string, tx: string | null, ok: boolean): void {
  db.query("INSERT INTO events (part, kind, tx, ok) VALUES (?, ?, ?, ?)").run(part, kind, tx, ok ? 1 : 0);
}

/**
 * Record one finished write call (its finalize stage ran): `rows` is the sum
 * of what its batches reported, carried to finalize in the call's state.
 */
export function logWriteCall(db: Database, part: string, op: string, tx: string | null, rows: number): void {
  db.query("INSERT INTO write_calls (part, op, tx, rows, finalized) VALUES (?, ?, ?, ?, 1)").run(part, op, tx, rows);
}

const UPDATABLE = new Set(["name", "qty"]);

function same(a: unknown, b: unknown): boolean {
  if (a == null || b == null) return a == null && b == null;
  return String(a) === String(b);
}

/** Refuse a negative qty now on the plain attach; a transactional one refuses it at commit. */
function checkQty(tx: string | null, qty: bigint | null): void {
  if (!tx && qty != null && qty < 0n) throw new Error("txfix: qty must be >= 0");
}

/**
 * INSERT `rows` (one batch). Validates every row before applying any: NULL
 * name, an id that is NULL or already present (committed, staged by `tx`, or
 * earlier in this batch; earlier batches of the statement are already applied
 * or staged), and on the plain attach a negative qty.
 */
export function applyInsert(db: Database, part: string, tx: string | null, rows: ItemRow[]): ItemRow[] {
  const current = visibleItems(db, part, tx);
  for (const row of rows) {
    if (row.id == null || Number.isNaN(row.id)) throw new Error("txfix: id required");
    if (row.name == null) throw new Error("txfix: name required");
    if (current.has(row.id)) throw new Error(`txfix: duplicate id ${row.id}`);
    checkQty(tx, row.qty);
    current.set(row.id, row);
  }
  for (const row of rows) putItem(db, part, tx, row);
  return rows;
}

/** UPDATE: `changes[i]` holds the new values of the row whose rowid is `rowids[i]`. */
export function applyUpdate(
  db: Database,
  part: string,
  tx: string | null,
  rowids: number[],
  changes: Record<string, unknown>[],
): { before: ItemRow[]; after: ItemRow[] } {
  const current = visibleItems(db, part, tx);
  const before: ItemRow[] = [];
  const after: ItemRow[] = [];
  // Validate every row before applying any.
  rowids.forEach((rowid, i) => {
    const old = current.get(rowid);
    if (!old) throw new Error(`txfix: no row with rowid ${rowid}`);
    const next: ItemRow = { ...old };
    for (const [name, value] of Object.entries(changes[i]!)) {
      if (same(value, old[name])) continue;
      if (name === "id") throw new Error("txfix: id is not updatable");
      if (!UPDATABLE.has(name)) throw new Error(`txfix: ${name} is not a column of items`);
      if (name === "name" && value == null) throw new Error("txfix: name must not be NULL");
      next[name] = name === "qty" && value != null ? BigInt(value as bigint) : value;
    }
    checkQty(tx, next.qty);
    before.push(old);
    after.push(next);
    current.set(rowid, next);
  });
  for (const r of after) putItem(db, part, tx, r);
  return { before, after };
}

/** DELETE by rowid (= id). A missing id is a no-op and a repeated one counts once. */
export function applyDelete(db: Database, part: string, tx: string | null, rowids: number[]): ItemRow[] {
  const current = visibleItems(db, part, tx);
  const deleted: ItemRow[] = [];
  for (const rowid of rowids) {
    const old = current.get(rowid);
    if (!old) continue;
    current.delete(rowid);
    deleted.push(old);
  }
  for (const r of deleted) putItem(db, part, tx, r, true);
  return deleted;
}

/**
 * Apply transaction `tx`'s staged changes, or refuse the commit (discarding
 * them) when a row it leaves behind has a negative qty. Returns the refusal.
 */
export function commitStaged(db: Database, part: string, tx: string): string | null {
  const staged = db.query("SELECT id, name, qty, deleted FROM staged_rows WHERE part = ? AND tx = ? ORDER BY id").all(part, tx) as any[];
  db.query("DELETE FROM staged_rows WHERE part = ? AND tx = ?").run(part, tx);
  const bad = staged.find((r) => Number(r.deleted) !== 1 && r.qty != null && BigInt(r.qty) < 0n);
  if (bad) {
    logEvent(db, part, "commit", tx, false);
    return `txfix: commit refused: qty ${bad.qty} for id ${bad.id}`;
  }
  for (const r of staged) putItem(db, part, null, toItem(r), Number(r.deleted) === 1);
  logEvent(db, part, "commit", tx, true);
  return null;
}

// ------------------------------------------------------------------ tables

// The rows of items, as scans and write results carry them.
const ITEMS = schema([field("id", int32(), false), field("name", utf8(), false), field("qty", int64(), true)]);
// The table's declared columns: `qty BIGINT DEFAULT 0`, a canonical
// vgi.column.default.v1 constant, which DuckDB fills in for an omitted qty
// before items_insert sees the row. (Column-expression metadata belongs on the
// table's columns only, never on a function's output schema.)
const QTY_DEFAULT = new Map([[DEFAULT_EXPRESSION_KEY, defaultExpressionMetadata("0", ["main"])]]);
const ITEMS_COLUMNS = schema([ITEMS.fields[0]!, ITEMS.fields[1]!, field("qty", int64(), true, QTY_DEFAULT)]);
const LABELS = schema([field("id", int32(), false), field("label", utf8(), true)]);

function constraints(table: string, notNull: string[], key: string): Uint8Array[] {
  const base = { schemaPath: ["main"], table, enforcement: "ENFORCED", validation: "ALL_ROWS" } as const;
  return [
    ...notNull.map((col) => constraintRecord({ ...base, id: `${col}_not_null`, kind: "NOT_NULL", columns: [[col]] })),
    constraintRecord({ ...base, id: "pk", kind: "PRIMARY_KEY", columns: [[key]] }),
  ];
}

interface Once {
  done: boolean;
}

/** A no-argument table function that emits one batch. */
function oneShot(
  name: string,
  description: string,
  output: VgiSchema,
  rows: (bindCall: { attach_opaque_data?: Uint8Array | null; transaction_opaque_data?: Uint8Array | null }) => Record<
    string,
    unknown[]
  >,
): VgiFunction {
  return defineTableFunction<Record<string, never>, Once>({
    name,
    description,
    stability: FunctionStability.VOLATILE,
    onBind: () => ({ outputSchema: output }),
    initialState: () => ({ done: false }),
    // Everything is read here, at process time, from the init call's bind call.
    process: (params, state, out) => {
      if (state.done) return out.finish();
      state.done = true;
      out.emit(batchFromColumns(rows(params.initCall.bind_call), output));
    },
  });
}

const itemsScan = defineTableFunction<Record<string, never>, Once>({
  name: "items_scan",
  description: "The rows of main.items (staged changes included inside a transaction), with rowid",
  stability: FunctionStability.VOLATILE,
  // DuckDB reads `rowid` as a virtual column, which needs projection pushdown.
  projectionPushdown: true,
  onBind: () => ({ outputSchema: schema([...ITEMS.fields, rowIdField()]) }),
  initialState: () => ({ done: false }),
  // The scan reads its snapshot once, when it starts, and emits it whole: so
  // INSERT INTO items SELECT ... FROM items never sees the rows it inserts.
  process: (params, state, out) => {
    if (state.done) return out.finish();
    state.done = true;
    const bindCall = params.initCall.bind_call;
    const rows = [...withState((db) => visibleItems(db, partOf(decodeAttach(bindCall.attach_opaque_data)), txOf(bindCall))).values()];
    const cols: Record<string, unknown[]> = {};
    for (const f of params.outputSchema.fields) {
      cols[f.name] = rows.map((r) => (f.name === "rowid" ? BigInt(r.id) : r[f.name]));
    }
    out.emit(batchFromColumns(cols, params.outputSchema));
  },
});

const labelsScan = oneShot("labels_scan", "The rows of main.labels", LABELS, (bindCall) => {
  const part = partOf(decodeAttach(bindCall.attach_opaque_data));
  const rows = withState((db) => {
    seed(db, part, false);
    return db.query("SELECT id, label FROM labels WHERE part = ? ORDER BY id").all(part) as any[];
  });
  return { id: rows.map((r) => Number(r.id)), label: rows.map((r) => r.label) };
});

type WriteOp = "insert" | "update" | "delete";

/** The call's attach partition and the transaction it stages under (transactional attaches only). */
function writeContext(params: { initCall: { bind_call: any } }) {
  const bindCall = params.initCall.bind_call;
  const attach = decodeAttach(bindCall.attach_opaque_data);
  return {
    bindCall,
    part: partOf(attach),
    // Staging (and writes().tx_id): only a transactional attach stages, under
    // the plaintext id; NULL on the non-transactional attach.
    tx: attach.transactional ? txOf(bindCall) : null,
  };
}

/** Per write call: the rows its batches reported so far (the SDK carries it to finalize). */
interface WriteCallState {
  rows: number;
}

/**
 * A write function of `items`. `apply` validates and applies (or stages) one
 * batch inside the state lock and returns the old/new row images for the
 * result. The call's state counts the rows; its finalize stage records the
 * call in `writes()`.
 */
function itemsWriteFunction(
  op: WriteOp,
  description: string,
  apply: (db: Database, part: string, tx: string | null, batch: VgiBatch) => { before: (ItemRow | null)[]; after: (ItemRow | null)[] },
): VgiFunction {
  return defineTableInOutFunction<Record<string, never>, WriteCallState>({
    name: `items_${op}`,
    description,
    stability: FunctionStability.VOLATILE,
    onBind: (params) => ({ outputSchema: writeResultSchema(writeOptions(params.bindCall).resultMode, ITEMS) }),
    initialState: () => ({ rows: 0 }),
    process: (params, state, batch: VgiBatch, out) => {
      const ctx = writeContext(params);
      const { before, after } = withState((db) => apply(db, ctx.part, ctx.tx, batch));
      state.rows += after.length;
      out.emit(writeResultBatch(writeOptions(ctx.bindCall).resultMode, ITEMS, before, after));
    },
    // DuckDB runs this once per write call after its last batch (also for a
    // call that got none), and never after a failed statement. No output: the
    // count is the batches' sum.
    finalize: (params, states) => {
      const ctx = writeContext(params);
      const rows = states.reduce((sum, st) => sum + (st?.rows ?? 0), 0);
      withState((db) => logWriteCall(db, ctx.part, op, ctx.tx, rows));
      return [];
    },
  });
}

const cell = (batch: VgiBatch, name: string, i: number): unknown => batch.getChild(name)?.get(i) ?? null;
const rowidsOf = (batch: VgiBatch): number[] => Array.from({ length: batch.numRows }, (_, i) => Number(cell(batch, "rowid", i)));

const itemsInsert = itemsWriteFunction(
  "insert",
  "Backs INSERT INTO main.items: inserts (or, in a transaction, stages) the new rows",
  (db, part, tx, batch) => {
    const rows: ItemRow[] = Array.from({ length: batch.numRows }, (_, i) => {
      const id = cell(batch, "id", i);
      const qty = cell(batch, "qty", i);
      return {
        id: id == null ? Number.NaN : Number(id),
        name: (cell(batch, "name", i) as string | null) ?? null,
        qty: qty == null ? null : BigInt(qty as bigint),
      };
    });
    const inserted = applyInsert(db, part, tx, rows);
    return { before: inserted.map(() => null), after: inserted };
  },
);

const itemsUpdate = itemsWriteFunction(
  "update",
  "Backs UPDATE main.items: applies (or, in a transaction, stages) the new values",
  (db, part, tx, batch) => {
    const names = batch.schema.fields.map((f) => f.name).filter((n) => n !== "rowid");
    const changes = Array.from({ length: batch.numRows }, (_, i) => Object.fromEntries(names.map((n) => [n, cell(batch, n, i)])));
    return applyUpdate(db, part, tx, rowidsOf(batch), changes);
  },
);

const itemsDelete = itemsWriteFunction(
  "delete",
  "Backs DELETE FROM main.items: deletes (or, in a transaction, stages the deletion of) rows by rowid",
  (db, part, tx, batch) => {
    const deleted = applyDelete(db, part, tx, rowidsOf(batch));
    return { before: deleted, after: deleted.map(() => null) };
  },
);

// --------------------------------------------------------------- functions

const resetFn = oneShot("reset", "Restore this token's initial rows; clear its staged changes, events and write log", schema([field("ok", bool(), true)]), (bindCall) => {
  const part = partOf(decodeAttach(bindCall.attach_opaque_data));
  withState((db) => seed(db, part, true));
  return { ok: [true] };
});

const whoamiFn = oneShot(
  "whoami",
  "The attach's options as functions see them",
  schema([field("token", utf8(), true), field("transactional", bool(), true), field("token_in_attach_data", bool(), true)]),
  (bindCall) => {
    const o = decodeAttach(bindCall.attach_opaque_data);
    return {
      token: [o.token],
      transactional: [o.transactional],
      token_in_attach_data: [tokenInAttachData(bindCall.attach_opaque_data!, o.token)],
    };
  },
);

const currentTxFn = oneShot(
  "current_tx",
  "The plaintext transaction id this call received (lowercase hex)",
  schema([field("in_tx", bool(), true), field("tx_id", utf8(), true)]),
  (bindCall) => {
    const tx = txOf(bindCall);
    return { in_tx: [tx != null], tx_id: [tx] };
  },
);

const eventsFn = oneShot(
  "events",
  "This token's transaction-hook event log",
  schema([field("seq", int64(), true), field("kind", utf8(), true), field("tx_id", utf8(), true), field("ok", bool(), true)]),
  (bindCall) => {
    const part = partOf(decodeAttach(bindCall.attach_opaque_data));
    const rows = withState((db) => db.query("SELECT seq, kind, tx, ok FROM events WHERE part = ? ORDER BY seq").all(part) as any[]);
    return {
      seq: rows.map((r) => BigInt(r.seq)),
      kind: rows.map((r) => r.kind),
      tx_id: rows.map((r) => r.tx ?? null),
      ok: rows.map((r) => Number(r.ok) === 1),
    };
  },
);

const writesFn = oneShot(
  "writes",
  "This token's write-call log: one row per call of items_insert / items_update / items_delete",
  schema([
    field("seq", int64(), true),
    field("op", utf8(), true),
    field("tx_id", utf8(), true),
    field("rows", int64(), true),
    field("finalized", bool(), true),
  ]),
  (bindCall) => {
    const part = partOf(decodeAttach(bindCall.attach_opaque_data));
    const rows = withState((db) => db.query("SELECT seq, op, tx, rows, finalized FROM write_calls WHERE part = ? ORDER BY seq").all(part) as any[]);
    return {
      seq: rows.map((r) => BigInt(r.seq)),
      op: rows.map((r) => r.op),
      tx_id: rows.map((r) => r.tx ?? null),
      rows: rows.map((r) => BigInt(r.rows)),
      finalized: rows.map((r) => Number(r.finalized) === 1),
    };
  },
);

const tagTxFn = defineTableInOutFunction({
  name: "tag_tx",
  description: "Pass every input row through, adding the transaction id seen while processing it",
  stability: FunctionStability.VOLATILE,
  onBind: (params) => ({
    outputSchema: schema([...(params.bindCall.input_schema?.fields ?? []), field("tx_id", utf8(), true)]),
  }),
  process: (params, _state, batch: VgiBatch, out) => {
    const tx = txOf(params.initCall.bind_call);
    const cols: Record<string, unknown[]> = {};
    for (const f of batch.schema.fields) {
      const child = batch.getChild(f.name);
      cols[f.name] = Array.from({ length: batch.numRows }, (_, i) => child?.get(i) ?? null);
    }
    cols.tx_id = Array.from({ length: batch.numRows }, () => tx);
    out.emit(batchFromColumns(cols, params.outputSchema));
  },
});

// ----------------------------------------------------------------- catalog

const itemsTable: TableDescriptor = {
  name: "items",
  columns: schema([...ITEMS_COLUMNS.fields, rowIdField()]),
  function: itemsScan,
  insertFunction: itemsInsert,
  updateFunction: itemsUpdate,
  deleteFunction: itemsDelete,
  writeResultModes: { insert: "changes", update: "changes", delete: "changes" },
  constraints: constraints("items", ["id", "name"], "id"),
};

const labelsTable: TableDescriptor = {
  name: "labels",
  columns: LABELS,
  function: labelsScan,
  // Protocol 3.3 fixture contract: labels_scan is callable; items_scan stays
  // table_only (the default), so it is hidden from duckdb_functions().
  exposeScan: true,
  constraints: constraints("labels", ["id"], "id"),
};

export const descriptor: CatalogDescriptor = {
  name: CATALOG_NAME,
  defaultSchema: "main",
  attachOptions: ATTACH_OPTIONS,
  schemas: [
    {
      name: "main",
      // items_scan, labels_scan and the items_* write functions are not listed
      // here: the SDK always sends a table's scan and write functions
      // (protocol 3.3), so DuckDB still sees the projection pushdown items_scan
      // needs for rowid and the write functions' has_finalize.
      functions: [resetFn, whoamiFn, currentTxFn, eventsFn, writesFn, tagTxFn],
      tables: [itemsTable, labelsTable],
    },
  ],
};

/** The descriptor catalog plus the ATTACH options and the transaction hooks. */
export class TxfixCatalog extends ReadOnlyCatalogInterface {
  override async attach(
    name: string,
    options?: Record<string, unknown>,
    dataVersionSpec?: string | null,
    implementationVersion?: string | null,
    clientCapabilities?: Uint8Array | null,
  ): Promise<CatalogAttachResult> {
    // super.attach mints a random attach id; the options (token included)
    // reach functions and hooks through attachOptions(), not these bytes.
    const result = await super.attach(name, options, dataVersionSpec, implementationVersion, clientCapabilities);
    return {
      ...result,
      // Per attach: only `transactional true` asks DuckDB for the hooks below.
      supports_transactions: options?.transactional === true,
    };
  }

  override transactionBegin(attachOpaqueData: Uint8Array): Uint8Array {
    const id = crypto.getRandomValues(new Uint8Array(16));
    const part = partOf(decodeAttach(attachOpaqueData));
    withState((db) => {
      seed(db, part, false);
      logEvent(db, part, "begin", hex(id), true);
    });
    return id;
  }

  override transactionCommit(attachOpaqueData: Uint8Array, transactionOpaqueData: Uint8Array): void {
    const part = partOf(decodeAttach(attachOpaqueData));
    const refusal = withState((db) => commitStaged(db, part, hex(transactionOpaqueData)));
    if (refusal) throw new Error(refusal);
  }

  override transactionRollback(attachOpaqueData: Uint8Array, transactionOpaqueData: Uint8Array): void {
    const part = partOf(decodeAttach(attachOpaqueData));
    const tx = hex(transactionOpaqueData);
    withState((db) => {
      db.query("DELETE FROM staged_rows WHERE part = ? AND tx = ?").run(part, tx);
      logEvent(db, part, "rollback", tx, true);
    });
  }
}

if (import.meta.main) {
  const args = process.argv.slice(2);
  const registry = new FunctionRegistry();
  const catalogInterface = new TxfixCatalog(descriptor, registry);
  if (args.includes("--http")) {
    const portAt = args.indexOf("--port");
    const server = serveVgiWorker({
      name: CATALOG_NAME,
      doc: "Writable-table and transaction-hook fixture (vgi docs/writable_tx_fixtures.md)",
      version: "0.0.0",
      registry,
      catalogInterface,
      hostname: "127.0.0.1",
      port: portAt >= 0 ? Number(args[portAt + 1]) : 0,
      quiet: true,
      // A fixture: one process, so a per-run state-token key is all it needs.
      signingKey: crypto.getRandomValues(new Uint8Array(32)),
      argv: [],
    });
    console.log(`PORT:${server.port}`);
  } else {
    new Worker({ registry, catalogInterface }).run(args);
  }
}
