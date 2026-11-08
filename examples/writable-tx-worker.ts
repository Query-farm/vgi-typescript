// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// The `txfix` fixture worker: the TypeScript implementation of the C++
// extension's writable-table and transaction-hook fixtures
// (Query-farm/vgi docs/writable_tx_fixtures.md), serving
// test/sql/integration/writable_tx/*.test (VGI_WRITABLE_TX_WORKER).
//
// Built only from the SDK's public API: a descriptor catalog whose `items`
// table declares an update function and write result modes, ATTACH options
// (one of them secret), a per-attach `supports_transactions`, and the
// catalog's transactionBegin / transactionCommit / transactionRollback hooks.
//
//   ATTACH 'txfix' AS t   (TYPE vgi, LOCATION '...', token 'tok-123');
//   ATTACH 'txfix' AS ttx (TYPE vgi, LOCATION '...', token 'tok-123', transactional true);
//
// On a transactional attach UPDATE stages its changes under the plaintext
// transaction id the function sees; scans on that attach read their own
// writes; the commit hook applies them (or refuses a negative qty) and the
// rollback hook discards them. State lives in one SQLite file under
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
  type CatalogAttachResult,
  type CatalogDescriptor,
  constraintRecord,
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

const MAGIC = "txfix1:";
const encoder = new TextEncoder();
const decoder = new TextDecoder();

function encodeAttach(options: Record<string, unknown> | undefined): Uint8Array {
  const token = typeof options?.token === "string" ? options.token : null;
  return encoder.encode(MAGIC + JSON.stringify({ token, transactional: options?.transactional === true }));
}

function decodeAttach(bytes: Uint8Array | null | undefined): AttachOptions {
  const text = bytes && bytes.length ? decoder.decode(bytes) : "";
  if (!text.startsWith(MAGIC)) throw new Error("txfix: attach_opaque_data missing or not minted by this worker");
  return JSON.parse(text.slice(MAGIC.length)) as AttachOptions;
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
      "CREATE TABLE IF NOT EXISTS staged (part TEXT NOT NULL, tx TEXT NOT NULL, id INTEGER NOT NULL, name TEXT, qty INTEGER, PRIMARY KEY (part, tx, id));" +
      "CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY AUTOINCREMENT, part TEXT NOT NULL, kind TEXT NOT NULL, tx TEXT, ok INTEGER NOT NULL);",
  );
  return db;
}

/** Run `fn` in one write transaction (BEGIN IMMEDIATE: the cross-process lock). */
function withState<T>(fn: (db: Database) => T): T {
  const db = openState();
  try {
    return db.transaction(fn).immediate(db);
  } finally {
    db.close();
  }
}

function seed(db: Database, part: string, force: boolean): void {
  const fresh = db.query("INSERT OR IGNORE INTO parts (part) VALUES (?)").run(part).changes === 1;
  if (!fresh && !force) return;
  for (const t of ["items", "labels", "staged", "events"]) db.query(`DELETE FROM ${t} WHERE part = ?`).run(part);
  for (const [id, name, qty] of INITIAL_ITEMS) {
    db.query("INSERT INTO items (part, id, name, qty) VALUES (?, ?, ?, ?)").run(part, id, name, qty);
  }
  for (const [id, label] of INITIAL_LABELS) {
    db.query("INSERT INTO labels (part, id, label) VALUES (?, ?, ?)").run(part, id, label);
  }
}

interface ItemRow {
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

/** The rows `part` sees: committed, with transaction `tx`'s staged rows on top. */
function visibleItems(db: Database, part: string, tx: string | null): Map<number, ItemRow> {
  seed(db, part, false);
  const rows = new Map<number, ItemRow>();
  for (const r of db.query("SELECT id, name, qty FROM items WHERE part = ? ORDER BY id").all(part) as any[]) {
    rows.set(Number(r.id), toItem(r));
  }
  if (tx) {
    for (const r of db.query("SELECT id, name, qty FROM staged WHERE part = ? AND tx = ?").all(part, tx) as any[]) {
      rows.set(Number(r.id), toItem(r));
    }
  }
  return new Map([...rows.entries()].sort((a, b) => a[0] - b[0]));
}

function logEvent(db: Database, part: string, kind: string, tx: string | null, ok: boolean): void {
  db.query("INSERT INTO events (part, kind, tx, ok) VALUES (?, ?, ?, ?)").run(part, kind, tx, ok ? 1 : 0);
}

// ------------------------------------------------------------------ tables

const ITEMS = schema([field("id", int32(), false), field("name", utf8(), false), field("qty", int64(), true)]);
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

const UPDATABLE = new Set(["name", "qty"]);

function same(a: unknown, b: unknown): boolean {
  if (a == null || b == null) return a == null && b == null;
  return String(a) === String(b);
}

const itemsUpdate = defineTableInOutFunction({
  name: "items_update",
  description: "Backs UPDATE main.items: applies (or, in a transaction, stages) the new values",
  stability: FunctionStability.VOLATILE,
  onBind: (params) => ({ outputSchema: writeResultSchema(writeOptions(params.bindCall).resultMode, ITEMS) }),
  process: (params, _state, batch: VgiBatch, out) => {
    const bindCall = params.initCall.bind_call;
    const attach = decodeAttach(bindCall.attach_opaque_data);
    const part = partOf(attach);
    const tx = attach.transactional ? txOf(bindCall) : null;
    const { before, after } = withState((db) => {
      const current = visibleItems(db, part, tx);
      const before: ItemRow[] = [];
      const after: ItemRow[] = [];
      // Validate every row before applying any.
      for (let i = 0; i < batch.numRows; i++) {
        const rowid = Number(batch.getChild("rowid")?.get(i));
        const old = current.get(rowid);
        if (!old) throw new Error(`txfix: no row with rowid ${rowid}`);
        const next: ItemRow = { ...old };
        for (const f of batch.schema.fields) {
          if (f.name === "rowid") continue;
          const value = batch.getChild(f.name)?.get(i) ?? null;
          if (same(value, old[f.name])) continue;
          if (f.name === "id") throw new Error("txfix: id is not updatable");
          if (!UPDATABLE.has(f.name)) throw new Error(`txfix: ${f.name} is not a column of items`);
          if (f.name === "name" && value == null) throw new Error("txfix: name must not be NULL");
          next[f.name] = f.name === "qty" && value != null ? BigInt(value as bigint) : value;
        }
        if (!tx && next.qty != null && next.qty < 0n) throw new Error("txfix: qty must be >= 0");
        before.push(old);
        after.push(next);
        current.set(rowid, next);
      }
      const stage = db.query("INSERT OR REPLACE INTO staged (part, tx, id, name, qty) VALUES (?, ?, ?, ?, ?)");
      const apply = db.query("UPDATE items SET name = ?, qty = ? WHERE part = ? AND id = ?");
      for (const r of after) {
        if (tx) stage.run(part, tx, r.id, r.name, r.qty);
        else apply.run(r.name, r.qty, part, r.id);
      }
      return { before, after };
    });
    out.emit(writeResultBatch(writeOptions(bindCall).resultMode, ITEMS, before, after));
  },
});

// --------------------------------------------------------------- functions

const resetFn = oneShot("reset", "Restore this token's initial rows; clear its staged changes and events", schema([field("ok", bool(), true)]), (bindCall) => {
  const part = partOf(decodeAttach(bindCall.attach_opaque_data));
  withState((db) => seed(db, part, true));
  return { ok: [true] };
});

const whoamiFn = oneShot(
  "whoami",
  "The attach's options as functions see them",
  schema([field("token", utf8(), true), field("transactional", bool(), true)]),
  (bindCall) => {
    const o = decodeAttach(bindCall.attach_opaque_data);
    return { token: [o.token], transactional: [o.transactional] };
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
  columns: schema([...ITEMS.fields, rowIdField()]),
  function: itemsScan,
  updateFunction: itemsUpdate,
  writeResultModes: { update: "changes" },
  constraints: constraints("items", ["id", "name"], "id"),
};

const labelsTable: TableDescriptor = {
  name: "labels",
  columns: LABELS,
  function: labelsScan,
  constraints: constraints("labels", ["id"], "id"),
};

export const descriptor: CatalogDescriptor = {
  name: CATALOG_NAME,
  defaultSchema: "main",
  attachOptions: ATTACH_OPTIONS,
  schemas: [
    {
      name: "main",
      // items_scan is listed so DuckDB sees its projection pushdown (rowid needs it).
      functions: [itemsScan, resetFn, whoamiFn, currentTxFn, eventsFn, tagTxFn],
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
    const result = await super.attach(name, options, dataVersionSpec, implementationVersion, clientCapabilities);
    return {
      ...result,
      attach_opaque_data: encodeAttach(options),
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
    const tx = hex(transactionOpaqueData);
    const refusal = withState((db) => {
      const rows = (db.query("SELECT id, name, qty FROM staged WHERE part = ? AND tx = ? ORDER BY id").all(part, tx) as any[]).map(toItem);
      db.query("DELETE FROM staged WHERE part = ? AND tx = ?").run(part, tx);
      const bad = rows.find((r) => r.qty != null && r.qty < 0n);
      if (bad) {
        logEvent(db, part, "commit", tx, false);
        return `txfix: commit refused: qty ${bad.qty} for id ${bad.id}`;
      }
      const apply = db.query("UPDATE items SET name = ?, qty = ? WHERE part = ? AND id = ?");
      for (const r of rows) apply.run(r.name, r.qty, part, r.id);
      logEvent(db, part, "commit", tx, true);
      return null;
    });
    if (refusal) throw new Error(refusal);
  }

  override transactionRollback(attachOpaqueData: Uint8Array, transactionOpaqueData: Uint8Array): void {
    const part = partOf(decodeAttach(attachOpaqueData));
    const tx = hex(transactionOpaqueData);
    withState((db) => {
      db.query("DELETE FROM staged WHERE part = ? AND tx = ?").run(part, tx);
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
