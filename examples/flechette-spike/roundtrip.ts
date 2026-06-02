// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Flechette feasibility spike: round-trip every type case from cases.ts
// through both directions of arrow-js <-> flechette IPC, and report what
// works versus what flechette can't produce/consume.
//
// Usage: bun run examples/flechette-spike/roundtrip.ts
//
// What it verifies (matches the plan at
// /Users/rusty/.claude/plans/right-now-we-re-using-floating-horizon.md):
//   1. arrow-js encode -> flechette decode (read path)
//   2. flechette encode -> arrow-js decode (write path)
//   3. Streaming concat: encode N small flechette tables, pass as
//      Uint8Array[] to tableFromIPC.

import {
  tableFromIPC as f_tableFromIPC,
  tableToIPC as f_tableToIPC,
  tableFromColumns as f_tableFromColumns,
  columnFromArray as f_columnFromArray,
  type Table as FTable,
} from "@uwdata/flechette";

import {
  RecordBatchReader,
  type RecordBatch,
} from "@query-farm/apache-arrow";

import { batchFromColumns } from "../../src/util/arrow/build.js";
import { serializeBatch } from "../../src/util/arrow/ipc.js";
import { CASES, type SpikeCase } from "./cases.js";

const FLECHETTE_OPTS = {
  useBigInt: true,
  useBigIntTimestamp: true,
  useDecimalInt: true, // BigInt for >=64-bit decimals
  useMap: false,       // [k,v] pairs match our input shape
};

// Build a flechette Table from a SpikeCase by columnFromArray-ing each field.
function buildFlechetteTable(c: SpikeCase): FTable {
  const cols: Record<string, ReturnType<typeof f_columnFromArray>> = {};
  for (const fld of c.flechetteFields) {
    cols[fld.name] = f_columnFromArray(c.columns[fld.name], fld.type, FLECHETTE_OPTS);
  }
  return f_tableFromColumns(cols);
}

// Normalize values for cross-implementation comparison. arrow-js and
// flechette return semantically equal values in slightly different shapes:
//   - arrow-js Decimal .get() -> Uint32Array view (raw LE bytes)
//   - flechette Decimal with useDecimalInt -> BigInt
//   - arrow-js List .get() -> Vector (iterable)
//   - flechette List .at() -> typed array (Int32Array etc.) or array
//   - arrow-js Map .get() -> plain object {k: v}
//   - flechette Map (useMap=false) -> [[k,v], ...]
// `decimalByteWidth` (when set) signals that a Uint32Array/Uint8Array at the
// leaf is a Decimal value to be reinterpreted as a BigInt.
function normalize(v: any, decimalByteWidth?: number): any {
  if (v === null || v === undefined) return null;
  if (typeof v === "bigint") return `bi:${v.toString()}`;

  // Decimal raw bytes -> signed LE BigInt (only when caller flagged the field as Decimal)
  if (decimalByteWidth !== undefined && ArrayBuffer.isView(v)) {
    const u8 = new Uint8Array(
      (v as ArrayBufferView).buffer,
      (v as ArrayBufferView).byteOffset,
      (v as ArrayBufferView).byteLength
    );
    let bi = 0n;
    for (let i = u8.length - 1; i >= 0; i--) bi = (bi << 8n) | BigInt(u8[i]);
    if (u8[u8.length - 1] & 0x80) bi -= 1n << BigInt(u8.length * 8);
    return `bi:${bi.toString()}`;
  }

  if (v instanceof Uint8Array) return `u8:${Array.from(v).join(",")}`;

  // Generic TypedArray (Int32Array, Float64Array, BigInt64Array, ...) -> plain array
  if (ArrayBuffer.isView(v)) {
    return Array.from(v as any).map((x) => normalize(x));
  }

  if (v instanceof Map) {
    return Array.from(v.entries())
      .map(([k, x]) => [normalize(k), normalize(x)])
      .sort((a, b) => String(a[0]).localeCompare(String(b[0])));
  }

  if (Array.isArray(v)) return v.map((x) => normalize(x));

  // Anything else iterable (arrow-js Vector, flechette Map [k,v] iterator)
  if (typeof v === "object" && typeof v[Symbol.iterator] === "function") {
    return Array.from(v).map((x) => normalize(x));
  }

  if (typeof v === "object") {
    // arrow-js StructRow / arrow-js Map's plain object / regular objects.
    // Sort keys for determinism and convert to [k,v] pairs so it compares
    // equally to a Map-as-pairs representation.
    const keys = Object.keys(v).sort();
    return keys.map((k) => [k, normalize(v[k])]);
  }
  return v;
}

function eq(a: any, b: any, decimalByteWidth?: number): boolean {
  return (
    JSON.stringify(normalize(a, decimalByteWidth)) ===
    JSON.stringify(normalize(b, decimalByteWidth))
  );
}

// Detect if a flechette field type is Decimal at the leaf (top-level).
// Top-level only: nested Decimal-in-List is not exercised by our cases.
function topLevelDecimalByteWidth(type: any): number | undefined {
  if (type && type.typeId === 7 /* Decimal */) {
    return (type.bitWidth ?? 128) / 8;
  }
  return undefined;
}

function compareColumn(
  caseName: string,
  fieldName: string,
  expected: any[],
  got: any[],
  decimalByteWidth?: number
): { ok: boolean; firstFail?: string } {
  if (expected.length !== got.length) {
    return {
      ok: false,
      firstFail: `length mismatch: expected ${expected.length}, got ${got.length}`,
    };
  }
  for (let i = 0; i < expected.length; i++) {
    if (!eq(expected[i], got[i], decimalByteWidth)) {
      return {
        ok: false,
        firstFail: `[${caseName}/${fieldName}/row=${i}] expected=${JSON.stringify(
          normalize(expected[i], decimalByteWidth)
        )} got=${JSON.stringify(normalize(got[i], decimalByteWidth))}`,
      };
    }
  }
  return { ok: true };
}

// arrow-js batch -> raw rows by column, using whatever shape arrow-js exposes.
// Special-case Timestamp/Duration: arrow-js's high-level .get() truncates
// nanosecond Int64 values to JS Number ms, which is lossy. Read the raw
// BigInt64Array directly to verify wire-level correctness.
function arrowBatchToColumns(batch: RecordBatch): Record<string, any[]> {
  const out: Record<string, any[]> = {};
  for (const f of batch.schema.fields) {
    const col = batch.getChild(f.name) as any;
    const t = f.type as any;
    // arrow-js's runtime class names are name-mangled (Timestamp_, Duration_).
    // Match by typeId instead: 10 = Timestamp, 18 = Duration.
    const isTimestampOrDuration = t && (t.typeId === 10 || t.typeId === 18);

    if (isTimestampOrDuration && col?.data?.[0]?.values?.constructor === BigInt64Array) {
      const raw = col.data[0].values as BigInt64Array;
      const nullBitmap = col.data[0].nullBitmap as Uint8Array | undefined;
      const arr: any[] = [];
      for (let i = 0; i < batch.numRows; i++) {
        const valid = !nullBitmap || nullBitmap.length === 0 || ((nullBitmap[i >> 3] >> (i & 7)) & 1);
        arr.push(valid ? raw[i] : null);
      }
      out[f.name] = arr;
      continue;
    }

    const arr: any[] = [];
    for (let i = 0; i < batch.numRows; i++) arr.push(col.get(i));
    out[f.name] = arr;
  }
  return out;
}

// flechette table -> raw rows by column
function flechetteTableToColumns(t: FTable): Record<string, any[]> {
  const out: Record<string, any[]> = {};
  for (let i = 0; i < t.numCols; i++) {
    const col = t.getChildAt(i);
    const name = String(t.names[i]);
    const arr: any[] = [];
    for (let j = 0; j < col.length; j++) arr.push(col.at(j));
    out[name] = arr;
  }
  return out;
}

interface CaseResult {
  name: string;
  arrowToFlechette: { ok: boolean; error?: string };
  flechetteToArrow: { ok: boolean; error?: string };
  flechetteSelf: { ok: boolean; error?: string };
  notes?: string;
}

function runCase(c: SpikeCase): CaseResult {
  const result: CaseResult = {
    name: c.name,
    arrowToFlechette: { ok: false },
    flechetteToArrow: { ok: false },
    flechetteSelf: { ok: false },
    notes: c.notes,
  };

  // ---- arrow-js -> flechette (read path) ----
  // Skip if the case has no arrow-js schema (e.g. union, which only
  // statistics.ts builds via a custom path).
  const hasArrowSchema = c.arrowSchema.fields.length > 0;
  if (hasArrowSchema) {
    try {
      const batch = batchFromColumns(c.columns, c.arrowSchema);
      const bytes = serializeBatch(batch);
      const ftable = f_tableFromIPC(bytes, FLECHETTE_OPTS);
      const got = flechetteTableToColumns(ftable);
      let cmp: { ok: boolean; firstFail?: string } = { ok: true };
      for (const fld of c.arrowSchema.fields) {
        const fFld = c.flechetteFields.find((f) => f.name === fld.name);
        const dw = fFld ? topLevelDecimalByteWidth(fFld.type) : undefined;
        const r = compareColumn(c.name, fld.name, c.columns[fld.name], got[fld.name], dw);
        if (!r.ok) {
          cmp = r;
          break;
        }
      }
      result.arrowToFlechette = cmp.ok
        ? { ok: true }
        : { ok: false, error: cmp.firstFail };
    } catch (e: any) {
      result.arrowToFlechette = { ok: false, error: `threw: ${e.message ?? e}` };
    }
  } else {
    result.arrowToFlechette = { ok: true, error: "skipped (no arrow-js schema)" };
  }

  // ---- flechette -> arrow-js (write path) ----
  let ftable: FTable | null = null;
  try {
    ftable = buildFlechetteTable(c);
    const bytes = f_tableToIPC(ftable, { format: "stream" }) as Uint8Array;
    const reader = RecordBatchReader.from(bytes);
    const batches = [...reader];
    if (batches.length === 0) throw new Error("arrow-js read 0 batches");
    const batch = batches[0];
    const got = arrowBatchToColumns(batch);
    let cmp: { ok: boolean; firstFail?: string } = { ok: true };
    for (const fld of c.flechetteFields) {
      const expected = c.columns[fld.name];
      const gotCol = got[fld.name];
      if (gotCol === undefined) {
        cmp = { ok: false, firstFail: `${fld.name} missing from arrow-js batch` };
        break;
      }
      const dw = topLevelDecimalByteWidth(fld.type);
      const r = compareColumn(c.name, fld.name, expected, gotCol, dw);
      if (!r.ok) {
        cmp = r;
        break;
      }
    }
    result.flechetteToArrow = cmp.ok ? { ok: true } : { ok: false, error: cmp.firstFail };
  } catch (e: any) {
    result.flechetteToArrow = { ok: false, error: `threw: ${e.message ?? e}` };
  }

  // ---- flechette self round-trip ----
  try {
    if (!ftable) ftable = buildFlechetteTable(c);
    const bytes = f_tableToIPC(ftable, { format: "stream" }) as Uint8Array;
    const decoded = f_tableFromIPC(bytes, FLECHETTE_OPTS);
    const got = flechetteTableToColumns(decoded);
    let cmp: { ok: boolean; firstFail?: string } = { ok: true };
    for (const fld of c.flechetteFields) {
      const dw = topLevelDecimalByteWidth(fld.type);
      const r = compareColumn(c.name, fld.name, c.columns[fld.name], got[fld.name], dw);
      if (!r.ok) {
        cmp = r;
        break;
      }
    }
    result.flechetteSelf = cmp.ok ? { ok: true } : { ok: false, error: cmp.firstFail };
  } catch (e: any) {
    result.flechetteSelf = { ok: false, error: `threw: ${e.message ?? e}` };
  }

  return result;
}

function runStreamingConcat(): { ok: boolean; error?: string } {
  // Verify that emitting one Table per batch via tableToIPC and concatenating
  // the resulting Uint8Arrays produces a stream that flechette can read in
  // one shot via tableFromIPC(Uint8Array[]). This replaces arrow-js's
  // RecordBatchStreamWriter.write(batch) per-batch pattern.
  try {
    const parts: Uint8Array[] = [];
    let totalRows = 0;
    for (let i = 0; i < 3; i++) {
      const col = f_columnFromArray(
        [i * 10 + 1, i * 10 + 2, i * 10 + 3],
        undefined,
        FLECHETTE_OPTS
      );
      const t = f_tableFromColumns({ x: col });
      parts.push(f_tableToIPC(t, { format: "stream" }) as Uint8Array);
      totalRows += t.numRows;
    }
    const merged = f_tableFromIPC(parts, FLECHETTE_OPTS);
    if (merged.numRows !== totalRows) {
      return {
        ok: false,
        error: `expected ${totalRows} rows, got ${merged.numRows}`,
      };
    }
    const xs = merged.getChild("x").toArray();
    const expected = [1, 2, 3, 11, 12, 13, 21, 22, 23];
    if (Array.from(xs as any).join(",") !== expected.join(",")) {
      return {
        ok: false,
        error: `value mismatch: ${Array.from(xs as any).join(",")} vs ${expected.join(",")}`,
      };
    }
    return { ok: true };
  } catch (e: any) {
    return { ok: false, error: `threw: ${e.message ?? e}` };
  }
}

function main() {
  console.log("flechette spike: type round-trip\n");

  const results: CaseResult[] = [];
  for (const c of CASES) {
    results.push(runCase(c));
  }

  const stream = runStreamingConcat();

  // Render
  const colWidth = Math.max(...results.map((r) => r.name.length), 8);
  const pad = (s: string) => s.padEnd(colWidth);
  const tick = (b: boolean) => (b ? "PASS" : "FAIL");
  console.log(
    `${pad("case")}  ${"arrow->flec".padEnd(12)}  ${"flec->arrow".padEnd(12)}  flec self`
  );
  console.log("-".repeat(colWidth + 50));
  for (const r of results) {
    console.log(
      `${pad(r.name)}  ${tick(r.arrowToFlechette.ok).padEnd(12)}  ${tick(
        r.flechetteToArrow.ok
      ).padEnd(12)}  ${tick(r.flechetteSelf.ok)}`
    );
  }
  console.log("-".repeat(colWidth + 50));
  console.log(`${pad("stream-concat")}  ${tick(stream.ok)}`);

  console.log("\nFailures:");
  let failures = 0;
  for (const r of results) {
    for (const [dir, res] of [
      ["arrow->flechette", r.arrowToFlechette],
      ["flechette->arrow", r.flechetteToArrow],
      ["flechette-self  ", r.flechetteSelf],
    ] as const) {
      if (!res.ok) {
        failures++;
        console.log(`  [${r.name}/${dir.trim()}] ${res.error}`);
      }
    }
  }
  if (!stream.ok) {
    failures++;
    console.log(`  [stream-concat] ${stream.error}`);
  }
  if (failures === 0) console.log("  (none)");

  console.log(
    `\n${results.length} cases, ${failures} failure direction(s), stream-concat=${
      stream.ok ? "ok" : "fail"
    }`
  );
}

main();
