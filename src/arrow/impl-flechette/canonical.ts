// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Canonical Arrow I/O for the flechette backend.
//
// The ONLY place in the flechette backend that knows flechette-specific
// build/read details. Produces and consumes the SAME canonical representation
// as impl-arrowjs/canonical.ts (see ../codec/registry.ts), so both backends
// agree byte-for-byte.
//
// flechette's `columnFromArray` already handles the complex cases (List, Map,
// Struct, Decimal, Timestamp[ns]) when given the right options, so the WRITE
// side is mostly delegation. The one wrinkle is Date: flechette stores a Date
// column correctly only when fed JS `Date` objects, but our canonical date32 is
// a day-number and date64 is ms-bigint — so we convert canonical -> Date here.
//
// On the READ side, flechette's Column/Data `.at()` already decodes every
// scalar to its canonical JS value LOSSLESSLY (timestamp/duration/time64 ->
// bigint, decimal -> unscaled bigint with useDecimalInt) — UNLIKE arrow-js,
// whose Vector.get() is lossy for timestamp. The only mismatch is Date, which
// `.at()` returns as epoch-ms; we convert that to days (date32) / ms-bigint
// (date64). Composites recurse so nested Date children are converted too.

import {
  columnFromArray as f_columnFromArray,
  field as f_field,
  Table,
  type Column,
} from "@query-farm/flechette";
import type { VgiSchema, VgiBatch, VgiDataType, VgiColumnData } from "../types.js";
import { toFlechetteType } from "./normalize-type.js";

const MS_PER_DAY = 86_400_000;

const COLUMN_OPTS = {
  useBigInt: true,
  useBigIntTimestamp: true,
  useDecimalInt: true,
} as const;

// ===========================================================================
// WRITE: canonical[] -> flechette Column
// ===========================================================================

/** Build a flechette `Column` from an array of CANONICAL values for `type`. */
export function writeCanonicalColumn(
  type: VgiDataType,
  canonical: unknown[],
): VgiColumnData {
  const ftype = toFlechetteType(type);
  return f_columnFromArray(prepareForFlechette(type, canonical), ftype, COLUMN_OPTS) as VgiColumnData;
}

/**
 * Build a whole batch (Table) from canonical column arrays. Kept here so the
 * per-field `nullable` flag and metadata are preserved on the output schema
 * (tableFromColumns would drop them, which the C++ extension rejects).
 */
export function writeCanonicalBatch(
  columns: Record<string, unknown[]>,
  schema: VgiSchema,
  numRows: number,
): VgiBatch {
  const childCols: Column<any>[] = [];
  const fields: any[] = [];
  for (const f of schema.fields) {
    const ftype = toFlechetteType(f.type);
    const values = columns[f.name];
    const col: Column<any> = values
      ? f_columnFromArray(prepareForFlechette(f.type, values), ftype, COLUMN_OPTS)
      : f_columnFromArray(new Array(numRows).fill(null), ftype, COLUMN_OPTS);
    childCols.push(col);
    const nullable = (f as any).nullable ?? true;
    const metadata = (f as any).metadata ?? null;
    fields.push(f_field(f.name, col.type as any, nullable, metadata));
  }
  const flechSchema = {
    version: 5,
    endianness: 0,
    fields,
    metadata: (schema as any).metadata ?? null,
  };
  return new Table(flechSchema as any, childCols) as unknown as VgiBatch;
}

/**
 * Convert canonical values into the shape flechette's columnFromArray needs.
 * Date columns: canonical day-number / ms-bigint -> JS Date. Map columns:
 * canonical Array<[k,v]> -> Map. Struct/List recurse. Everything else passes
 * through.
 */
function prepareForFlechette(type: VgiDataType, values: unknown[]): unknown[] {
  const tid = type.typeId;
  if (tid === 8) {
    const isDay = (type as any).unit === 0;
    return values.map((v) => {
      if (v == null) return null;
      if (v instanceof Date) return v;
      const n = typeof v === "bigint" ? Number(v) : (v as number);
      return new Date(isDay ? n * MS_PER_DAY : n);
    });
  }
  if (tid === 17) {
    return values.map((v) => {
      if (v == null) return null;
      if (v instanceof Map) return v;
      if (Array.isArray(v)) return new Map(v as Array<[unknown, unknown]>);
      if (typeof v === "object") return new Map(Object.entries(v as any));
      return v;
    });
  }
  if (tid === 13) {
    const children = (type as any).children as Array<{ name: string; type: VgiDataType }>;
    if (children.some((c) => needsPrepare(c.type))) {
      return values.map((row) => {
        if (row == null) return null;
        const out: Record<string, unknown> = {};
        for (const c of children) {
          out[c.name] = prepareForFlechette(c.type, [(row as any)[c.name] ?? null])[0];
        }
        return out;
      });
    }
    return values;
  }
  if (tid === 12 || tid === 16) {
    const childType = (type as any).children?.[0]?.type as VgiDataType;
    if (childType && needsPrepare(childType)) {
      return values.map((row) => {
        if (row == null) return null;
        const items = Array.isArray(row) ? row : Array.from(row as Iterable<unknown>);
        return prepareForFlechette(childType, items);
      });
    }
    return values;
  }
  return values;
}

function needsPrepare(type: VgiDataType): boolean {
  const tid = type.typeId;
  if (tid === 8 || tid === 17) return true;
  if (tid === 13) return ((type as any).children ?? []).some((c: any) => needsPrepare(c.type));
  if (tid === 12 || tid === 16) {
    const ct = (type as any).children?.[0]?.type;
    return ct ? needsPrepare(ct) : false;
  }
  return false;
}

// ===========================================================================
// READ: flechette column + index -> canonical value
// ===========================================================================

/** Read a single CANONICAL value at `index` from a flechette Column. */
export function readCanonicalValue(
  type: VgiDataType,
  column: unknown,
  index: number,
): unknown {
  const at = (column as any)?.at?.(index);
  return canonicalize(type, at);
}

/**
 * Normalize a flechette-decoded value (from Column/Data `.at()`) into the
 * canonical representation. `.at()` already produces canonical for every
 * scalar except Date; composites recurse so nested Dates are handled.
 */
function canonicalize(type: VgiDataType, value: unknown): unknown {
  if (value === null || value === undefined) return null;
  const tid = type.typeId;
  switch (tid) {
    case 8: { // Date: .at() returns epoch-ms number
      const ms = value instanceof Date ? value.getTime()
        : typeof value === "bigint" ? Number(value) : (value as number);
      if ((type as any).unit === 0) return Math.round(ms / MS_PER_DAY); // date32 -> days
      return BigInt(ms);                                                // date64 -> ms bigint
    }
    case 4: // Binary
    case 19: // LargeBinary
    case 15: // FixedSizeBinary
      return toUint8(value);
    case 13: { // Struct -> { field: canonical }
      const children = (type as any).children as Array<{ name: string; type: VgiDataType }>;
      const obj = value as any;
      const out: Record<string, unknown> = {};
      for (const c of children) out[c.name] = canonicalize(c.type, obj[c.name] ?? null);
      return out;
    }
    case 12: // List
    case 16: { // FixedSizeList -> canonical[]
      const childType = (type as any).children[0].type as VgiDataType;
      const arr = Array.isArray(value) ? value : Array.from(value as Iterable<unknown>);
      return arr.map((v) => canonicalize(childType, v));
    }
    case 17: { // Map -> Array<[k,v]>
      const entries = (type as any).children[0].type.children;
      const keyType = entries[0].type as VgiDataType;
      const valueType = entries[1].type as VgiDataType;
      const pairs: Array<[unknown, unknown]> = value instanceof Map ? Array.from(value.entries())
        : Array.isArray(value) ? (value as Array<[unknown, unknown]>)
        : Object.entries(value as Record<string, unknown>);
      return pairs.map(([k, v]) => [canonicalize(keyType, k), canonicalize(valueType, v)]);
    }
    case -1: // Dictionary -> decoded value's canonical
      return canonicalize((type as any).dictionary as VgiDataType, value);
    default:
      // bool/int/float/utf8/decimal/time/timestamp/duration: already canonical.
      return value;
  }
}

function toUint8(v: any): Uint8Array | null {
  if (v == null) return null;
  if (v instanceof Uint8Array) return v;
  if (ArrayBuffer.isView(v)) {
    const view = v as ArrayBufferView;
    return new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
  }
  return v;
}
