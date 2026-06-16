// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Build batches from row objects or column arrays for the flechette backend.
//
// flechette's `columnFromArray(values, type)` handles all the complex cases
// (List, Map, Struct, Decimal, Timestamp[ns]) that the arrow-js backend has
// to assemble manually with `makeData` + offset arrays. Most of build.ts in
// impl-arrowjs (~415 lines) collapses to this much shorter file.

import {
  columnFromArray as f_columnFromArray,
  field as f_field,
  Table,
  type Column,
} from "@query-farm/flechette";
import type { VgiSchema, VgiBatch, VgiDataType, VgiColumnData } from "../types.js";
import { emptyBatch } from "./empty.js";
import { toFlechetteType } from "./normalize-type.js";

/**
 * Build an opaque column-data handle from a JS array. Mirrors the
 * arrow-js backend's `columnFromArray` — flechette's native
 * `columnFromArray` returns a Column directly, so this is a simple
 * cast at the boundary.
 */
export function columnFromArray(values: any[], type: VgiDataType): VgiColumnData {
  return f_columnFromArray(values, type as any, {
    useBigInt: true,
    useBigIntTimestamp: true,
    useDecimalInt: true,
  }) as VgiColumnData;
}

/**
 * Build a batch from row objects.
 */
export function batchFromRows(
  rows: Record<string, any>[],
  schema: VgiSchema,
): VgiBatch {
  if (rows.length === 0) return emptyBatch(schema);
  const columns: Record<string, any[]> = {};
  for (const f of schema.fields) {
    columns[f.name] = rows.map((r) => r[f.name] ?? null);
  }
  return batchFromColumns(columns, schema);
}

/**
 * Build a batch from column arrays.
 */
export function batchFromColumns(
  columns: Record<string, any[]>,
  schema: VgiSchema,
): VgiBatch {
  // We can't go through `tableFromColumns` because it discards the per-field
  // `nullable` flag — every output field becomes nullable. The vgi-rpc wire
  // protocol cares: the C++ extension validates response schemas exactly,
  // and a `nullable` mismatch on a `not null` field rejects the batch with
  // "Worker returned an out-of-date Apache Arrow schema". Build the Table
  // directly with a schema that preserves both `nullable` and per-field
  // metadata from the source VgiSchema.
  const childCols: Column<any>[] = [];
  const fields: any[] = [];
  for (const f of schema.fields) {
    // Normalize a (possibly foreign arrow-js) field type to flechette-native.
    // flechette's columnFromArray tolerates foreign type objects when reading
    // values, but the resulting Column.type would not be writer-safe (Utf8
    // corrupts, stale-dist Int64 throws). Reconstructing yields a native type
    // so both the column build and the serialized schema are correct.
    const ftype = toFlechetteType(f.type);
    let col: Column<any>;
    const values = columns[f.name];
    if (values) {
      // flechette's Map builder iterates values via for-of, so plain objects
      // (e.g. `{}` returned from a handler that hasn't migrated to Map) blow
      // up with "value is not iterable". Coerce plain objects to Map per row
      // for Map-typed fields so producer code that worked under arrow-js
      // continues to work here.
      const coerced = isMapType(f.type) ? values.map(coerceToMap)
        : isDateType(f.type) ? values.map((v) => coerceToDate(v, f.type))
        : values;
      col = f_columnFromArray(coerced, ftype, {
        useBigInt: true,
        useBigIntTimestamp: true,
        useDecimalInt: true,
      });
    } else {
      const rowCount = inferRowCount(columns, schema);
      col = f_columnFromArray(new Array(rowCount).fill(null), ftype);
    }
    childCols.push(col);
    // f.nullable on a VgiSchema field defaults to true if not specified.
    const nullable = (f as any).nullable ?? true;
    const metadata = (f as any).metadata ?? null;
    fields.push(f_field(f.name, col.type as any, nullable, metadata));
  }
  const flechSchema = {
    version: 5,
    endianness: 0, // Little
    fields,
    metadata: (schema as any).metadata ?? null,
  };
  return new Table(flechSchema as any, childCols) as unknown as VgiBatch;
}

function isMapType(t: VgiDataType): boolean {
  // Arrow Type enum: Map = 17 in the FlatBuffer schema. flechette exposes
  // this via DataType.typeId (same numeric encoding as arrow-js).
  return (t as any)?.typeId === 17;
}

function coerceToMap(v: any): Map<any, any> | null {
  if (v == null) return null;
  if (v instanceof Map) return v;
  if (Array.isArray(v)) return new Map(v);
  if (typeof v === "object") return new Map(Object.entries(v));
  return v;
}

function isDateType(t: VgiDataType): boolean {
  // Arrow Type enum: Date = 8. flechette's columnFromArray only stores Date
  // columns correctly when fed JS `Date` objects — raw unit values
  // (day-numbers for date32, epoch-ms for date64, which is what the wire and
  // the arrow-js backend use) are silently written as zeros.
  return (t as any)?.typeId === 8;
}

const MS_PER_DAY = 86_400_000;

function coerceToDate(v: any, t: VgiDataType): Date | null {
  if (v == null) return null;
  if (v instanceof Date) return v;
  if (typeof v !== "number" && typeof v !== "bigint") return v;
  const n = typeof v === "bigint" ? Number(v) : v;
  // DateUnit: DAY = 0 (date32, day-numbers) → ms; MILLISECOND = 1 (date64).
  const unit = (t as any)?.unit;
  return new Date(unit === 0 ? n * MS_PER_DAY : n);
}

function inferRowCount(
  columns: Record<string, any[]>,
  schema: VgiSchema,
): number {
  for (const f of schema.fields) {
    if (columns[f.name]) return columns[f.name].length;
  }
  return 0;
}
