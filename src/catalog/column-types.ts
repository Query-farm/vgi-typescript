// Copyright 2026 Query Farm LLC - https://query.farm
// How the DuckDB vgi extension classifies an Arrow field, for the checks the
// SDK mirrors (constraint paths, CHECK operands, column-expression types).

import { isStruct, TypeId, type VgiDataType, type VgiField } from "../arrow/index.js";

/** The top-level column carrying `is_row_id` metadata (DuckDB's rowid, never a user column). */
export function isRowIdField(field: VgiField): boolean {
  return field.metadata?.has("is_row_id") ?? false;
}

/** Whether the field carries an Arrow extension type or a VGI logical type. */
export function hasLogicalType(field: VgiField): boolean {
  for (const key of field.metadata?.keys() ?? [])
    if (key === "ARROW:extension:name" || key.startsWith("vgi.logical_type.")) return true;
  return false;
}

/** A field's children when it is an ordinary STRUCT (not an extension or logical type), else null. */
export function ordinaryStructChildren(field: VgiField): readonly VgiField[] | null {
  if (!isStruct(field.type) || hasLogicalType(field)) return null;
  return (field.type as VgiDataType & { children: readonly VgiField[] }).children;
}

/** Whether the field is a generated column (canonical or legacy metadata). */
export function isGeneratedField(field: VgiField): boolean {
  const m = field.metadata;
  return m != null && (m.has("vgi.column.generated.v1") || m.has("vgi.column.generation.v1") || m.has("generated_expression"));
}

/**
 * The DuckDB primitive a field maps to for the bounded expression and CHECK
 * profiles, or null for every other type (unsigned, large/dictionary strings,
 * temporal, nested, extension and logical types).
 */
export function duckdbPrimitive(field: VgiField): string | null {
  if (hasLogicalType(field)) return null;
  const t = field.type as any;
  switch (t.typeId) {
    case TypeId.Bool: return "BOOLEAN";
    case TypeId.Utf8: return "VARCHAR";
    case TypeId.Int: {
      if (!(t.isSigned ?? t.signed ?? true)) return null;
      return ({ 8: "TINYINT", 16: "SMALLINT", 32: "INTEGER", 64: "BIGINT" } as Record<number, string>)[t.bitWidth] ?? null;
    }
    case TypeId.Float: return t.precision === 1 ? "FLOAT" : t.precision === 2 ? "DOUBLE" : null;
    case TypeId.Decimal: return (t.bitWidth ?? 128) === 128 ? "DECIMAL" : null;
    default: return null;
  }
}
