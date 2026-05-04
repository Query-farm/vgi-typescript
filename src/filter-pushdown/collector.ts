// FilteringOutputCollector — wraps an OutputCollector and applies pushdown
// filters to every emitted batch — plus human-readable formatters used in
// debug logs.

import { RecordBatch, Schema } from "@query-farm/apache-arrow";
import type { OutputCollector } from "vgi-rpc";
import { batchFromColumns } from "../util/arrow.js";
import { ComparisonOp, type ExprNode, type Filter } from "./types.js";
import { PushdownFilters } from "./evaluate.js";

/**
 * Format pushdown filters as a human-readable SQL-like string.
 * Returns "(none)" when no filters exist.
 */
export function formatPushedFilters(filters: PushdownFilters | undefined): string {
  if (!filters || filters.filters.length === 0) return "(none)";
  const sql = filters.toSql();
  return sql || "(none)";
}

/**
 * Format pushdown filters using Python-style `repr()` — e.g.
 * `PushdownFilters([ConstantFilter(n < 4999)])`. Matches vgi-python's
 * `_format_pushed_filters_safe` output so tests that pattern-match the
 * repr (e.g. `dynamic_filter` checking for `ConstantFilter(n <`) work
 * identically across workers.
 */
export function reprPushedFilters(filters: PushdownFilters | undefined): string {
  if (!filters || filters.filters.length === 0) return "(none)";
  const parts = filters.filters.map(reprFilter);
  return `PushdownFilters([${parts.join(", ")}])`;
}

function opSymbol(op: ComparisonOp): string {
  switch (op) {
    case ComparisonOp.EQ: return "==";
    case ComparisonOp.NE: return "!=";
    case ComparisonOp.GT: return ">";
    case ComparisonOp.GE: return ">=";
    case ComparisonOp.LT: return "<";
    case ComparisonOp.LE: return "<=";
  }
}

function reprFilter(f: Filter): string {
  switch (f.type) {
    case "constant":
      return `ConstantFilter(${f.columnName} ${opSymbol(f.op)} ${reprValue(f.value)})`;
    case "is_null":
      return `IsNullFilter(${f.columnName} IS NULL)`;
    case "is_not_null":
      return `IsNotNullFilter(${f.columnName} IS NOT NULL)`;
    case "in": {
      const values = [...f.values];
      const preview = values.length > 5
        ? `${JSON.stringify(values.slice(0, 3))}...(${values.length} total)`
        : JSON.stringify(values);
      return `InFilter(${f.columnName} IN ${preview})`;
    }
    case "and": {
      const kids = f.children.map(reprFilter).join(" AND ");
      return `AndFilter(${kids})`;
    }
    case "or": {
      const kids = f.children.map(reprFilter).join(" OR ");
      return `OrFilter(${kids})`;
    }
    case "struct":
      return `StructFilter(${f.columnName}.${f.childName}: ${reprFilter(f.childFilter)})`;
    case "expression":
      return `ExpressionFilter(${f.columnName}: ${reprExpr(f.expr)})`;
  }
}

function reprExpr(e: ExprNode): string {
  switch (e.expr_type) {
    case "column_ref": return `col#${e.index}`;
    case "constant": return reprValue(e.value);
    case "function": return `${e.function_name}(${e.children.map(reprExpr).join(", ")})`;
    case "comparison": return `(${reprExpr(e.left)} ${opSymbol(e.op)} ${reprExpr(e.right)})`;
    case "conjunction":
      return `(${e.children.map(reprExpr).join(e.conjunction_type === "and" ? " AND " : " OR ")})`;
  }
}

function reprValue(v: any): string {
  if (typeof v === "string") return JSON.stringify(v);
  if (typeof v === "bigint") return String(v);
  if (v === null || v === undefined) return "null";
  return String(v);
}

/**
 * Wraps an OutputCollector to automatically apply pushdown filters
 * to every emitted batch.
 */
export class FilteringOutputCollector {
  constructor(
    private inner: OutputCollector,
    private filters: PushdownFilters,
  ) {}

  get outputSchema(): Schema {
    return this.inner.outputSchema;
  }

  get finished(): boolean {
    return this.inner.finished;
  }

  emit(batch: RecordBatch, metadata?: Map<string, string>): void;
  emit(columns: Record<string, any[]>): void;
  emit(
    batchOrColumns: RecordBatch | Record<string, any[]>,
    metadata?: Map<string, string>,
  ): void {
    let batch: RecordBatch;
    if (batchOrColumns instanceof RecordBatch) {
      batch = batchOrColumns;
    } else {
      batch = batchFromColumns(batchOrColumns, this.inner.outputSchema);
    }
    const filtered = this.filters.apply(batch);
    this.inner.emit(filtered, metadata);
  }

  emitRow(values: Record<string, any>): void {
    const columns: Record<string, any[]> = {};
    for (const [key, value] of Object.entries(values)) {
      columns[key] = [value];
    }
    this.emit(columns);
  }

  finish(): void {
    this.inner.finish();
  }

  clientLog(
    level: string,
    message: string,
    extra?: Record<string, string>,
  ): void {
    this.inner.clientLog(level, message, extra);
  }
}
