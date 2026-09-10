// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// FilteringOutputCollector — wraps an OutputCollector and applies pushdown
// filters to every emitted batch — plus human-readable formatters used in
// debug logs.

import { type VgiBatch, type VgiSchema, isBatch } from "../arrow/index.js";
import type { OutputCollector } from "@query-farm/vgi-rpc";
import { batchFromColumns } from "../util/arrow/index.js";
import { ComparisonOp, type FilterExpression } from "./types.js";
import { expressionToSql, PushdownFilters } from "./evaluate.js";

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
    case ComparisonOp.DISTINCT_FROM: return "IS DISTINCT FROM";
    case ComparisonOp.NOT_DISTINCT_FROM: return "IS NOT DISTINCT FROM";
  }
}

function reprFilter(f: FilterExpression): string {
  switch (f.node) {
    case "comparison": {
      if (f.left.node === "column_ref" && f.right.node === "literal") {
        return `ConstantFilter(${f.left.columnName} ${opSymbol(f.op)} ${reprValue(f.right.value)})`;
      }
      return `V2ExpressionFilter(${expressionToSql(f)})`;
    }
    case "is_null":
      return `Is${f.negated ? "Not" : ""}NullFilter(${expressionToSql(f)})`;
    case "in": {
      const values = f.set.values;
      const preview = values.length > 5
        ? `[${values.slice(0, 3).map(reprValue).join(", ")}]...(${values.length} total)`
        : `[${values.map(reprValue).join(", ")}]`;
      return `InFilter(${expressionToSql(f.expression)} IN ${preview})`;
    }
    case "and": case "or": {
      const kids = f.children.map(reprFilter).join(f.node === "and" ? " AND " : " OR ");
      return `${f.node === "and" ? "And" : "Or"}Filter(${kids})`;
    }
    default:
      return `V2ExpressionFilter(${expressionToSql(f)})`;
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

  get outputSchema(): VgiSchema {
    return this.inner.outputSchema;
  }

  get finished(): boolean {
    return this.inner.finished;
  }

  emit(batch: VgiBatch, metadata?: Map<string, string>): void;
  emit(columns: Record<string, any[]>): void;
  emit(
    batchOrColumns: VgiBatch | Record<string, any[]>,
    metadata?: Map<string, string>,
  ): void {
    let batch: VgiBatch;
    if (isBatch(batchOrColumns)) {
      batch = batchOrColumns;
    } else {
      batch = batchFromColumns(batchOrColumns as Record<string, any[]>, this.inner.outputSchema as any);
    }
    const filtered = this.filters.apply(batch as any);
    this.inner.emit(filtered as any, metadata);
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
