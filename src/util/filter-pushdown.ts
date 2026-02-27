// Filter pushdown: deserialize, evaluate, and apply DuckDB filter predicates.
// Ported from vgi-python/vgi/table_filter_pushdown.py

import { RecordBatch, Schema } from "apache-arrow";
import type { OutputCollector } from "vgi-rpc";
import { batchFromColumns, emptyBatch } from "./arrow.js";

// ============================================================================
// Filter types
// ============================================================================

export enum ComparisonOp {
  EQ = "eq",
  NE = "ne",
  GT = "gt",
  GE = "ge",
  LT = "lt",
  LE = "le",
}

export interface ConstantFilter {
  type: "constant";
  columnName: string;
  columnIndex: number;
  op: ComparisonOp;
  value: any;
}

export interface IsNullFilter {
  type: "is_null";
  columnName: string;
  columnIndex: number;
}

export interface IsNotNullFilter {
  type: "is_not_null";
  columnName: string;
  columnIndex: number;
}

export interface InFilter {
  type: "in";
  columnName: string;
  columnIndex: number;
  values: Set<any>;
}

export interface AndFilter {
  type: "and";
  columnName: string;
  columnIndex: number;
  children: Filter[];
}

export interface OrFilter {
  type: "or";
  columnName: string;
  columnIndex: number;
  children: Filter[];
}

export interface StructFilter {
  type: "struct";
  columnName: string;
  columnIndex: number;
  childIndex: number;
  childName: string;
  childFilter: Filter;
}

export type Filter =
  | ConstantFilter
  | IsNullFilter
  | IsNotNullFilter
  | InFilter
  | AndFilter
  | OrFilter
  | StructFilter;

// ============================================================================
// Comparison helpers
// ============================================================================

function compare(a: any, b: any, op: ComparisonOp): boolean {
  switch (op) {
    case ComparisonOp.EQ: return a === b;
    case ComparisonOp.NE: return a !== b;
    case ComparisonOp.GT: return a > b;
    case ComparisonOp.GE: return a >= b;
    case ComparisonOp.LT: return a < b;
    case ComparisonOp.LE: return a <= b;
  }
}

// ============================================================================
// Filter evaluation (row-by-row)
// ============================================================================

function evaluateFilter(
  filter: Filter,
  batch: RecordBatch,
  mask: Uint8Array,
): void {
  switch (filter.type) {
    case "constant":
      evaluateConstant(filter, batch, mask);
      break;
    case "is_null":
      evaluateIsNull(filter, batch, mask);
      break;
    case "is_not_null":
      evaluateIsNotNull(filter, batch, mask);
      break;
    case "in":
      evaluateIn(filter, batch, mask);
      break;
    case "and":
      evaluateAnd(filter, batch, mask);
      break;
    case "or":
      evaluateOr(filter, batch, mask);
      break;
    case "struct":
      evaluateStruct(filter, batch, mask);
      break;
  }
}

function evaluateConstant(
  filter: ConstantFilter,
  batch: RecordBatch,
  mask: Uint8Array,
): void {
  const col = batch.getChildAt(filter.columnIndex)!;
  const n = batch.numRows;
  for (let i = 0; i < n; i++) {
    if (!mask[i]) continue;
    if (!col.isValid(i)) {
      mask[i] = 0;
    } else {
      mask[i] = compare(col.get(i), filter.value, filter.op) ? 1 : 0;
    }
  }
}

function evaluateIsNull(
  filter: IsNullFilter,
  batch: RecordBatch,
  mask: Uint8Array,
): void {
  const col = batch.getChildAt(filter.columnIndex)!;
  const n = batch.numRows;
  for (let i = 0; i < n; i++) {
    if (!mask[i]) continue;
    mask[i] = col.isValid(i) ? 0 : 1;
  }
}

function evaluateIsNotNull(
  filter: IsNotNullFilter,
  batch: RecordBatch,
  mask: Uint8Array,
): void {
  const col = batch.getChildAt(filter.columnIndex)!;
  const n = batch.numRows;
  for (let i = 0; i < n; i++) {
    if (!mask[i]) continue;
    mask[i] = col.isValid(i) ? 1 : 0;
  }
}

function evaluateIn(
  filter: InFilter,
  batch: RecordBatch,
  mask: Uint8Array,
): void {
  const col = batch.getChildAt(filter.columnIndex)!;
  const n = batch.numRows;
  for (let i = 0; i < n; i++) {
    if (!mask[i]) continue;
    if (!col.isValid(i)) {
      mask[i] = 0;
    } else {
      mask[i] = filter.values.has(col.get(i)) ? 1 : 0;
    }
  }
}

function evaluateAnd(
  filter: AndFilter,
  batch: RecordBatch,
  mask: Uint8Array,
): void {
  // AND: start with all-pass, then narrow with each child
  const n = batch.numRows;
  const childMask = new Uint8Array(n);

  for (const child of filter.children) {
    childMask.fill(1);
    evaluateFilter(child, batch, childMask);
    for (let i = 0; i < n; i++) {
      mask[i] &= childMask[i];
    }
  }
}

function evaluateOr(
  filter: OrFilter,
  batch: RecordBatch,
  mask: Uint8Array,
): void {
  // OR: start with all-fail, then widen with each child
  const n = batch.numRows;
  const result = new Uint8Array(n); // all zeros
  const childMask = new Uint8Array(n);

  for (const child of filter.children) {
    childMask.fill(1);
    evaluateFilter(child, batch, childMask);
    for (let i = 0; i < n; i++) {
      result[i] |= childMask[i];
    }
  }

  // Combine with incoming mask
  for (let i = 0; i < n; i++) {
    mask[i] &= result[i];
  }
}

function evaluateStruct(
  filter: StructFilter,
  batch: RecordBatch,
  mask: Uint8Array,
): void {
  // Extract the struct column's child field
  const structCol = batch.getChildAt(filter.columnIndex)!;
  const childCol = (structCol as any).getChildAt(filter.childIndex);
  if (!childCol) {
    // Child field not found — fail all rows
    mask.fill(0);
    return;
  }

  // Build a minimal wrapper that looks like a single-column batch to evaluateFilter.
  // We create a fake batch with just the child column so the child filter (with
  // columnIndex remapped to 0) can evaluate against it.
  const childSchema = new Schema([
    batch.schema.fields[filter.columnIndex].type.children[filter.childIndex],
  ]);
  const childBatch = batchFromColumns(
    { [filter.childName]: Array.from({ length: batch.numRows }, (_, i) => childCol.get(i)) },
    childSchema,
  );

  // Remap child filter's columnIndex to 0 for the wrapper batch
  const remapped = { ...filter.childFilter, columnIndex: 0 };
  evaluateFilter(remapped, childBatch, mask);
}

// ============================================================================
// PushdownFilters
// ============================================================================

export class PushdownFilters {
  constructor(
    readonly filters: Filter[],
    readonly version: string,
  ) {}

  /** Evaluate filters against a batch, returning a Uint8Array mask (0=fail, 1=pass). */
  evaluate(batch: RecordBatch): Uint8Array {
    const n = batch.numRows;
    const mask = new Uint8Array(n);
    mask.fill(1);

    for (const filter of this.filters) {
      evaluateFilter(filter, batch, mask);
    }

    return mask;
  }

  /** Apply filters to a batch, returning only passing rows. */
  apply(batch: RecordBatch): RecordBatch {
    if (batch.numRows === 0 || this.filters.length === 0) return batch;

    const mask = this.evaluate(batch);

    // Fast paths
    let passCount = 0;
    for (let i = 0; i < mask.length; i++) {
      if (mask[i]) passCount++;
    }

    if (passCount === batch.numRows) return batch;
    if (passCount === 0) return emptyBatch(batch.schema);

    // Collect passing indices
    const indices: number[] = [];
    for (let i = 0; i < mask.length; i++) {
      if (mask[i]) indices.push(i);
    }

    // Rebuild batch with only passing rows
    const columns: Record<string, any[]> = {};
    for (const field of batch.schema.fields) {
      const col = batch.getChild(field.name)!;
      columns[field.name] = indices.map((i) => col.get(i));
    }
    return batchFromColumns(columns, batch.schema);
  }
}

// ============================================================================
// Deserialization
// ============================================================================

function parseFilter(
  spec: any,
  getValue: (ref: number) => any,
): Filter {
  const columnName: string = spec.column_name ?? "";
  const columnIndex: number = spec.column_index ?? 0;

  switch (spec.type) {
    case "constant":
      return {
        type: "constant",
        columnName,
        columnIndex,
        op: spec.op as ComparisonOp,
        value: getValue(spec.value_ref),
      };

    case "is_null":
      return { type: "is_null", columnName, columnIndex };

    case "is_not_null":
      return { type: "is_not_null", columnName, columnIndex };

    case "in": {
      const listScalar = getValue(spec.value_ref);
      // listScalar is an Arrow list value — extract elements into a Set
      const values = new Set<any>();
      if (listScalar && typeof listScalar[Symbol.iterator] === "function") {
        for (const v of listScalar) {
          values.add(v);
        }
      } else if (Array.isArray(listScalar)) {
        for (const v of listScalar) {
          values.add(v);
        }
      }
      return { type: "in", columnName, columnIndex, values };
    }

    case "and":
      return {
        type: "and",
        columnName,
        columnIndex,
        children: (spec.children ?? []).map((c: any) => parseFilter(c, getValue)),
      };

    case "or":
      return {
        type: "or",
        columnName,
        columnIndex,
        children: (spec.children ?? []).map((c: any) => parseFilter(c, getValue)),
      };

    case "struct":
      return {
        type: "struct",
        columnName,
        columnIndex,
        childIndex: spec.child_index ?? 0,
        childName: spec.child_name ?? "",
        childFilter: parseFilter(spec.child_filter, getValue),
      };

    default:
      throw new Error(`Unknown filter type: ${spec.type}`);
  }
}

/**
 * Deserialize pushdown filters from an Arrow RecordBatch.
 *
 * Wire format:
 * - Column 0: UTF8 — JSON array of filter specs. Field metadata: vgi_filter_version: "1"
 * - Columns 1+: Scalar values referenced by value_ref → column N+1
 */
export function deserializeFilters(batch: RecordBatch): PushdownFilters {
  // Validate version
  const field0 = batch.schema.fields[0];
  const version = field0.metadata?.get("vgi_filter_version");
  if (!version) {
    throw new Error(
      "Filter batch missing vgi_filter_version metadata on field 0",
    );
  }
  if (version !== "1") {
    throw new Error(`Unsupported filter version: ${version}`);
  }

  // Parse JSON filter specs from column 0, row 0
  const jsonCol = batch.getChildAt(0)!;
  const jsonStr = jsonCol.get(0) as string;
  let specs: any[];
  try {
    specs = JSON.parse(jsonStr);
  } catch (e) {
    throw new Error(`Failed to parse filter JSON: ${e}`);
  }

  // Value resolver: value_ref N → column N+1, row 0
  const getValue = (ref: number): any => {
    const col = batch.getChildAt(ref + 1);
    if (!col) {
      throw new Error(`Filter value_ref ${ref} references non-existent column ${ref + 1}`);
    }
    return col.get(0);
  };

  const filters = specs.map((spec) => parseFilter(spec, getValue));
  return new PushdownFilters(filters, version);
}

// ============================================================================
// FilteringOutputCollector
// ============================================================================

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
