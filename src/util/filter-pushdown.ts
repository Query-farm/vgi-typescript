// Filter pushdown: deserialize, evaluate, and apply DuckDB filter predicates.
// Ported from vgi-python/vgi/table_filter_pushdown.py

import { RecordBatch, Schema } from "@query-farm/apache-arrow";
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
  | StructFilter
  | ExpressionFilter;

/**
 * Expression-tree filter. The expr is a parsed AST; evaluation matches
 * DuckDB's semantics for the function names the table function advertises in
 * `supportedExpressionFilters` (e.g. list_contains, starts_with, contains,
 * &&/st_intersects_extent for spatial).
 */
export interface ExpressionFilter {
  type: "expression";
  columnName: string;
  columnIndex: number;
  expr: ExprNode;
}

export type ExprNode =
  | { expr_type: "column_ref"; index: number }
  | { expr_type: "constant"; value: any }
  | { expr_type: "function"; function_name: string; children: ExprNode[] }
  | { expr_type: "comparison"; op: ComparisonOp; left: ExprNode; right: ExprNode }
  | { expr_type: "conjunction"; conjunction_type: "and" | "or"; children: ExprNode[] };

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
    case "expression":
      evaluateExpression(filter, batch, mask);
      break;
  }
}

// Recursively evaluate an expression AST against a row of the batch, returning
// the scalar value it produces. column_ref reads from the batch using the
// caller-supplied remap (DuckDB rewrites column refs inside an ExpressionFilter
// so index=0 means "this filter's column"); constant returns its pre-resolved
// JS value; function dispatches on function_name; comparison/conjunction
// combine children.
function evaluateExpr(
  node: ExprNode,
  batch: RecordBatch,
  rowIndex: number,
  colRefToBatchIndex: (index: number) => number,
): any {
  switch (node.expr_type) {
    case "column_ref": {
      const col = batch.getChildAt(colRefToBatchIndex(node.index));
      return col ? col.get(rowIndex) : null;
    }
    case "constant":
      return node.value;
    case "function":
      return evalFunction(
        node.function_name,
        node.children.map((c) => evaluateExpr(c, batch, rowIndex, colRefToBatchIndex)),
      );
    case "comparison": {
      const left = evaluateExpr(node.left, batch, rowIndex, colRefToBatchIndex);
      const right = evaluateExpr(node.right, batch, rowIndex, colRefToBatchIndex);
      if (left == null || right == null) return null;
      return compare(left, right, node.op);
    }
    case "conjunction": {
      if (node.conjunction_type === "and") {
        for (const c of node.children) {
          const v = evaluateExpr(c, batch, rowIndex, colRefToBatchIndex);
          if (v !== true) return v === false ? false : null;
        }
        return true;
      }
      let sawNull = false;
      for (const c of node.children) {
        const v = evaluateExpr(c, batch, rowIndex, colRefToBatchIndex);
        if (v === true) return true;
        if (v == null) sawNull = true;
      }
      return sawNull ? null : false;
    }
  }
}

// Dispatch DuckDB scalar-function names we support as pushdown filters.
// Return null for any missing argument (SQL three-valued logic).
function evalFunction(name: string, args: any[]): any {
  if (args.some((a) => a == null)) return null;
  switch (name) {
    case "list_contains":
    case "array_contains": {
      // args[0] is an Arrow ListVector row (iterable), args[1] is the needle.
      const [list, needle] = args;
      if (!list || typeof list[Symbol.iterator] !== "function") return false;
      for (const v of list) {
        if (v === needle) return true;
      }
      return false;
    }
    case "starts_with":
    case "prefix": {
      const [hay, needle] = args;
      return typeof hay === "string" && typeof needle === "string"
        ? hay.startsWith(needle) : false;
    }
    case "contains": {
      const [hay, needle] = args;
      return typeof hay === "string" && typeof needle === "string"
        ? hay.includes(needle) : false;
    }
    case "&&":
    case "st_intersects_extent": {
      // Bounding-box intersection between two WKB geometries. Parses each
      // geometry's XY bbox and checks for overlap in both dimensions.
      const [a, b] = args;
      const ba = wkbBBox(a);
      const bb = wkbBBox(b);
      if (!ba || !bb) return false;
      return !(ba.maxX < bb.minX || ba.minX > bb.maxX ||
               ba.maxY < bb.minY || ba.minY > bb.maxY);
    }
    default:
      // Unsupported functions shouldn't make it past the C++-side
      // `ExpressionTreeIsSupported` gate, but be defensive: evaluate to null
      // so the row is filtered out conservatively rather than silently
      // producing wrong results.
      return null;
  }
}

// Parse the XY bounding box of a WKB geometry. Supports Point and Polygon for
// ST_MakeEnvelope output; extend as new geometry kinds are needed.
function wkbBBox(bytes: any): { minX: number; minY: number; maxX: number; maxY: number } | null {
  if (!(bytes instanceof Uint8Array)) return null;
  if (bytes.length < 5) return null;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const le = view.getUint8(0) === 1;
  const type = view.getUint32(1, le);
  // Point (type=1): 21 bytes total (5 header + 8+8 coords)
  if (type === 1 && bytes.length >= 21) {
    const x = view.getFloat64(5, le);
    const y = view.getFloat64(13, le);
    return { minX: x, minY: y, maxX: x, maxY: y };
  }
  // Polygon (type=3): used by ST_MakeEnvelope. One ring, 4 points, close.
  // Header 5 + num_rings 4 + num_points 4 + 4*16 coord bytes = 81 bytes.
  if (type === 3 && bytes.length >= 81) {
    const numRings = view.getUint32(5, le);
    if (numRings < 1) return null;
    const numPoints = view.getUint32(9, le);
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    let off = 13;
    for (let i = 0; i < numPoints && off + 16 <= bytes.length; i++, off += 16) {
      const x = view.getFloat64(off, le);
      const y = view.getFloat64(off + 8, le);
      if (x < minX) minX = x;
      if (x > maxX) maxX = x;
      if (y < minY) minY = y;
      if (y > maxY) maxY = y;
    }
    if (minX === Infinity) return null;
    return { minX, minY, maxX, maxY };
  }
  return null;
}

function evaluateExpression(
  filter: ExpressionFilter,
  batch: RecordBatch,
  mask: Uint8Array,
): void {
  const n = batch.numRows;
  // DuckDB's FilterSerializer rewrites bound references inside an
  // ExpressionFilter so that column_ref index 0 refers to the filter's own
  // column (see ReplaceWithBoundReference in DuckDB's optimizer). Higher
  // indices on the same filter aren't produced — the filter is always
  // scoped to a single column in the outer batch — so we map every
  // column_ref back to `filter.columnIndex` by name, resolving through the
  // current batch in case projection reordered columns.
  const colName = filter.columnName;
  let targetIdx = batch.schema.fields.findIndex((f) => f.name === colName);
  if (targetIdx < 0) targetIdx = filter.columnIndex;
  const colRef = (_nodeIndex: number) => targetIdx;
  for (let i = 0; i < n; i++) {
    if (!mask[i]) continue;
    const v = evaluateExpr(filter.expr, batch, i, colRef);
    mask[i] = v === true ? 1 : 0;
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

const OP_SYMBOLS: Record<ComparisonOp, string> = {
  [ComparisonOp.EQ]: "=",
  [ComparisonOp.NE]: "!=",
  [ComparisonOp.GT]: ">",
  [ComparisonOp.GE]: ">=",
  [ComparisonOp.LT]: "<",
  [ComparisonOp.LE]: "<=",
};

function formatValue(v: any): string {
  if (typeof v === "string") return `'${v}'`;
  if (typeof v === "bigint") return String(v);
  return String(v);
}

function filterToSql(f: Filter): string {
  const col = f.columnName;

  switch (f.type) {
    case "constant":
      return `${col} ${OP_SYMBOLS[f.op]} ${formatValue(f.value)}`;
    case "is_null":
      return `${col} IS NULL`;
    case "is_not_null":
      return `${col} IS NOT NULL`;
    case "in": {
      // Match Python's repr: summarize large lists as `N values`, otherwise
      // render each element. Threshold matches vgi-python's InFilter.__repr__.
      const size = f.values.size;
      if (size > 20) {
        return `${col} IN (${size} values)`;
      }
      const vals = [...f.values].map(formatValue).join(", ");
      return `${col} IN (${vals})`;
    }
    case "and": {
      const parts = f.children.map(filterToSql);
      return `(${parts.join(" AND ")})`;
    }
    case "or": {
      const parts = f.children.map(filterToSql);
      return `(${parts.join(" OR ")})`;
    }
    case "struct": {
      const nested = `${f.columnName}.${f.childName}`;
      const remapped = { ...f.childFilter, columnName: nested };
      return filterToSql(remapped);
    }
    case "expression":
      // Expression filters don't have a natural SQL rendering here — the
      // tree came from DuckDB's bound-expression serializer. Use a generic
      // placeholder so debug output stays readable.
      return `${f.columnName} MATCHES (expression)`;
  }
}

export class PushdownFilters {
  constructor(
    readonly filters: Filter[],
    readonly version: string,
  ) {}

  /** Convert filters to a human-readable SQL-like string. */
  toSql(): string {
    if (this.filters.length === 0) return "";
    return this.filters.map(filterToSql).join(" AND ");
  }

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
  getJoinKeysColumn?: (columnName: string) => any[] | null,
): Filter | null {
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

    case "and": {
      // AND: drop unparseable children; the remaining ones still filter correctly
      // (conjunction is weaker with fewer children, which is safe — extra rows
      // pass but DuckDB re-filters client-side).
      const andChildren = (spec.children ?? [])
        .map((c: any) => parseFilter(c, getValue, getJoinKeysColumn))
        .filter((f: Filter | null): f is Filter => f !== null);
      if (andChildren.length === 0) return null;
      return { type: "and", columnName, columnIndex, children: andChildren };
    }

    case "or": {
      // OR: dropping a child would strengthen the filter (fewer rows pass),
      // which is unsafe. If any child fails to parse, drop the whole OR.
      const orChildren: Filter[] = [];
      for (const c of spec.children ?? []) {
        const parsed = parseFilter(c, getValue, getJoinKeysColumn);
        if (parsed === null) return null;
        orChildren.push(parsed);
      }
      return { type: "or", columnName, columnIndex, children: orChildren };
    }

    case "struct": {
      const childFilter = parseFilter(spec.child_filter, getValue, getJoinKeysColumn);
      if (childFilter === null) return null;
      return {
        type: "struct",
        columnName,
        columnIndex,
        childIndex: spec.child_index ?? 0,
        childName: spec.child_name ?? "",
        childFilter,
      };
    }

    case "join_keys": {
      // vgi-python `FilterType.JOIN_KEYS`. DuckDB's dynamic_or_filter
      // optimizer can promote IN / OR lists or join-predicate pushdowns into
      // this shape. Look the keys_column up in the init request's join_keys
      // batches and materialize as an InFilter. If no callback or no batch
      // matches, fall through as null (graceful degradation).
      const keysColumn: string = spec.keys_column ?? "";
      if (!getJoinKeysColumn) return null;
      const values = getJoinKeysColumn(keysColumn);
      if (values === null) return null;
      const set = new Set<any>(values);
      return { type: "in", columnName, columnIndex, values: set };
    }

    case "expression": {
      // Parse the serialized expression tree (DuckDB bound-expression → JSON
      // emitted by FilterSerializer::SerializeExpression in C++). The tree
      // may reference constants by `value_ref` — resolve those now so the
      // evaluator sees plain JS values.
      const expr = parseExprNode(spec.expr, getValue);
      if (!expr) return null;
      return { type: "expression", columnName, columnIndex, expr };
    }

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
/**
 * Build a getJoinKeysColumn lookup from an InitRequest's `joinKeys` batches.
 * Each batch typically has a single column whose name is the join-keys column
 * referenced by `spec.keys_column` on a join_keys filter.
 */
export function buildJoinKeysLookup(
  joinKeyBatches: RecordBatch[],
): (columnName: string) => any[] | null {
  const index = new Map<string, any[]>();
  for (const batch of joinKeyBatches) {
    for (const field of batch.schema.fields) {
      const col = batch.getChild(field.name);
      if (!col) continue;
      const values: any[] = [];
      for (let i = 0; i < batch.numRows; i++) {
        values.push(col.get(i));
      }
      index.set(field.name, values);
    }
  }
  return (name: string) => index.get(name) ?? null;
}

// Parse a serialized expression subtree. Returns null if the shape doesn't
// look like one we can evaluate (unknown expr_type); the caller skips the
// whole filter in that case, which is equivalent to not pushing it down.
function parseExprNode(
  spec: any,
  getValue: (ref: number) => any,
): ExprNode | null {
  if (!spec || typeof spec !== "object") return null;
  switch (spec.expr_type) {
    case "column_ref":
      return { expr_type: "column_ref", index: Number(spec.index ?? 0) };
    case "constant":
      return { expr_type: "constant", value: getValue(Number(spec.value_ref)) };
    case "function": {
      const kids: ExprNode[] = [];
      for (const c of spec.children ?? []) {
        const k = parseExprNode(c, getValue);
        if (!k) return null;
        kids.push(k);
      }
      return { expr_type: "function", function_name: String(spec.function_name ?? ""), children: kids };
    }
    case "comparison": {
      const left = parseExprNode(spec.left, getValue);
      const right = parseExprNode(spec.right, getValue);
      if (!left || !right) return null;
      return {
        expr_type: "comparison",
        op: opFromString(spec.op),
        left,
        right,
      };
    }
    case "conjunction": {
      const kids: ExprNode[] = [];
      for (const c of spec.children ?? []) {
        const k = parseExprNode(c, getValue);
        if (!k) return null;
        kids.push(k);
      }
      return {
        expr_type: "conjunction",
        conjunction_type: spec.conjunction_type === "or" ? "or" : "and",
        children: kids,
      };
    }
    default:
      return null;
  }
}

function opFromString(s: string): ComparisonOp {
  switch (s) {
    case "eq": case "==": return ComparisonOp.EQ;
    case "ne": case "!=": return ComparisonOp.NE;
    case "gt": case ">": return ComparisonOp.GT;
    case "ge": case ">=": return ComparisonOp.GE;
    case "lt": case "<": return ComparisonOp.LT;
    case "le": case "<=": return ComparisonOp.LE;
    default: return ComparisonOp.EQ;
  }
}

export function deserializeFilters(
  batch: RecordBatch,
  getJoinKeysColumn?: (columnName: string) => any[] | null,
): PushdownFilters {
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

  const filters = specs
    .map((spec) => parseFilter(spec, getValue, getJoinKeysColumn))
    .filter((f): f is Filter => f !== null);
  return new PushdownFilters(filters, version);
}

// ============================================================================
// FilteringOutputCollector
// ============================================================================

/**
 * Wraps an OutputCollector to automatically apply pushdown filters
 * to every emitted batch.
 */
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
