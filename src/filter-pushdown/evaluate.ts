// Filter pushdown evaluation: row-by-row predicate evaluation against
// Arrow RecordBatches, plus the PushdownFilters class that owns a parsed
// filter set and applies it to batches.

import { type VgiBatch, type VgiSchema, schema } from "../arrow/index.js";
import { batchFromColumns, emptyBatch } from "../util/arrow/index.js";
import {
  ComparisonOp,
  type AndFilter,
  type ConstantFilter,
  type ExpressionFilter,
  type ExprNode,
  type Filter,
  type InFilter,
  type IsNotNullFilter,
  type IsNullFilter,
  type OrFilter,
  type StructFilter,
} from "./types.js";

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
  batch: VgiBatch,
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
  batch: VgiBatch,
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
  batch: VgiBatch,
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
  batch: VgiBatch,
  mask: Uint8Array,
): void {
  const col = batch.getChildAt(filter.columnIndex)!;
  const n = batch.numRows;
  for (let i = 0; i < n; i++) {
    if (!mask[i]) continue;
    if (!(col.get(i) !== null)) {
      mask[i] = 0;
    } else {
      mask[i] = compare(col.get(i), filter.value, filter.op) ? 1 : 0;
    }
  }
}

function evaluateIsNull(
  filter: IsNullFilter,
  batch: VgiBatch,
  mask: Uint8Array,
): void {
  const col = batch.getChildAt(filter.columnIndex)!;
  const n = batch.numRows;
  for (let i = 0; i < n; i++) {
    if (!mask[i]) continue;
    mask[i] = (col.get(i) !== null) ? 0 : 1;
  }
}

function evaluateIsNotNull(
  filter: IsNotNullFilter,
  batch: VgiBatch,
  mask: Uint8Array,
): void {
  const col = batch.getChildAt(filter.columnIndex)!;
  const n = batch.numRows;
  for (let i = 0; i < n; i++) {
    if (!mask[i]) continue;
    mask[i] = (col.get(i) !== null) ? 1 : 0;
  }
}

function evaluateIn(
  filter: InFilter,
  batch: VgiBatch,
  mask: Uint8Array,
): void {
  const col = batch.getChildAt(filter.columnIndex)!;
  const n = batch.numRows;
  for (let i = 0; i < n; i++) {
    if (!mask[i]) continue;
    if (!(col.get(i) !== null)) {
      mask[i] = 0;
    } else {
      mask[i] = filter.values.has(col.get(i)) ? 1 : 0;
    }
  }
}

function evaluateAnd(
  filter: AndFilter,
  batch: VgiBatch,
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
  batch: VgiBatch,
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
  batch: VgiBatch,
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
  const childSchema = schema([
    (batch.schema.fields[filter.columnIndex].type as any).children[filter.childIndex],
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
// SQL formatting
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

// ============================================================================
// PushdownFilters
// ============================================================================

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
  evaluate(batch: VgiBatch): Uint8Array {
    const n = batch.numRows;
    const mask = new Uint8Array(n);
    mask.fill(1);

    for (const filter of this.filters) {
      evaluateFilter(filter, batch, mask);
    }

    return mask;
  }

  /** Apply filters to a batch, returning only passing rows. */
  apply(batch: VgiBatch): VgiBatch {
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
