// Copyright 2025, 2026 Query Farm LLC - https://query.farm

import {
  type VgiBatch,
  type VgiDataType,
  isBool,
  isFloat,
  isInt,
  isUtf8,
  readCanonicalValue,
} from "../arrow/index.js";
import { filterBatch } from "../util/arrow/index.js";
import { applyFilterDelta, type DeserializeFilterOptions, FilterV2Error } from "./deserialize.js";
import { filterFunctionIdentityKey } from "./capabilities.js";
import {
  ComparisonOp,
  type EvaluationContext,
  type FilterExpression,
  type FilterPredicate,
  type FunctionIdentity,
} from "./types.js";

type SqlBoolean = boolean | null;

/**
 * A batch's columns by name, resolved once per batch.
 *
 * Filters are evaluated row by row, and resolving a `column_ref` per row --
 * a field search plus `getChildAt`, which builds a fresh Vector on arrow-js --
 * cost more than the comparison itself: ~0.3 ms per 1000-row batch per
 * referenced column. A batch is immutable, so what it resolves to never changes.
 */
const RESOLVED_COLUMNS = new WeakMap<object, Map<string, { type: VgiDataType; column: unknown } | null>>();

function resolveColumn(batch: VgiBatch, columnName: string): { type: VgiDataType; column: unknown } | null {
  let byName = RESOLVED_COLUMNS.get(batch);
  if (!byName) {
    byName = new Map();
    RESOLVED_COLUMNS.set(batch, byName);
  }
  let resolved = byName.get(columnName);
  if (resolved === undefined) {
    const index = batch.schema.fields.findIndex((field) => field.name === columnName);
    if (index < 0) {
      throw new FilterV2Error(`filter column ${columnName} is unavailable in emitted batch`);
    }
    const column = batch.getChildAt(index);
    resolved = column ? { type: batch.schema.fields[index].type, column } : null;
    byName.set(columnName, resolved);
  }
  return resolved;
}

function deepEqual(left: unknown, right: unknown): boolean {
  if (typeof left === "number" && typeof right === "number") {
    return left === right || (Number.isNaN(left) && Number.isNaN(right));
  }
  if (left instanceof Uint8Array && right instanceof Uint8Array) {
    return left.length === right.length && left.every((value, index) => value === right[index]);
  }
  if (Array.isArray(left) && Array.isArray(right)) {
    return left.length === right.length && left.every((value, index) => deepEqual(value, right[index]));
  }
  if (left && right && typeof left === "object" && typeof right === "object") {
    const entries = Object.entries(left as Record<string, unknown>);
    const rightObject = right as Record<string, unknown>;
    return entries.length === Object.keys(rightObject).length &&
      entries.every(([key, value]) => key in rightObject && deepEqual(value, rightObject[key]));
  }
  return left === right;
}

function orderedCompare(left: unknown, right: unknown): number {
  if (typeof left === "number" && typeof right === "number") {
    const leftNan = Number.isNaN(left);
    const rightNan = Number.isNaN(right);
    if (leftNan || rightNan) return leftNan === rightNan ? 0 : leftNan ? 1 : -1;
  }
  if (deepEqual(left, right)) return 0;
  return (left as any) < (right as any) ? -1 : 1;
}

function compare(left: unknown, right: unknown, op: ComparisonOp): SqlBoolean {
  if (op === ComparisonOp.DISTINCT_FROM) {
    return left === null || right === null ? left !== right : !deepEqual(left, right);
  }
  if (op === ComparisonOp.NOT_DISTINCT_FROM) {
    return left === null || right === null ? left === right : deepEqual(left, right);
  }
  if (left === null || right === null) return null;
  const order = orderedCompare(left, right);
  switch (op) {
    case ComparisonOp.EQ: return order === 0;
    case ComparisonOp.NE: return order !== 0;
    case ComparisonOp.LT: return order < 0;
    case ComparisonOp.LE: return order <= 0;
    case ComparisonOp.GT: return order > 0;
    case ComparisonOp.GE: return order >= 0;
    default: throw new FilterV2Error(`unknown comparison operator ${op}`);
  }
}

function andKleene(values: SqlBoolean[]): SqlBoolean {
  if (values.some((value) => value === false)) return false;
  return values.some((value) => value === null) ? null : true;
}

function orKleene(values: SqlBoolean[]): SqlBoolean {
  if (values.some((value) => value === true)) return true;
  return values.some((value) => value === null) ? null : false;
}

function integerBounds(type: VgiDataType): [bigint, bigint] | null {
  if (!isInt(type)) return null;
  const width = BigInt((type as any).bitWidth ?? 32);
  const signed = (type as any).isSigned ?? (type as any).signed ?? true;
  return signed
    ? [-(1n << (width - 1n)), (1n << (width - 1n)) - 1n]
    : [0n, (1n << width) - 1n];
}

function castValue(value: unknown, type: VgiDataType): unknown {
  if (value === null) return null;
  if (isUtf8(type)) return String(value);
  if (isBool(type)) {
    if (typeof value === "boolean") return value;
    if (value === 0 || value === 0n || value === "false") return false;
    if (value === 1 || value === 1n || value === "true") return true;
    throw new FilterV2Error(`cannot cast ${String(value)} to BOOLEAN`);
  }
  if (isFloat(type)) {
    const result = Number(value);
    if (Number.isNaN(result) && String(value).toLowerCase() !== "nan") {
      throw new FilterV2Error(`cannot cast ${String(value)} to floating point`);
    }
    return result;
  }
  const bounds = integerBounds(type);
  if (bounds) {
    let result: bigint;
    try {
      result = typeof value === "bigint" ? value : BigInt(typeof value === "number" ? Math.trunc(value) : String(value));
    } catch {
      throw new FilterV2Error(`cannot cast ${String(value)} to integer`);
    }
    if (result < bounds[0] || result > bounds[1]) throw new FilterV2Error("integer cast overflow");
    return (type as any).bitWidth === 64 ? result : Number(result);
  }
  throw new FilterV2Error(`no vgi.duckdb.standard.v1 cast evaluator for Arrow type ${type.typeId}`);
}

function numericBinary(
  op: "add" | "subtract" | "multiply" | "divide" | "modulo",
  left: unknown,
  right: unknown,
  context: EvaluationContext,
): unknown {
  if (left === null || right === null) return null;
  if ((typeof left !== "number" && typeof left !== "bigint") ||
      (typeof right !== "number" && typeof right !== "bigint")) {
    throw new FilterV2Error("arithmetic operands must be numeric");
  }
  if (typeof left === "bigint" && typeof right === "bigint") {
    if ((op === "divide" || op === "modulo") && right === 0n) throw new FilterV2Error("division by zero");
    switch (op) {
      case "add": return left + right;
      case "subtract": return left - right;
      case "multiply": return left * right;
      case "divide": return context.integerDivision ? left / right : Number(left) / Number(right);
      case "modulo": return left % right;
    }
  }
  const a = Number(left);
  const b = Number(right);
  if ((op === "divide" || op === "modulo") && b === 0 && context.ieeeFloatingPointOps === false) {
    throw new FilterV2Error("division by zero");
  }
  switch (op) {
    case "add": return a + b;
    case "subtract": return a - b;
    case "multiply": return a * b;
    case "divide": return a / b;
    case "modulo": return a % b;
  }
}

interface Bounds {
  minX: number;
  minY: number;
  maxX: number;
  maxY: number;
}

class WkbBoundsReader {
  private readonly view: DataView;
  private offset = 0;

  constructor(bytes: Uint8Array) {
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  read(): Bounds | null {
    const bounds = this.geometry();
    if (this.offset !== this.view.byteLength) throw new FilterV2Error("WKB has trailing bytes");
    return bounds;
  }

  private require(length: number): void {
    if (length < 0 || this.offset + length > this.view.byteLength) throw new FilterV2Error("truncated WKB geometry");
  }

  private byte(): number {
    this.require(1);
    return this.view.getUint8(this.offset++);
  }

  private uint32(littleEndian: boolean): number {
    this.require(4);
    const value = this.view.getUint32(this.offset, littleEndian);
    this.offset += 4;
    return value;
  }

  private float64(littleEndian: boolean): number {
    this.require(8);
    const value = this.view.getFloat64(this.offset, littleEndian);
    this.offset += 8;
    return value;
  }

  private point(littleEndian: boolean, dimensions: number): Bounds | null {
    const x = this.float64(littleEndian);
    const y = this.float64(littleEndian);
    for (let dimension = 2; dimension < dimensions; dimension++) this.float64(littleEndian);
    if (Number.isNaN(x) && Number.isNaN(y)) return null;
    if (!Number.isFinite(x) || !Number.isFinite(y)) throw new FilterV2Error("WKB coordinate must be finite");
    return { minX: x, minY: y, maxX: x, maxY: y };
  }

  private count(littleEndian: boolean): number {
    const count = this.uint32(littleEndian);
    if (count > Math.floor((this.view.byteLength - this.offset) / 4)) {
      throw new FilterV2Error("WKB element count exceeds payload size");
    }
    return count;
  }

  private merge(left: Bounds | null, right: Bounds | null): Bounds | null {
    if (!left) return right;
    if (!right) return left;
    return {
      minX: Math.min(left.minX, right.minX),
      minY: Math.min(left.minY, right.minY),
      maxX: Math.max(left.maxX, right.maxX),
      maxY: Math.max(left.maxY, right.maxY),
    };
  }

  private points(littleEndian: boolean, dimensions: number): Bounds | null {
    let bounds: Bounds | null = null;
    const count = this.count(littleEndian);
    for (let index = 0; index < count; index++) bounds = this.merge(bounds, this.point(littleEndian, dimensions));
    return bounds;
  }

  private geometry(): Bounds | null {
    const byteOrder = this.byte();
    if (byteOrder !== 0 && byteOrder !== 1) throw new FilterV2Error("invalid WKB byte order");
    const littleEndian = byteOrder === 1;
    const encodedType = this.uint32(littleEndian);
    const hasZ = (encodedType & 0x80000000) !== 0;
    const hasM = (encodedType & 0x40000000) !== 0;
    const hasSrid = (encodedType & 0x20000000) !== 0;
    let type = encodedType & 0x0fffffff;
    let dimensions = 2 + Number(hasZ) + Number(hasM);
    if (type >= 3000) {
      type -= 3000;
      dimensions = 4;
    } else if (type >= 2000) {
      type -= 2000;
      dimensions = 3;
    } else if (type >= 1000) {
      type -= 1000;
      dimensions = 3;
    }
    if (hasSrid) this.uint32(littleEndian);

    if (type === 1) return this.point(littleEndian, dimensions);
    if (type === 2) return this.points(littleEndian, dimensions);
    if (type === 3) {
      let bounds: Bounds | null = null;
      const rings = this.count(littleEndian);
      for (let ring = 0; ring < rings; ring++) bounds = this.merge(bounds, this.points(littleEndian, dimensions));
      return bounds;
    }
    if (type >= 4 && type <= 7) {
      let bounds: Bounds | null = null;
      const geometries = this.count(littleEndian);
      for (let index = 0; index < geometries; index++) bounds = this.merge(bounds, this.geometry());
      return bounds;
    }
    throw new FilterV2Error(`unsupported WKB geometry type ${type}`);
  }
}

function wkbBBox(bytes: unknown): Bounds | null {
  if (!(bytes instanceof Uint8Array)) throw new FilterV2Error("spatial extent arguments must be WKB binary values");
  return new WkbBoundsReader(bytes).read();
}

function evaluateCall(expression: Extract<FilterExpression, { node: "call" }>, args: unknown[]): unknown {
  if (args.some((value) => value === null)) return null;
  if (typeof expression.function === "string") {
    const [left, right] = args;
    switch (expression.function) {
      case "starts_with": return String(left).startsWith(String(right));
      case "ends_with": return String(left).endsWith(String(right));
      case "contains": return String(left).includes(String(right));
      case "list_contains": return Array.isArray(left) && left.some((value) => value !== null && deepEqual(value, right));
    }
  }
  if (filterFunctionIdentityKey(expression.function as FunctionIdentity) === "duckdb.spatial/intersects_extent@1") {
    const left = wkbBBox(args[0]);
    const right = wkbBBox(args[1]);
    return !!left && !!right && !(left.maxX < right.minX || left.minX > right.maxX ||
      left.maxY < right.minY || left.minY > right.maxY);
  }
  throw new FilterV2Error("no evaluator for extension filter function");
}

function evaluateExpression(expression: FilterExpression, batch: VgiBatch, row: number, context: EvaluationContext): unknown {
  switch (expression.node) {
    case "column_ref": {
      const resolved = resolveColumn(batch, expression.columnName);
      return resolved ? readCanonicalValue(resolved.type, resolved.column as any, row) : null;
    }
    case "field_ref": {
      const parent = evaluateExpression(expression.expression, batch, row, context);
      if (parent === null) return null;
      if (Array.isArray(parent)) return parent[expression.fieldIndex] ?? null;
      return (parent as Record<string, unknown>)[expression.fieldName] ?? null;
    }
    case "literal": return expression.value;
    case "comparison": return compare(evaluateExpression(expression.left, batch, row, context),
      evaluateExpression(expression.right, batch, row, context), expression.op);
    case "and": return andKleene(expression.children.map((child) => evaluateExpression(child, batch, row, context) as SqlBoolean));
    case "or": return orKleene(expression.children.map((child) => evaluateExpression(child, batch, row, context) as SqlBoolean));
    case "not": {
      const value = evaluateExpression(expression.expression, batch, row, context) as SqlBoolean;
      return value === null ? null : !value;
    }
    case "is_null": {
      const result = evaluateExpression(expression.expression, batch, row, context) === null;
      return expression.negated ? !result : result;
    }
    case "in": {
      const input = evaluateExpression(expression.expression, batch, row, context);
      if (input === null) return null;
      const matched = expression.set.values.some((value) => value !== null && deepEqual(input, value));
      let result: SqlBoolean = matched ? true : expression.set.values.some((value) => value === null) ? null : false;
      if (expression.negated && result !== null) result = !result;
      return result;
    }
    case "cast": return castValue(evaluateExpression(expression.expression, batch, row, context), expression.field.type);
    case "arithmetic": return numericBinary(expression.op, evaluateExpression(expression.left, batch, row, context),
      evaluateExpression(expression.right, batch, row, context), context);
    case "negate": {
      const value = evaluateExpression(expression.expression, batch, row, context);
      return value === null ? null : typeof value === "bigint" ? -value : -Number(value);
    }
    case "call": return evaluateCall(expression, expression.arguments.map((argument) => evaluateExpression(argument, batch, row, context)));
    case "runtime_filter": throw new FilterV2Error("runtime-filter artifact evaluator is unavailable");
  }
}

function expressionColumns(expression: FilterExpression, output = new Map<number, string>()): Map<number, string> {
  switch (expression.node) {
    case "column_ref": output.set(expression.columnIndex, expression.columnName); break;
    case "field_ref": case "not": case "is_null": case "cast": case "negate":
      expressionColumns(expression.expression, output); break;
    case "comparison": case "arithmetic":
      expressionColumns(expression.left, output); expressionColumns(expression.right, output); break;
    case "and": case "or": expression.children.forEach((child) => expressionColumns(child, output)); break;
    case "in": expressionColumns(expression.expression, output); break;
    case "call": expression.arguments.forEach((argument) => expressionColumns(argument, output)); break;
    case "runtime_filter": expressionColumns(expression.input, output); break;
  }
  return output;
}

/**
 * `WHERE flag` / `WHERE NOT flag` — a BOOLEAN column is a predicate on its own.
 *
 * DuckDB pushes such a predicate down as a bare `column_ref` rather than
 * rewriting it to `flag = true`, and the schema admits that: `coreExpression`
 * lists `columnRef` first. `flag` on its own is legal SQL, so this is not
 * about the fragment being invalid — it is that the shape is invisible to
 * everything that looks for a column/constant pair: `getColumnValues` returns
 * nothing for it, and the rendering disagrees with every other VGI SDK, all
 * of which spell it `flag = true`.
 *
 * The projection is exact rather than approximate, including under NULLs:
 * `WHERE flag` keeps only TRUE (a NULL predicate is not satisfied) and so does
 * `flag = true`; `WHERE NOT flag` keeps only FALSE (`NOT NULL` is NULL) and so
 * does `flag = false`. Three-valued logic makes both pairs agree on every
 * input, which is what lets this be a projection and not a change of meaning.
 *
 * A column that is not BOOLEAN is left alone rather than guessed at.
 *
 * @returns the column name and the constant it compares equal to, or
 *   undefined when `expression` is neither shape.
 */
function booleanColumnLeaf(expression: FilterExpression): { columnName: string; value: boolean } | undefined {
  const column = (node: FilterExpression) =>
    node.node === "column_ref" && node.dataType !== undefined && isBool(node.dataType)
      ? node.columnName
      : undefined;
  if (expression.node === "not") {
    const name = column(expression.expression);
    return name === undefined ? undefined : { columnName: name, value: false };
  }
  const name = column(expression);
  return name === undefined ? undefined : { columnName: name, value: true };
}

function discreteValues(expression: FilterExpression, columnName: string): unknown[] | null {
  // `WHERE flag` pins `flag` to TRUE exactly as `flag = true` does, so a
  // value-pruning caller sees the constant either way round.
  const leaf = booleanColumnLeaf(expression);
  if (leaf && leaf.columnName === columnName) return [leaf.value];
  if (expression.node === "comparison" && expression.op === ComparisonOp.EQ) {
    if (expression.left.node === "column_ref" && expression.left.columnName === columnName && expression.right.node === "literal") return [expression.right.value];
    if (expression.right.node === "column_ref" && expression.right.columnName === columnName && expression.left.node === "literal") return [expression.left.value];
  }
  if (expression.node === "in" && expression.expression.node === "column_ref" &&
      expression.expression.columnName === columnName && !expression.negated) return expression.set.values;
  if (expression.node === "and") {
    for (const child of expression.children) {
      const values = discreteValues(child, columnName);
      if (values) return values;
    }
  }
  if (expression.node === "or") {
    const branches = expression.children.map((child) => discreteValues(child, columnName));
    if (branches.some((values) => values === null)) return null;
    const result: unknown[] = [];
    for (const values of branches as unknown[][]) {
      for (const value of values) if (!result.some((known) => deepEqual(known, value))) result.push(value);
    }
    return result;
  }
  return null;
}

export class PushdownFilters {
  readonly version = "2";

  constructor(
    readonly predicates: FilterPredicate[],
    readonly evaluationContext: EvaluationContext,
    readonly options: DeserializeFilterOptions,
    readonly revisions = new Map<string, number>(),
    readonly requiredIds = new Set<string>(),
  ) {}

  get filters(): FilterExpression[] {
    return this.predicates.map((predicate) => predicate.expression);
  }

  applyDelta(batch: VgiBatch): PushdownFilters {
    return applyFilterDelta(this, batch);
  }

  /**
   * These filters with their predicates arranged in `order`.
   *
   * Only the order changes, and `order` must name exactly the live predicate
   * ids. Restores the order a stream had before its delta history was
   * compacted: an id removed and later re-added moves to the end, which a
   * shorter replay does not reproduce by itself.
   */
  withPredicateOrder(order: readonly string[]): PushdownFilters {
    const predicates = this.predicates;
    if (predicates.length === order.length && predicates.every((predicate, i) => predicate.id === order[i])) {
      return this;
    }
    const byId = new Map(predicates.map((predicate) => [predicate.id, predicate]));
    if (order.length !== byId.size || new Set(order).size !== order.length || !order.every((id) => byId.has(id))) {
      throw new FilterV2Error("recorded predicate order does not match the replayed filter state");
    }
    return new PushdownFilters(
      order.map((id) => byId.get(id)!),
      this.evaluationContext,
      this.options,
      new Map(this.revisions),
      new Set(this.requiredIds),
    );
  }

  evaluate(batch: VgiBatch): Uint8Array {
    const mask = new Uint8Array(batch.numRows);
    mask.fill(1);
    for (const predicate of this.predicates) {
      if (predicate.expression.node === "runtime_filter" && !predicate.expression.supported) continue;
      let values: unknown[];
      try {
        values = Array.from({ length: batch.numRows }, (_, row) => evaluateExpression(
          predicate.expression, batch, row, this.evaluationContext));
      } catch (error) {
        if (predicate.mode === "advisory") continue;
        throw error;
      }
      for (let row = 0; row < batch.numRows; row++) mask[row] &= values[row] === true ? 1 : 0;
    }
    return mask;
  }

  apply(batch: VgiBatch): VgiBatch {
    if (!batch.numRows || !this.predicates.length) return batch;
    return filterBatch(batch, this.evaluate(batch));
  }

  filteredColumns(): string[] {
    const columns = new Set<string>();
    for (const predicate of this.predicates) {
      for (const name of expressionColumns(predicate.expression).values()) columns.add(name);
    }
    return [...columns].sort();
  }

  hasFilterForColumn(columnName: string): boolean {
    return this.filteredColumns().includes(columnName);
  }

  getColumnValues(columnName: string): unknown[] | null {
    for (const predicate of this.predicates) {
      const values = discreteValues(predicate.expression, columnName);
      if (values) return values;
    }
    return null;
  }

  toSql(): string {
    return this.predicates.map((predicate) => predicateToSql(predicate.expression)).join(" AND ");
  }
}

function quote(value: unknown): string {
  if (value === null) return "NULL";
  if (typeof value === "string") return `'${value.replaceAll("'", "''")}'`;
  if (value instanceof Uint8Array) return `X'${[...value].map((byte) => byte.toString(16).padStart(2, "0")).join("")}'`;
  if (Array.isArray(value)) return `[${value.map(quote).join(", ")}]`;
  return String(value);
}

/**
 * Render one predicate, projecting a bare boolean column onto its equality
 * form first. Predicate positions are the root and the children of `and`/`or`
 * — the shape that actually turns up is `other = x AND NOT flag`.
 */
export function predicateToSql(expression: FilterExpression): string {
  const leaf = booleanColumnLeaf(expression);
  return leaf ? `${leaf.columnName} = ${quote(leaf.value)}` : expressionToSql(expression);
}

export function expressionToSql(expression: FilterExpression): string {
  switch (expression.node) {
    case "column_ref": return expression.columnName;
    case "field_ref": return `${expressionToSql(expression.expression)}.${expression.fieldName}`;
    case "literal": return quote(expression.value);
    case "comparison": {
      const symbols: Record<ComparisonOp, string> = {
        [ComparisonOp.EQ]: "=", [ComparisonOp.NE]: "!=", [ComparisonOp.LT]: "<", [ComparisonOp.LE]: "<=",
        [ComparisonOp.GT]: ">", [ComparisonOp.GE]: ">=", [ComparisonOp.DISTINCT_FROM]: "IS DISTINCT FROM",
        [ComparisonOp.NOT_DISTINCT_FROM]: "IS NOT DISTINCT FROM",
      };
      return `${expressionToSql(expression.left)} ${symbols[expression.op]} ${expressionToSql(expression.right)}`;
    }
    case "and": case "or": return `(${expression.children.map(predicateToSql).join(` ${expression.node.toUpperCase()} `)})`;
    case "not": return `(NOT ${expressionToSql(expression.expression)})`;
    case "is_null": return `${expressionToSql(expression.expression)} IS ${expression.negated ? "NOT " : ""}NULL`;
    case "in": return `${expressionToSql(expression.expression)} ${expression.negated ? "NOT " : ""}IN (${expression.set.values.map(quote).join(", ")})`;
    case "cast": return `CAST(${expressionToSql(expression.expression)} AS type_${expression.typeRef})`;
    case "arithmetic": {
      const symbols = { add: "+", subtract: "-", multiply: "*", divide: "/", modulo: "%" } as const;
      return `(${expressionToSql(expression.left)} ${symbols[expression.op]} ${expressionToSql(expression.right)})`;
    }
    case "negate": return `(-${expressionToSql(expression.expression)})`;
    case "call": {
      const name = typeof expression.function === "string"
        ? expression.function
        : filterFunctionIdentityKey(expression.function);
      return `${name}(${expression.arguments.map(expressionToSql).join(", ")})`;
    }
    case "runtime_filter":
      return `${filterFunctionIdentityKey(expression.algorithm)}(${expressionToSql(expression.input)})`;
  }
}
