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
import {
  ComparisonOp,
  type EvaluationContext,
  type FilterExpression,
  type FilterPredicate,
  type FunctionIdentity,
} from "./types.js";

type SqlBoolean = boolean | null;

function readCell(batch: VgiBatch, index: number, row: number): unknown {
  const column = batch.getChildAt(index);
  return column ? readCanonicalValue(batch.schema.fields[index].type, column, row) : null;
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

function identityKey(identity: FunctionIdentity): string {
  return `${identity.namespace}/${identity.name}@${identity.version}`;
}

function wkbBBox(bytes: unknown): { minX: number; minY: number; maxX: number; maxY: number } | null {
  if (!(bytes instanceof Uint8Array) || bytes.length < 5) return null;
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const littleEndian = view.getUint8(0) === 1;
  const type = view.getUint32(1, littleEndian);
  if (type === 1 && bytes.length >= 21) {
    const x = view.getFloat64(5, littleEndian);
    const y = view.getFloat64(13, littleEndian);
    return { minX: x, minY: y, maxX: x, maxY: y };
  }
  if (type !== 3 || bytes.length < 13 || view.getUint32(5, littleEndian) === 0) return null;
  const points = view.getUint32(9, littleEndian);
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (let index = 0, offset = 13; index < points && offset + 16 <= bytes.length; index++, offset += 16) {
    const x = view.getFloat64(offset, littleEndian);
    const y = view.getFloat64(offset + 8, littleEndian);
    minX = Math.min(minX, x); minY = Math.min(minY, y);
    maxX = Math.max(maxX, x); maxY = Math.max(maxY, y);
  }
  return minX === Infinity ? null : { minX, minY, maxX, maxY };
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
  if (identityKey(expression.function as FunctionIdentity) === "duckdb.spatial/intersects_extent@1") {
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
      const projectedIndex = batch.schema.fields.findIndex((field) => field.name === expression.columnName);
      if (projectedIndex < 0) {
        throw new FilterV2Error(`filter column ${expression.columnName} is unavailable in emitted batch`);
      }
      return readCell(batch, projectedIndex, row);
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

function discreteValues(expression: FilterExpression, columnName: string): unknown[] | null {
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
    return this.predicates.map((predicate) => expressionToSql(predicate.expression)).join(" AND ");
  }
}

function quote(value: unknown): string {
  if (value === null) return "NULL";
  if (typeof value === "string") return `'${value.replaceAll("'", "''")}'`;
  if (value instanceof Uint8Array) return `X'${[...value].map((byte) => byte.toString(16).padStart(2, "0")).join("")}'`;
  if (Array.isArray(value)) return `[${value.map(quote).join(", ")}]`;
  return String(value);
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
    case "and": case "or": return `(${expression.children.map(expressionToSql).join(` ${expression.node.toUpperCase()} `)})`;
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
      const name = typeof expression.function === "string" ? expression.function : identityKey(expression.function);
      return `${name}(${expression.arguments.map(expressionToSql).join(", ")})`;
    }
    case "runtime_filter": return `${identityKey(expression.algorithm)}(${expressionToSql(expression.input)})`;
  }
}
