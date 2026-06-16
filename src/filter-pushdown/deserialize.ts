// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Deserialize pushdown filters from the wire format (Arrow RecordBatch with
// JSON specs in column 0 and value-ref scalars in columns 1+).

import { type VgiBatch, type VgiDataType, readCanonicalValue } from "../arrow/index.js";
import { ComparisonOp, type ExprNode, type Filter } from "./types.js";
import { PushdownFilters } from "./evaluate.js";

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
 * Build a getJoinKeysColumn lookup from an InitRequest's `joinKeys` batches.
 * Each batch typically has a single column whose name is the join-keys column
 * referenced by `spec.keys_column` on a join_keys filter.
 */
export function buildJoinKeysLookup(
  joinKeyBatches: VgiBatch[],
): (columnName: string) => any[] | null {
  const index = new Map<string, any[]>();
  for (const batch of joinKeyBatches) {
    for (const field of batch.schema.fields) {
      const col = batch.getChild(field.name);
      if (!col) continue;
      const type = field.type as unknown as VgiDataType;
      const values: any[] = [];
      for (let i = 0; i < batch.numRows; i++) {
        // Canonical read so join keys match the column cells they filter
        // against (built via readCanonicalValue in evaluate.ts).
        values.push(readCanonicalValue(type, col, i));
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

/**
 * Deserialize pushdown filters from an Arrow RecordBatch.
 *
 * Wire format:
 * - Column 0: UTF8 — JSON array of filter specs. Field metadata: vgi_filter_version: "1"
 * - Columns 1+: Scalar values referenced by value_ref → column N+1
 */
export function deserializeFilters(
  batch: VgiBatch,
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

  // Value resolver: value_ref N → column N+1, row 0. Read through the canonical
  // reader so filter literals land in the SAME representation as the column
  // cells they're compared against (e.g. timestamp -> bigint on both backends),
  // and so it works on flechette (no arrow-js `.get()`).
  const getValue = (ref: number): any => {
    const col = batch.getChildAt(ref + 1);
    if (!col) {
      throw new Error(`Filter value_ref ${ref} references non-existent column ${ref + 1}`);
    }
    const type = batch.schema.fields[ref + 1].type as unknown as VgiDataType;
    return readCanonicalValue(type, col, 0);
  };

  const filters = specs
    .map((spec) => parseFilter(spec, getValue, getJoinKeysColumn))
    .filter((f): f is Filter => f !== null);
  return new PushdownFilters(filters, version);
}
