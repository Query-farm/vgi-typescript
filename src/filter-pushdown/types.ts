// Filter pushdown type definitions.

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
