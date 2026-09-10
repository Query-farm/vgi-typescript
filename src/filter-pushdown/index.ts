// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Filter pushdown — types, deserialization from the wire format, evaluation
// against Arrow batches, and an OutputCollector wrapper that applies filters
// transparently. Ported from vgi-python/vgi/table_filter_pushdown.py.

export {
  FILTER_ENCODING,
  FILTER_VERSION,
  DUCKDB_STANDARD_V1,
  NO_EVALUATION_CONTEXT,
  DUCKDB_SESSION_CONTEXT,
  ComparisonOp,
  type ArithmeticOp,
  type EvaluationContext,
  type Filter,
  type AndFilter,
  type ConstantFilter,
  type ExpressionFilter,
  type ExprNode,
  type InFilter,
  type IsNotNullFilter,
  type IsNullFilter,
  type OrFilter,
  type StructFilter,
  type FilterExpression,
  type FilterPredicate,
  type FilterSet,
  type FunctionIdentity,
  type PredicateMode,
  type PredicateSource,
  type StandardFilterFunction,
} from "./types.js";

export { PushdownFilters } from "./evaluate.js";

export { FilterV2Error, applyFilterDelta, buildJoinKeysLookup, deserializeFilters } from "./deserialize.js";

export {
  FilteringOutputCollector,
  formatPushedFilters,
  reprPushedFilters,
} from "./collector.js";
