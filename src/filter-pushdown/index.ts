// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Filter pushdown — types, deserialization from the wire format, evaluation
// against Arrow batches, and an OutputCollector wrapper that applies filters
// transparently. Ported from vgi-python/vgi/table_filter_pushdown.py.

export {
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

export { PushdownFilters } from "./evaluate.js";

export { buildJoinKeysLookup, deserializeFilters } from "./deserialize.js";

export {
  FilteringOutputCollector,
  formatPushedFilters,
  reprPushedFilters,
} from "./collector.js";
