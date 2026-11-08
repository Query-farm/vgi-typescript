// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Protocol 3.2 filtered scan planning (docs/protocol/scan-planning.md in
// vgi-python). Mirrors vgi-python's vgi/scan_planning.py and the
// `scan_planning` validation in vgi/catalog/catalog_interface.py.
//
// A table declaring `TableInfo.scan_planning = "filtered"` receives the
// query's predicates in `catalog_table_scan_branches_get`. The catalog's
// `tableScanBranchesPlan` hook gets them as a `ScanPlanningFilters` and may
// use them to return fewer branches or fewer files. Every predicate is
// advisory: the client re-applies all of them, so a worker may ignore any
// predicate it does not understand, but must never drop a branch or file that
// could hold a matching row.

import {
  deserializeBatch, isDate, isDecimal, isDuration, isInterval, isTime, isTimestamp,
  type VgiDataType, type VgiSchema,
} from "../arrow/index.js";
import { deserializeFilters } from "../filter-pushdown/deserialize.js";
import type { PushdownFilters } from "../filter-pushdown/evaluate.js";
import { ComparisonOp, type FilterExpression } from "../filter-pushdown/types.js";

/** Protocol 3.2 `TableInfo.scan_planning`: resolve the scan redirect at bind time (3.1 behaviour). */
export const SCAN_PLANNING_BIND = "bind";
/** Protocol 3.2 `TableInfo.scan_planning`: resolve it per scan with the query's filters. */
export const SCAN_PLANNING_FILTERED = "filtered";
/** When the client resolves a table's scan redirect. */
export type ScanPlanningMode = typeof SCAN_PLANNING_BIND | typeof SCAN_PLANNING_FILTERED;
/** Every legal `TableInfo.scan_planning` value. */
export const SCAN_PLANNING_MODES: readonly ScanPlanningMode[] = Object.freeze([SCAN_PLANNING_BIND, SCAN_PLANNING_FILTERED]);

/**
 * Validate a table's `scan_planning` value and return it (`"bind"` when
 * absent). Unknown values are a protocol error, and a `"filtered"` table must
 * not inline `scan_function`: its scan redirect is resolved per query with
 * the query's filters.
 */
export function validateScanPlanning(
  tableName: string,
  scanPlanning: unknown,
  hasInlineScanFunction: boolean,
): ScanPlanningMode {
  const mode = scanPlanning ?? SCAN_PLANNING_BIND;
  if (mode !== SCAN_PLANNING_BIND && mode !== SCAN_PLANNING_FILTERED) {
    throw new Error(
      `Table '${tableName}': scan_planning must be one of ${JSON.stringify(SCAN_PLANNING_MODES)}, got ${JSON.stringify(mode)}`,
    );
  }
  if (mode === SCAN_PLANNING_FILTERED && hasInlineScanFunction) {
    throw new Error(
      `Table '${tableName}': scan_planning='filtered' cannot be combined with an inlined scan_function; ` +
        "the scan redirect must be resolved per query with the query's filters",
    );
  }
  return mode;
}

/**
 * Per-column statistics describing a scan candidate (a file, a partition, a
 * branch) — the same claims `ScanBranch.column_statistics` makes: every
 * non-null value of the column lies within `[min, max]`; `hasNull: false`
 * means the column has no NULLs; `hasNotNull: false` means it is entirely
 * NULL. A missing `min`/`max`/flag claims nothing. `ColumnStatistics` (the
 * type `serializeColumnStatistics` takes) satisfies this shape.
 */
export interface ScanCandidateStatistics {
  columnName: string;
  /** Arrow type of `min`/`max`; defaults to the column's declared type. */
  arrowType?: VgiDataType | null;
  min?: unknown;
  max?: unknown;
  hasNull?: boolean | null;
  hasNotNull?: boolean | null;
}

type Awaitable<T> = T | Promise<T>;

/**
 * The query predicates sent with a filtered `catalog_table_scan_branches_get`.
 *
 * `raw` is the wire bytes: an Arrow IPC stream holding one VGI Filter
 * Encoding v2 snapshot batch whose column indexes refer to the table's
 * declared columns (`TableInfo.columns`). `decode()` resolves those columns
 * (at most once) and decodes the predicates; nothing is decoded until a hook
 * asks.
 */
export class ScanPlanningFilters {
  private _decoded: Promise<PushdownFilters> | null = null;

  constructor(
    /** The wire bytes of the filter v2 snapshot. */
    readonly raw: Uint8Array,
    /** Resolves the table's declared columns, the schema the column indexes refer to. */
    private readonly tableColumns: () => Awaitable<VgiSchema | null>,
  ) {}

  /** Decode the predicates against the table's declared columns (cached). */
  decode(): Promise<PushdownFilters> {
    if (this._decoded == null) {
      this._decoded = (async () => {
        const columns = await this.tableColumns();
        if (columns == null) {
          throw new Error("pushdown_filters: the table's declared columns could not be resolved");
        }
        return deserializeFilters(deserializeBatch(this.raw), { outputSchema: columns });
      })();
      // A failed decode is not cached: the next call retries.
      this._decoded.catch(() => { this._decoded = null; });
    }
    return this._decoded;
  }

  /**
   * False only when the predicates provably exclude every row `statistics`
   * describes. See `filtersMayMatch`.
   */
  async mayMatch(
    statistics: Iterable<ScanCandidateStatistics> | Readonly<Record<string, ScanCandidateStatistics>>,
  ): Promise<boolean> {
    return filtersMayMatch(await this.decode(), statistics);
  }
}

/**
 * Return false only when `filters` provably exclude every row described by
 * `statistics`; use it to prune a manifest (files, partitions) before
 * returning branches.
 *
 * Reasons over top-level conjuncts built from `AND`/`OR`, comparisons between
 * a column and a literal (`=`, `!=`, `<`, `<=`, `>`, `>=`), `IN` lists and
 * `IS [NOT] NULL`. Columns, predicates or values it cannot reason about —
 * casts, function calls, incomparable literal types, columns without
 * statistics — count as "may match", so the answer errs towards keeping data.
 */
export function filtersMayMatch(
  filters: PushdownFilters,
  statistics: Iterable<ScanCandidateStatistics> | Readonly<Record<string, ScanCandidateStatistics>>,
): boolean {
  const list: Iterable<ScanCandidateStatistics> = Symbol.iterator in (statistics as object)
    ? (statistics as Iterable<ScanCandidateStatistics>)
    : Object.values(statistics as Record<string, ScanCandidateStatistics>);
  const byName = new Map<string, ScanCandidateStatistics>();
  for (const stat of list) byName.set(stat.columnName, stat);
  if (byName.size === 0) return true;
  for (const predicate of filters.predicates) {
    if (provablyFalse(predicate.expression, byName)) return false;
  }
  return true;
}

const FLIPPED: Partial<Record<ComparisonOp, ComparisonOp>> = {
  [ComparisonOp.EQ]: ComparisonOp.EQ,
  [ComparisonOp.NE]: ComparisonOp.NE,
  [ComparisonOp.LT]: ComparisonOp.GT,
  [ComparisonOp.LE]: ComparisonOp.GE,
  [ComparisonOp.GT]: ComparisonOp.LT,
  [ComparisonOp.GE]: ComparisonOp.LE,
};

/** A column reference with statistics, and the Arrow type its statistic values are in. */
interface StatRef {
  stat: ScanCandidateStatistics;
  type: VgiDataType | undefined;
}

function columnStat(expr: FilterExpression, stats: Map<string, ScanCandidateStatistics>): StatRef | null {
  if (expr.node !== "column_ref") return null;
  const stat = stats.get(expr.columnName);
  // Statistic values are in the statistic's own Arrow type when it names one
  // (that is how they serialize), else in the column's declared type.
  return stat == null ? null : { stat, type: stat.arrowType ?? expr.dataType };
}

/** Whether every non-null value in `[lo, hi]` differs from `value` (`value` lies outside the range). */
function outside(ref: StatRef, value: unknown, valueType: VgiDataType | undefined): boolean {
  const lo = ref.stat.min ?? null;
  const hi = ref.stat.max ?? null;
  const cmpLo = lo == null ? null : compare(lo, ref.type, value, valueType);
  const cmpHi = hi == null ? null : compare(hi, ref.type, value, valueType);
  return (cmpLo != null && cmpLo > 0) || (cmpHi != null && cmpHi < 0);
}

/** True only when `expr` is false or NULL for every row `stats` describe. */
function provablyFalse(expr: FilterExpression, stats: Map<string, ScanCandidateStatistics>): boolean {
  switch (expr.node) {
    case "and":
      return expr.children.some((child) => provablyFalse(child, stats));
    case "or":
      return expr.children.length > 0 && expr.children.every((child) => provablyFalse(child, stats));
    case "comparison": {
      let op: ComparisonOp | undefined = expr.op;
      let ref = columnStat(expr.left, stats);
      let literal = expr.right;
      if (ref == null) {
        ref = columnStat(expr.right, stats);
        literal = expr.left;
        op = FLIPPED[expr.op];
      }
      // IS [NOT] DISTINCT FROM is never NULL, so it is not in FLIPPED.
      if (ref == null || op == null || !(op in FLIPPED) || literal.node !== "literal") return false;
      if (ref.stat.hasNotNull === false) return true; // all NULL: the comparison is NULL
      const c = literal.value;
      if (c == null) return true; // a comparison with NULL is NULL
      const litType = literal.field?.type;
      const lo = ref.stat.min ?? null;
      const hi = ref.stat.max ?? null;
      const cmpLo = lo == null ? null : compare(lo, ref.type, c, litType);
      const cmpHi = hi == null ? null : compare(hi, ref.type, c, litType);
      switch (op) {
        case ComparisonOp.EQ:
          return (cmpLo != null && cmpLo > 0) || (cmpHi != null && cmpHi < 0);
        case ComparisonOp.NE:
          return cmpLo === 0 && cmpHi === 0;
        case ComparisonOp.LT:
          return cmpLo != null && cmpLo >= 0;
        case ComparisonOp.LE:
          return cmpLo != null && cmpLo > 0;
        case ComparisonOp.GT:
          return cmpHi != null && cmpHi <= 0;
        case ComparisonOp.GE:
          return cmpHi != null && cmpHi < 0;
        default:
          return false;
      }
    }
    case "in": {
      if (expr.negated || expr.set.kind !== "literal") return false;
      const ref = columnStat(expr.expression, stats);
      if (ref == null) return false;
      if (ref.stat.hasNotNull === false) return true;
      if (ref.stat.min == null && ref.stat.max == null) return false;
      const setType = expr.set.field?.type;
      const values = expr.set.values.filter((v) => v != null);
      if (values.length === 0) return true; // x IN (NULL, ...) is never true
      return values.every((v) => outside(ref, v, setType));
    }
    case "is_null": {
      const ref = columnStat(expr.expression, stats);
      if (ref == null) return false;
      return expr.negated ? ref.stat.hasNotNull === false : ref.stat.hasNull === false;
    }
    default:
      return false;
  }
}

/**
 * Sign of `a - b`, or null when the two values are not comparable. Each value
 * is first normalized by its Arrow type (a date32 literal arrives as a day
 * count, a date statistic is usually a `Date`, a timestamp is a bigint in its
 * unit), then values compare only within one kind (numeric, string, boolean,
 * date, timestamp), so a representation it does not understand reads as
 * "unknown", never as a wrong answer.
 */
function compare(a: unknown, aType: VgiDataType | undefined, b: unknown, bType: VgiDataType | undefined): number | null {
  const x = comparable(a, aType);
  const y = comparable(b, bType);
  if (x == null || y == null || x.kind !== y.kind) return null;
  const u = x.value;
  const v = y.value;
  if (typeof u === "number" && Number.isNaN(u)) return null;
  if (typeof v === "number" && Number.isNaN(v)) return null;
  if (typeof u === "string" && typeof v === "string") return compareCodePoints(u, v);
  // Mixed number/bigint compare exactly with the relational operators.
  return u < v ? -1 : u > v ? 1 : 0;
}

/** Code-point (= UTF-8 byte) order, which is how DuckDB orders VARCHAR; `<` on JS strings is UTF-16 order. */
function compareCodePoints(a: string, b: string): number {
  const ia = a[Symbol.iterator]();
  const ib = b[Symbol.iterator]();
  for (;;) {
    const x = ia.next();
    const y = ib.next();
    if (x.done || y.done) return x.done && y.done ? 0 : x.done ? -1 : 1;
    const cx = x.value.codePointAt(0)!;
    const cy = y.value.codePointAt(0)!;
    if (cx !== cy) return cx < cy ? -1 : 1;
  }
}

const MS_PER_DAY = 86_400_000;
// Arrow TimeUnit SECOND=0, MILLISECOND=1, MICROSECOND=2, NANOSECOND=3 -> nanoseconds per unit.
const NS_PER_UNIT = [1_000_000_000n, 1_000_000n, 1_000n, 1n] as const;

type Comparable = { kind: string; value: number | bigint | string };

function comparable(v: unknown, type: VgiDataType | undefined): Comparable | null {
  if (type != null) {
    if (isDate(type)) {
      // Milliseconds since the epoch; a raw value is days (date32) or ms (date64).
      if (v instanceof Date) return { kind: "date", value: v.getTime() };
      if (typeof v === "number" || typeof v === "bigint") {
        return { kind: "date", value: (type as { unit?: number }).unit === 0 ? Number(v) * MS_PER_DAY : Number(v) };
      }
      return null;
    }
    if (isTimestamp(type)) {
      // Nanoseconds since the epoch; a raw value is in the type's unit.
      const scale = NS_PER_UNIT[(type as { unit?: number }).unit ?? -1];
      if (scale == null) return null;
      if (v instanceof Date) return { kind: "timestamp", value: BigInt(v.getTime()) * 1_000_000n };
      if (typeof v === "bigint") return { kind: "timestamp", value: v * scale };
      if (typeof v === "number" && Number.isSafeInteger(v)) return { kind: "timestamp", value: BigInt(v) * scale };
      return null;
    }
    // Decimals, times, intervals, nested types: representation-dependent; never prune.
    if (isDecimal(type) || isTime(type) || isInterval(type) || isDuration(type)) return null;
  }
  if (v instanceof Date) return null; // a Date without a date/timestamp type is ambiguous
  if (typeof v === "number" || typeof v === "bigint") return { kind: "numeric", value: v };
  if (typeof v === "string") return { kind: "string", value: v };
  if (typeof v === "boolean") return { kind: "boolean", value: v ? 1 : 0 };
  return null;
}
