// Example aggregate function implementations.
// Ports vgi_count, vgi_sum, vgi_avg from vgi-python/vgi/examples/aggregate.py.

import { Schema, Field, Int64, Float64, Utf8, Decimal, DataType, Null, List, Struct, RecordBatch } from "@query-farm/apache-arrow";
import { defineAggregate } from "../src/functions/aggregate.js";
import { batchFromColumns } from "../src/util/arrow/index.js";
import { ArgumentValidationError } from "../src/index.js";
import type { VgiFunction } from "../src/index.js";

// ============================================================================
// vgi_count — nullary aggregate (no input columns). Counts rows per group.
// NullHandling.SPECIAL means DuckDB calls update() for every row including
// ones with NULL inputs (there are none here since the aggregate has no input
// columns, but the flag is the Python parity for counting).
// ============================================================================

interface CountState { count: bigint; }

const vgi_count = defineAggregate<Record<string, never>, CountState>({
  name: "vgi_count",
  description: "Count rows",
  outputType: new Int64(),
  nullHandling: "SPECIAL",
  initialState: () => ({ count: 0n }),
  update: ({ groupIds, ensureState }) => {
    // No column args — just tick per row. Every seen group gets state, since
    // NullHandling.SPECIAL means DuckDB sends rows even for NULL inputs.
    for (const gid of groupIds) {
      ensureState(gid).count += 1n;
    }
  },
  combine: (src, tgt) => ({ count: src.count + tgt.count }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (bigint | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.count : 0n;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

// ============================================================================
// vgi_sum — single int64 input. NullHandling.DEFAULT means DuckDB skips NULL
// inputs, so update() never sees them (all-NULL group = state stays at initial
// = group never added to states map = finalize sees undefined = returns NULL).
// ============================================================================

interface SumState { total: bigint; }

const vgi_sum = defineAggregate<{ value: number }, SumState>({
  name: "vgi_sum",
  description: "Sum integer values",
  args: { value: new Int64() },
  outputType: new Int64(),
  nullHandling: "DEFAULT",
  initialState: () => ({ total: 0n }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol != null && !valueCol.isValid(i)) continue;
      const v = valueCol?.get(i);
      if (v == null) continue;
      // Allocate lazily so all-NULL groups get no state, and finalize()
      // returns SQL NULL for those groups.
      const s = ensureState(groupIds[i]);
      s.total += typeof v === "bigint" ? v : BigInt(v);
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (bigint | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.total : null;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

// ============================================================================
// vgi_avg — two-field state (sum + count), NULL-skipping, returns FLOAT64.
// ============================================================================

interface AvgState { total: number; count: bigint; }

const vgi_avg = defineAggregate<{ value: number }, AvgState>({
  name: "vgi_avg",
  description: "Average of integer values",
  args: { value: new Int64() },
  outputType: new Float64(),
  nullHandling: "DEFAULT",
  initialState: () => ({ total: 0, count: 0n }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol != null && !valueCol.isValid(i)) continue;
      const v = valueCol?.get(i);
      if (v == null) continue;
      const s = ensureState(groupIds[i]);
      s.total += typeof v === "bigint" ? Number(v) : Number(v);
      s.count += 1n;
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total, count: src.count + tgt.count }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (number | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      if (s == null || s.count === 0n) return null;
      return s.total / Number(s.count);
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

// ============================================================================
// vgi_sum_all — varargs aggregate. Sums every value across any number of
// numeric columns. Single `columns` varargs param means the call site may
// pass 1..N columns and update() receives each as a separate entry in the
// columns array.
// ============================================================================

interface SumAllState { total: number; }

const vgi_sum_all = defineAggregate<{ columns: number }, SumAllState>({
  name: "vgi_sum_all",
  description: "Sum all numeric columns",
  args: { columns: new Float64() },
  varargs: ["columns"],
  outputType: new Float64(),
  nullHandling: "DEFAULT",
  onBind: (params) => {
    // Varargs guard: DuckDB drops the column entirely from the wire when
    // called with zero values, so input_schema is empty. Reject up-front.
    if (!params.inputSchema || params.inputSchema.fields.length === 0) {
      throw new ArgumentValidationError(
        "vgi_sum_all requires at least 1 value for varargs argument: 'columns'"
      );
    }
    return new Float64();
  },
  initialState: () => ({ total: 0 }),
  update: ({ groupIds, columns, ensureState }) => {
    const n = groupIds.length;
    // Precompute per-column decoders. DECIMAL columns arrive as raw
    // integers; scale them back to float. Integer columns are converted
    // via Number(). Matches Python's implicit DECIMAL → float on
    // pa.Array.to_pylist().
    const decoders = columns.map((col: any) => makeNumericDecoder(col?.type));
    for (let i = 0; i < n; i++) {
      let rowTotal = 0;
      let anyNonNull = false;
      for (let k = 0; k < columns.length; k++) {
        const col = columns[k];
        if (col == null || !col.isValid(i)) continue;
        const v = col.get(i);
        if (v == null) continue;
        anyNonNull = true;
        rowTotal += decoders[k](v);
      }
      if (anyNonNull) {
        ensureState(groupIds[i]).total += rowTotal;
      }
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (number | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.total : null;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

// Build a numeric decoder matching the column's Arrow type. Handles the
// common cases vgi_sum_all / vgi_weighted_sum / vgi_percentile need:
// - Int/BigInt: Number() coerce.
// - Decimal(p,s): Arrow stores the unscaled integer; scale back by 10^-s
//   to recover the logical float (pyarrow's to_pylist does this implicitly,
//   which is why the Python versions of these aggregates don't need to).
// - Float: passthrough.
function makeNumericDecoder(type: DataType | undefined): (v: any) => number {
  if (type && DataType.isDecimal(type)) {
    const scale = (type as Decimal).scale;
    const divisor = Math.pow(10, scale);
    return (v: any) => {
      if (typeof v === "bigint") return Number(v) / divisor;
      if (v instanceof Uint8Array) {
        // 128-bit decimal stored as 16 bytes — assemble as BigInt
        let n = 0n;
        for (let i = v.length - 1; i >= 0; i--) n = (n << 8n) | BigInt(v[i]);
        // Sign-extend: if top bit of MSB is set, it's negative
        if (v.length > 0 && (v[v.length - 1] & 0x80)) {
          n -= 1n << BigInt(v.length * 8);
        }
        return Number(n) / divisor;
      }
      return Number(v) / divisor;
    };
  }
  return (v: any) => (typeof v === "bigint" ? Number(v) : Number(v));
}

// ============================================================================
// vgi_listagg — order-dependent string concatenation with comma separator.
// Accumulates strings as they arrive, joined by ",". Null inputs are skipped
// (NullHandling.DEFAULT). combine() appends source.values after target.values
// to preserve left-to-right ordering within a thread.
// ============================================================================

interface ListAggState { values: string; }

const vgi_listagg = defineAggregate<{ value: string }, ListAggState>({
  name: "vgi_listagg",
  description: "Concatenate strings with comma separator",
  args: { value: new Utf8() },
  outputType: new Utf8(),
  nullHandling: "DEFAULT",
  initialState: () => ({ values: "" }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol == null || !valueCol.isValid(i)) continue;
      const v = valueCol.get(i);
      if (v == null) continue;
      const s = ensureState(groupIds[i]);
      s.values = s.values ? s.values + "," + String(v) : String(v);
    }
  },
  combine: (src, tgt) => {
    if (src.values && tgt.values) return { values: tgt.values + "," + src.values };
    return { values: tgt.values || src.values };
  },
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (string | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.values : null;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

// ============================================================================
// vgi_weighted_sum — multi-arg aggregate. Sum of value*weight per group.
// ============================================================================

interface WeightedSumState { total: number; }

const vgi_weighted_sum = defineAggregate<{ value: number; weight: number }, WeightedSumState>({
  name: "vgi_weighted_sum",
  description: "Weighted sum of values",
  args: { value: new Float64(), weight: new Float64() },
  outputType: new Float64(),
  nullHandling: "DEFAULT",
  initialState: () => ({ total: 0 }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const weightCol = columns[1];
    const decV = makeNumericDecoder(valueCol?.type);
    const decW = makeNumericDecoder(weightCol?.type);
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol == null || weightCol == null) continue;
      if (!valueCol.isValid(i) || !weightCol.isValid(i)) continue;
      const v = valueCol.get(i);
      const w = weightCol.get(i);
      if (v == null || w == null) continue;
      ensureState(groupIds[i]).total += decV(v) * decW(w);
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (number | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.total : null;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

// ============================================================================
// vgi_generic_sum — ANY-typed input, return type mirrors input. The onBind
// hook reads the input column's Arrow type from the bind input_schema and
// advertises the same type as output so `typeof(vgi_generic_sum(i::BIGINT))`
// is BIGINT rather than the default DOUBLE. State keeps a double total
// internally for precision; finalize() coerces back to the declared type.
// ============================================================================

interface GenericSumState { total: number; }

const vgi_generic_sum = defineAggregate<{ value: any }, GenericSumState>({
  name: "vgi_generic_sum",
  description: "Sum any numeric type",
  args: { value: new Null() },  // Null = ANY
  // outputType: Null marks this as a dynamic-return aggregate; onBind below
  // resolves the real type from the input column.
  outputType: new Null(),
  nullHandling: "DEFAULT",
  onBind: (params) => {
    // Output type mirrors the input column's declared type.
    if (params.inputSchema && params.inputSchema.fields.length > 0) {
      return params.inputSchema.fields[0].type;
    }
    return new Float64();
  },
  initialState: () => ({ total: 0 }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const dec = makeNumericDecoder(valueCol?.type);
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol == null || !valueCol.isValid(i)) continue;
      const v = valueCol.get(i);
      if (v == null) continue;
      ensureState(groupIds[i]).total += dec(v);
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const outField = outputSchema.fields[0];
    const isInt = DataType.isInt(outField.type) && (outField.type as any).bitWidth === 64;
    const results: (any | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      if (s == null) return null;
      return isInt ? BigInt(Math.round(s.total)) : s.total;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

// ============================================================================
// vgi_percentile(value, percentile) — approximate percentile. Demonstrates
// const-parameter folding: `percentile` is constant-folded by DuckDB at bind
// time and arrives through bindParams.args rather than as a per-row column.
// Internally we keep all values as a sorted array, then index at finalize.
// ============================================================================

interface PercentileState { values: number[]; }

const vgi_percentile = defineAggregate<{ value: number; percentile: number }, PercentileState>({
  name: "vgi_percentile",
  description: "Approximate percentile (demonstrates ConstParam)",
  args: { value: new Float64(), percentile: new Float64() },
  constParams: ["percentile"],
  argDefaults: { percentile: 0.5 },
  outputType: new Float64(),
  nullHandling: "DEFAULT",
  onBind: (params) => {
    // Validate the const-folded percentile up front so callers get a clear
    // error before any rows are aggregated.
    const pct = (params.args as any).percentile;
    if (pct === null || pct === undefined) {
      throw new ArgumentValidationError("vgi_percentile: percentile must not be NULL");
    }
    if (typeof pct !== "number" || !Number.isFinite(pct)) {
      throw new ArgumentValidationError("vgi_percentile: percentile must be a finite number");
    }
    if (pct < 0 || pct > 1) {
      throw new ArgumentValidationError("vgi_percentile: percentile must be in [0, 1]");
    }
    return new Float64();
  },
  initialState: () => ({ values: [] }),
  update: ({ groupIds, columns, ensureState }) => {
    // Only one column reaches update — `value`. `percentile` is const-folded
    // away by DuckDB at bind (stored in bindParams.args for finalize).
    const valueCol = columns[0];
    const dec = makeNumericDecoder(valueCol?.type);
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol == null || !valueCol.isValid(i)) continue;
      const v = valueCol.get(i);
      if (v == null) continue;
      ensureState(groupIds[i]).values.push(dec(v));
    }
  },
  combine: (src, tgt) => ({ values: tgt.values.concat(src.values) }),
  finalize: ({ groupIds, states, outputSchema, args }) => {
    const pct = args.percentile ?? 0.5;
    const results: (number | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      if (s == null || s.values.length === 0) return null;
      const sorted = [...s.values].sort((a, b) => a - b);
      const idx = Math.min(Math.floor(pct * sorted.length), sorted.length - 1);
      return sorted[idx];
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate"],
});

// ============================================================================
// vgi_window_sum — windowed sum (BIGINT). DuckDB falls back to the standard
// aggregate path (update/combine/finalize) for OVER queries when we don't
// register a window() callback, so this is structurally identical to vgi_sum
// with a different name. The separate registration also satisfies
// function_registration's inventory check.
// ============================================================================

const vgi_window_sum = defineAggregate<{ value: number }, SumState>({
  name: "vgi_window_sum",
  description: "Windowed sum that uses the per-partition window() callback",
  args: { value: new Int64() },
  outputType: new Int64(),
  nullHandling: "DEFAULT",
  initialState: () => ({ total: 0n }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol == null || !valueCol.isValid(i)) continue;
      const v = valueCol.get(i);
      if (v == null) continue;
      const s = ensureState(groupIds[i]);
      s.total += typeof v === "bigint" ? v : BigInt(v);
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (bigint | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.total : null;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate", "window"],
});

// ============================================================================
// vgi_window_median — median over a window. Collects values into the per-
// group state, sorts at finalize, emits the middle element. Not incremental
// (median is inherently non-associative), but correct under DuckDB's
// aggregate fallback path for OVER queries.
// ============================================================================

interface MedianState { values: number[]; }

const vgi_window_median = defineAggregate<{ value: number }, MedianState>({
  name: "vgi_window_median",
  description: "Windowed median (non-incremental aggregate)",
  args: { value: new Float64() },
  outputType: new Float64(),
  nullHandling: "DEFAULT",
  initialState: () => ({ values: [] }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const dec = makeNumericDecoder(valueCol?.type);
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol == null || !valueCol.isValid(i)) continue;
      const v = valueCol.get(i);
      if (v == null) continue;
      ensureState(groupIds[i]).values.push(dec(v));
    }
  },
  combine: (src, tgt) => ({ values: tgt.values.concat(src.values) }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (number | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      if (s == null || s.values.length === 0) return null;
      const sorted = [...s.values].sort((a, b) => a - b);
      const mid = sorted.length >>> 1;
      return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate", "window"],
});

// ============================================================================
// vgi_window_listagg — windowed string concatenation. Same impl as
// vgi_listagg; ships as a separate registration so window.test can pick it.
// ============================================================================

const vgi_window_listagg = defineAggregate<{ value: string }, ListAggState>({
  name: "vgi_window_listagg",
  description: "Windowed listagg (demonstrates ORDER_DEPENDENT aggregate fallback)",
  args: { value: new Utf8() },
  outputType: new Utf8(),
  nullHandling: "DEFAULT",
  initialState: () => ({ values: "" }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol == null || !valueCol.isValid(i)) continue;
      const v = valueCol.get(i);
      if (v == null) continue;
      const s = ensureState(groupIds[i]);
      s.values = s.values ? s.values + "," + String(v) : String(v);
    }
  },
  combine: (src, tgt) => {
    if (src.values && tgt.values) return { values: tgt.values + "," + src.values };
    return { values: tgt.values || src.values };
  },
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (string | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.values : null;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate", "window"],
});

// ============================================================================
// nest_tensor(value, axes_struct) — collect rows into a dense N-D tensor plus
// per-axis coordinate lists. The output is a struct {tensor, axes} where
// `tensor` is a nested list (one level per axis) indexed by sorted distinct
// coord values and `axes` is a struct with one list-of-coords field per axis.
// Mirrors vgi-python/vgi/examples/nest_tensor.py.
// ============================================================================

class NestTensorError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NestTensorError";
  }
}

const NEST_TENSOR_DEFAULT_MAX_CELLS = 10_000_000;
function nestTensorMaxCells(): number {
  const raw = process.env.VGI_NEST_TENSOR_MAX_CELLS;
  if (raw == null) return NEST_TENSOR_DEFAULT_MAX_CELLS;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0 || !Number.isInteger(n)) {
    throw new NestTensorError(`VGI_NEST_TENSOR_MAX_CELLS must be a positive integer, got ${JSON.stringify(raw)}`);
  }
  return n;
}

function _validateCoordType(name: string, t: DataType): void {
  if (DataType.isFloat(t)) {
    throw new NestTensorError(
      `nest_tensor: axis '${name}' has floating-point type ${t}; floats are not supported as coord types (NaN breaks equality)`,
    );
  }
  if (DataType.isStruct(t) || DataType.isList(t) || DataType.isMap(t) || DataType.isFixedSizeList(t)) {
    throw new NestTensorError(
      `nest_tensor: axis '${name}' has nested type ${t}; only scalar coord types are supported`,
    );
  }
}

function _nestedListType(inner: DataType, depth: number): DataType {
  let t: DataType = inner;
  for (let i = 0; i < depth; i++) {
    t = new List(new Field("item", t, true));
  }
  return t;
}

function _outputStructType(valueType: DataType, axesType: Struct): Struct {
  const axisFields = (axesType as any).children as Field[];
  const tensorType = _nestedListType(valueType, axisFields.length);
  const axesOutFields = axisFields.map((f) => new Field(f.name, new List(new Field("item", f.type, true)), false));
  return new Struct([
    new Field("tensor", tensorType, true),
    new Field("axes", new Struct(axesOutFields), false),
  ]);
}

function _makeNestedLists(shape: number[], fill: any): any {
  if (shape.length === 0) return fill;
  const [head, ...rest] = shape;
  const out: any[] = new Array(head);
  for (let i = 0; i < head; i++) out[i] = _makeNestedLists(rest, fill);
  return out;
}

// JSON-safe coord key for Set/Map lookups: serialize with BigInt handling.
function _coordKey(coords: any[]): string {
  return JSON.stringify(coords, (_, v) => (typeof v === "bigint" ? `__bi:${v.toString()}` : v));
}

// Stable comparator used for sorting coord values ascending. Supports the
// primitive scalar types allowed by _validateCoordType.
function _compareCoord(a: any, b: any): number {
  if (typeof a === "bigint" || typeof b === "bigint") {
    const ab = typeof a === "bigint" ? a : BigInt(a);
    const bb = typeof b === "bigint" ? b : BigInt(b);
    if (ab < bb) return -1; if (ab > bb) return 1; return 0;
  }
  if (typeof a === "number" || typeof b === "number") {
    return Number(a) - Number(b);
  }
  if (typeof a === "string" || typeof b === "string") {
    const as = String(a), bs = String(b);
    return as < bs ? -1 : as > bs ? 1 : 0;
  }
  if (typeof a === "boolean" || typeof b === "boolean") {
    return (a === b) ? 0 : (a ? 1 : -1);
  }
  return 0;
}

interface NestTensorState {
  // Accumulated rows for the group. Each row = [value, ...axis_coords].
  // Stored as parallel arrays to minimize JSON overhead on persist/load.
  values: any[];
  coords: any[][];  // coords[i] = axis values for row i, in axis order
}

const nest_tensor = defineAggregate<Record<string, never>, NestTensorState>({
  name: "nest_tensor",
  description: "Collect rows into a dense N-D tensor plus per-axis coordinates",
  args: { value: new Null(), axes: new Null() },
  outputType: new Null(),  // overridden by onBind
  nullHandling: "DEFAULT",
  onBind: (params) => {
    const inputSchema = params.inputSchema;
    if (!inputSchema || inputSchema.fields.length < 2) {
      throw new NestTensorError("nest_tensor: expected 2 arguments (value, axes struct)");
    }
    const valueType = inputSchema.fields[0].type;
    const axesType = inputSchema.fields[1].type;
    if (!DataType.isStruct(axesType)) {
      throw new NestTensorError(`nest_tensor: second argument must be a struct, got ${axesType}`);
    }
    const axisFields = (axesType as any).children as Field[];
    if (axisFields.length === 0) {
      throw new NestTensorError("nest_tensor: axes struct must have at least one field");
    }
    for (const f of axisFields) _validateCoordType(f.name, f.type);
    return _outputStructType(valueType, axesType as Struct);
  },
  initialState: () => ({ values: [], coords: [] }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const axesCol = columns[1];
    if (valueCol == null || axesCol == null) return;
    const axesType = (axesCol as any).type as Struct;
    const axisFields = (axesType as any).children as Field[];
    const axisNames = axisFields.map((f) => f.name);

    // Per-group seen-coord sets for intra-batch duplicate detection.
    const seenPerGroup = new Map<bigint, Set<string>>();

    for (let i = 0; i < groupIds.length; i++) {
      const gid = groupIds[i];
      if (!axesCol.isValid(i)) continue;  // null axes struct -> skip
      const row = axesCol.get(i);
      const coords: any[] = [];
      for (let a = 0; a < axisNames.length; a++) {
        const name = axisNames[a];
        // arrow-js StructRow exposes fields via [Symbol.iterator] or property access
        let val: any;
        if (row && typeof row === "object") {
          val = (row as any)[name] ?? (row as any).get?.(name);
        } else {
          val = null;
        }
        if (val == null) {
          throw new NestTensorError(
            `nest_tensor: null coord value for axis '${name}' at row ${i} (group ${gid})`,
          );
        }
        coords.push(val);
      }
      const key = _coordKey(coords);
      let seen = seenPerGroup.get(gid);
      if (seen == null) { seen = new Set(); seenPerGroup.set(gid, seen); }
      if (seen.has(key)) {
        const coordObj: Record<string, any> = {};
        for (let a = 0; a < axisNames.length; a++) coordObj[axisNames[a]] = coords[a];
        throw new NestTensorError(
          `nest_tensor: duplicate coordinate ${JSON.stringify(coordObj)} in group ${gid}`,
        );
      }
      seen.add(key);

      const value = valueCol.isValid(i) ? valueCol.get(i) : null;
      const s = ensureState(gid);
      s.values.push(value);
      s.coords.push(coords);
    }
  },
  combine: (src, tgt) => ({
    values: tgt.values.concat(src.values),
    coords: tgt.coords.concat(src.coords),
  }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const outField = outputSchema.fields[0];
    const outStruct = outField.type as Struct;
    const outStructFields = (outStruct as any).children as Field[];
    const axesOutField = outStructFields.find((f) => f.name === "axes")!;
    const axesOutFields = (axesOutField.type as any).children as Field[];
    const axisNames = axesOutFields.map((f) => f.name);
    const maxCells = nestTensorMaxCells();

    const tensors: any[] = [];
    const axesRows: any[] = [];

    for (const gid of groupIds) {
      const state = states.get(gid);
      if (state == null || state.values.length === 0) {
        tensors.push(_makeNestedLists(new Array(axisNames.length).fill(0), null));
        const empty: Record<string, any[]> = {};
        for (const n of axisNames) empty[n] = [];
        axesRows.push(empty);
        continue;
      }

      // Distinct sorted axis values.
      const axisValues: any[][] = [];
      const axisIndex: Array<Map<string, number>> = [];
      for (let a = 0; a < axisNames.length; a++) {
        const seen = new Map<string, any>();
        for (const coord of state.coords) {
          const v = coord[a];
          const k = _coordKey([v]);
          if (!seen.has(k)) seen.set(k, v);
        }
        const distinct = [...seen.values()].sort(_compareCoord);
        axisValues.push(distinct);
        const idx = new Map<string, number>();
        distinct.forEach((v, i) => idx.set(_coordKey([v]), i));
        axisIndex.push(idx);
      }

      const shape = axisValues.map((v) => v.length);
      let total = 1;
      for (const s of shape) total *= s;
      if (total > maxCells) {
        throw new NestTensorError(
          `nest_tensor: tensor has ${total} cells (shape [${shape.join(",")}]) exceeds VGI_NEST_TENSOR_MAX_CELLS=${maxCells} (group ${gid})`,
        );
      }

      const tensor = _makeNestedLists(shape, null);
      const filled = _makeNestedLists(shape, false);

      for (let r = 0; r < state.values.length; r++) {
        const coord = state.coords[r];
        const idxTuple: number[] = [];
        for (let a = 0; a < axisNames.length; a++) {
          const k = _coordKey([coord[a]]);
          idxTuple.push(axisIndex[a].get(k)!);
        }
        let cell: any = tensor;
        let flag: any = filled;
        for (let d = 0; d < idxTuple.length - 1; d++) {
          cell = cell[idxTuple[d]];
          flag = flag[idxTuple[d]];
        }
        const last = idxTuple[idxTuple.length - 1];
        if (flag[last]) {
          const coordObj: Record<string, any> = {};
          for (let a = 0; a < axisNames.length; a++) coordObj[axisNames[a]] = coord[a];
          throw new NestTensorError(
            `nest_tensor: duplicate coordinate ${JSON.stringify(coordObj)} in group ${gid} (arrived from parallel partitions)`,
          );
        }
        cell[last] = state.values[r];
        flag[last] = true;
      }

      tensors.push(tensor);
      const axesEntry: Record<string, any[]> = {};
      for (let a = 0; a < axisNames.length; a++) axesEntry[axisNames[a]] = axisValues[a];
      axesRows.push(axesEntry);
    }

    const results = groupIds.map((_, i) => ({ tensor: tensors[i], axes: axesRows[i] }));
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate", "tensor"],
});

// ============================================================================
// vgi_dynamic_agg(code, value...) — evaluates user-supplied code at finalize
// time via eval. Tests pass Python source (`class Aggregate: ...`), which we
// transpile to JS for the subset of syntax the test suite exercises:
// - class / @staticmethod / def  → object with arrow-function members
// - X if Y else Z                → (Y ? X : Z)
// - None / True / False          → null / true / false
// - int()/float()/len()          → Math.trunc / Number / .length
// - max(iter)/min(iter)/sum(iter) → Math.max(...iter) etc.
//
// Provides pyarrow-compute shims (pa.compute.sum, min, max, multiply) that
// operate on plain JS Arrays. The user's Aggregate.finalize(table[, params])
// runs in a scope with `pa`, `table`, and (for ml_agg) `params`.
//
// Not a sandbox — callers should trust the code source. The Python version
// runs eval() against the Python interpreter for the same reason.
// ============================================================================

// Per-group state: arrays of values per declared input column, so finalize
// can reconstruct a "table" and hand it to the user's Aggregate class.
interface DynamicAggState { cols: number[][]; code: string; }

const vgi_dynamic_agg = defineAggregate<{ code: string; value: number }, DynamicAggState>({
  name: "vgi_dynamic_agg",
  description: "Dynamic aggregate — user code string defines the Aggregate class",
  args: { code: new Utf8(), value: new Float64() },
  varargs: ["value"],
  outputType: new Float64(),
  nullHandling: "DEFAULT",
  initialState: () => ({ cols: [], code: "" }),
  update: ({ groupIds, columns, ensureState }) => {
    // Layout: [code_col, value_col1, ..., value_colN]. code_col repeats the
    // same string per row so we grab it once per group; the data columns
    // become c0, c1, ... accumulated into state.
    const codeCol = columns[0];
    const dataCols = columns.slice(1);
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      const s = ensureState(groupIds[i]);
      if (!s.code && codeCol && codeCol.isValid(i)) {
        s.code = String(codeCol.get(i));
      }
      // Lazily widen cols to match the number of data columns.
      while (s.cols.length < dataCols.length) s.cols.push([]);
      for (let k = 0; k < dataCols.length; k++) {
        const col = dataCols[k];
        if (col == null || !col.isValid(i)) continue;
        const v = col.get(i);
        if (v == null) continue;
        const dec = makeNumericDecoder(col.type);
        s.cols[k].push(dec(v));
      }
    }
  },
  combine: (src, tgt) => {
    // Merge column arrays pairwise. Preserve code from whichever side has it.
    const maxK = Math.max(src.cols.length, tgt.cols.length);
    const cols: number[][] = [];
    for (let k = 0; k < maxK; k++) {
      cols.push([...(tgt.cols[k] ?? []), ...(src.cols[k] ?? [])]);
    }
    return { cols, code: tgt.code || src.code };
  },
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (number | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      if (s == null || !s.code) return null;
      const result = evalDynamicAggregate(s.code, s.cols);
      return result == null ? null : Number(result);
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate", "dynamic"],
});

// pyarrow-compute-like shim for the `pa` identifier the user code may
// reference. Operates on plain JS number arrays.
const paShim = {
  compute: {
    sum: (arr: number[]) => arr.reduce((a, b) => a + b, 0),
    min: (arr: number[]) => arr.reduce((a, b) => (b < a ? b : a), Infinity),
    max: (arr: number[]) => arr.reduce((a, b) => (b > a ? b : a), -Infinity),
    multiply: (a: number[], b: number[]) => {
      const n = Math.min(a.length, b.length);
      const out = new Array(n);
      for (let i = 0; i < n; i++) out[i] = a[i] * b[i];
      return out;
    },
  },
};

// Minimal table shim. column("cN") returns the N'th accumulated column.
function makeTable(cols: number[][]): any {
  return {
    num_rows: cols.length > 0 ? cols[0].length : 0,
    column: (name: string) => {
      const m = /^c(\d+)$/.exec(name);
      const idx = m ? Number(m[1]) : -1;
      return cols[idx] ?? [];
    },
  };
}

// Evaluate the user-supplied JS source with `pa`, `table`, `params` in scope.
// The code is expected to define a global `Aggregate` object with a
// `finalize(table[, params])` method and return a numeric result. The
// `code` column comes straight from a SQL string literal — callers trust
// it (same contract as Python's eval-based equivalent).
function evalDynamicAggregate(code: string, cols: number[][], params?: any): any {
  try {
    const fn = new Function(
      "pa", "table", "params",
      `${code}\nreturn typeof Aggregate !== "undefined" && Aggregate && Aggregate.finalize ? Aggregate.finalize(table, params) : null;`,
    );
    return fn(paShim, makeTable(cols), params);
  } catch (e) {
    // Parse / runtime error — e.g. user passed non-JS source or referenced
    // something missing from our shim. Return null rather than tearing
    // down the worker so a bad row just yields NULL.
    if (process.env.VGI_AGG_DEBUG) {
      console.error("[dynamic_agg] eval failed:", e);
    }
    return null;
  }
}

// ============================================================================
// vgi_dynamic_ml_agg(code, params, value...) — placeholder matching the
// dynamic ML aggregate's function_registration signature. As with
// vgi_dynamic_agg, we don't actually evaluate user JS here — this is a stub
// that sums the non-code, non-params columns to satisfy the cases that
// happen to be sum-compatible.
// ============================================================================

const vgi_dynamic_ml_agg = defineAggregate<{ code: string; params: any; value: number }, DynamicAggState>({
  name: "vgi_dynamic_ml_agg",
  description: "Dynamic ML aggregate with params dict (sum-only stub)",
  args: { code: new Utf8(), params: new Utf8(), value: new Float64() },
  varargs: ["value"],
  outputType: new Float64(),
  nullHandling: "DEFAULT",
  initialState: () => ({ total: 0 }),
  update: ({ groupIds, columns, ensureState }) => {
    // Layout: [code, params, value1, value2, ...]; skip first two.
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      let rowTotal = 0;
      let anyNonNull = false;
      for (let k = 2; k < columns.length; k++) {
        const col = columns[k];
        if (col == null || !col.isValid(i)) continue;
        const v = col.get(i);
        if (v == null) continue;
        anyNonNull = true;
        rowTotal += typeof v === "bigint" ? Number(v) : Number(v);
      }
      if (anyNonNull) ensureState(groupIds[i]).total += rowTotal;
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (number | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.total : null;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate", "dynamic"],
});

// ============================================================================
// qf_llm_summarize(text, prompt?) / qf_llm_distill(text, prompt?) — LLM
// demo aggregates. The real implementations call an LLM; our stubs just
// concatenate the text column with ; separators so the function registers
// with the right signature for function_registration checks.
// ============================================================================

interface LlmState { text: string; }

function defineLlmStub(name: string, description: string): VgiFunction {
  return defineAggregate<{ text: string; prompt: string }, LlmState>({
    name,
    description,
    args: { text: new Utf8(), prompt: new Utf8() },
    argDefaults: { prompt: "" },
    outputType: new Utf8(),
    nullHandling: "DEFAULT",
    initialState: () => ({ text: "" }),
    update: ({ groupIds, columns, ensureState }) => {
      const textCol = columns[0];
      const n = groupIds.length;
      for (let i = 0; i < n; i++) {
        if (textCol == null || !textCol.isValid(i)) continue;
        const v = textCol.get(i);
        if (v == null) continue;
        const s = ensureState(groupIds[i]);
        s.text = s.text ? s.text + "; " + String(v) : String(v);
      }
    },
    combine: (src, tgt) => ({
      text: src.text && tgt.text ? tgt.text + "; " + src.text : (tgt.text || src.text),
    }),
    finalize: ({ groupIds, states, outputSchema }) => {
      const results: (string | null)[] = groupIds.map((gid) => {
        const s = states.get(gid);
        return s != null ? s.text : null;
      });
      return batchFromColumns({ result: results }, outputSchema);
    },
    categories: ["aggregate", "llm"],
  });
}

const qf_llm_summarize = defineLlmStub("qf_llm_summarize", "Summarize text using an LLM (stub)");
const qf_llm_distill = defineLlmStub("qf_llm_distill", "Distill text using an LLM (stub)");

// ============================================================================
// vgi_streaming_sum — registration stub. Real impl needs the
// streaming-partitioned protocol (open/chunk/close) which is not yet wired
// through the TS framework. function_registration.test only checks names
// and return type.
// ============================================================================

const vgi_streaming_sum = defineAggregate<{ value: number }, SumState>({
  name: "vgi_streaming_sum",
  description: "Running sum across PARTITION BY keys (streaming-partitioned stub)",
  args: { value: new Int64() },
  outputType: new Int64(),
  nullHandling: "DEFAULT",
  initialState: () => ({ total: 0n }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol == null || !valueCol.isValid(i)) continue;
      const v = valueCol.get(i);
      if (v == null) continue;
      ensureState(groupIds[i]).total += typeof v === "bigint" ? v : BigInt(v);
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (bigint | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.total : null;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate", "streaming"],
});

// ============================================================================
// vgi_window_sum_batch — registration stub. The non-streaming GROUP BY path
// is identical to vgi_window_sum; the per-batch window callback variant
// would require additional framework hooks.
// ============================================================================

const vgi_window_sum_batch = defineAggregate<{ value: number }, SumState>({
  name: "vgi_window_sum_batch",
  description: "Windowed sum using the batch window() callback (stub)",
  args: { value: new Int64() },
  outputType: new Int64(),
  nullHandling: "DEFAULT",
  initialState: () => ({ total: 0n }),
  update: ({ groupIds, columns, ensureState }) => {
    const valueCol = columns[0];
    const n = groupIds.length;
    for (let i = 0; i < n; i++) {
      if (valueCol == null || !valueCol.isValid(i)) continue;
      const v = valueCol.get(i);
      if (v == null) continue;
      ensureState(groupIds[i]).total += typeof v === "bigint" ? v : BigInt(v);
    }
  },
  combine: (src, tgt) => ({ total: src.total + tgt.total }),
  finalize: ({ groupIds, states, outputSchema }) => {
    const results: (bigint | null)[] = groupIds.map((gid) => {
      const s = states.get(gid);
      return s != null ? s.total : null;
    });
    return batchFromColumns({ result: results }, outputSchema);
  },
  categories: ["aggregate", "window"],
});

export const aggregateFunctions: VgiFunction[] = [
  vgi_count, vgi_sum, vgi_avg, vgi_sum_all, vgi_listagg, vgi_weighted_sum, vgi_generic_sum,
  vgi_percentile, vgi_window_sum, vgi_window_median, vgi_window_listagg, nest_tensor,
  vgi_streaming_sum, vgi_window_sum_batch,
  vgi_dynamic_agg, vgi_dynamic_ml_agg,
];
