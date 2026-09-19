// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Arguments container for VGI function invocations.
// Holds positional and named scalar values from DuckDB.

import type { VgiSchema } from "../arrow/index.js";

export class Arguments {
  readonly positional: (any | null)[];
  readonly named: Map<string, any | null>;
  /** Schema of the arguments batch (preserves original Arrow types). */
  readonly argumentsSchema: VgiSchema | null;

  constructor(
    positional: (any | null)[] = [],
    named: Map<string, any | null> = new Map(),
    argumentsSchema: VgiSchema | null = null,
  ) {
    this.positional = positional;
    this.named = named;
    this.argumentsSchema = argumentsSchema;
  }

  /**
   * Get an argument by position (number) or name (string).
   */
  get(position: number | string, defaultValue?: any): any {
    if (typeof position === "number") {
      if (position < this.positional.length) {
        const val = this.positional[position];
        if (val === null || val === undefined) {
          return defaultValue !== undefined ? defaultValue : null;
        }
        return unwrapScalar(val);
      }
      if (defaultValue !== undefined) return defaultValue;
      throw new Error(`Positional argument ${position} not found`);
    }
    if (this.named.has(position)) {
      const val = this.named.get(position);
      if (val === null || val === undefined) {
        return defaultValue !== undefined ? defaultValue : null;
      }
      return unwrapScalar(val);
    }
    if (defaultValue !== undefined) return defaultValue;
    throw new Error(`Named argument '${position}' not found`);
  }

  get length(): number {
    return this.positional.length;
  }
}

/**
 * Unwrap an Arrow Scalar to a plain JS value.
 *
 * A `Date` is returned as-is. Arguments are decoded to their RICH values
 * (deserializeArguments runs every value through the codec), and a JS `Date`
 * is the rich value of date32/date64 — `valueOf()` would turn it into epoch
 * MILLISECONDS, while a bare number in a date32 column means DAYS. So a DATE
 * argument read through `get()` (every typed `params.args.x`, every scalar
 * const) and written back out as a DATE came back 86,400,000x too far out:
 * '2024-01-15' as 480510-12-09 on arrow-js, and an invalid Date on flechette.
 */
function unwrapScalar(val: any): any {
  if (val === null || val === undefined) return null;
  // If it's already a primitive, return it
  if (typeof val !== "object") return val;
  if (val instanceof Date) return val;
  // Arrow Scalar objects have a valueOf() method
  if (typeof val.valueOf === "function") {
    let v: any;
    try {
      v = val.valueOf();
    } catch {
      // valueOf() may throw for large Decimal values (e.g. HUGEINT)
      return val;
    }
    // BigInt -> Number for safe integers
    if (typeof v === "bigint") {
      if (v >= Number.MIN_SAFE_INTEGER && v <= Number.MAX_SAFE_INTEGER) {
        return Number(v);
      }
      return v;
    }
    return v;
  }
  return val;
}
