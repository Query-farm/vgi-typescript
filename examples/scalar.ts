// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Example scalar function implementations.
// Ports all 22 scalar function groups from vgi-python/vgi/examples/scalar.py.

import {
  Schema,
  Field,
  Int8,
  Int16,
  Int32,
  Int64,
  Uint8,
  Uint16,
  Uint32,
  Uint64,
  Float32,
  Float64,
  Bool,
  Utf8,
  Binary,
  Null,
  DataType,
  Struct,
  List,
  FixedSizeList,
  Decimal,
  RecordBatch,
} from "@query-farm/apache-arrow";
import {
  defineScalarFunction,
  FunctionStability,
  NullHandling,
  type ScalarBindParameters,
  type ScalarParameterDef,
  ArgumentValidationError,
} from "../src/index.js";
import type { VgiFunction } from "../src/index.js";
// Typed facade factories + branded raw helpers (phase 3).
import {
  timestampMicros as f_timestampMicros,
  asTimestampMicros,
} from "../src/index.js";
import type { TimestampMicros } from "../src/index.js";

// ============================================================================
// Helper: type promotion for addition (matches Python _promote_for_addition)
// ============================================================================

function isAddableType(dtype: DataType): boolean {
  return (
    DataType.isInt(dtype) ||
    DataType.isFloat(dtype) ||
    DataType.isDecimal(dtype) ||
    DataType.isDate(dtype) ||
    DataType.isTime(dtype) ||
    DataType.isTimestamp(dtype)
  );
}

function promoteForAddition(dtype: DataType): DataType {
  if (DataType.isDate(dtype) || DataType.isTime(dtype) || DataType.isTimestamp(dtype)) {
    return dtype;
  }
  if (DataType.isFloat(dtype)) {
    // Float16/Float32 → Float64 to reduce overflow risk
    if (!(dtype instanceof Float64)) return new Float64();
    return dtype;
  }
  if (DataType.isInt(dtype)) {
    const intType = dtype as any;
    const bitWidth: number = intType.bitWidth;
    const isSigned: boolean = intType.isSigned;
    if (isSigned) {
      if (bitWidth === 8) return new Int16();
      if (bitWidth === 16) return new Int32();
      return new Int64();
    } else {
      if (bitWidth === 8) return new Uint16();
      if (bitWidth === 16) return new Uint32();
      return new Uint64();
    }
  }
  if (DataType.isDecimal(dtype)) {
    // Decimal gains one digit of precision (capped at 38), scale unchanged —
    // matches vgi-go/rust/python promote_for_addition. NOTE: arrow-js's Decimal
    // constructor is (scale, precision, bitWidth) — scale FIRST (the "A3"
    // arg-order gotcha). Read precision/scale via the properties and rebuild in
    // that order so .precision/.scale report correctly on both backends.
    const dec = dtype as Decimal;
    const newPrecision = Math.min(dec.precision + 1, 38);
    return new Decimal(dec.scale, newPrecision, dec.bitWidth);
  }
  throw new Error(`Unsupported numeric type for addition: ${dtype}`);
}

// ============================================================================
// Helper: find wider numeric type (for binary operations like add)
// ============================================================================

function numericTypeRank(dtype: DataType): number {
  if (DataType.isInt(dtype)) {
    const intType = dtype as any;
    const signed = intType.isSigned;
    const bw = intType.bitWidth;
    // Signed: 8→1, 16→2, 32→3, 64→4; Unsigned: 8→1, 16→2, 32→3, 64→4
    return bw / 8;
  }
  if (DataType.isFloat(dtype)) {
    // Float32 → 5, Float64 → 6
    return dtype instanceof Float64 ? 6 : 5;
  }
  return 0;
}

function widerNumericType(a: DataType, b: DataType): DataType {
  // If either is float, result is float (Float64)
  if (DataType.isFloat(a) || DataType.isFloat(b)) {
    return new Float64();
  }
  // Both are int — use the wider one
  const ra = numericTypeRank(a);
  const rb = numericTypeRank(b);
  return ra >= rb ? a : b;
}

// ============================================================================
// Helper: safe numeric operations on Arrow column values
// ============================================================================

function getColumnValues(batch: RecordBatch, colIndex: number): any[] {
  const col = batch.getChildAt(colIndex);
  if (!col) return [];
  const values: any[] = [];
  for (let i = 0; i < col.length; i++) {
    values.push(col.get(i));
  }
  return values;
}

function toNumber(val: any): number | null {
  if (val === null || val === undefined) return null;
  if (typeof val === "bigint") return Number(val);
  return val as number;
}

// ============================================================================
// 1. multiply - Param(int64) + ConstParam(int64), multiply values
// ============================================================================

const multiply = defineScalarFunction({
  name: "multiply",
  description: "Multiplies a value by a constant factor",
  params: { value: new Int64() },
  constParams: { factor: new Int64() },
  returns: new Int64(),
  compute: (batch: RecordBatch, consts: Record<string, any>) => {
    const factor = typeof consts.factor === "bigint" ? consts.factor : BigInt(consts.factor);
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => {
      if (v === null || v === undefined) return null;
      const bigV = typeof v === "bigint" ? v : BigInt(v);
      return bigV * factor;
    });
  },
  examples: [
    { sql: "SELECT multiply(price, 2) FROM products", description: "Double all prices" },
    { sql: "SELECT multiply(quantity, 10) FROM inventory", description: "Scale quantities by 10" },
  ],
});

// ============================================================================
// 2. conditional_message - Multiple ConstParams (int, str) + Param(bool)
// ============================================================================

const conditional_message = defineScalarFunction({
  name: "conditional_message",
  description: "Returns repeated message when condition is true",
  parameters: [
    { name: "repeat_count", type: new Int64(), const: true },
    { name: "message", type: new Utf8(), const: true },
    { name: "condition", type: new Bool() },
  ],
  returns: new Utf8(),
  compute: (batch: RecordBatch, consts: Record<string, any>) => {
    const repeatCount = typeof consts.repeat_count === "bigint"
      ? Number(consts.repeat_count)
      : (consts.repeat_count as number);
    const message = consts.message as string;
    const repeatedMessage = message.repeat(repeatCount);
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => {
      if (v === null || v === undefined) return null;
      return v ? repeatedMessage : "";
    });
  },
  examples: [
    { sql: "SELECT conditional_message(3, 'Alert! ', flag) FROM items", description: "Show alert message for flagged items" },
  ],
});

// ============================================================================
// 3. binary_packet - Binary/struct ConstParams
// ============================================================================

const _configStructType = new Struct([
  new Field("label", new Utf8(), true),
  new Field("version", new Int64(), true),
]);

const binary_packet = defineScalarFunction({
  name: "binary_packet",
  description: "Build binary packets with header, payload, and config",
  parameters: [
    { name: "header", type: new Binary(), const: true },
    { name: "payload", type: new Binary() },
    { name: "config", type: _configStructType, const: true },
  ],
  returns: new Binary(),
  compute: (batch: RecordBatch, consts: Record<string, any>) => {
    const header: Uint8Array = consts.header instanceof Uint8Array
      ? consts.header
      : new Uint8Array(consts.header);

    const config = consts.config;
    const label: string = config.label ?? config.get?.("label") ?? "";
    const version: number = typeof config.version === "bigint"
      ? Number(config.version)
      : (config.get?.("version") ?? config.version ?? 0);

    // Build suffix: label bytes + version as single byte
    const encoder = new TextEncoder();
    const labelBytes = encoder.encode(label);
    const suffix = new Uint8Array(labelBytes.length + 1);
    suffix.set(labelBytes);
    suffix[labelBytes.length] = version & 0xff;

    const values = getColumnValues(batch, 0);
    return values.map((v: any) => {
      const payloadBytes: Uint8Array = v instanceof Uint8Array
        ? v
        : (v ? new Uint8Array(v) : new Uint8Array(0));

      const result = new Uint8Array(header.length + payloadBytes.length + suffix.length);
      result.set(header);
      result.set(payloadBytes, header.length);
      result.set(suffix, header.length + payloadBytes.length);
      return result;
    });
  },
  examples: [
    { sql: "SELECT binary_packet(x'FF', payload, {'tag': 'msg', 1}) FROM t", description: "Build packets with 0xFF header" },
  ],
});

// ============================================================================
// 4. double - AnyArrow (any numeric type), doubles values
// ============================================================================

const double_fn = defineScalarFunction({
  name: "double",
  description: "Doubles numeric values",
  params: { value: new Null() },
  outputType: (params: ScalarBindParameters) => {
    const field = params.argumentsSchema.fields[0];
    const t = field.type;
    // Reject temporal types up front — pc.multiply has no kernel for them.
    // Match Python's _is_multipliable_type message so callers see the same
    // diagnostic regardless of language.
    if (
      DataType.isDate(t) || DataType.isTime(t) || DataType.isTimestamp(t) ||
      DataType.isDuration(t) || DataType.isInterval(t)
    ) {
      throw new ArgumentValidationError(
        `double: _is_multipliable_type rejected ${t}`
      );
    }
    return promoteForAddition(t);
  },
  compute: (batch: RecordBatch, _consts: any, ctx: any) => {
    const inputType = batch.schema.fields[0]?.type;
    // Decimal128 max value at the declared precision. We multiply by 2 below;
    // detect overflow up front so the user sees "does not fit in precision N"
    // instead of a silent wrap-around in the unscaled int.
    const decimalPrecision = inputType && DataType.isDecimal(inputType)
      ? Number((inputType as any).precision ?? 38)
      : null;
    const decimalCap = decimalPrecision != null
      ? 10n ** BigInt(decimalPrecision) - 1n
      : null;
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => {
      if (v === null || v === undefined) return null;
      // For huge Decimal values (out of JS Number range), arithmetic via
      // valueOf() throws "not safe to convert". Try a BigInt path first
      // for any value typed-array-shaped, then fall back to number.
      if (typeof v === "bigint" || (v && typeof v === "object" && "subarray" in v)) {
        let big: bigint;
        if (typeof v === "bigint") {
          big = v;
        } else {
          // DecimalBigNum is little-endian Uint32Array of 4 (Decimal128) or
          // 8 (Decimal256) words. Reassemble as BigInt.
          big = 0n;
          const u32 = v as Uint32Array;
          for (let j = u32.length - 1; j >= 0; j--) big = (big << 32n) | BigInt(u32[j] >>> 0);
        }
        const out = big * 2n;
        if (decimalCap != null && (out > decimalCap || out < -decimalCap)) {
          throw new Error(`double: result does not fit in precision ${decimalPrecision}`);
        }
        return out;
      }
      const out = v * 2;
      if (decimalCap != null) {
        const big = BigInt(Math.trunc(out));
        if (big > decimalCap || big < -decimalCap) {
          throw new Error(`double: result does not fit in precision ${decimalPrecision}`);
        }
      }
      return out;
    });
  },
  examples: [
    { sql: "SELECT double(price) FROM products", description: "Double the price values" },
    { sql: "SELECT double(quantity) FROM inventory", description: "Double inventory quantities" },
  ],
});

// ============================================================================
// 5. add_values - Two AnyArrow params, type promotion
// ============================================================================

// Read a decimal cell as its UNSCALED integer (bigint), regardless of how the
// backend surfaces it: bigint, JS number, decimal string (e.g. "1.50"), or a
// little-endian Uint32Array (DecimalBigNum). `scale` is the column's scale.
function decimalCellToUnscaled(v: any, scale: number): bigint | null {
  if (v === null || v === undefined) return null;
  if (typeof v === "bigint") return v;
  if (typeof v === "object" && "subarray" in v) {
    let big = 0n;
    const u32 = v as Uint32Array;
    for (let j = u32.length - 1; j >= 0; j--) big = (big << 32n) | BigInt(u32[j] >>> 0);
    return big;
  }
  const s = typeof v === "number" ? v.toString() : String(v);
  // A decimal string carries the decimal point; scale it to the unscaled int.
  if (s.includes(".")) {
    const neg = s.startsWith("-");
    const [intPart, fracPartRaw] = (neg ? s.slice(1) : s).split(".");
    const fracPart = (fracPartRaw + "0".repeat(scale)).slice(0, scale);
    const big = BigInt(intPart + fracPart);
    return neg ? -big : big;
  }
  // No decimal point: already an unscaled integer (or an integer-valued number).
  return BigInt(s);
}

const add_values = defineScalarFunction({
  name: "add_values",
  description: "Adds two numeric values",
  params: {
    col1: new Null(),
    col2: new Null(),
  },
  outputType: (params: ScalarBindParameters) => {
    const t1 = params.argumentsSchema.fields[0].type;
    const t2 = params.argumentsSchema.fields[1].type;
    // Decimal branch: merge precision/scale, then promote for overflow headroom.
    // Result scale = max(s1, s2); result precision grows to hold the sum (+1).
    // The test asserts only the value, not the exact type, so any decimal type
    // that holds 3.750 is acceptable.
    if (DataType.isDecimal(t1) && DataType.isDecimal(t2)) {
      const d1 = t1 as Decimal;
      const d2 = t2 as Decimal;
      const scale = Math.max(d1.scale, d2.scale);
      const intDigits = Math.max(d1.precision - d1.scale, d2.precision - d2.scale);
      const precision = Math.min(intDigits + scale + 1, 38);
      return new Decimal(scale, precision, d1.bitWidth);
    }
    // Find the wider type, then promote for overflow safety
    const commonType = widerNumericType(t1, t2);
    return promoteForAddition(commonType);
  },
  compute: (batch: RecordBatch) => {
    const t1 = batch.schema.fields[0]?.type;
    const t2 = batch.schema.fields[1]?.type;
    const col1 = getColumnValues(batch, 0);
    const col2 = getColumnValues(batch, 1);

    // Decimal branch: rescale both unscaled integers to the common output scale
    // and add, returning the unscaled bigint the decimal codec expects.
    if (t1 && t2 && DataType.isDecimal(t1) && DataType.isDecimal(t2)) {
      const s1 = (t1 as Decimal).scale;
      const s2 = (t2 as Decimal).scale;
      const outScale = Math.max(s1, s2);
      return col1.map((v1: any, i: number) => {
        const u1 = decimalCellToUnscaled(v1, s1);
        const u2 = decimalCellToUnscaled(col2[i], s2);
        if (u1 === null || u2 === null) return null;
        const a1 = u1 * 10n ** BigInt(outScale - s1);
        const a2 = u2 * 10n ** BigInt(outScale - s2);
        return a1 + a2;
      });
    }

    return col1.map((v1: any, i: number) => {
      const v2 = col2[i];
      if (v1 === null || v1 === undefined || v2 === null || v2 === undefined) return null;
      if (typeof v1 === "bigint" || typeof v2 === "bigint") {
        const b1 = typeof v1 === "bigint" ? v1 : BigInt(v1);
        const b2 = typeof v2 === "bigint" ? v2 : BigInt(v2);
        return b1 + b2;
      }
      return v1 + v2;
    });
  },
  examples: [
    { sql: "SELECT add_values(price, tax) FROM orders", description: "Calculate total by adding price and tax" },
    { sql: "SELECT add_values(quantity, reserved) FROM inventory", description: "Sum quantity and reserved amounts" },
  ],
});

// ============================================================================
// 6. upper_case - String uppercase
// ============================================================================

const upper_case = defineScalarFunction({
  name: "upper_case",
  description: "Converts string values to uppercase",
  params: { value: new Utf8() },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => {
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => {
      if (v === null || v === undefined) return null;
      return String(v).toUpperCase();
    });
  },
  examples: [
    { sql: "SELECT upper_case(name) FROM users", description: "Convert user names to uppercase" },
    { sql: "SELECT upper_case(status) FROM orders WHERE id = 1", description: "Uppercase the status field" },
  ],
});

// ============================================================================
// 7. sum_values - Varargs with type_bound
// ============================================================================

const sum_values = defineScalarFunction({
  name: "sum_values",
  description: "Sum multiple numeric values",
  parameters: [
    { name: "values", type: new Null(), varargs: true },
  ],
  outputType: (params: ScalarBindParameters) => {
    if (!params.argumentsSchema || params.argumentsSchema.fields.length === 0) {
      throw new ArgumentValidationError("sum_values requires at least 1 value");
    }
    const firstType = params.argumentsSchema.fields[0].type;
    return promoteForAddition(firstType);
  },
  compute: (batch: RecordBatch) => {
    const numCols = batch.schema.fields.length;
    if (numCols === 0) return [];

    const numRows = batch.numRows;
    const result: any[] = [];

    for (let row = 0; row < numRows; row++) {
      let sum: any = null;
      let hasNull = false;
      for (let col = 0; col < numCols; col++) {
        const child = batch.getChildAt(col);
        const v = child ? child.get(row) : null;
        if (v === null || v === undefined) {
          hasNull = true;
          break;
        }
        if (sum === null) {
          sum = v;
        } else {
          if (typeof sum === "bigint" || typeof v === "bigint") {
            const b1 = typeof sum === "bigint" ? sum : BigInt(sum);
            const b2 = typeof v === "bigint" ? v : BigInt(v);
            sum = b1 + b2;
          } else {
            sum = sum + v;
          }
        }
      }
      result.push(hasNull ? null : sum);
    }
    return result;
  },
  examples: [
    { sql: "SELECT sum_values(price, tax, shipping) FROM orders", description: "Calculate total cost from multiple values" },
  ],
});

// ============================================================================
// 8. null_handling - NullHandling.SPECIAL
// ============================================================================

const null_handling = defineScalarFunction({
  name: "null_handling",
  description: "Returns value or -5000 if null",
  params: { value: new Int64() },
  returns: new Int64(),
  nullHandling: NullHandling.SPECIAL,
  compute: (batch: RecordBatch) => {
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => {
      if (v === null || v === undefined) return BigInt(-5000);
      return v;
    });
  },
  examples: [
    { sql: "SELECT null_handling(value) FROM data", description: "Replace null values with -5000" },
  ],
});

// ============================================================================
// 9. random_int - VOLATILE
// ============================================================================

const random_int = defineScalarFunction({
  name: "random_int",
  description: "Generate random integers (demonstrates VOLATILE stability)",
  params: {
    min_val: new Int64(),
    max_val: new Int64(),
  },
  returns: new Int64(),
  stability: FunctionStability.VOLATILE,
  compute: (batch: RecordBatch) => {
    const minValues = getColumnValues(batch, 0);
    const maxValues = getColumnValues(batch, 1);
    return minValues.map((minV: any, i: number) => {
      const maxV = maxValues[i];
      if (minV === null || maxV === null) return null;
      const min = typeof minV === "bigint" ? Number(minV) : minV;
      const max = typeof maxV === "bigint" ? Number(maxV) : maxV;
      return BigInt(Math.floor(Math.random() * (max - min + 1)) + min);
    });
  },
  examples: [
    { sql: "SELECT random_int(min_col, max_col) FROM data", description: "Generate random integers between min and max values" },
  ],
});

// ============================================================================
// 10. bernoulli - No input params, uses batch numRows
// ============================================================================

const bernoulli = defineScalarFunction({
  name: "bernoulli",
  description: "Generate random booleans (demonstrates VOLATILE stability)",
  returns: new Bool(),
  stability: FunctionStability.VOLATILE,
  compute: (batch: RecordBatch) => {
    const result: boolean[] = [];
    for (let i = 0; i < batch.numRows; i++) {
      result.push(Math.random() < 0.5);
    }
    return result;
  },
  examples: [
    { sql: "SELECT bernoulli() FROM data", description: "Generate samples from the bernoulli distribution" },
  ],
});

// ============================================================================
// 11. random_bytes - ConstParams + batch numRows
// ============================================================================

const random_bytes = defineScalarFunction({
  name: "random_bytes",
  description: "Generate pseudo-random binary blobs from seed and length",
  constParams: {
    seed: new Int64(),
    byte_length: new Int64(),
  },
  returns: new Binary(),
  stability: FunctionStability.CONSISTENT,
  compute: (batch: RecordBatch, consts: Record<string, any>) => {
    const seed = typeof consts.seed === "bigint" ? Number(consts.seed) : (consts.seed as number);
    const byteLength = typeof consts.byte_length === "bigint" ? Number(consts.byte_length) : (consts.byte_length as number);

    if (byteLength < 0) {
      throw new Error("byte_length must be >= 0");
    }

    // Simple seeded PRNG (mulberry32)
    function mulberry32(a: number) {
      return function () {
        let t = (a += 0x6d2b79f5);
        t = Math.imul(t ^ (t >>> 15), t | 1);
        t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
      };
    }

    const rng = mulberry32(seed);
    const result: Uint8Array[] = [];
    for (let i = 0; i < batch.numRows; i++) {
      const bytes = new Uint8Array(byteLength);
      for (let j = 0; j < byteLength; j++) {
        bytes[j] = Math.floor(rng() * 256);
      }
      result.push(bytes);
    }
    return result;
  },
  examples: [
    { sql: "SELECT random_bytes(42, 16) FROM data", description: "Generate a deterministic 16-byte blob per input row" },
  ],
});

// ============================================================================
// 12. multiply_by_setting - Uses settings from DuckDB
// ============================================================================

const multiply_by_setting = defineScalarFunction({
  name: "multiply_by_setting",
  description: "Multiply the input value by a setting value",
  params: { value: new Int64() },
  returns: new Int64(),
  requiredSettings: ["multiplier"],
  compute: (
    batch: RecordBatch,
    _consts: Record<string, any>,
    info: { settings: Record<string, any>; secrets: Record<string, Record<string, any>> }
  ) => {
    const multiplierRaw = info.settings.multiplier;
    const multiplier = typeof multiplierRaw === "bigint"
      ? multiplierRaw
      : BigInt(multiplierRaw ?? 1);

    const values = getColumnValues(batch, 0);
    return values.map((v: any) => {
      if (v === null || v === undefined) return null;
      const bigV = typeof v === "bigint" ? v : BigInt(v);
      return bigV * multiplier;
    });
  },
  examples: [
    { sql: "SELECT multiply_by_setting(5)", description: "Multiply the input value by a setting's value" },
  ],
});

// ============================================================================
// 12b. scale_by_setting - float (DOUBLE) Setting() read via get_f64
// value (DOUBLE) -> DOUBLE; multiplies by the `scale_factor` setting (default 1.0).
// Ports vgi-go ScaleBySettingFunction / vgi-rust scale_by_setting / vgi-python.
// ============================================================================

const scale_by_setting = defineScalarFunction({
  name: "scale_by_setting",
  description: "Scale the input value by the float setting `scale_factor`",
  params: { value: new Float64() },
  returns: new Float64(),
  requiredSettings: ["scale_factor"],
  compute: (
    batch: RecordBatch,
    _consts: Record<string, any>,
    info: { settings: Record<string, any>; secrets: Record<string, Record<string, any>> }
  ) => {
    const raw = info.settings.scale_factor;
    const scale = raw === null || raw === undefined ? 1.0 : Number(raw);
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => {
      if (v === null || v === undefined) return null;
      return Number(v) * scale;
    });
  },
  examples: [
    { sql: "SELECT scale_by_setting(4.0)", description: "Scale a value by the scale_factor setting" },
  ],
});

// ============================================================================
// 13. return_secret_value - Uses secrets from DuckDB
// ============================================================================

const return_secret_value = defineScalarFunction({
  name: "return_secret_value",
  description: "Return a secret's value",
  returns: new Utf8(),
  requiredSecrets: ["vgi_example"],
  compute: (
    batch: RecordBatch,
    _consts: Record<string, any>,
    info: { settings: Record<string, any>; secrets: Record<string, Record<string, any>> }
  ) => {
    const secretDict = info.secrets.vgi_example ?? {};
    // Convert values to plain JS for JSON serialization
    const plainDict: Record<string, any> = {};
    for (const [k, v] of Object.entries(secretDict)) {
      if (typeof v === "bigint") {
        plainDict[k] = Number(v);
      } else if (v && typeof v === "object" && typeof v.valueOf === "function") {
        plainDict[k] = v.valueOf();
      } else {
        plainDict[k] = v;
      }
    }
    const jsonStr = JSON.stringify(plainDict);
    const result: string[] = [];
    for (let i = 0; i < batch.numRows; i++) {
      result.push(jsonStr);
    }
    return result;
  },
  examples: [
    { sql: "SELECT return_secret_value()", description: "Return a secret's value" },
  ],
});

// ============================================================================
// 13b. secret_field - Secret() parameter, looks up individual fields by name.
// Builds "port=<port>;name=<secret_string>": `port` is read by NAMED lookup on
// the vgi_example secret; `secret_string` (rendered as `name`) is read from the
// first secret carrying a field of that name. Missing → empty string.
// Ports vgi-go SecretFieldFunction / vgi-rust secret_field / vgi-python.
// ============================================================================

function renderSecretValue(val: any): string {
  if (val === null || val === undefined) return "";
  if (typeof val === "bigint") return val.toString();
  if (typeof val === "number") {
    // int32/port renders without a decimal point.
    return Number.isInteger(val) ? String(val) : String(val);
  }
  if (typeof val === "object" && typeof val.valueOf === "function") {
    const v = val.valueOf();
    if (typeof v === "bigint") return v.toString();
    return String(v);
  }
  return String(val);
}

const secret_field = defineScalarFunction({
  name: "secret_field",
  description: "Look up secret fields by name",
  returns: new Utf8(),
  requiredSecrets: ["vgi_example"],
  compute: (
    batch: RecordBatch,
    _consts: Record<string, any>,
    info: { settings: Record<string, any>; secrets: Record<string, Record<string, any>> }
  ) => {
    // Named lookup: `port` field on the vgi_example secret specifically.
    const named = info.secrets.vgi_example ?? {};
    const port = renderSecretValue(named.port);
    // Any-secret lookup: first secret carrying a `secret_string` field.
    let name = "";
    for (const secretDict of Object.values(info.secrets ?? {})) {
      if (secretDict && "secret_string" in secretDict && secretDict.secret_string != null) {
        name = renderSecretValue(secretDict.secret_string);
        break;
      }
    }
    const s = `port=${port};name=${name}`;
    const result: string[] = [];
    for (let i = 0; i < batch.numRows; i++) result.push(s);
    return result;
  },
  examples: [
    { sql: "SELECT secret_field()", description: "Look up named/positional secret fields" },
  ],
});

// ============================================================================
// 14. hash_seed - Deterministic integers from a constant seed
// ============================================================================

const hash_seed = defineScalarFunction({
  name: "hash_seed",
  description: "Generate deterministic integers from a constant seed",
  constParams: { seed: new Int64() },
  returns: new Int64(),
  stability: FunctionStability.CONSISTENT,
  compute: (batch: RecordBatch, consts: Record<string, any>) => {
    const seed = typeof consts.seed === "bigint" ? Number(consts.seed) : (consts.seed as number);
    const result: bigint[] = [];
    for (let i = 0; i < batch.numRows; i++) {
      result.push(BigInt(seed + i));
    }
    return result;
  },
  examples: [
    { sql: "SELECT hash_seed(42) FROM data", description: "Generate deterministic integers seeded at 42" },
  ],
});

// ============================================================================
// 15. format_number (3 overloads by ConstParam count)
// ============================================================================

const format_number_default = defineScalarFunction({
  name: "format_number",
  description: "Format number with default precision (0 decimals)",
  params: { value: new Float64() },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => {
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => {
      if (v === null || v === undefined) return null;
      return Number(v).toFixed(0);
    });
  },
});

const format_number_precision = defineScalarFunction({
  name: "format_number",
  description: "Format number with specified precision",
  parameters: [
    { name: "precision", type: new Int64(), const: true },
    { name: "value", type: new Float64() },
  ],
  returns: new Utf8(),
  compute: (batch: RecordBatch, consts: Record<string, any>) => {
    const precision = Math.max(0, Math.min(100, typeof consts.precision === "bigint"
      ? Number(consts.precision)
      : (consts.precision as number)));
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => {
      if (v === null || v === undefined) return null;
      return Number(v).toFixed(precision);
    });
  },
});

const format_number_full = defineScalarFunction({
  name: "format_number",
  description: "Format number with precision and prefix",
  parameters: [
    { name: "precision", type: new Int64(), const: true },
    { name: "prefix", type: new Utf8(), const: true },
    { name: "value", type: new Float64() },
  ],
  returns: new Utf8(),
  compute: (batch: RecordBatch, consts: Record<string, any>) => {
    const precision = Math.max(0, Math.min(100, typeof consts.precision === "bigint"
      ? Number(consts.precision)
      : (consts.precision as number)));
    const prefix = consts.prefix as string;
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => {
      if (v === null || v === undefined) return null;
      return prefix + Number(v).toFixed(precision);
    });
  },
});

// ============================================================================
// 16. type_info (5 overloads by column type)
// ============================================================================

const type_info_int32 = defineScalarFunction({
  name: "type_info",
  description: "Returns type name for int32 values",
  params: { v: new Int32() },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => {
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => v === null || v === undefined ? null : "int32");
  },
});

const type_info_int64 = defineScalarFunction({
  name: "type_info",
  description: "Returns type name for int64 values",
  params: { v: new Int64() },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => {
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => v === null || v === undefined ? null : "int64");
  },
});

const type_info_uint32 = defineScalarFunction({
  name: "type_info",
  description: "Returns type name for uint32 values",
  params: { v: new Uint32() },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => {
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => v === null || v === undefined ? null : "uint32");
  },
});

const type_info_uint64 = defineScalarFunction({
  name: "type_info",
  description: "Returns type name for uint64 values",
  params: { v: new Uint64() },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => {
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => v === null || v === undefined ? null : "uint64");
  },
});

const type_info_string = defineScalarFunction({
  name: "type_info",
  description: "Returns type name for string values",
  params: { v: new Utf8() },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => {
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => v === null || v === undefined ? null : "varchar");
  },
});

// ============================================================================
// 17. smart_format (2 overloads by ConstParam type)
// ============================================================================

function formatFloat(v: number): string {
  const s = String(v);
  // Ensure at least one decimal digit (like Python's str(float))
  if (!s.includes(".") && !s.includes("e") && !s.includes("E")) {
    return s + ".0";
  }
  return s;
}

const smart_format_width = defineScalarFunction({
  name: "smart_format",
  description: "Right-align a number in a field of given width",
  parameters: [
    { name: "width", type: new Int64(), const: true },
    { name: "value", type: new Float64() },
  ],
  returns: new Utf8(),
  compute: (batch: RecordBatch, consts: Record<string, any>) => {
    const width = typeof consts.width === "bigint"
      ? Number(consts.width)
      : (consts.width as number);
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => {
      if (v === null || v === undefined) return null;
      return formatFloat(Number(v)).padStart(width);
    });
  },
});

const smart_format_prefix = defineScalarFunction({
  name: "smart_format",
  description: "Prepend a prefix string to a formatted number",
  parameters: [
    { name: "prefix", type: new Utf8(), const: true },
    { name: "value", type: new Float64() },
  ],
  returns: new Utf8(),
  compute: (batch: RecordBatch, consts: Record<string, any>) => {
    const prefix = consts.prefix as string;
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => {
      if (v === null || v === undefined) return null;
      return prefix + formatFloat(Number(v));
    });
  },
});

// ============================================================================
// 18. pair_type (3 overloads by column types)
// ============================================================================

const pair_type_int_int = defineScalarFunction({
  name: "pair_type",
  description: "Returns pair type for two int64 columns",
  params: { a: new Int64(), b: new Int64() },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => {
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => v === null || v === undefined ? null : "int+int");
  },
});

const pair_type_str_str = defineScalarFunction({
  name: "pair_type",
  description: "Returns pair type for two string columns",
  params: { a: new Utf8(), b: new Utf8() },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => {
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => v === null || v === undefined ? null : "str+str");
  },
});

const pair_type_int_str = defineScalarFunction({
  name: "pair_type",
  description: "Returns pair type for int64 + string columns",
  params: { a: new Int64(), b: new Utf8() },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => {
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => v === null || v === undefined ? null : "int+str");
  },
});

// ============================================================================
// 19. concat_values (2 overloads by varargs type)
// ============================================================================

const concat_values_int = defineScalarFunction({
  name: "concat_values",
  description: "Sum integer varargs and return as string",
  parameters: [
    { name: "values", type: new Int64(), varargs: true },
  ],
  returns: new Utf8(),
  compute: (batch: RecordBatch) => {
    const numCols = batch.schema.fields.length;
    const numRows = batch.numRows;
    const result: (string | null)[] = [];
    for (let row = 0; row < numRows; row++) {
      let sum = 0n;
      let hasNull = false;
      for (let col = 0; col < numCols; col++) {
        const child = batch.getChildAt(col);
        const v = child ? child.get(row) : null;
        if (v === null || v === undefined) { hasNull = true; break; }
        sum += typeof v === "bigint" ? v : BigInt(v);
      }
      result.push(hasNull ? null : String(sum));
    }
    return result;
  },
});

const concat_values_str = defineScalarFunction({
  name: "concat_values",
  description: "Concatenate string varargs",
  parameters: [
    { name: "values", type: new Utf8(), varargs: true },
  ],
  returns: new Utf8(),
  compute: (batch: RecordBatch) => {
    const numCols = batch.schema.fields.length;
    const numRows = batch.numRows;
    const result: (string | null)[] = [];
    for (let row = 0; row < numRows; row++) {
      const parts: string[] = [];
      let hasNull = false;
      for (let col = 0; col < numCols; col++) {
        const child = batch.getChildAt(col);
        const v = child ? child.get(row) : null;
        if (v === null || v === undefined) { hasNull = true; break; }
        parts.push(String(v));
      }
      result.push(hasNull ? null : parts.join(""));
    }
    return result;
  },
});

// ============================================================================
// 20. any_mixed (2 overloads)
// ============================================================================

const any_mixed_int = defineScalarFunction({
  name: "any_mixed",
  description: "Any + int64 parameter pair",
  params: { a: new Null(), b: new Int64() },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => {
    const bValues = getColumnValues(batch, 1);
    return bValues.map((v: any) => {
      if (v === null || v === undefined) return null;
      return `any+int: ${typeof v === "bigint" ? Number(v) : v}`;
    });
  },
});

const any_mixed_str = defineScalarFunction({
  name: "any_mixed",
  description: "Any + string parameter pair",
  params: { a: new Null(), b: new Utf8() },
  returns: new Utf8(),
  compute: (batch: RecordBatch) => {
    const bValues = getColumnValues(batch, 1);
    return bValues.map((v: any) => {
      if (v === null || v === undefined) return null;
      return `any+str: ${v}`;
    });
  },
});

// ============================================================================
// 21. geo_distance (3 distinct-named functions)
// ============================================================================

const GEO_STRUCT_TYPE = new Struct([
  new Field("lat", new Float64(), true),
  new Field("lon", new Float64(), true),
]);

function euclideanDistance(lat1: number, lon1: number, lat2: number, lon2: number): number {
  return Math.sqrt((lat2 - lat1) ** 2 + (lon2 - lon1) ** 2);
}

function extractStructLatLon(v: any): { lat: number; lon: number } | null {
  if (v === null || v === undefined) return null;
  const lat = v.lat ?? v.get?.("lat") ?? (typeof v.toJSON === "function" ? v.toJSON().lat : null);
  const lon = v.lon ?? v.get?.("lon") ?? (typeof v.toJSON === "function" ? v.toJSON().lon : null);
  if (lat == null || lon == null) return null;
  return { lat: Number(lat), lon: Number(lon) };
}

function extractListLatLon(v: any): { lat: number; lon: number } | null {
  if (v === null || v === undefined) return null;
  const arr = Array.isArray(v) ? v : [...v];
  if (arr.length < 2) return null;
  return { lat: Number(arr[0]), lon: Number(arr[1]) };
}

const geo_distance_struct = defineScalarFunction({
  name: "geo_distance_struct",
  description: "Euclidean distance between two struct points",
  params: { p1: GEO_STRUCT_TYPE, p2: GEO_STRUCT_TYPE },
  returns: new Float64(),
  compute: (batch: RecordBatch) => {
    const p1s = getColumnValues(batch, 0);
    const p2s = getColumnValues(batch, 1);
    return p1s.map((p1: any, i: number) => {
      const a = extractStructLatLon(p1);
      const b = extractStructLatLon(p2s[i]);
      if (!a || !b) return null;
      return euclideanDistance(a.lat, a.lon, b.lat, b.lon);
    });
  },
});

const GEO_LIST_TYPE = new List(new Field("item", new Float64(), true));

const geo_distance_list = defineScalarFunction({
  name: "geo_distance_list",
  description: "Euclidean distance between two list points",
  params: { p1: GEO_LIST_TYPE, p2: GEO_LIST_TYPE },
  returns: new Float64(),
  compute: (batch: RecordBatch) => {
    const p1s = getColumnValues(batch, 0);
    const p2s = getColumnValues(batch, 1);
    return p1s.map((p1: any, i: number) => {
      const a = extractListLatLon(p1);
      const b = extractListLatLon(p2s[i]);
      if (!a || !b) return null;
      return euclideanDistance(a.lat, a.lon, b.lat, b.lon);
    });
  },
});

const GEO_FIXED_LIST_TYPE = new FixedSizeList(2, new Field("item", new Float64(), true));

const geo_distance_fixed = defineScalarFunction({
  name: "geo_distance_fixed",
  description: "Euclidean distance between two fixed-size list points",
  params: { p1: GEO_FIXED_LIST_TYPE, p2: GEO_FIXED_LIST_TYPE },
  returns: new Float64(),
  compute: (batch: RecordBatch) => {
    const p1s = getColumnValues(batch, 0);
    const p2s = getColumnValues(batch, 1);
    return p1s.map((p1: any, i: number) => {
      const a = extractListLatLon(p1);
      const b = extractListLatLon(p2s[i]);
      if (!a || !b) return null;
      return euclideanDistance(a.lat, a.lon, b.lat, b.lon);
    });
  },
});

// ============================================================================
// 22. geo_centroid (3 distinct-named functions)
// ============================================================================

function computeCentroid(lats: number[], lons: number[]): { lat: number; lon: number } {
  const n = lats.length;
  const avgLat = lats.reduce((a, b) => a + b, 0) / n;
  const avgLon = lons.reduce((a, b) => a + b, 0) / n;
  return { lat: avgLat, lon: avgLon };
}

const geo_centroid_struct = defineScalarFunction({
  name: "geo_centroid_struct",
  description: "Centroid of N struct points",
  parameters: [
    { name: "points", type: GEO_STRUCT_TYPE, varargs: true },
  ],
  returns: GEO_STRUCT_TYPE,
  compute: (batch: RecordBatch) => {
    const numCols = batch.schema.fields.length;
    const numRows = batch.numRows;
    const result: ({ lat: number; lon: number } | null)[] = [];
    for (let row = 0; row < numRows; row++) {
      const lats: number[] = [];
      const lons: number[] = [];
      let hasNull = false;
      for (let col = 0; col < numCols; col++) {
        const child = batch.getChildAt(col);
        const v = child ? child.get(row) : null;
        const pt = extractStructLatLon(v);
        if (!pt) { hasNull = true; break; }
        lats.push(pt.lat);
        lons.push(pt.lon);
      }
      result.push(hasNull ? null : computeCentroid(lats, lons));
    }
    return result;
  },
});

const geo_centroid_list = defineScalarFunction({
  name: "geo_centroid_list",
  description: "Centroid of N list points",
  parameters: [
    { name: "points", type: GEO_LIST_TYPE, varargs: true },
  ],
  returns: GEO_STRUCT_TYPE,
  compute: (batch: RecordBatch) => {
    const numCols = batch.schema.fields.length;
    const numRows = batch.numRows;
    const result: ({ lat: number; lon: number } | null)[] = [];
    for (let row = 0; row < numRows; row++) {
      const lats: number[] = [];
      const lons: number[] = [];
      let hasNull = false;
      for (let col = 0; col < numCols; col++) {
        const child = batch.getChildAt(col);
        const v = child ? child.get(row) : null;
        const pt = extractListLatLon(v);
        if (!pt) { hasNull = true; break; }
        lats.push(pt.lat);
        lons.push(pt.lon);
      }
      result.push(hasNull ? null : computeCentroid(lats, lons));
    }
    return result;
  },
});

const geo_centroid_fixed = defineScalarFunction({
  name: "geo_centroid_fixed",
  description: "Centroid of N fixed-size list points",
  parameters: [
    { name: "points", type: GEO_FIXED_LIST_TYPE, varargs: true },
  ],
  returns: GEO_STRUCT_TYPE,
  compute: (batch: RecordBatch) => {
    const numCols = batch.schema.fields.length;
    const numRows = batch.numRows;
    const result: ({ lat: number; lon: number } | null)[] = [];
    for (let row = 0; row < numRows; row++) {
      const lats: number[] = [];
      const lons: number[] = [];
      let hasNull = false;
      for (let col = 0; col < numCols; col++) {
        const child = batch.getChildAt(col);
        const v = child ? child.get(row) : null;
        const pt = extractListLatLon(v);
        if (!pt) { hasNull = true; break; }
        lats.push(pt.lat);
        lons.push(pt.lon);
      }
      result.push(hasNull ? null : computeCentroid(lats, lons));
    }
    return result;
  },
});

// ============================================================================
// 23. whoami — Returns the authenticated principal name
// ============================================================================

const whoami = defineScalarFunction({
  name: "whoami",
  description: "Return the authenticated principal name",
  params: { x: new Int64() },
  returns: new Utf8(),
  compute: (batch, _consts, info) => {
    const name = info.auth.principal ?? "anonymous";
    return Array.from({ length: batch.numRows }, () => name);
  },
  categories: ["auth", "testing"],
});

// ============================================================================
// unnest_tensor — inverse of nest_tensor aggregate. Takes a {tensor, axes}
// struct and returns a list of {value, axes} structs — one per cell of the
// Cartesian product, including unfilled cells (value is NULL there).
// ============================================================================

class UnnestTensorError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnnestTensorError";
  }
}

function _tensorDepth(t: DataType): { depth: number; inner: DataType } {
  let depth = 0;
  let inner = t;
  while (DataType.isList(inner) || DataType.isFixedSizeList(inner)) {
    depth += 1;
    inner = (inner as any).children[0].type;
  }
  return { depth, inner };
}

function _cartesianProduct(lengths: number[]): number[][] {
  if (lengths.length === 0) return [[]];
  const result: number[][] = [];
  const rec = (acc: number[], depth: number) => {
    if (depth === lengths.length) {
      result.push([...acc]);
      return;
    }
    for (let i = 0; i < lengths[depth]; i++) {
      acc.push(i);
      rec(acc, depth + 1);
      acc.pop();
    }
  };
  rec([], 0);
  return result;
}

function _unnestTensorOutputType(structType: DataType): DataType {
  if (!DataType.isStruct(structType)) {
    throw new UnnestTensorError(`unnest_tensor: argument must be a struct, got ${structType}`);
  }
  const children = (structType as any).children as Field[];
  const fieldNames = new Set(children.map((f) => f.name));
  if (!fieldNames.has("tensor") || !fieldNames.has("axes")) {
    throw new UnnestTensorError(
      `unnest_tensor: struct must have 'tensor' and 'axes' fields, got [${[...fieldNames].sort().join(", ")}]`,
    );
  }
  const axesField = children.find((f) => f.name === "axes")!;
  if (!DataType.isStruct(axesField.type)) {
    throw new UnnestTensorError(`unnest_tensor: 'axes' field must be a struct, got ${axesField.type}`);
  }
  const axesChildren = (axesField.type as any).children as Field[];
  const tensorField = children.find((f) => f.name === "tensor")!;
  const { depth, inner } = _tensorDepth(tensorField.type);
  if (depth !== axesChildren.length) {
    throw new UnnestTensorError(
      `unnest_tensor: tensor nesting depth ${depth} does not match number of axes ${axesChildren.length}`,
    );
  }
  const outAxesType = new Struct(
    axesChildren.map((f) => new Field(f.name, (f.type as any).children[0].type, true)),
  );
  const rowType = new Struct([new Field("value", inner, true), new Field("axes", outAxesType, true)]);
  return new List(new Field("item", rowType, true));
}

function _asPlainAxes(axesScalar: any, axisNames: string[]): Record<string, any[]> {
  // arrow-js StructRow for axes — fields are List<coord_type> each.
  const out: Record<string, any[]> = {};
  for (const name of axisNames) {
    let arr: any;
    if (axesScalar && typeof axesScalar === "object") {
      arr = axesScalar[name] ?? axesScalar.get?.(name);
    }
    if (arr == null) { out[name] = []; continue; }
    // arr is a Vector-like with .get(i) or an iterable
    if (typeof arr[Symbol.iterator] === "function") {
      out[name] = [...arr];
    } else {
      out[name] = [];
    }
  }
  return out;
}

function _indexNested(t: any, idx: number[]): any {
  let cell = t;
  for (const d of idx) {
    if (cell == null) return null;
    cell = cell[d] ?? cell.get?.(d) ?? null;
  }
  return cell;
}

const unnest_tensor = defineScalarFunction({
  name: "unnest_tensor",
  description: "Invert nest_tensor: list of {value, axes} structs per cell",
  params: { tensor: new Null() },
  outputType: (params: ScalarBindParameters) => {
    const structType = params.argumentsSchema.fields[0].type;
    return _unnestTensorOutputType(structType);
  },
  compute: (batch: RecordBatch) => {
    const col = batch.getChildAt(0);
    if (col == null) return new Array(batch.numRows).fill(null);
    const structType = (col as any).type as DataType;
    const axesField = ((structType as any).children as Field[]).find((f) => f.name === "axes")!;
    const axisNames = ((axesField.type as any).children as Field[]).map((f) => f.name);

    const results: Array<any[] | null> = [];
    for (let i = 0; i < batch.numRows; i++) {
      if (!col.isValid(i)) { results.push(null); continue; }
      const row = col.get(i);
      if (row == null) { results.push(null); continue; }
      const tensorVal = (row as any).tensor ?? (row as any).get?.("tensor");
      const axesVal = (row as any).axes ?? (row as any).get?.("axes");
      const axesPlain = _asPlainAxes(axesVal, axisNames);
      const lengths = axisNames.map((n) => axesPlain[n].length);
      if (lengths.some((l) => l === 0)) { results.push([]); continue; }

      const cells: any[] = [];
      for (const idxTuple of _cartesianProduct(lengths)) {
        const value = _indexNested(tensorVal, idxTuple);
        const axes: Record<string, any> = {};
        for (let a = 0; a < axisNames.length; a++) {
          axes[axisNames[a]] = axesPlain[axisNames[a]][idxTuple[a]];
        }
        cells.push({ value, axes });
      }
      results.push(cells);
    }
    return results;
  },
  examples: [
    { sql: "SELECT * FROM UNNEST(unnest_tensor(nest_tensor(v, {i: i}))) FROM (VALUES (1,0),(2,1)) t(v,i)", description: "Round-trip nest_tensor" },
  ],
});

// ============================================================================
// 24. shift_timestamp_us — phase-3 RAW MODE end-to-end demo.
//
// Uses the typed facade factory `timestampMicros()` and `repr: 'raw'`, so the
// compute() output is statically typed as a BRANDED `TimestampMicros` (raw us).
// Returning a plain bigint or a Date here would be a COMPILE error. The runtime
// builds the output column through the raw<->canonical codec path.
// ============================================================================

const ONE_HOUR_US = 3_600_000_000n;

const shift_timestamp_us = defineScalarFunction({
  name: "shift_timestamp_us",
  description: "Shift a timestamp[us] forward by one hour (raw-mode branded demo)",
  params: { ts: f_timestampMicros() },
  returns: f_timestampMicros(),
  repr: "raw",
  compute: (batch) => {
    const col = batch.getChildAt(0);
    const out: (TimestampMicros | null)[] = [];
    for (let i = 0; i < batch.numRows; i++) {
      const v = col ? col.get(i) : null;
      if (v === null || v === undefined) { out.push(null); continue; }
      const raw = typeof v === "bigint" ? v : BigInt(v as any);
      // asTimestampMicros validates + brands; the static return type enforces it.
      out.push(asTimestampMicros(raw + ONE_HOUR_US));
    }
    return out;
  },
  examples: [
    { sql: "SELECT shift_timestamp_us(ts) FROM events", description: "Add one hour to each timestamp" },
  ],
});

// Demonstrate (compile-time only) that the wrong representation is rejected.
// Uncommenting the body below would be a COMPILE error because raw mode expects
// a branded TimestampMicros, not a plain bigint:
//   compute: () => [42n],   // ts-error: bigint not assignable to TimestampMicros

// ============================================================================
// Export all scalar functions
// ============================================================================

export const scalarFunctions: VgiFunction[] = [
  multiply,
  conditional_message,
  binary_packet,
  double_fn,
  add_values,
  upper_case,
  sum_values,
  null_handling,
  random_int,
  bernoulli,
  random_bytes,
  multiply_by_setting,
  scale_by_setting,
  return_secret_value,
  secret_field,
  hash_seed,
  format_number_default,
  format_number_precision,
  format_number_full,
  type_info_int32,
  type_info_int64,
  type_info_uint32,
  type_info_uint64,
  type_info_string,
  smart_format_width,
  smart_format_prefix,
  pair_type_int_int,
  pair_type_str_str,
  pair_type_int_str,
  concat_values_int,
  concat_values_str,
  any_mixed_int,
  any_mixed_str,
  geo_distance_struct,
  geo_distance_list,
  geo_distance_fixed,
  geo_centroid_struct,
  geo_centroid_list,
  geo_centroid_fixed,
  whoami,
  unnest_tensor,
];

// `shift_timestamp_us` (above) demonstrates the phase-3 typed `repr: 'raw'`
// author API end-to-end. It is intentionally NOT added to `scalarFunctions`:
// the example worker's registered inventory is asserted byte-for-byte by the
// C++ extension's `function_registration` golden test, and the raw-mode build +
// read round-trip is already covered on both backends by
// src/arrow/__tests__/raw-mode.test.ts. Reference it so it is still
// type-checked / tree-shake-retained as a usage example.
export const rawModeScalarExample: VgiFunction = shift_timestamp_us;
