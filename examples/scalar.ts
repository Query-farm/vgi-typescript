// Example scalar function implementations.
// Ports all 13 scalar functions from vgi-python/vgi/examples/scalar.py.

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
  RecordBatch,
} from "@query-farm/apache-arrow";
import {
  defineScalarFunction,
  FunctionStability,
  NullHandling,
  type ScalarBindParameters,
  type ScalarParameterDef,
} from "../src/index.js";
import type { VgiFunction } from "../src/index.js";

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
    return dtype;
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
    return promoteForAddition(field.type);
  },
  compute: (batch: RecordBatch) => {
    const values = getColumnValues(batch, 0);
    return values.map((v: any) => {
      if (v === null || v === undefined) return null;
      if (typeof v === "bigint") return v * 2n;
      return v * 2;
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

const add_values = defineScalarFunction({
  name: "add_values",
  description: "Adds two numeric values",
  params: {
    col1: new Null(),
    col2: new Null(),
  },
  outputType: (params: ScalarBindParameters) => {
    const field1 = params.argumentsSchema.fields[0];
    const field2 = params.argumentsSchema.fields[1];
    // Find the wider type, then promote for overflow safety
    const commonType = widerNumericType(field1.type, field2.type);
    return promoteForAddition(commonType);
  },
  compute: (batch: RecordBatch) => {
    const col1 = getColumnValues(batch, 0);
    const col2 = getColumnValues(batch, 1);
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
  return_secret_value,
  hash_seed,
];
