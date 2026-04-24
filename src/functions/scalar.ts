// Scalar function implementation.
// Scalar functions receive columnar input and produce a single output column
// with 1:1 row mapping.

import {
  Schema,
  Field,
  RecordBatch,
  DataType,
  Null,
  Struct,
  makeData,
  vectorFromArray,
  Int64,
  Type,
} from "@query-farm/apache-arrow";
import type { OutputCollector, AuthContext } from "vgi-rpc";
import {
  FunctionType,
  FunctionStability,
  NullHandling,
  DEFAULT_MAX_WORKERS,
} from "../types.js";
import { RowCountMismatchError } from "../errors.js";
import type {
  BindRequest,
  BindResponse,
  InitRequest,
  GlobalInitResponse,
} from "../protocol/types.js";
import type {
  VgiFunction,
  FunctionMeta,
  StreamHandlers,
  FunctionExample,
} from "./types.js";
import type { ArgumentSpec } from "../arguments/argument-spec.js";
import { argumentSpecsToSchema } from "../arguments/argument-spec.js";
import { batchToScalarDict, batchToSecretDict, projectBatch, safeNumber } from "../util/arrow.js";

// ============================================================================
// Scalar Bind Parameters
// ============================================================================

export interface ScalarBindParameters {
  /** Constant argument values (resolved at bind time) */
  constArgs: Record<string, any>;
  /** Schema of all arguments including columnar params */
  argumentsSchema: Schema;
  /** Settings from DuckDB */
  settings: Record<string, any>;
  /** Secrets from DuckDB */
  secrets: Record<string, Record<string, any>>;
}

// ============================================================================
// Functional API
// ============================================================================

/** Ordered parameter definition for scalar functions. */
export interface ScalarParameterDef {
  name: string;
  type: DataType;
  /** If true, this is a constant parameter (scalar value resolved at bind time). */
  const?: boolean;
  /** If true, this parameter accepts variable number of arguments. */
  varargs?: boolean;
}

export interface ScalarFunctionConfig {
  name: string;
  description?: string;
  /** Columnar params (receive Arrow arrays at process time) */
  params?: Record<string, DataType>;
  /** Constant params (receive scalar values resolved at bind time) */
  constParams?: Record<string, DataType>;
  /**
   * Ordered parameter list. When provided, overrides `params` and `constParams`.
   * Use this when parameters need non-default ordering (e.g. const, param, const).
   */
  parameters?: ScalarParameterDef[];
  /** Output type (static) */
  returns?: DataType;
  /** Dynamic output type at bind time */
  outputType?: (params: ScalarBindParameters) => DataType;
  /** Process: receives columnar batch + const values, returns output array or values */
  compute: (
    batch: RecordBatch,
    consts: Record<string, any>,
    info: {
      settings: Record<string, any>;
      secrets: Record<string, Record<string, any>>;
      auth: AuthContext;
    }
  ) => any;
  // Metadata
  stability?: FunctionStability;
  nullHandling?: NullHandling;
  examples?: FunctionExample[];
  categories?: string[];
  tags?: Record<string, string>;
  maxWorkers?: number;
  requiredSettings?: string[];
  requiredSecrets?: string[];
}

const _decimalScratchBuf = new ArrayBuffer(8);
const _decimalScratchDv = new DataView(_decimalScratchBuf);

/**
 * Extract a Decimal column's numeric value for row `i`.
 *
 * The VGI DuckDB extension encodes Float64 literal arguments (e.g. `3.14`) as
 * 128-bit DECIMAL columns where the low 64 bits hold the IEEE 754 double bit
 * pattern and the upper 64 bits are zero. That's not a genuine scaled-integer
 * decimal, so the scale-aware path (`bn.valueOf(scale)`) produces a value that
 * overflows JS safe-integer range. Detect that case — the low 64 bits as a
 * bigint >2^53 — and reinterpret them as a Float64. Otherwise use the ordinary
 * scale path for honest Decimal wire values.
 */
function decimalCellToNumber(col: any, i: number, scale: number): number | null {
  const raw = col.get(i);
  if (raw === null || raw === undefined) return null;

  // Extract a bigint representation of the raw storage. BN.toString() emits the
  // integer decimal form (no scale applied), suitable for BigInt().
  let asBigint: bigint;
  if (typeof raw === "bigint") {
    asBigint = raw;
  } else if (typeof raw === "number") {
    asBigint = BigInt(Math.trunc(raw));
  } else if (typeof raw === "object" && typeof (raw as any).toString === "function") {
    try {
      asBigint = BigInt((raw as any).toString());
    } catch {
      return null;
    }
  } else {
    return null;
  }

  const low64 = BigInt.asIntN(64, asBigint);
  const MAX_SAFE = BigInt(Number.MAX_SAFE_INTEGER);
  if (low64 >= -MAX_SAFE && low64 <= MAX_SAFE && asBigint === low64) {
    // Genuine small integer Decimal; apply scale.
    const n = Number(low64);
    return scale === 0 ? n : n / Math.pow(10, scale);
  }
  // Reinterpret low 64 bits as IEEE 754 double (little-endian) — the Float64
  // literal-as-Decimal encoding path.
  _decimalScratchDv.setBigInt64(0, low64, true);
  return _decimalScratchDv.getFloat64(0, true);
}

/**
 * Cast each column of `batch` whose declared type differs from the wire type
 * into a JS-native representation matching the declared type. Rebuilds the
 * batch with a schema whose field types come from `declaredColumnTypes`. If
 * no casts are needed, returns the original batch unchanged.
 *
 * Currently handles: Decimal → Int/Float (reads via `decimalCellToNumber`).
 * Other same-family casts fall through to `vectorFromArray` which handles
 * int↔int and float↔float promotions losslessly.
 */
function castBatchColumns(
  batch: RecordBatch,
  declaredColumnTypes: DataType[],
): RecordBatch {
  if (declaredColumnTypes.length === 0 || batch.numRows === 0) return batch;

  let needsCast = false;
  const newFields: Field[] = [];
  const newChildren: any[] = [];

  for (let colIdx = 0; colIdx < batch.schema.fields.length; colIdx++) {
    const field = batch.schema.fields[colIdx];
    const declared = declaredColumnTypes[colIdx];
    const srcType = field.type;

    // Skip when we have no declared type (unexpected extra column) or Null-typed
    // declared (function accepts any) or types already match.
    if (
      !declared ||
      declared.typeId === Type.Null ||
      srcType.typeId === declared.typeId
    ) {
      newFields.push(field);
      newChildren.push(batch.data.children[colIdx]);
      continue;
    }

    // Currently only Decimal → numeric needs a custom cast path; everything
    // else goes through arrow-js's vectorFromArray with the raw values.
    const col = batch.getChildAt(colIdx);
    if (!col) {
      newFields.push(field);
      newChildren.push(batch.data.children[colIdx]);
      continue;
    }

    const values: any[] = [];
    if (srcType.typeId === Type.Decimal) {
      const scale = (srcType as any).scale as number;
      for (let i = 0; i < batch.numRows; i++) {
        values.push(decimalCellToNumber(col, i, scale));
      }
    } else {
      for (let i = 0; i < batch.numRows; i++) {
        const v = col.get(i);
        if (v === null || v === undefined) {
          values.push(null);
        } else if (typeof v === "bigint") {
          values.push(v);
        } else {
          values.push(v);
        }
      }
    }

    // Coerce to BigInt for 64-bit integer targets.
    let emitValues = values;
    if (DataType.isInt(declared) && (declared as any).bitWidth === 64) {
      emitValues = values.map((v: any) =>
        v == null ? null : typeof v === "bigint" ? v : BigInt(Math.trunc(v as number)),
      );
    }

    const arr = vectorFromArray(emitValues, declared);
    newFields.push(new Field(field.name, declared, field.nullable, field.metadata));
    newChildren.push(arr.data[0]);
    needsCast = true;
  }

  if (!needsCast) return batch;
  const newSchema = new Schema(newFields);
  const structType = new Struct(newFields);
  const data = makeData({
    type: structType,
    length: batch.numRows,
    children: newChildren,
    nullCount: batch.data.nullCount,
    nullBitmap: batch.data.nullBitmap,
  });
  return new RecordBatch(newSchema, data);
}

export function defineScalarFunction(config: ScalarFunctionConfig): VgiFunction {
  // Build argument specs
  const specs: ArgumentSpec[] = [];

  if (config.parameters) {
    // Ordered parameter list — supports arbitrary interleaving of const/non-const
    for (const [idx, p] of config.parameters.entries()) {
      const isAny = p.type instanceof Null;
      specs.push({
        name: p.name,
        position: idx,
        arrowType: isAny ? new Null() : p.type,
        isAnyType: isAny,
        isConst: p.const,
        isVarargs: p.varargs,
      });
    }
  } else {
    // Legacy: params first, then constParams
    let posIdx = 0;

    if (config.params) {
      for (const [name, type] of Object.entries(config.params)) {
        const isAny = type instanceof Null;
        specs.push({
          name,
          position: posIdx++,
          arrowType: isAny ? new Null() : type,
          isAnyType: isAny,
        });
      }
    }

    if (config.constParams) {
      for (const [name, type] of Object.entries(config.constParams)) {
        specs.push({
          name,
          position: posIdx++,
          arrowType: type,
          isConst: true,
        });
      }
    }
  }

  const meta: FunctionMeta = {
    name: config.name,
    description: config.description,
    stability: config.stability,
    nullHandling: config.nullHandling,
    examples: config.examples,
    categories: config.categories,
    tags: config.tags,
    maxWorkers: config.maxWorkers,
    requiredSettings: config.requiredSettings,
    requiredSecrets: config.requiredSecrets,
  };

  // Default output schema for catalog registration.
  // Static returns: use the declared type.
  // Dynamic outputType: use null with vgi:any metadata (matches Python convention).
  const defaultOutputSchema = config.returns
    ? new Schema([new Field("result", config.returns, true)])
    : new Schema([new Field("result", new Null(), true, new Map([["vgi:any", "true"]]))]);

  return {
    kind: "scalar",
    meta,
    argumentSpecs: specs,
    defaultOutputSchema,

    bind(request: BindRequest): BindResponse {
      const settings = batchToScalarDict(request.settings);
      const secrets = batchToSecretDict(request.secrets);

      // Extract constant args from the Arguments.
      // DuckDB only sends const arg values in Arguments (column args are in inputSchema).
      // Arguments are indexed sequentially (0, 1, ...) for const args only.
      const constArgs: Record<string, any> = {};
      {
        let constIdx = 0;
        for (const spec of specs) {
          if (spec.isConst) {
            constArgs[spec.name] = request.arguments.get(constIdx, undefined);
            constIdx++;
          }
        }
      }

      // Determine output type
      let outputType: DataType;
      if (config.outputType) {
        outputType = config.outputType({
          constArgs,
          argumentsSchema: request.inputSchema ?? new Schema([]),
          settings,
          secrets,
        });
      } else if (config.returns) {
        outputType = config.returns;
      } else {
        throw new Error(
          `Scalar function '${config.name}' must specify either 'returns' or 'outputType'`
        );
      }

      const outputSchema = new Schema([
        new Field("result", outputType, true),
      ]);

      return { outputSchema, opaqueData: null };
    },

    globalInit(request: InitRequest): GlobalInitResponse {
      const executionId = new Uint8Array(16);
      crypto.getRandomValues(executionId);
      return {
        maxWorkers: config.maxWorkers ?? DEFAULT_MAX_WORKERS,
        executionId,
        opaqueData: null,
      };
    },

    createStreamHandlers(
      request: InitRequest,
      response: GlobalInitResponse
    ): StreamHandlers {
      const outputSchema = request.outputSchema;
      const settings = batchToScalarDict(request.bindCall.settings);
      const secrets = batchToSecretDict(request.bindCall.secrets);

      // Extract const args (DuckDB only sends const values, indexed sequentially)
      const constArgs: Record<string, any> = {};
      let constIdx = 0;
      for (const spec of specs) {
        if (spec.isConst) {
          constArgs[spec.name] = request.bindCall.arguments.get(constIdx, undefined);
          constIdx++;
        }
      }

      const inputSchema = request.bindCall.inputSchema;

      // Build an ordered list of declared column types from the function's
      // non-const / non-table-input params, to cast incoming batches column-by-
      // column. DuckDB may send Decimal-backed values for numeric literals
      // (e.g. `3.14` → Decimal(3,2)) when the user function declared Float64;
      // cast at the framework boundary so compute() always sees values matching
      // the advertised signature.
      const declaredColumnTypes: DataType[] = specs
        .filter((s) => !s.isConst && !s.isTableInput)
        .map((s) => s.arrowType);

      return {
        outputSchema,
        inputSchema: inputSchema ?? undefined,
        exchangeInit: () => ({}),
        exchangeFn: (state: any, input: RecordBatch, out: OutputCollector) => {
          const castInput = castBatchColumns(input, declaredColumnTypes);
          const result = config.compute(castInput, constArgs, { settings, secrets, auth: out.auth });

          // Build output batch
          let outputBatch: RecordBatch;
          if (result instanceof RecordBatch) {
            outputBatch = result;
          } else if (Array.isArray(result)) {
            // Array of values -> single column
            // Coerce numbers to BigInt for Int64 output types
            const outputType = outputSchema.fields[0].type;
            const values = (DataType.isInt(outputType) && (outputType as any).bitWidth === 64)
              ? result.map((v: any) => v == null ? null : typeof v === "bigint" ? v : BigInt(v))
              : result;
            const arr = vectorFromArray(values, outputType);
            const structType = new Struct(outputSchema.fields);
            const data = makeData({
              type: structType,
              length: result.length,
              children: [arr.data[0]],
              nullCount: 0,
            });
            outputBatch = new RecordBatch(outputSchema, data);
          } else {
            // Assume it's a Vector/typed array
            const structType = new Struct(outputSchema.fields);
            const data = makeData({
              type: structType,
              length: result.length,
              children: [result.data[0]],
              nullCount: 0,
            });
            outputBatch = new RecordBatch(outputSchema, data);
          }

          // Validate row count
          if (outputBatch.numRows !== input.numRows) {
            throw new RowCountMismatchError(
              input.numRows,
              outputBatch.numRows
            );
          }

          out.emit(outputBatch);
        },
      };
    },
  };
}
