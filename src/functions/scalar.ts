// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Scalar function implementation.
// Scalar functions receive columnar input and produce a single output column
// with 1:1 row mapping.

import {
  type VgiSchema,
  type VgiField,
  type VgiBatch,
  type VgiDataType,
  type ValueFor,
  type Repr,
  schema as makeSchema,
  field,
  nullType,
  isNull,
  isBatch,
} from "../arrow/index.js";
import type { OutputCollector, AuthContext } from "@query-farm/vgi-rpc";
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
import {
  argumentSpecsToSchema,
  constraintSpecFields,
  validateConstConstraints,
  type ArgumentConstraints,
} from "../arguments/argument-spec.js";
import { batchToScalarDict, batchToSecretDict, batchFromColumns, projectBatch, safeNumber } from "../util/arrow/index.js";
import { cacheControlMetadata, type CacheControl } from "../cache-control.js";

// ============================================================================
// Scalar Bind Parameters
// ============================================================================

export interface ScalarBindParameters {
  /** Constant argument values (resolved at bind time) */
  constArgs: Record<string, any>;
  /** Schema of all arguments including columnar params */
  argumentsSchema: VgiSchema;
  /** Settings from DuckDB */
  settings: Record<string, any>;
  /** Secrets from DuckDB */
  secrets: Record<string, Record<string, any>>;
  /** Original bind request — exposes attach_opaque_data, transaction_opaque_data, function_type, etc. */
  bindCall: BindRequest;
}

// ============================================================================
// Functional API
// ============================================================================

/** Ordered parameter definition for scalar functions. */
export interface ScalarParameterDef {
  name: string;
  type: VgiDataType;
  /** If true, this is a constant parameter (scalar value resolved at bind time). */
  const?: boolean;
  /** If true, this parameter accepts variable number of arguments. */
  varargs?: boolean;
  /** Human-readable per-argument description (surfaced as `vgi_doc`). */
  doc?: string;
  // Discovery-facing validation constraints (surfaced via
  // `vgi_function_arguments()`). All optional; see `ArgumentConstraints`.
  /** Closed set of allowed values (surfaced as `vgi_choices`). */
  choices?: readonly unknown[];
  /** Inclusive lower bound, `>=` (folded into `vgi_range`). */
  ge?: number;
  /** Inclusive upper bound, `<=` (folded into `vgi_range`). */
  le?: number;
  /** Exclusive lower bound, `>` (folded into `vgi_range`). */
  gt?: number;
  /** Exclusive upper bound, `<` (folded into `vgi_range`). */
  lt?: number;
  /** Regex the value must match (surfaced as `vgi_pattern`). */
  pattern?: string;
  /** Default value (surfaced as `vgi_default`, JSON-encoded). */
  default?: unknown;
}

/**
 * The typed compute row for a scalar function: one property per declared
 * columnar param, valued in the representation (`rich` / `raw`) selected by the
 * function's `repr`. (Provided as a convenience type for authors who build rows
 * out of the input batch.)
 */
export type ScalarComputeRow<
  P extends Record<string, VgiDataType>,
  M extends Repr,
> = { [K in keyof P]: ValueFor<P[K], M> | null };

/** A single output value for a scalar function under representation `M`. */
export type ScalarOutputValue<R extends VgiDataType, M extends Repr> =
  ValueFor<R, M> | null;

/**
 * Accepted compute return shapes: an array (or iterable) of output values in
 * the declared representation, or a pre-built VgiBatch. The representation is
 * enforced statically — returning a `Date` under `repr: 'raw'` (which expects a
 * branded `Date32`/`Date64Ms`), or a branded value under `repr: 'rich'` (which
 * expects a `Date`), is a COMPILE error.
 */
export type ScalarComputeResult<R extends VgiDataType, M extends Repr> =
  | Array<ScalarOutputValue<R, M>>
  | Iterable<ScalarOutputValue<R, M>>
  | VgiBatch;

export interface ScalarFunctionConfig<
  P extends Record<string, VgiDataType> = Record<string, VgiDataType>,
  R extends VgiDataType = VgiDataType,
  M extends Repr = "rich",
> {
  name: string;
  description?: string;
  /** Columnar params (receive Arrow arrays at process time) */
  params?: P;
  /** Constant params (receive scalar values resolved at bind time) */
  constParams?: Record<string, VgiDataType>;
  /**
   * Per-argument descriptions keyed by param name, surfaced as `vgi_doc` field
   * metadata (and via the extension's `vgi_function_arguments()`). Applies to
   * `params` and `constParams`; for the ordered `parameters` list use each
   * entry's `doc` field instead.
   */
  argDocs?: Record<string, string>;
  /**
   * Ordered parameter list. When provided, overrides `params` and `constParams`.
   * Use this when parameters need non-default ordering (e.g. const, param, const).
   */
  parameters?: ScalarParameterDef[];
  /** Output type (static) */
  returns?: R;
  /** Dynamic output type at bind time */
  outputType?: (params: ScalarBindParameters) => VgiDataType | Promise<VgiDataType>;
  /**
   * Value representation for compute I/O. `'rich'` (default) uses JS `Date` for
   * date32/date64 and plain number/bigint elsewhere. `'raw'` uses the branded
   * unit-carrying aliases (Date32, TimestampMicros, UnscaledDecimal, …). The
   * choice flows into compute()'s statically-checked return type and selects
   * the runtime converter used to build the output column.
   */
  repr?: M;
  /**
   * Process: receives the columnar input batch + const values, returns the
   * output column as an array/iterable of values (statically typed from
   * `returns` and `repr`) or a pre-built VgiBatch.
   */
  compute: (
    batch: VgiBatch,
    consts: Record<string, any>,
    info: {
      settings: Record<string, any>;
      secrets: Record<string, Record<string, any>>;
      auth: AuthContext;
    }
    // NoInfer pins R/M from `returns`/`repr` so the compute return type is
    // CHECKED against them rather than widening them to fit a wrong value.
  ) => ScalarComputeResult<NoInfer<R>, NoInfer<M>>;
  // Metadata
  stability?: FunctionStability;
  nullHandling?: NullHandling;
  examples?: FunctionExample[];
  categories?: string[];
  tags?: Record<string, string>;
  maxWorkers?: number;
  requiredSettings?: string[];
  requiredSecrets?: string[];
  /**
   * Result-cache opt-in: when set, this CacheControl's `vgi.cache.*` metadata
   * rides every output batch's custom metadata (per-batch — NOT the schema,
   * which the IPC stream fixes at open), so the extension can memoize the
   * scalar's output per distinct input value. A pure, deterministic scalar
   * only — advertising this on a non-pure scalar serves stale rows. Mirrors
   * vgi-python's `ScalarFunction.CACHE_CONTROL`.
   */
  cacheControl?: CacheControl;
}

export function defineScalarFunction<
  P extends Record<string, VgiDataType> = Record<string, VgiDataType>,
  R extends VgiDataType = VgiDataType,
  M extends Repr = "rich",
>(config: ScalarFunctionConfig<P, R, M>): VgiFunction {
  const repr: Repr = config.repr ?? "rich";
  // Build argument specs
  const specs: ArgumentSpec[] = [];

  if (config.parameters) {
    // Ordered parameter list — supports arbitrary interleaving of const/non-const
    for (const [idx, p] of config.parameters.entries()) {
      const isAny = isNull(p.type);
      specs.push({
        name: p.name,
        position: idx,
        arrowType: isAny ? nullType() : p.type,
        isAnyType: isAny,
        isConst: p.const,
        isVarargs: p.varargs,
        doc: p.doc,
        ...constraintSpecFields(p),
      });
    }
  } else {
    // Legacy: params first, then constParams
    let posIdx = 0;

    if (config.params) {
      for (const [name, type] of Object.entries(config.params)) {
        const isAny = isNull(type);
        specs.push({
          name,
          position: posIdx++,
          arrowType: isAny ? nullType() : type,
          isAnyType: isAny,
          doc: config.argDocs?.[name],
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
          doc: config.argDocs?.[name],
        });
      }
    }
  }

  // Raw const-arg constraints for bind-time enforcement, keyed by arg name.
  // Only the ordered `parameters` API carries constraints (the legacy
  // params/constParams path does not), so only its const params appear here.
  const constConstraints = new Map<string, ArgumentConstraints>();
  if (config.parameters) {
    for (const p of config.parameters) {
      if (
        p.const &&
        (p.choices !== undefined ||
          p.ge !== undefined ||
          p.le !== undefined ||
          p.gt !== undefined ||
          p.lt !== undefined ||
          p.pattern !== undefined)
      ) {
        constConstraints.set(p.name, p);
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
    ? makeSchema([field("result", config.returns, true)])
    : makeSchema([field("result", nullType(), true, new Map([["vgi:any", "true"]]))]);

  return {
    kind: "scalar",
    meta,
    argumentSpecs: specs,
    defaultOutputSchema,

    async bind(request: BindRequest): Promise<BindResponse> {
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
            const value = request.arguments.get(constIdx, undefined);
            constArgs[spec.name] = value;
            // Enforce declared const-arg constraints at bind (choices/range/pattern).
            const constraints = constConstraints.get(spec.name);
            if (constraints) validateConstConstraints(spec.name, constraints, value);
            constIdx++;
          }
        }
      }

      // Determine output type
      let outputType: VgiDataType;
      if (config.outputType) {
        outputType = await config.outputType({
          constArgs,
          argumentsSchema: request.input_schema ?? makeSchema([]),
          settings,
          secrets,
          bindCall: request,
        });
      } else if (config.returns) {
        outputType = config.returns;
      } else {
        throw new Error(
          `Scalar function '${config.name}' must specify either 'returns' or 'outputType'`
        );
      }

      const outputSchema = makeSchema([
        field("result", outputType, true),
      ]);

      return { output_schema: outputSchema, opaque_data: null };
    },

    globalInit(request: InitRequest): GlobalInitResponse {
      const executionId = new Uint8Array(16);
      crypto.getRandomValues(executionId);
      return {
        max_workers: config.maxWorkers ?? DEFAULT_MAX_WORKERS,
        execution_id: executionId,
        opaque_data: null,
      };
    },

    createStreamHandlers(
      request: InitRequest,
      response: GlobalInitResponse
    ): StreamHandlers {
      const outputSchema = request.output_schema;
      const settings = batchToScalarDict(request.bind_call.settings);
      const secrets = batchToSecretDict(request.bind_call.secrets);

      // Extract const args (DuckDB only sends const values, indexed sequentially)
      const constArgs: Record<string, any> = {};
      let constIdx = 0;
      for (const spec of specs) {
        if (spec.isConst) {
          constArgs[spec.name] = request.bind_call.arguments.get(constIdx, undefined);
          constIdx++;
        }
      }

      const inputSchema = request.bind_call.input_schema;

      return {
        outputSchema,
        inputSchema: inputSchema ?? undefined,
        exchangeInit: () => ({}),
        exchangeFn: (state: any, input: VgiBatch, out: OutputCollector) => {
          const result = config.compute(input as any, constArgs, { settings, secrets, auth: out.auth });

          // Build output batch
          let outputBatch: VgiBatch;
          if (isBatch(result)) {
            outputBatch = result as VgiBatch;
          } else if (Array.isArray(result)) {
            // Array of values -> single column. Route through batchFromColumns
            // so complex types (Decimal, BigInt-backed Timestamp/Duration,
            // List, Map, Struct) are built via our buildColumnData. The `repr`
            // selects raw<->canonical (branded) vs rich<->canonical conversion.
            const fieldName = outputSchema.fields[0].name;
            outputBatch = batchFromColumns({ [fieldName]: result }, outputSchema, repr);
          } else {
            // User returned an Arrow Vector / typed-array-shaped column. Iterate
            // it into a JS array and re-route through batchFromColumns. Adds an
            // O(n) copy but matches both backends' contract.
            const fieldName = outputSchema.fields[0].name;
            outputBatch = batchFromColumns({ [fieldName]: [...(result as Iterable<any>)] }, outputSchema, repr);
          }

          // Validate row count
          if (outputBatch.numRows !== input.numRows) {
            throw new RowCountMismatchError(
              input.numRows,
              outputBatch.numRows
            );
          }

          // Result-cache opt-in: a scalar declaring cacheControl rides its
          // vgi.cache.* keys on the emit path's per-batch custom metadata so
          // the extension can memoize the output per distinct input value.
          out.emit(
            outputBatch,
            config.cacheControl ? cacheControlMetadata(config.cacheControl) : undefined,
          );
        },
      };
    },
  };
}
