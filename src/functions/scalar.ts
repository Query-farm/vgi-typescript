// Scalar function implementation.
// Scalar functions receive columnar input and produce a single output column
// with 1:1 row mapping.

import {
  type VgiSchema,
  type VgiField,
  type VgiBatch,
  type VgiDataType,
  schema as makeSchema,
  field,
  nullType,
  isNull,
  isBatch,
} from "../arrow/index.js";
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
import { batchToScalarDict, batchToSecretDict, batchFromColumns, projectBatch, safeNumber } from "../util/arrow/index.js";

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
}

export interface ScalarFunctionConfig {
  name: string;
  description?: string;
  /** Columnar params (receive Arrow arrays at process time) */
  params?: Record<string, VgiDataType>;
  /** Constant params (receive scalar values resolved at bind time) */
  constParams?: Record<string, VgiDataType>;
  /**
   * Ordered parameter list. When provided, overrides `params` and `constParams`.
   * Use this when parameters need non-default ordering (e.g. const, param, const).
   */
  parameters?: ScalarParameterDef[];
  /** Output type (static) */
  returns?: VgiDataType;
  /** Dynamic output type at bind time */
  outputType?: (params: ScalarBindParameters) => VgiDataType | Promise<VgiDataType>;
  /** Process: receives columnar batch + const values, returns output array or values */
  compute: (
    batch: VgiBatch,
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

export function defineScalarFunction(config: ScalarFunctionConfig): VgiFunction {
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
            constArgs[spec.name] = request.arguments.get(constIdx, undefined);
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
            // List, Map, Struct) are built via our buildColumnData.
            const fieldName = outputSchema.fields[0].name;
            outputBatch = batchFromColumns({ [fieldName]: result }, outputSchema);
          } else {
            // User returned an Arrow Vector / typed-array-shaped column. Iterate
            // it into a JS array and re-route through batchFromColumns. Adds an
            // O(n) copy but matches both backends' contract.
            const fieldName = outputSchema.fields[0].name;
            outputBatch = batchFromColumns({ [fieldName]: [...(result as Iterable<any>)] }, outputSchema);
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
