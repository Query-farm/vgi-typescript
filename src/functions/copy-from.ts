// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Custom `COPY ... FROM` format reader.
//
// A CopyFromFunction lets a VGI catalog act as a remote file-format reader: the
// user runs `COPY target FROM 'path' (FORMAT <name>, opt val, ...)` and the
// worker parses the source and streams Arrow batches that DuckDB inserts into
// the local `target` table. FROM only (no COPY TO).
//
// Mechanically this is an ordinary producer-mode table function (it reuses the
// table-function bind/init/scan path on both sides). What makes it a COPY format
// is twofold:
//   * it sets `meta.copyFromFormat` to the SQL FORMAT identifier, and
//   * the catalog advertises it via `copyFromFormats()` (the
//     `catalog_copy_from_formats` RPC), so the VGI DuckDB extension registers a
//     DuckDB CopyFunction for it.
//
// The COPY statement's file path and the target table's schema arrive on the
// bind through `BindRequest.copy_from` (CopyFromContext). The COPY options
// arrive as the function's normal named arguments. Mirrors vgi-python's
// `vgi.copy_from_function.CopyFromFunction`.

import { type VgiSchema, type VgiDataType } from "../arrow/index.js";
import type { OutputCollector } from "@query-farm/vgi-rpc";
import { DEFAULT_MAX_WORKERS } from "../types.js";
import type {
  BindRequest,
  BindResponse,
  InitRequest,
  GlobalInitResponse,
} from "../protocol/types.js";
import type { VgiFunction, FunctionMeta, StreamHandlers, FunctionExample } from "./types.js";
import type { ArgumentSpec } from "../arguments/argument-spec.js";
import { batchToScalarDict, batchToSecretDict, safeNumber } from "../util/arrow/index.js";
import { BoundStorage, storage as globalStorage } from "./storage.js";
import type { TableProcessParams } from "./table.js";

/** Declaration of a single COPY option (a named function argument). */
export interface CopyFromOption {
  /** Arrow type of the option value (e.g. `utf8()`, `int64()`). */
  type: VgiDataType;
  /** Per-option description, surfaced by `vgi_copy_formats()`. */
  doc?: string;
  /**
   * Default value. When omitted the option is REQUIRED — the worker throws a
   * clear error at COPY bind if the user does not supply it. Mirrors
   * vgi-python's `Arg(..., default=...)` (no default => required).
   */
  default?: unknown;
  /**
   * Optional allowed value set, validated worker-side at bind. Mirrors
   * vgi-python's `Arg(..., choices=[...])`.
   */
  choices?: unknown[];
}

export interface CopyFromReadParams<TArgs = Record<string, unknown>> {
  /** Source path from the `COPY ... FROM 'path'` statement. */
  path: string;
  /** Parsed COPY options (defaults applied). */
  options: TArgs;
  /**
   * The COPY target's schema. Every emitted batch must have this exact schema
   * (names + types, in order) — DuckDB inserts no cast before the INSERT.
   */
  expectedSchema: VgiSchema;
  /** Full process parameters (settings, secrets, storage). */
  processParams: TableProcessParams<TArgs>;
  /** Collector to emit batches / log. `finish()` is called for you. */
  out: OutputCollector;
}

export interface CopyFromFunctionConfig<TArgs = Record<string, unknown>> {
  /** Handler name (the function's registered name; `Meta.name`). */
  name: string;
  /** SQL `FORMAT` identifier users type, e.g. `COPY t FROM 'x' (FORMAT myfmt)`. */
  format: string;
  /** Function description (intrinsic documentation; `Meta.description`). */
  description?: string;
  /** Optional free-text comment surfaced by `vgi_copy_formats()`. */
  comment?: string | null;
  /** Reserved for a future `COPY ... TO`; only `"from"` is supported today. */
  direction?: string;
  /** COPY options, keyed by option name. The `file_path` is NOT an option. */
  options?: Record<string, CopyFromOption>;
  categories?: string[];
  tags?: Record<string, string>;
  examples?: FunctionExample[];
  requiredSettings?: string[];
  requiredSecrets?: string[];
  /** Parse `path` and emit Arrow batches matching `expectedSchema`. */
  read: (params: CopyFromReadParams<TArgs>) => void | Promise<void>;
}

/**
 * Define a custom `COPY ... FROM` format reader, returned as a `VgiFunction`
 * (kind `"table"`) carrying the `copyFromFormat` metadata marker. Register it in
 * the catalog's function list like any table function; the catalog's
 * `copyFromFormats()` introspection picks it up and advertises the format.
 */
export function defineCopyFromFunction<TArgs = Record<string, unknown>>(
  config: CopyFromFunctionConfig<TArgs>,
): VgiFunction {
  // Build named-argument specs from the options. COPY passes options by name,
  // so every option is a named argument (string position).
  const specs: ArgumentSpec[] = [];
  for (const [name, opt] of Object.entries(config.options ?? {})) {
    specs.push({
      name,
      position: name, // named argument
      arrowType: opt.type,
      doc: opt.doc,
    });
  }

  const options = config.options ?? {};
  const requiredNames = Object.entries(options)
    .filter(([, opt]) => !("default" in opt))
    .map(([name]) => name);

  const meta: FunctionMeta = {
    name: config.name,
    description: config.description,
    categories: config.categories,
    tags: config.tags,
    examples: config.examples,
    requiredSettings: config.requiredSettings,
    requiredSecrets: config.requiredSecrets,
    copyFromFormat: config.format,
    copyFromDirection: config.direction ?? "from",
    copyFromComment: config.comment ?? null,
  };

  // Extract + validate the COPY options from the bind/init arguments. Options
  // arrive as named arguments; required ones (no default) must be present.
  function extractOptions(request: BindRequest): TArgs {
    const args: Record<string, unknown> = {};
    for (const [name, opt] of Object.entries(options)) {
      const hasDefault = "default" in opt;
      let val: unknown;
      try {
        val = request.arguments.get(name, hasDefault ? opt.default : undefined);
      } catch {
        val = hasDefault ? opt.default : undefined;
      }
      if (val === undefined || val === null) {
        if (!hasDefault) {
          throw new Error(
            `COPY (FORMAT ${config.format}): missing required option '${name}'`,
          );
        }
        val = opt.default;
      }
      if (typeof val === "bigint") val = safeNumber(val);
      if (opt.choices && !opt.choices.includes(val)) {
        throw new Error(
          `COPY (FORMAT ${config.format}): option '${name}' value ${JSON.stringify(val)} ` +
            `is not one of ${JSON.stringify(opt.choices)}`,
        );
      }
      args[name] = val;
    }
    return args as TArgs;
  }

  function requireCopyFrom(request: BindRequest) {
    const cf = request.copy_from;
    if (cf == null) {
      throw new Error(
        `${config.name} is a COPY FROM format reader; invoke it via ` +
          `COPY <table> FROM '<path>' (FORMAT ${config.format}), not as a table function.`,
      );
    }
    return cf;
  }

  return {
    kind: "table",
    meta,
    argumentSpecs: specs,
    // Table functions determine output at bind time; the COPY target schema is
    // not known until bind, so advertise an empty default output schema.

    bind(request: BindRequest): BindResponse {
      const cf = requireCopyFrom(request);
      // Validate options eagerly so a missing/invalid option fails at COPY bind.
      extractOptions(request);
      // DuckDB forces the scan's output to the target table's columns, so a
      // COPY-FROM reader must produce exactly expected_schema.
      return {
        output_schema: cf.expected_schema,
        opaque_data: null,
      };
    },

    globalInit(request: InitRequest): GlobalInitResponse {
      const executionId = new Uint8Array(16);
      crypto.getRandomValues(executionId);
      if (request.execution_id) {
        return { max_workers: 1, execution_id: request.execution_id, opaque_data: null };
      }
      return { max_workers: 1, execution_id: executionId, opaque_data: null };
    },

    createStreamHandlers(request: InitRequest, response: GlobalInitResponse): StreamHandlers {
      const cf = requireCopyFrom(request.bind_call);
      const args = extractOptions(request.bind_call);
      const settings = batchToScalarDict(request.bind_call.settings);
      const secrets = batchToSecretDict(request.bind_call.secrets);
      const outputSchema = request.output_schema;
      const boundStorage = new BoundStorage(globalStorage, response.execution_id);

      const processParams: TableProcessParams<TArgs> = {
        args,
        initCall: request,
        initResponse: response,
        outputSchema,
        settings,
        secrets,
        storage: boundStorage,
        atUnit: request.bind_call.at_unit ?? undefined,
        atValue: request.bind_call.at_value ?? undefined,
      };

      return {
        outputSchema,
        // Single-shot: the whole source is read on the first producer tick. The
        // `done` guard lives under `.state` so it round-trips across HTTP
        // exchanges (the framework serializes handlerState.state).
        producerInit: () => ({ state: { done: false } }),
        producerFn: async (
          pState: { state: { done: boolean } },
          out: OutputCollector,
        ) => {
          if (pState.state.done) {
            out.finish();
            return;
          }
          await config.read({
            path: cf.file_path,
            options: args,
            expectedSchema: outputSchema,
            processParams,
            out,
          });
          pState.state.done = true;
          out.finish();
        },
      };
    },
  };
}
