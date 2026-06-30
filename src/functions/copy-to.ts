// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Custom `COPY ... TO` format writer.
//
// A CopyToFunction lets a VGI catalog act as a remote sink: the user runs
// `COPY (query|table) TO 'path' (FORMAT <name>, opt val, ...)` and DuckDB
// streams the source rows out to the worker, which writes them to a
// destination. The worker reports the row count; there is no Source phase.
//
// Mechanically a COPY-TO writer is a buffered (Sink+Combine) function with NO
// Source phase — it reuses the entire table_buffering_process /
// table_buffering_combine machinery on both sides:
//   * `write` is called once per input batch (the buffered process() step,
//     fanned out across DuckDB's sink threads / per-thread workers). Persist the
//     batch to a shard via `params.storage` (execution_id-scoped).
//   * `close` is called exactly once on the coordinator worker (the buffered
//     combine() step, driven by DuckDB's once-only copy_to_finalize). Read the
//     shards back and perform the terminal write+flush+close of the destination.
//
// There is no finalize/drain phase, so the destination MUST be fully written and
// closed inside `close` — a writer that forgets leaves a silent partial file.
//
// CROSS-PROCESS INVARIANT: write() and close() may run on different worker
// processes (pool rotation / HTTP). Any shard state close() needs MUST live in
// cross-process storage scoped by params.executionId (params.storage), not on
// `self` / module globals.
//
// What makes it a COPY format is twofold:
//   * it sets `meta.copyToFormat` to the SQL FORMAT identifier, and
//   * the catalog advertises it via `copyFromFormats()` (the
//     `catalog_copy_from_formats` RPC returns all directions), so the VGI DuckDB
//     extension registers a DuckDB CopyFunction for it.
//
// The destination path + format arrive on the bind through `BindRequest.copy_to`
// (CopyToContext). The COPY options arrive as the function's normal named
// arguments. The source schema rides `BindRequest.input_schema`. Mirrors
// vgi-python's `vgi.copy_to_function.CopyToFunction`.

import { type VgiSchema, schema, type VgiBatch, type VgiDataType } from "../arrow/index.js";
import type { BindRequest } from "../protocol/types.js";
import type { FunctionMeta, FunctionExample } from "./types.js";
import { safeNumber } from "../util/arrow/index.js";
import {
  defineTableBufferingFunction,
  type TableBufferingParams,
  type TableBufferingVgiFunction,
} from "./table-buffering.js";

/**
 * A single secret to resolve via the two-phase secret bind, returned from a COPY
 * format's `onSecrets` hook. Mirrors a vgi-python `SecretLookupEntry`.
 */
export interface CopySecretLookup {
  /** DuckDB secret type to resolve (e.g. `"s3"`, `"vgi_example"`). */
  secretType: string;
  /** Optional scope for longest-prefix matching — typically the COPY path. */
  scope?: string;
  /** Optional secret name for name-based lookup. */
  name?: string;
}

/** Declaration of a single COPY option (a named function argument). */
export interface CopyToOption {
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
  /**
   * Optional inclusive lower bound for numeric options, validated worker-side at
   * bind. Mirrors vgi-python's `Arg(..., ge=...)`.
   */
  ge?: number;
  /**
   * Optional inclusive upper bound for numeric options, validated worker-side at
   * bind. Mirrors vgi-python's `Arg(..., le=...)`.
   */
  le?: number;
}

/** Parameters for the per-batch `write` hook. */
export interface CopyToWriteParams<TArgs = Record<string, unknown>> {
  /** One input batch from the COPY source. */
  batch: VgiBatch;
  /** Parsed COPY options (defaults applied). */
  options: TArgs;
  /** Destination path from the `COPY ... TO 'path'` statement. */
  filePath: string;
  /** Full buffering parameters (settings, secrets, storage, executionId). */
  params: TableBufferingParams<TArgs>;
}

/** Parameters for the terminal `close` hook. */
export interface CopyToCloseParams<TArgs = Record<string, unknown>> {
  /** Parsed COPY options (defaults applied). */
  options: TArgs;
  /** Destination path from the `COPY ... TO 'path'` statement. */
  filePath: string;
  /** Full buffering parameters (settings, secrets, storage, executionId). */
  params: TableBufferingParams<TArgs>;
}

export interface CopyToFunctionConfig<TArgs = Record<string, unknown>> {
  /** Handler name (the function's registered name; `Meta.name`). */
  name: string;
  /** SQL `FORMAT` identifier users type, e.g. `COPY t TO 'x' (FORMAT myfmt)`. */
  format: string;
  /** Function description (intrinsic documentation; `Meta.description`). */
  description?: string;
  /** Optional free-text comment surfaced by `vgi_copy_formats()`. */
  comment?: string | null;
  /** COPY direction; only `"to"` is supported here. */
  direction?: string;
  /**
   * When true, the writer requires rows in source order — discovery advertises
   * `ordered=true` and the extension uses a single-threaded sink
   * (`REGULAR_COPY_TO_FILE`) so one worker receives every batch in source
   * order. Mirrors vgi-python's `Meta.sink_order_dependent`.
   */
  ordered?: boolean;
  /** COPY options, keyed by option name. The `file_path` is NOT an option. */
  options?: Record<string, CopyToOption>;
  categories?: string[];
  tags?: Record<string, string>;
  examples?: FunctionExample[];
  requiredSettings?: string[];
  requiredSecrets?: string[];
  /**
   * Optional secret-bind hook: forward CREATE SECRET credentials for
   * secret-backed cloud writes (S3/GCS/HTTP/…). Called during bind (only on the
   * first pass); return the secrets to resolve — typically scoped by the
   * destination `filePath`. The framework's two-phase secret bind resolves each
   * lookup from the caller's SecretManager and surfaces the resolved values on
   * `params.secrets` at `write`/`close` time. Mirrors vgi-python's
   * `CopyToFunction.on_secrets`.
   */
  onSecrets?: (params: {
    options: TArgs;
    filePath: string;
    bindCall: BindRequest;
  }) => CopySecretLookup[] | void;
  /** Persist one input `batch` to a shard (called per sink batch). */
  write: (params: CopyToWriteParams<TArgs>) => void | Promise<void>;
  /**
   * Write the destination and close it, once (called on the coordinator). Read
   * the shards persisted by `write` and perform the terminal write. Called even
   * for empty input. Return the row count (informational).
   */
  close: (params: CopyToCloseParams<TArgs>) => number | void | Promise<number | void>;
}

/**
 * Define a custom `COPY ... TO` format writer, returned as a
 * `TableBufferingVgiFunction` (kind `"table_buffering"`) carrying the
 * `copyToFormat` metadata marker. Register it in the catalog's function list
 * like any function; the catalog's `copyFromFormats()` introspection picks it
 * up and advertises the format (direction `"to"`). The worker's
 * `table_buffering_process` / `table_buffering_combine` RPCs drive write/close.
 */
export function defineCopyToFunction<TArgs = Record<string, unknown>>(
  config: CopyToFunctionConfig<TArgs>,
): TableBufferingVgiFunction {
  const options = config.options ?? {};

  // Named-argument types + defaults for the buffering function. COPY passes
  // options by name, so each option is a named argument.
  const namedArgs: Record<string, VgiDataType> = {};
  const argDefaults: Record<string, unknown> = {};
  for (const [name, opt] of Object.entries(options)) {
    namedArgs[name] = opt.type;
    if ("default" in opt) argDefaults[name] = opt.default;
  }

  // Extract + validate the COPY options. Required options (no default) must be
  // present; `choices` are enforced worker-side. Mirrors copy-from.ts.
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
      if (typeof val === "number") {
        if (opt.ge !== undefined && val < opt.ge) {
          throw new Error(
            `COPY (FORMAT ${config.format}): option '${name}' value ${val} ` +
              `is below the minimum ${opt.ge}`,
          );
        }
        if (opt.le !== undefined && val > opt.le) {
          throw new Error(
            `COPY (FORMAT ${config.format}): option '${name}' value ${val} ` +
              `is above the maximum ${opt.le}`,
          );
        }
      }
      args[name] = val;
    }
    return args as TArgs;
  }

  function requireCopyTo(params: TableBufferingParams<TArgs>): string {
    const ct = params.initCall.bind_call.copy_to;
    if (ct == null) {
      throw new Error(
        `${config.name} is a COPY TO format writer; invoke it via ` +
          `COPY <source> TO '<path>' (FORMAT ${config.format}), not as a function.`,
      );
    }
    return ct.file_path;
  }

  const func = defineTableBufferingFunction<TArgs, null>({
    name: config.name,
    description: config.description,
    namedArgs,
    argDefaults,
    categories: config.categories,
    tags: config.tags,
    examples: config.examples,
    requiredSettings: config.requiredSettings,
    requiredSecrets: config.requiredSecrets,
    // A sink produces no rows — bind to an empty output schema. Validate the
    // options eagerly so a missing/invalid option fails at COPY bind. If the
    // writer declared an onSecrets hook, forward its requested lookups on the
    // first bind pass so the two-phase secret bind resolves them (the resolved
    // values reach write/close via params.secrets).
    onBind: ({ bindCall, resolvedSecretsProvided }) => {
      const opts = extractOptions(bindCall);
      if (config.onSecrets && !resolvedSecretsProvided) {
        const lookups =
          config.onSecrets({
            options: opts,
            filePath: bindCall.copy_to?.file_path ?? "",
            bindCall,
          }) ?? [];
        if (lookups.length > 0) {
          return {
            outputSchema: schema([]),
            lookupSecretTypes: lookups.map((l) => l.secretType),
            lookupScopes: lookups.map((l) => l.scope ?? ""),
            lookupNames: lookups.map((l) => l.name ?? ""),
          };
        }
      }
      return { outputSchema: schema([]) };
    },
    // SINK: persist one batch to a shard, bucket by execution_id.
    process: async (batch, params) => {
      const opts = extractOptions(params.initCall.bind_call);
      await config.write({
        batch,
        options: opts,
        filePath: requireCopyTo(params),
        params,
      });
      return params.executionId;
    },
    // COMBINE: terminal write+close, once on the coordinator. No finalize ids,
    // so the COPY path never drains output.
    combine: async (_stateIds, params) => {
      const opts = extractOptions(params.initCall.bind_call);
      await config.close({
        options: opts,
        filePath: requireCopyTo(params),
        params,
      });
      return [];
    },
    // Never invoked on the COPY-TO path (combine returns no finalize ids).
    finalize: (_params, _fid, _state, out) => {
      out.finish();
    },
    // Ordered writers need a single-thread sink (REGULAR_COPY_TO_FILE).
    sinkOrderDependent: config.ordered === true,
  });

  // Decorate the function's metadata with the COPY-TO format markers + attach
  // per-option docs to the argument specs so `vgi_copy_formats()` surfaces the
  // option descriptions (the buffering builder doesn't carry option docs).
  const meta = func.meta as FunctionMeta;
  meta.copyToFormat = config.format;
  meta.copyToDirection = config.direction ?? "to";
  meta.copyToComment = config.comment ?? null;
  for (const spec of func.argumentSpecs) {
    if (spec.isTableInput) continue;
    const opt = options[spec.name];
    if (opt?.doc) spec.doc = opt.doc;
  }

  return func;
}
