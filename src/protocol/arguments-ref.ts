// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Argument references (protocol 3.4): resolving `BindRequest.arguments_location`.
//
// A client binding a function with a large argument over HTTP uploads the
// arguments once, through this worker's `__upload_url__` grant, and sends a
// BindRequest whose `arguments` is null and whose `arguments_location` carries
// the grant's sealed `ref` plus the upload's size and SHA-256 (vgi
// docs/large-values-3.4.md §2, §5a). Every request embedding that bind call
// (cardinality, statistics, plan, projection, dynamic-to-string, init) carries
// the small location, never the value.
//
// The framework resolves the location once per request, at its entry (where a
// call context exists), and hands function code a bind call whose `arguments`
// holds the resolved value -- so code reading `bindCall.arguments` keeps
// working. That bind call still re-encodes as the bytes the client sent
// (serializers/bind.ts `bindCallBytes`), so the reference, never the value,
// goes back on the wire, into stream state or into table-buffering storage.
// vgi-rpc does the opening, the object cache (one `get` per object id per
// process, single-flight) and the size / SHA-256 checks.
//
// A worker advertises support with the `VGI-Arguments-Ref-Bytes` capability
// header ({@link argumentsRefHeaderValue}); without it the extension never
// externalizes arguments.

import { StatusError, isObjectStore } from "@query-farm/vgi-rpc";
import { Arguments } from "../arguments/arguments.js";
import { deserializeArguments } from "./serializers/arguments.js";
import type { BindRequest } from "./types.js";

/** The capability header telling the client this worker accepts argument references, and above how many bytes of arguments IPC to use one. */
export const ARGUMENTS_REF_BYTES_HEADER = "VGI-Arguments-Ref-Bytes";
/** Operator override of the advertised value. */
export const ARGUMENTS_REF_BYTES_ENV = "VGI_ARGUMENTS_REF_BYTES";
/** The default advertised value: 4 MiB. */
export const DEFAULT_ARGUMENTS_REF_BYTES = 4 * 1024 * 1024;
/**
 * Test switch: the HTTP server stops sending the header (the extension's
 * `arguments_ref_fallback.test`), behaving like a worker without argument
 * references while still vending upload URLs.
 */
export const DISABLE_ARGUMENTS_REF_ENV = "VGI_TEST_DISABLE_ARGUMENTS_REF";

/** Kind of the refusal for a bind that does not set exactly one of `arguments` / `arguments_location`. */
export const INVALID_BIND_REQUEST_KIND = "invalid_bind_request";
/** Its exact message (large-values spec §2). */
export const INVALID_BIND_REQUEST_MESSAGE = "BindRequest must set exactly one of 'arguments' and 'arguments_location'";
/** Kind of the refusal for a malformed or unopenable argument reference. */
export const INVALID_ARGUMENTS_REF_KIND = "invalid_arguments_ref";
/** Kind of the refusal for an argument object storage reports gone. */
export const ARGUMENTS_REF_MISSING_KIND = "arguments_ref_missing";
/**
 * The exact message for it (large-values spec §5a, "Prepared statements"):
 * argument objects live about a day, so a prepared statement's large argument
 * can expire, and there is no transparent rebind. Same text as vgi-python.
 */
export const ARGUMENTS_REF_MISSING_MESSAGE = "this prepared statement's large argument has expired; prepare it again";

const MAX_HEADER_DIGITS = 19;

/** `invalid_bind_request` (`INVALID_ARGUMENT`). */
export function invalidBindRequest(): StatusError {
  return new StatusError(INVALID_BIND_REQUEST_MESSAGE, { code: "INVALID_ARGUMENT", kind: INVALID_BIND_REQUEST_KIND });
}

/**
 * `invalid_arguments_ref` (`INVALID_ARGUMENT`). The message names the kind
 * too: a client that shows only "<type>: <message>" (the DuckDB extension)
 * would otherwise never show the reason a caller can act on.
 */
export function invalidArgumentsRef(message: string): StatusError {
  return new StatusError(`${INVALID_ARGUMENTS_REF_KIND}: ${message}`, {
    code: "INVALID_ARGUMENT",
    kind: INVALID_ARGUMENTS_REF_KIND,
  });
}

// ---------------------------------------------------------------------------
// The unresolved placeholder
// ---------------------------------------------------------------------------

const placeholders = new WeakSet<object>();

function notResolved(): never {
  throw new StatusError("BindRequest arguments are externalized (arguments_location) and were not resolved", {
    code: "FAILED_PRECONDITION",
    kind: "arguments_not_resolved",
  });
}

/**
 * The `arguments` of a bind call decoded with an `arguments_location`, until
 * the framework resolves it. Every read throws `arguments_not_resolved`, so a
 * path that forgot to resolve fails loudly instead of running the function
 * with zero arguments (what a 3.3 worker silently did with a 3.4 bind).
 */
export function unresolvedArguments(): Arguments {
  const placeholder = Object.create(Arguments.prototype) as Arguments;
  for (const key of ["positional", "named", "argumentsSchema", "length"]) {
    Object.defineProperty(placeholder, key, { get: notResolved, enumerable: false });
  }
  Object.defineProperty(placeholder, "get", { value: notResolved, enumerable: false });
  placeholders.add(placeholder);
  return placeholder;
}

/** Whether `bindCall.arguments` holds a value (inline, or already resolved). */
export function argumentsResolved(bindCall: BindRequest): boolean {
  return !placeholders.has(bindCall.arguments);
}

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

/** What resolution needs from a call context (vgi-rpc's CallContext, StreamInitContext and OutputCollector all have it). */
export interface ArgumentRefResolver {
  resolveArgumentRef(ref: Uint8Array, location: { size: number; sha256: string }): Promise<Uint8Array>;
}

function errorKindOf(error: unknown): string {
  if (error === null || typeof error !== "object") return "";
  const own = (error as { errorKind?: unknown }).errorKind;
  if (typeof own === "string") return own;
  const ctor = (error as { constructor?: { errorKind?: unknown } }).constructor;
  return typeof ctor?.errorKind === "string" ? ctor.errorKind : "";
}

/**
 * Resolve a bind call's `arguments_location` in place, at the request's
 * entry: open the sealed reference under this request's principal and check
 * its size and SHA-256 equal the location's; look the object up in vgi-rpc's
 * object cache by `object_id`; on a miss fetch it once (single-flight); verify
 * its length and SHA-256; decode it as the arguments IPC stream.
 *
 * Only `arguments` changes; `arguments_location` and the received bytes stay,
 * so the bind call still re-encodes as the reference. A bind call whose
 * arguments came inline (or are already resolved) is returned untouched.
 *
 * @throws StatusError `invalid_arguments_ref` (`INVALID_ARGUMENT`) for a
 *   reference that does not open or match, no storage, or no context;
 *   `arguments_ref_integrity` (`DATA_LOSS`) for fetched bytes that differ;
 *   `arguments_ref_missing` (`NOT_FOUND`) with the prepared-statement message
 *   when storage reports the object gone. vgi-rpc's `storage_unavailable`
 *   passes through for transient storage failures.
 */
export async function resolveBindCall<B extends BindRequest>(
  bindCall: B,
  ctx: ArgumentRefResolver | null | undefined,
): Promise<B> {
  if (argumentsResolved(bindCall)) return bindCall;
  const location = bindCall.arguments_location;
  if (location == null) notResolved();
  if (!ctx || typeof ctx.resolveArgumentRef !== "function") {
    throw invalidArgumentsRef("argument references need the request's context to resolve");
  }
  let data: Uint8Array;
  try {
    data = await ctx.resolveArgumentRef(location.ref, { size: location.size, sha256: location.sha256 });
  } catch (error) {
    const kind = errorKindOf(error);
    if (kind === ARGUMENTS_REF_MISSING_KIND) {
      throw new StatusError(ARGUMENTS_REF_MISSING_MESSAGE, { code: "NOT_FOUND", kind: ARGUMENTS_REF_MISSING_KIND });
    }
    if (kind === INVALID_ARGUMENTS_REF_KIND || kind === "arguments_ref_integrity") {
      const message = error instanceof Error ? error.message : String(error);
      throw new StatusError(message.startsWith(`${kind}:`) ? message : `${kind}: ${message}`, {
        code: kind === INVALID_ARGUMENTS_REF_KIND ? "INVALID_ARGUMENT" : "DATA_LOSS",
        kind,
      });
    }
    throw error;
  }
  let args: Arguments;
  try {
    args = deserializeArguments(data);
  } catch (error) {
    // The bytes are exactly what the client uploaded and claimed (size and
    // SHA-256 checked), so a bad stream is the client's.
    throw invalidArgumentsRef(
      `the uploaded arguments are not a valid arguments IPC stream: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  bindCall.arguments = args;
  return bindCall;
}

/** Resolve the bind call a request embeds (`request.bind_call`), in place. */
export async function resolveRequestArguments<R extends { bind_call: BindRequest }>(
  request: R,
  ctx: ArgumentRefResolver | null | undefined,
): Promise<R> {
  await resolveBindCall(request.bind_call, ctx);
  return request;
}

// ---------------------------------------------------------------------------
// The capability header
// ---------------------------------------------------------------------------

type EnvLike = Readonly<Record<string, string | undefined>>;

function processEnv(): EnvLike {
  return (globalThis as { process?: { env?: EnvLike } }).process?.env ?? {};
}

/**
 * The `VGI-Arguments-Ref-Bytes` value to advertise: `VGI_ARGUMENTS_REF_BYTES`
 * when set, else 4 MiB.
 *
 * @throws Error when the variable is not a positive decimal integer of at most
 *   19 digits (the header's grammar), so a typo stops startup instead of
 *   silently advertising nothing.
 */
export function argumentsRefBytesFromEnv(env: EnvLike = processEnv()): number {
  const raw = (env[ARGUMENTS_REF_BYTES_ENV] ?? "").trim();
  if (!raw) return DEFAULT_ARGUMENTS_REF_BYTES;
  if (!/^[1-9][0-9]*$/.test(raw) || raw.length > MAX_HEADER_DIGITS || !Number.isSafeInteger(Number(raw))) {
    throw new Error(
      `${ARGUMENTS_REF_BYTES_ENV} must be a positive integer number of bytes ` +
        `(no sign, no leading zero, at most ${MAX_HEADER_DIGITS} digits), got ${JSON.stringify(raw)}`,
    );
  }
  return Number(raw);
}

/**
 * The `VGI-Arguments-Ref-Bytes` value an HTTP server advertises, or `null`
 * when it advertises nothing.
 *
 * The header is the mandatory capability signal (large-values spec §5): sent
 * whenever the server vends upload URLs (the grant) AND its storage can fetch
 * the uploaded object back by id (vgi-rpc `ObjectStore`), never otherwise.
 * `VGI_TEST_DISABLE_ARGUMENTS_REF=1` turns it off (the extension's fallback
 * test).
 *
 * @param explicit A server option overriding `VGI_ARGUMENTS_REF_BYTES`.
 */
export function argumentsRefHeaderValue(opts: {
  uploadUrlProvider?: unknown;
  externalLocation?: { storage?: unknown } | null | undefined;
  explicit?: number | undefined;
  env?: EnvLike;
}): number | null {
  const env = opts.env ?? processEnv();
  if (opts.uploadUrlProvider == null) return null;
  if (!isObjectStore(opts.externalLocation?.storage)) {
    console.warn(
      "argument references disabled: the configured storage cannot fetch objects by id (no ObjectStore)",
    );
    return null;
  }
  if (["1", "true", "yes"].includes((env[DISABLE_ARGUMENTS_REF_ENV] ?? "").trim().toLowerCase())) return null;
  if (opts.explicit !== undefined) {
    if (!Number.isSafeInteger(opts.explicit) || opts.explicit < 1) {
      throw new Error(`argumentsRefBytes must be a positive integer, got ${opts.explicit}`);
    }
    return opts.explicit;
  }
  return argumentsRefBytesFromEnv(env);
}
