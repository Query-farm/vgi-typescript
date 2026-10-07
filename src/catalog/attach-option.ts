// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Attach-time option descriptors for declarative worker option discovery.
// Mirrors `vgi.catalog.attach_option` on the Python side — same Arrow IPC
// wire format so the C++ extension (which owns ATTACH option validation)
// sees identical shapes regardless of which language the worker is in.
//
// Wire format of one serialized AttachOptionSpec (per
// vgi-python/vgi/catalog/attach_option.py):
//
//   RecordBatch of:
//     name: Utf8
//     description: Utf8
//     type: Binary   -- serialized Arrow Schema with a single "value" field
//                       of the option's DataType
//     default_value: Binary nullable -- serialized single-row RecordBatch with
//                                       the default value under "value", or
//                                       null when no default is declared
//     required: Bool nullable -- the caller must supply this option at ATTACH
//                                time. Nullable and appended LAST so a peer
//                                that predates the column reads the batch by
//                                name and simply doesn't see it; absent and
//                                explicit-null both mean "not required".
//     secret: Bool nullable -- the option carries a credential. Appended AFTER
//                              `required` under the same rule: readers look it
//                              up by name, and absent or null reads as false.
//
// The extension reads the outer `attach_option_specs: list<binary>` column
// from CatalogInfo and validates user-supplied ATTACH options against the
// per-spec declared type before forwarding them to the worker.

import {
  type VgiDataType,
  schema as makeSchema,
  field,
  utf8,
  binary,
  bool,
  batchFromColumns,
  iterRows,
  serializeBatch,
  deserializeBatch,
  serializeSchema,
  deserializeSchema,
} from "../arrow/index.js";
import { toUint8Array } from "../util/bytes.js";

/**
 * Declarative spec for a single attach-time option.
 *
 * The catalog's `catalogsInfo()` emits these (serialized) in
 * `CatalogInfo.attach_option_specs` so the DuckDB extension can validate
 * user-supplied ATTACH options and cast them to the declared type before
 * forwarding to the worker's `catalog_attach` handler.
 */
export interface AttachOptionSpec {
  /** Option name — matches the key users pass in the ATTACH statement. */
  name: string;
  /** Human-readable description (shown in discovery UIs). */
  description: string;
  /** Arrow data type the extension should cast user input to. */
  type: VgiDataType;
  /**
   * Default value used when the user omits this option. Passed through to
   * the worker's catalog_attach handler as-is if no override is given.
   * Use `null` for "no default" (an unset option will then be absent from
   * the options dict delivered to attach()).
   */
  default?: unknown;
  /**
   * The caller must supply this option at ATTACH time. A catalog that cannot
   * be attached without it advertises that fact at discovery, so a client can
   * say so before attempting the attach rather than surfacing a failure that
   * reads like an empty catalog.
   *
   * Mutually exclusive with `default` — an option that falls back to a value
   * is by definition satisfiable without the caller.
   */
  required?: boolean;
  /**
   * The option carries a credential: an API key, a token, a password, or
   * anything else that must not be shown, stored or logged in plain text.
   *
   * Credential options **MUST** be declared `secret: true`. The value is still
   * passed inline as an ordinary ATTACH option; the flag tells the DuckDB
   * extension to redact it from `duckdb_databases()`, keep only a salted hash
   * of it in its result-cache key, and never log it. Clients use the flag to
   * mask the field and keep the value out of shared links and exported
   * configuration.
   *
   * To keep the credential out of the SQL text itself, pass it as an
   * expression:
   *
   * ```sql
   * ATTACH 'sales' (TYPE vgi, LOCATION 'https://worker.example.com',
   *                 api_key getenv('SALES_API_KEY'));
   * ```
   *
   * Combines with `required` (a credential the catalog cannot attach without,
   * which a client can then ask for before attaching). It is allowed together
   * with `default`, but a secret option normally has none: a default would be
   * a credential shipped in the catalog's own discovery metadata.
   *
   * Defaults to `false`. On the wire it is a nullable `secret` Bool column
   * appended after `required`; a peer that predates it reads it as `false`.
   */
  secret?: boolean;
}

/**
 * Reserved for attach tickets: the framework reads an ATTACH option with this
 * name as a `vgi_attach_ticket` before any catalog code runs, so no catalog may
 * declare an attach option called this, compared case-insensitively.
 */
export const RESERVED_ATTACH_OPTION = "vgi_attach_ticket";

/**
 * Throw if any spec uses the reserved {@link RESERVED_ATTACH_OPTION} name.
 *
 * @throws Error naming the offending option.
 */
export function assertNoReservedAttachOption(specs: Iterable<Pick<AttachOptionSpec, "name">>): void {
  for (const spec of specs) {
    if (spec.name.toLowerCase() === RESERVED_ATTACH_OPTION) {
      throw new Error(
        `Attach option '${spec.name}' uses the reserved name '${RESERVED_ATTACH_OPTION}': the framework ` +
          "reads it as an attach ticket before any catalog code runs. Rename the option.",
      );
    }
  }
}

const SPEC_SCHEMA = makeSchema([
  field("name", utf8(), false),
  field("description", utf8(), false),
  field("type", binary(), false),
  field("default_value", binary(), true),
  field("required", bool(), true),
  field("secret", bool(), true),
]);

/**
 * Serialize an AttachOptionSpec to the wire format the extension expects
 * (one IPC-serialized RecordBatch with a single row).
 */
export function serializeAttachOptionSpec(spec: AttachOptionSpec): Uint8Array {
  assertNoReservedAttachOption([spec]);
  // Mirrors AttachOptionSpec.__post_init__ on the Python side: an option that
  // falls back to a value is always satisfiable without the caller, so the
  // combination is a declaration bug rather than a runtime condition.
  if (spec.required && spec.default !== undefined && spec.default !== null) {
    throw new Error(
      `Attach option '${spec.name}' is required but also declares a default ` +
        `(${JSON.stringify(spec.default)}); an option with a default is always ` +
        "satisfiable without the caller. Drop one.",
    );
  }

  // `type` is encoded as a serialized Arrow Schema with one field named
  // "value" of the option's DataType. This lets the extension peek at the
  // logical type without a separate enum.
  const typeSchema = makeSchema([field("value", spec.type, true)]);
  const typeBytes = serializeSchema(typeSchema);

  let defaultBytes: Uint8Array | null = null;
  if (spec.default !== undefined && spec.default !== null) {
    // Build a 1-row batch with one column "value" of the option's type.
    const defaultBatch = batchFromColumns(
      { value: [spec.default] },
      typeSchema,
    );
    defaultBytes = serializeBatch(defaultBatch);
  }

  const batch = batchFromColumns(
    {
      name: [spec.name],
      description: [spec.description],
      type: [typeBytes],
      default_value: [defaultBytes],
      // Written explicitly rather than left null so a reader sees `false`, not
      // NULL, for an option that simply isn't required.
      required: [spec.required ?? false],
      // Same reasoning: an ordinary option reads `false`, not NULL.
      secret: [spec.secret ?? false],
    },
    SPEC_SCHEMA,
  );
  return serializeBatch(batch);
}

/**
 * Convenience: serialize many specs at once for CatalogInfo.attach_option_specs.
 */
export function serializeAttachOptionSpecs(
  specs: Iterable<AttachOptionSpec>,
): Uint8Array[] {
  return Array.from(specs, serializeAttachOptionSpec);
}

/**
 * Deserialize one AttachOptionSpec from the wire format above.
 *
 * Reads by column name, so a spec written by a peer that predates the
 * `required` or `secret` column deserializes with that flag `false` rather
 * than failing — the same tolerance the Python and C++ readers have.
 */
export function deserializeAttachOptionSpec(
  bytes: Uint8Array,
): AttachOptionSpec {
  const row = [...iterRows(deserializeBatch(bytes))][0] as
    | Record<string, unknown>
    | undefined;
  if (row === undefined) {
    throw new Error("Cannot deserialize AttachOptionSpec from an empty batch");
  }

  // `type` is a serialized Arrow Schema whose single "value" field carries the
  // option's DataType.
  const typeSchema = deserializeSchema(toUint8Array(row.type as Uint8Array));
  const type = typeSchema.fields[0]!.type;

  let value: unknown;
  const defaultBytes = row.default_value;
  if (defaultBytes != null) {
    const asBytes = toUint8Array(defaultBytes as Uint8Array);
    if (asBytes.length > 0) {
      const defaultRow = [...iterRows(deserializeBatch(asBytes))][0] as
        | Record<string, unknown>
        | undefined;
      value = defaultRow?.value;
    }
  }

  return {
    name: String(row.name),
    description: String(row.description ?? ""),
    type,
    ...(value === undefined || value === null ? {} : { default: value }),
    required: row.required === true,
    secret: row.secret === true,
  };
}

/** Deserialize many specs — the shape `CatalogInfo.attach_option_specs` holds. */
export function deserializeAttachOptionSpecs(
  specs: Iterable<Uint8Array>,
): AttachOptionSpec[] {
  return Array.from(specs, deserializeAttachOptionSpec);
}

/**
 * Raised when `attach` omits options declared `required`.
 *
 * Carries the option names in `missing` so a caller can act on them without
 * parsing the message. Mirrors `MissingAttachOptionsError` on the Python side,
 * message included — the extension's integration suite matches on its text.
 */
export class MissingAttachOptionsError extends Error {
  readonly missing: string[];

  constructor(catalogName: string, missing: string[]) {
    const joined = missing.map((name) => `'${name}'`).join(", ");
    super(
      `Catalog '${catalogName}' cannot be attached without the required ` +
        `option${missing.length > 1 ? "s" : ""} ${joined}.`,
    );
    this.name = "MissingAttachOptionsError";
    this.missing = missing;
  }
}

/**
 * Throw if any `required` spec has no corresponding entry in `options`.
 *
 * Option names are matched case-insensitively, mirroring DuckDB's handling of
 * ATTACH option keys.
 *
 * @throws {MissingAttachOptionsError} If a required option was not supplied.
 */
export function validateRequiredAttachOptions(
  catalogName: string,
  specs: Iterable<AttachOptionSpec>,
  options: Record<string, unknown>,
): void {
  specs = Array.from(specs);
  assertNoReservedAttachOption(specs);
  const supplied = new Set(Object.keys(options).map((key) => key.toLowerCase()));
  const missing = Array.from(specs)
    .filter((spec) => spec.required && !supplied.has(spec.name.toLowerCase()))
    .map((spec) => spec.name);
  if (missing.length > 0) {
    throw new MissingAttachOptionsError(catalogName, missing);
  }
}

/** Raised when a catalog declares the reserved attach option name. */
export class ReservedAttachOptionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ReservedAttachOptionError";
  }
}

/**
 * Startup check: refuse a catalog that declares an attach option named
 * {@link RESERVED_ATTACH_OPTION}, reading the specs it advertises through
 * `catalogsInfo()`.
 *
 * TypeScript has no declaration-time hook for a plain spec object, so the
 * worker entry points run this before serving. Only the reserved-name refusal
 * propagates (as {@link ReservedAttachOptionError}); any other failure reading
 * `catalogsInfo()` -- a catalog whose discovery needs I/O that is not up yet
 * -- is left for discovery itself to report.
 */
export async function checkReservedAttachOptions(catalog: {
  catalogsInfo?(): unknown;
} | undefined): Promise<void> {
  if (!catalog?.catalogsInfo) return;
  let infos: unknown;
  try {
    infos = await catalog.catalogsInfo();
  } catch (e) {
    if (e instanceof Error && e.message.includes(`reserved name '${RESERVED_ATTACH_OPTION}'`)) {
      throw new ReservedAttachOptionError(e.message);
    }
    return;
  }
  for (const info of (infos ?? []) as Array<{ attach_option_specs?: unknown[] | null }>) {
    for (const raw of info.attach_option_specs ?? []) {
      let name: string;
      try {
        name = deserializeAttachOptionSpec(toUint8Array(raw as Uint8Array)).name;
      } catch {
        continue;
      }
      try {
        assertNoReservedAttachOption([{ name }]);
      } catch (e) {
        throw new ReservedAttachOptionError((e as Error).message);
      }
    }
  }
}
