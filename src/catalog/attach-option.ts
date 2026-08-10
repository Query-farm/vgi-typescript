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
  serializeBatch,
  serializeSchema,
} from "../arrow/index.js";

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
}

const SPEC_SCHEMA = makeSchema([
  field("name", utf8(), false),
  field("description", utf8(), false),
  field("type", binary(), false),
  field("default_value", binary(), true),
  field("required", bool(), true),
]);

/**
 * Serialize an AttachOptionSpec to the wire format the extension expects
 * (one IPC-serialized RecordBatch with a single row).
 */
export function serializeAttachOptionSpec(spec: AttachOptionSpec): Uint8Array {
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
  const supplied = new Set(Object.keys(options).map((key) => key.toLowerCase()));
  const missing = Array.from(specs)
    .filter((spec) => spec.required && !supplied.has(spec.name.toLowerCase()))
    .map((spec) => spec.name);
  if (missing.length > 0) {
    throw new MissingAttachOptionsError(catalogName, missing);
  }
}
