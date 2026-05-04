// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

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
//
// The extension reads the outer `attach_option_specs: list<binary>` column
// from CatalogInfo and validates user-supplied ATTACH options against the
// per-spec declared type before forwarding them to the worker.

import {
  type DataType,
  Field,
  RecordBatch,
  Schema,
  RecordBatchStreamWriter,
  Struct,
  makeData,
  Binary,
  Utf8,
} from "@query-farm/apache-arrow";
import { batchFromColumns, serializeBatch, serializeSchema } from "../util/arrow/index.js";

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
  type: DataType;
  /**
   * Default value used when the user omits this option. Passed through to
   * the worker's catalog_attach handler as-is if no override is given.
   * Use `null` for "no default" (an unset option will then be absent from
   * the options dict delivered to attach()).
   */
  default?: unknown;
}

const SPEC_SCHEMA = new Schema([
  new Field("name", new Utf8(), false),
  new Field("description", new Utf8(), false),
  new Field("type", new Binary(), false),
  new Field("default_value", new Binary(), true),
]);

/**
 * Serialize an AttachOptionSpec to the wire format the extension expects
 * (one IPC-serialized RecordBatch with a single row).
 */
export function serializeAttachOptionSpec(spec: AttachOptionSpec): Uint8Array {
  // `type` is encoded as a serialized Arrow Schema with one field named
  // "value" of the option's DataType. This lets the extension peek at the
  // logical type without a separate enum.
  const typeSchema = new Schema([new Field("value", spec.type, true)]);
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
