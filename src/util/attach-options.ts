// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Serialize a plain JS options map into a single-row Arrow RecordBatch
// suitable for the `options` field of CatalogAttachRequest. Column types
// are inferred per-value. This is the ergonomic layer that lets callers
// write `{ region: "us-east-1", maxRows: 1000n }` instead of hand-building
// an Arrow batch.

import {
  Bool,
  Binary,
  DataType,
  Field,
  Float64,
  Int64,
  Null,
  Schema,
  Utf8,
} from "@query-farm/apache-arrow";
import { batchFromColumns, serializeBatch } from "./arrow/index.js";

/**
 * Values supported in a CatalogAttach options map. The client infers an
 * Arrow type per value:
 *
 *   string     → Utf8
 *   bigint     → Int64
 *   number     → Float64 (JS numbers are doubles — safe for small ints too)
 *   boolean    → Bool
 *   Uint8Array → Binary
 *   null       → Null (column with no concrete type; value is null)
 *
 * If you need types this can't express (Decimal, Timestamp, Int32 vs Int64,
 * nested structs), drop down to `optionsBytes` on CatalogAttachOptions and
 * build the RecordBatch yourself.
 */
export type AttachOptionValue = string | number | bigint | boolean | null | Uint8Array;

/**
 * Infer an Arrow DataType for a single JS value. Exported for tests; not
 * part of the stable public API.
 */
export function inferAttachOptionArrowType(value: AttachOptionValue): DataType {
  if (value === null) return new Null();
  if (typeof value === "string") return new Utf8();
  if (typeof value === "bigint") return new Int64();
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new TypeError(
        `Attach option number values must be finite (got ${value}). ` +
        `Use a bigint for integers or a finite number for floats.`,
      );
    }
    return new Float64();
  }
  if (typeof value === "boolean") return new Bool();
  if (value instanceof Uint8Array) return new Binary();
  throw new TypeError(
    `Unsupported attach option value type: ${typeof value}. ` +
    `Supported: string, number, bigint, boolean, null, Uint8Array.`,
  );
}

/**
 * Serialize a plain key→value options map to an Arrow IPC RecordBatch.
 *
 * Returns `null` when the map is empty or has no entries — the wire layer
 * then sends a null options field rather than a zero-column batch (which
 * pyarrow's IPC reader rejects).
 */
export function serializeAttachOptions(
  options: Record<string, AttachOptionValue> | undefined | null,
): Uint8Array | null {
  if (!options) return null;
  const entries = Object.entries(options);
  if (entries.length === 0) return null;

  const fields: Field[] = [];
  const columns: Record<string, unknown[]> = {};
  for (const [key, value] of entries) {
    const type = inferAttachOptionArrowType(value);
    fields.push(new Field(key, type, true));
    columns[key] = [value];
  }
  const schema = new Schema(fields);
  const batch = batchFromColumns(columns, schema);
  return serializeBatch(batch);
}
