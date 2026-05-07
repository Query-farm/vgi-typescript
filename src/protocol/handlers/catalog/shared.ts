// Shared param schemas and helpers for catalog protocol handlers.

import { type VgiSchema, schema, type VgiField, field, type VgiDataType, binary, utf8, bool } from "../../../arrow/index.js";
import type { CatalogInterface } from "../../../catalog/interface.js";
import { NoCatalogError } from "../../../errors.js";
import { deserializeBatch } from "../../../util/arrow/index.js";
import { toUint8Array } from "../../../util/bytes.js";

export type GetCatalog = () => CatalogInterface;

export function makeGetCatalog(catalog: CatalogInterface | undefined): GetCatalog {
  return () => {
    if (!catalog) throw new NoCatalogError();
    return catalog;
  };
}

/**
 * Decode the `options` field of a catalog_attach request into a plain
 * {name: value} dict. The wire field is either null (no options) or an
 * IPC-serialized RecordBatch of typed columns — one column per option,
 * one row with the value. Returns `{}` for null / empty input.
 */
export function decodeOptionsBatch(bytes: any): Record<string, unknown> {
  if (bytes == null) return {};
  const buf = toUint8Array(bytes);
  if (buf.byteLength === 0) return {};
  const batch = deserializeBatch(buf);
  if (batch.numRows === 0) return {};
  const out: Record<string, unknown> = {};
  for (const field of batch.schema.fields) {
    const col = batch.getChild(field.name);
    out[field.name] = col ? col.get(0) : null;
  }
  return out;
}

// ---------------------------------------------------------------------------
// Common param schemas
// ---------------------------------------------------------------------------

export const emptyResultSchema = schema([]);

export const attachIdParam = schema([
  field("attach_id", binary(), true),
]);

export const attachIdTxnParams = schema([
  field("attach_id", binary(), true),
  field("transaction_id", binary(), true),
]);

export const attachIdNameTxnParams = schema([
  field("attach_id", binary(), true),
  field("name", utf8(), false),
  field("transaction_id", binary(), true),
]);

export const attachIdSchemaNameTxnParams = schema([
  field("attach_id", binary(), true),
  field("schema_name", utf8(), false),
  field("name", utf8(), false),
  field("transaction_id", binary(), true),
]);

export const schemaNameIgnoreNotFoundTxnParams = schema([
  field("attach_id", binary(), true),
  field("schema_name", utf8(), false),
  field("name", utf8(), false),
  field("ignore_not_found", bool(), true),
  field("transaction_id", binary(), true),
]);

export const schemaNameCommentParams = schema([
  field("attach_id", binary(), true),
  field("schema_name", utf8(), false),
  field("name", utf8(), false),
  field("comment", utf8(), true),
  field("ignore_not_found", bool(), true),
  field("transaction_id", binary(), true),
]);

export const schemaNameRenameParams = schema([
  field("attach_id", binary(), true),
  field("schema_name", utf8(), false),
  field("name", utf8(), false),
  field("new_name", utf8(), false),
  field("ignore_not_found", bool(), true),
  field("transaction_id", binary(), true),
]);

export const columnOpParams = schema([
  field("attach_id", binary(), true),
  field("schema_name", utf8(), false),
  field("name", utf8(), false),
  field("column_name", utf8(), false),
  field("ignore_not_found", bool(), true),
  field("transaction_id", binary(), true),
]);
