// Shared param schemas and helpers for catalog protocol handlers.

import { Schema, Field, Binary, Utf8, Bool } from "@query-farm/apache-arrow";
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

export const emptyResultSchema = new Schema([]);

export const attachIdParam = new Schema([
  new Field("attach_id", new Binary(), true),
]);

export const attachIdTxnParams = new Schema([
  new Field("attach_id", new Binary(), true),
  new Field("transaction_id", new Binary(), true),
]);

export const attachIdNameTxnParams = new Schema([
  new Field("attach_id", new Binary(), true),
  new Field("name", new Utf8(), false),
  new Field("transaction_id", new Binary(), true),
]);

export const attachIdSchemaNameTxnParams = new Schema([
  new Field("attach_id", new Binary(), true),
  new Field("schema_name", new Utf8(), false),
  new Field("name", new Utf8(), false),
  new Field("transaction_id", new Binary(), true),
]);

export const schemaNameIgnoreNotFoundTxnParams = new Schema([
  new Field("attach_id", new Binary(), true),
  new Field("schema_name", new Utf8(), false),
  new Field("name", new Utf8(), false),
  new Field("ignore_not_found", new Bool(), true),
  new Field("transaction_id", new Binary(), true),
]);

export const schemaNameCommentParams = new Schema([
  new Field("attach_id", new Binary(), true),
  new Field("schema_name", new Utf8(), false),
  new Field("name", new Utf8(), false),
  new Field("comment", new Utf8(), true),
  new Field("ignore_not_found", new Bool(), true),
  new Field("transaction_id", new Binary(), true),
]);

export const schemaNameRenameParams = new Schema([
  new Field("attach_id", new Binary(), true),
  new Field("schema_name", new Utf8(), false),
  new Field("name", new Utf8(), false),
  new Field("new_name", new Utf8(), false),
  new Field("ignore_not_found", new Bool(), true),
  new Field("transaction_id", new Binary(), true),
]);

export const columnOpParams = new Schema([
  new Field("attach_id", new Binary(), true),
  new Field("schema_name", new Utf8(), false),
  new Field("name", new Utf8(), false),
  new Field("column_name", new Utf8(), false),
  new Field("ignore_not_found", new Bool(), true),
  new Field("transaction_id", new Binary(), true),
]);
