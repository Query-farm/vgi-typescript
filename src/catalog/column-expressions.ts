// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import {
  deserializeBatch,
  deserializeSchema,
  type VgiSchema,
} from "../arrow/index.js";
import { SqlExpressionSchema } from "../generated/vgi-protocol-schemas.js";
import {
  decodeSqlExpression,
  encodeSqlExpression,
} from "../generated/vgi-protocol-types.js";
import type { SqlExpression } from "../generated/vgi-client.js";

export const GENERATED_EXPRESSION_KEY = "vgi.column.generated.v1";
export const GENERATION_KEY = "vgi.column.generation.v1";
const PROFILE = "vgi.expression.duckdb.scalar.v1";
const MAX_BYTES = 8 * 1024 * 1024;
const base64 = (bytes: Uint8Array): string =>
  btoa(Array.from(bytes, (value) => String.fromCharCode(value)).join(""));
function text(value: unknown, maximum: number): asserts value is string {
  if (
    typeof value !== "string" ||
    !value ||
    value.includes("\0") ||
    new TextEncoder().encode(value).length > maximum
  )
    throw new Error("invalid generated expression text");
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff) {
      const d = value.charCodeAt(++i);
      if (!(d >= 0xdc00 && d <= 0xdfff))
        throw new Error("invalid generated expression Unicode");
    } else if (c >= 0xdc00 && c <= 0xdfff)
      throw new Error("invalid generated expression Unicode");
  }
}
function expression(value: SqlExpression, owner: string[]): void {
  text(value.sql, 1024 * 1024);
  if (!value.sql.trim() || value.profile !== PROFILE)
    throw new Error("unsupported generated expression profile or empty SQL");
  if (
    !Array.isArray(value.schema_path) ||
    !value.schema_path.length ||
    value.schema_path.length > 32
  )
    throw new Error("invalid generated expression owner");
  value.schema_path.forEach((part) => text(part, 4096));
  if (JSON.stringify(value.schema_path) !== JSON.stringify(owner))
    throw new Error("generated expression owner differs from table schema");
  const context = value.evaluation_context;
  if (!context)
    throw new Error(
      "generated expression requires explicit evaluation context",
    );
  text(context.timezone, 256);
  text(context.collation, 256);
  if (
    !["asc", "desc"].includes(context.default_order) ||
    ![
      "nulls_first",
      "nulls_last",
      "nulls_first_on_asc_last_on_desc",
      "nulls_last_on_asc_first_on_desc",
    ].includes(context.default_null_order) ||
    typeof context.preserve_insertion_order !== "boolean"
  )
    throw new Error("invalid generated expression evaluation context");
}
// Check the complete stream before facade readers select its first batch.
function envelope(bytes: Uint8Array): Uint8Array[] {
  if (!bytes.length || bytes.length > MAX_BYTES)
    throw new Error("invalid generated expression size");
  const data = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const kinds: number[] = [];
  const textBuffers: Uint8Array[] = [];
  let offset = 0;
  while (offset < bytes.length) {
    const prefix = data.getInt32(offset, true) === -1 ? 8 : 4;
    const length = data.getInt32(offset + prefix - 4, true);
    offset += prefix;
    if (!length) {
      if (offset !== bytes.length || kinds.join(",") !== "1,3")
        throw new Error(
          "SqlExpression requires exactly one complete IPC batch",
        );
      return textBuffers;
    }
    if (length < 0 || offset + length > bytes.length)
      throw new Error("invalid expression IPC length");
    const p = offset + data.getUint32(offset, true),
      v = p - data.getInt32(p, true);
    const slot = (id: number) =>
      4 + id * 2 < data.getUint16(v, true)
        ? data.getUint16(v + 4 + id * 2, true)
        : 0;
    const kind = slot(1),
      body = slot(3);
    if (!kind) throw new Error("invalid expression IPC message");
    kinds.push(data.getUint8(p + kind));
    const bodyLength = body ? Number(data.getBigInt64(p + body, true)) : 0;
    if (bodyLength < 0 || bodyLength > bytes.length - offset - length)
      throw new Error("invalid expression IPC body");
    if (data.getUint8(p + kind) === 3) {
      const headerSlot = slot(2);
      const record = p + headerSlot + data.getUint32(p + headerSlot, true);
      const table = record - data.getInt32(record, true);
      const recordSlot = (id: number) =>
        4 + id * 2 < data.getUint16(table, true)
          ? data.getUint16(table + 4 + id * 2, true)
          : 0;
      if (recordSlot(3))
        throw new Error("compressed expression IPC is unsupported");
      const bufferSlot = recordSlot(2);
      const buffers =
        record + bufferSlot + data.getUint32(record + bufferSlot, true);
      if (data.getUint32(buffers, true) !== 26)
        throw new Error("invalid SqlExpression buffer layout");
      // SqlExpression's seven Utf8 value buffers, including path and context.
      for (const index of [2, 5, 10, 14, 17, 20, 23]) {
        const buffer = buffers + 4 + index * 16;
        const start = Number(data.getBigInt64(buffer, true));
        const size = Number(data.getBigInt64(buffer + 8, true));
        if (start < 0 || size < 0 || start + size > bodyLength)
          throw new Error("invalid expression string buffer");
        textBuffers.push(
          bytes.subarray(
            offset + length + start,
            offset + length + start + size,
          ),
        );
      }
    }
    offset += length + bodyLength;
  }
  throw new Error("expression IPC lacks end marker");
}
function shape(schema: VgiSchema): string {
  const field = (f: any): unknown => [
    f.name,
    f.nullable,
    String(f.type),
    (f.type.children ?? []).map(field),
  ];
  return JSON.stringify(schema.fields.map(field));
}
function validateEncoded(encoded: string, owner: string[]): void {
  if (!encoded.length || encoded.length > 4 * Math.ceil(MAX_BYTES / 3))
    throw new Error("invalid generated expression size");
  const bytes = Uint8Array.from(atob(encoded), (c) => c.charCodeAt(0));
  if (base64(bytes) !== encoded)
    throw new Error("generated expression requires canonical base64");
  const textBuffers = envelope(bytes);
  const batch = deserializeBatch(bytes);
  if (batch.numRows !== 1 || shape(batch.schema) !== shape(SqlExpressionSchema))
    throw new Error("invalid SqlExpression record schema");
  const utf8 = new TextDecoder("utf-8", { fatal: true });
  textBuffers.forEach((buffer) => utf8.decode(buffer));
  expression(decodeSqlExpression(bytes), owner);
}
export function generatedExpressionMetadata(
  sql: string,
  owner: string[],
): string {
  const value: SqlExpression = {
    sql,
    profile: PROFILE,
    schema_path: [...owner],
    evaluation_context: {
      timezone: "UTC",
      collation: "binary",
      default_order: "asc",
      default_null_order: "nulls_last",
      preserve_insertion_order: true,
    },
  };
  expression(value, owner);
  return base64(encodeSqlExpression(value));
}
/** Validate canonical VIRTUAL metadata without evaluating or rewriting SQL. */
export function validateColumnExpressions(
  schema: VgiSchema,
  owner: string[],
  allowGenerated = true,
): void {
  const type = (t: any): void => {
    for (const f of t?.children ?? t?.fields ?? []) field(f);
    if (t?.dictionary) type(t.dictionary);
  };
  const field = (f: any): void => {
    const metadata: Map<string, string> = f.metadata ?? new Map();
    for (const key of metadata.keys()) {
      if (
        (key.startsWith("vgi.column.generated.") &&
          key !== GENERATED_EXPRESSION_KEY) ||
        (key.startsWith("vgi.column.generation.") && key !== GENERATION_KEY)
      )
        throw new Error("unknown generated metadata version");
    }
    if (metadata.has("generated_expression"))
      throw new Error(
        "legacy generated expression requires explicit canonical translation",
      );
    if (metadata.has(GENERATED_EXPRESSION_KEY) !== metadata.has(GENERATION_KEY))
      throw new Error("generated expression and category must occur together");
    if (metadata.has(GENERATED_EXPRESSION_KEY)) {
      if (metadata.get(GENERATION_KEY) !== "virtual")
        throw new Error("only VIRTUAL generated columns are supported");
      if (metadata.has("default") || metadata.has("vgi.column.default.v1"))
        throw new Error("generated column cannot have a default");
      validateEncoded(metadata.get(GENERATED_EXPRESSION_KEY)!, owner);
      if (!allowGenerated)
        throw new Error("generated columns unsupported by this catalog");
    }
    type(f.type);
  };
  schema.fields.forEach(field);
}
export function validateColumnExpressionsIpc(
  bytes: Uint8Array,
  owner: string[],
  allowGenerated = true,
): void {
  validateColumnExpressions(deserializeSchema(bytes), owner, allowGenerated);
}
