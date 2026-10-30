// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import { validateColumnExpressionsIpc } from "./column-expressions.js";
import type { VgiSchema } from "../arrow/index.js";
import { encodeTableInfo, type TableInfo } from "../generated/vgi-client.js";
import { servedTableInfo } from "./constraints.js";

function scalarText(value: string): void {
  for (let i = 0; i < value.length; i++) {
    const c = value.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff) {
      const d = value.charCodeAt(++i);
      if (!(d >= 0xdc00 && d <= 0xdfff))
        throw new Error("column metadata contains an unpaired surrogate");
    } else if (c >= 0xdc00 && c <= 0xdfff)
      throw new Error("column metadata contains an unpaired surrogate");
  }
}
function tags(value: string): void {
  let i = 0;
  const ws = () => {
    while (i < value.length && /[ \t\r\n]/.test(value[i])) i++;
  };
  const str = (): string => {
    ws();
    const start = i;
    if (value[i++] !== '"')
      throw new Error("column tags require string keys and values");
    while (i < value.length) {
      const c = value[i++];
      if (c === "\\") {
        i++;
        continue;
      }
      if (c === '"') {
        const result = JSON.parse(value.slice(start, i));
        scalarText(result);
        return result;
      }
    }
    throw new Error("unterminated column tag string");
  };
  ws();
  if (value[i++] !== "{") throw new Error("column tags must be a JSON object");
  ws();
  const seen = new Set<string>();
  if (value[i] !== "}")
    for (;;) {
      const key = str();
      if (seen.has(key)) throw new Error("duplicate column tag key");
      seen.add(key);
      ws();
      if (value[i++] !== ":") throw new Error("invalid column tags JSON");
      str();
      ws();
      if (value[i] !== ",") break;
      i++;
    }
  if (value[i++] !== "}") throw new Error("invalid column tags JSON");
  ws();
  if (i !== value.length) throw new Error("trailing column tags JSON");
}
function entry(key: string, value: string): void {
  if (key === "comment" || key === "tags") {
    scalarText(value);
    if (key === "tags") tags(value);
  }
}

/** Validate reserved metadata without replacing the original schema or values. */
export function validateColumnMetadata(schema: VgiSchema): void {
  const type = (t: any): void => {
    for (const f of t?.children ?? t?.fields ?? []) field(f);
    if (t?.dictionary) type(t.dictionary);
  };
  const field = (f: any): void => {
    for (const [k, v] of f.metadata ?? []) entry(k, v);
    type(f.type);
  };
  for (const f of schema.fields) field(f);
}

/** Inspect raw schema-message metadata before Arrow can replace malformed UTF-8.
 * FlatBuffer slots come from Arrow's Message, Schema, Field and KeyValue tables.
 * Only reserved field values are interpreted; all original IPC bytes stay intact.
 */
export function validateColumnMetadataIpc(bytes: Uint8Array): void {
  const input = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const prefix = input.getInt32(0, true) === -1 ? 8 : 4;
  const size = input.getInt32(prefix - 4, true);
  if (size <= 0 || prefix + size > bytes.length)
    throw new Error("invalid column schema IPC");
  const raw = bytes.subarray(prefix, prefix + size);
  const view = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
  const u32 = (p: number) => view.getUint32(p, true),
    i32 = (p: number) => view.getInt32(p, true),
    u16 = (p: number) => view.getUint16(p, true);
  const slot = (p: number, id: number): number => {
    const v = p - i32(p),
      o = 4 + id * 2;
    return o < u16(v) ? u16(v + o) : 0;
  };
  const ref = (p: number, id: number): number => {
    const o = slot(p, id);
    return o ? p + o + u32(p + o) : 0;
  };
  const vector = (p: number, id: number, fn: (p: number) => void): void => {
    const v = ref(p, id);
    if (!v) return;
    const n = u32(v);
    if (n > (raw.length - v - 4) / 4)
      throw new Error("invalid column metadata vector");
    for (let j = 0; j < n; j++) {
      const q = v + 4 + j * 4;
      fn(q + u32(q));
    }
  };
  const text = (p: number, id: number): string => {
    const v = ref(p, id);
    if (!v) return "";
    const n = u32(v);
    if (v + 4 + n > raw.length)
      throw new Error("invalid column metadata string");
    return new TextDecoder("utf-8", { fatal: true }).decode(
      raw.subarray(v + 4, v + 4 + n),
    );
  };
  const field = (p: number, depth: number): void => {
    if (depth > 64) throw new Error("column metadata nesting too deep");
    const seen = new Set<string>();
    vector(p, 6, (m) => {
      const k = text(m, 0);
      if (seen.has(k)) throw new Error("duplicate column metadata key");
      seen.add(k);
      if (k === "comment" || k === "tags") entry(k, text(m, 1));
    });
    vector(p, 5, (c) => field(c, depth + 1));
  };
  const message = u32(0),
    kind = slot(message, 1);
  if (!kind || view.getUint8(message + kind) !== 1)
    throw new Error("expected column schema message");
  const schema = ref(message, 2);
  if (!schema) throw new Error("missing column schema");
  vector(schema, 1, (f) => field(f, 0));
}
export function validateTableInfoColumns(info: TableInfo): void {
  if (info.columns.length) { validateColumnMetadataIpc(info.columns); validateColumnExpressionsIpc(info.columns, info.schema_path); }
}
/**
 * The TableInfo a worker sends: raw column metadata checked on the original
 * bytes, then the served schema (see `servedTableInfo`: constraint records
 * checked as DuckDB checks them, nullability derived from them, legacy
 * generated metadata translated), whose column expressions are checked last.
 */
export function servedValidatedTableInfo(info: TableInfo): TableInfo {
  if (info.columns.length) validateColumnMetadataIpc(info.columns);
  const served = servedTableInfo(info);
  if (served.columns.length) {
    try {
      validateColumnExpressionsIpc(served.columns, served.schema_path);
    } catch (error) {
      throw new Error(`Table '${served.name}': ${(error as Error).message}`);
    }
  }
  return served;
}
export function encodeValidatedTableInfo(info: TableInfo): Uint8Array {
  return encodeTableInfo(servedValidatedTableInfo(info));
}
