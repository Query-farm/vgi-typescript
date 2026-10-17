// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Test-only wire checker: what a strict client (the DuckDB extension's
// generated record codecs) would reject in an encoded protocol record.
//
//  - a null in a column / struct child the schema calls non-nullable;
//  - a null list item or map value (except `argument_names`, the one
//    `list[str | None]` field in the protocol — see OPTIONAL_ITEM_FIELDS);
//  - a dictionary-encoded enum column whose value is not a member NAME of the
//    vgi-python enum (`function_type` must be `TABLE`, not `table`).

import { type VgiDataType, type VgiField, deserializeBatch, isDictionary, isList, isMap, isStruct } from "../../arrow/index.js";
import { OPTIONAL_ITEM_FIELDS } from "../../codec/conform.js";

/** vgi-python enum member names, by the wire field that carries them. */
export const ENUM_NAMES: Record<string, readonly string[]> = {
  function_type: ["SCALAR", "TABLE", "TABLE_BUFFERING", "AGGREGATE"],
  macro_type: ["SCALAR", "TABLE"],
  stability: ["CONSISTENT", "VOLATILE", "CONSISTENT_WITHIN_QUERY"],
  null_handling: ["DEFAULT", "SPECIAL"],
  order_preservation: ["PRESERVES_ORDER", "NO_ORDER_GUARANTEE", "FIXED_ORDER"],
  partition_kind: ["NOT_PARTITIONED", "SINGLE_VALUE_PARTITIONS", "OVERLAPPING_PARTITIONS", "DISJOINT_PARTITIONS"],
  order_dependent: ["ORDER_DEPENDENT", "NOT_ORDER_DEPENDENT"],
  distinct_dependent: ["DISTINCT_DEPENDENT", "NOT_DISTINCT_DEPENDENT"],
  constraint_type: ["NONE", "UNIQUE", "PRIMARY"],
  on_conflict: ["ERROR", "IGNORE", "REPLACE"],
  phase: ["INPUT", "FINALIZE", "TABLE_BUFFERING", "TABLE_BUFFERING_FINALIZE"],
  order_by_direction: ["ASC", "DESC"],
  order_by_null_order: ["NULLS_FIRST", "NULLS_LAST"],
};

function children(type: VgiDataType): VgiField[] {
  return ((type as any).children ?? []) as VgiField[];
}

function iter(raw: any): any[] {
  if (raw == null) return [];
  if (Array.isArray(raw)) return raw;
  // Not toArray(): for a primitive child it is a typed array, where a null
  // item reads as 0 and the violation disappears.
  if (typeof raw[Symbol.iterator] === "function") return Array.from(raw);
  return Object.entries(raw);
}

function mapEntries(raw: any): Array<[unknown, unknown]> {
  if (raw instanceof Map) return Array.from(raw.entries());
  if (typeof raw?.[Symbol.iterator] === "function") {
    return Array.from(raw as Iterable<any>, (e: any) => (Array.isArray(e) ? [e[0], e[1]] : [e?.key ?? e?.[0], e?.value ?? e?.[1]]));
  }
  return Object.entries(raw ?? {});
}

function walk(type: VgiDataType, raw: any, nullable: boolean, name: string, path: string, out: string[]): void {
  if (raw == null) {
    if (!nullable) out.push(`${path}: null in a non-nullable ${isList(type) ? "list" : isMap(type) ? "map" : "column"}`);
    return;
  }
  if (isDictionary(type)) {
    const allowed = ENUM_NAMES[name];
    if (allowed && !allowed.includes(String(raw))) out.push(`${path}: enum value '${String(raw)}' is not a ${name} member name`);
    return;
  }
  if (isList(type)) {
    const item = children(type)[0];
    iter(raw).forEach((v, i) => walk(item.type, v, OPTIONAL_ITEM_FIELDS.has(name), name, `${path}[${i}]`, out));
    return;
  }
  if (isMap(type)) {
    const valueType = children(children(type)[0].type)[1].type;
    for (const [k, v] of mapEntries(raw)) walk(valueType, v, false, name, `${path}{${String(k)}}`, out);
    return;
  }
  if (isStruct(type)) {
    for (const c of children(type)) walk(c.type, raw[c.name], c.nullable, c.name, `${path}.${c.name}`, out);
  }
}

/** Every strict-client violation in an encoded record batch (all rows). */
export function wireViolations(bytes: Uint8Array, record = "record"): string[] {
  const batch = deserializeBatch(bytes);
  const out: string[] = [];
  for (const f of batch.schema.fields) {
    const col = batch.getChild(f.name);
    for (let r = 0; r < batch.numRows; r++) {
      walk(f.type, col ? col.get(r) : null, f.nullable, f.name, `${record}.${f.name}${batch.numRows > 1 ? `#${r}` : ""}`, out);
    }
  }
  return out;
}
