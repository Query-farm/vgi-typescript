// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Catalog-wide collection streams, independent of delivery and attachment state.
import { servedValidatedTableInfo, validateTableInfoColumns } from "./column-metadata.js";
import { encodeASDCollection, decodeASDCollection } from "../codec/asd.js";
import {
  SchemaInfoSchema, TableInfoSchema, ViewInfoSchema, FunctionInfoSchema, MacroInfoSchema, IndexInfoSchema,
} from "../generated/vgi-protocol-schemas.js";
import type { CatalogContentsResponse, SchemaInfo, TableInfo, ViewInfo, FunctionInfo, MacroInfo, IndexInfo } from "../generated/vgi-client.js";
import type { SchemaContentsInfo } from "./interface.js";

// Preserve component boundaries even when a schema name contains delimiters.
const schemaPathKey = (path: readonly string[]) => JSON.stringify(path.map((part) => part.toLowerCase()));

export const CATALOG_STREAM_KINDS = ["schemas", "tables", "views", "functions", "macros", "indexes"] as const;
export type CatalogStreams = Pick<CatalogContentsResponse, typeof CATALOG_STREAM_KINDS[number]>;

export function encodeCatalogStreams(inventory: readonly SchemaContentsInfo[]): CatalogStreams {
  const tables = inventory.flatMap((s) => s.tables.map(servedValidatedTableInfo));
  const streams = {
    schemas: encodeASDCollection(SchemaInfoSchema, inventory.map((s) => s.schema)),
    tables: encodeASDCollection(TableInfoSchema, tables),
    views: encodeASDCollection(ViewInfoSchema, inventory.flatMap((s) => s.views)),
    functions: encodeASDCollection(FunctionInfoSchema, inventory.flatMap((s) => [...s.scalar_functions, ...s.aggregate_functions, ...s.table_functions])),
    macros: encodeASDCollection(MacroInfoSchema, inventory.flatMap((s) => [...s.scalar_macros, ...s.table_macros])),
    indexes: encodeASDCollection(IndexInfoSchema, inventory.flatMap((s) => s.indexes)),
  };
  return streams;
}

/** Preserve raw path components, overloads and every table-function subtype. */
export function decodeCatalogStreams(response: CatalogContentsResponse, ifNoneMatch?: string | null): SchemaContentsInfo[] {
  if (response.not_modified) {
    if (response.etag == null || (ifNoneMatch !== undefined && response.etag !== ifNoneMatch)
      || CATALOG_STREAM_KINDS.some((kind) => response[kind] != null)) {
      throw new Error("invalid catalog_contents not_modified response");
    }
    return [];
  }
  const infos = decodeASDCollection<SchemaInfo>(SchemaInfoSchema, response.schemas);
  const schemas: SchemaContentsInfo[] = [];
  const index = new Map<string, SchemaContentsInfo>();
  for (const schema of infos) {
    if (!Array.isArray(schema.path) || schema.path.length === 0) throw new Error("catalog schema has an empty path");
    const key = schemaPathKey(schema.path);
    if (index.has(key)) throw new Error("catalog contains duplicate schema paths");
    if (schema.path.length > 1 && !index.has(schemaPathKey(schema.path.slice(0, -1)))) {
      throw new Error("catalog schema is missing its preceding parent");
    }
    const entry: SchemaContentsInfo = { schema, tables: [], views: [], scalar_functions: [], aggregate_functions: [], table_functions: [], scalar_macros: [], table_macros: [], indexes: [] };
    index.set(key, entry);
    schemas.push(entry);
  }
  const owner = (item: { schema_path: string[] }): SchemaContentsInfo => {
    const entry = index.get(schemaPathKey(item.schema_path));
    if (!entry) throw new Error("catalog object references an absent schema");
    return entry;
  };
  for (const item of decodeASDCollection<TableInfo>(TableInfoSchema, response.tables)) {
    validateTableInfoColumns(item);
    owner(item).tables.push(item);
  }
  for (const item of decodeASDCollection<ViewInfo>(ViewInfoSchema, response.views)) owner(item).views.push(item);
  for (const item of decodeASDCollection<FunctionInfo>(FunctionInfoSchema, response.functions)) {
    const entry = owner(item);
    switch (String(item.function_type).toUpperCase()) {
      case "SCALAR": entry.scalar_functions.push(item); break;
      case "AGGREGATE": entry.aggregate_functions.push(item); break;
      case "TABLE": case "TABLE_BUFFERING": entry.table_functions.push(item); break;
      default: throw new Error(`unknown catalog function type ${item.function_type}`);
    }
  }
  for (const item of decodeASDCollection<MacroInfo>(MacroInfoSchema, response.macros)) {
    const entry = owner(item);
    switch (String(item.macro_type).toUpperCase()) {
      case "SCALAR": entry.scalar_macros.push(item); break;
      case "TABLE": entry.table_macros.push(item); break;
      default: throw new Error(`unknown catalog macro type ${item.macro_type}`);
    }
  }
  for (const item of decodeASDCollection<IndexInfo>(IndexInfoSchema, response.indexes)) owner(item).indexes.push(item);
  return schemas;
}
