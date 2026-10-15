// Copyright 2026 Query Farm LLC - https://query.farm
// Metadata-only fixture declarations; no physical index or uniqueness is claimed.
import { makeData, RecordBatch, Schema, Struct, Table, tableToIPC } from "@query-farm/apache-arrow";
import { encodeASD } from "../src/codec/asd.js";
import { CatalogObjectRefSchema, IndexDefinitionSchema, IndexKeySchema, OptionStateSchema } from "../src/generated/vgi-protocol-schemas.js";

export function fixtureIndex(table: string, column: string, constraint: "NONE"):
  { definition: Uint8Array; options: Uint8Array } {
  const data = makeData({ type: new Struct([]), length: 1, nullCount: 0, children: [] });
  const values = tableToIPC(new Table([new RecordBatch(new Schema([]), data)]), "stream");
  return {
    definition: encodeASD(IndexDefinitionSchema, {
      table: encodeASD(CatalogObjectRefSchema, { kind: "table", schema_path: ["data"], name: table, owner_table: null }),
      algorithm: "fixture.metadata",
      keys: [encodeASD(IndexKeySchema, { column_path: [column], expression: null })],
      options: values,
      constraint_type: constraint,
      associated_constraint: null,
    }),
    options: encodeASD(OptionStateSchema, { values, explicit_names: [] }),
  };
}
