// Copyright 2026 Query Farm LLC - https://query.farm
// Canonical index definitions (`IndexInfo.definition` / `IndexInfo.options`).

import { encodeASD } from "../codec/asd.js";
import {
  CatalogObjectRefSchema, IndexDefinitionSchema, IndexKeySchema, OptionStateSchema,
} from "../generated/vgi-protocol-schemas.js";
import type { IndexConstraintType } from "../generated/vgi-client.js";

// A one-row, zero-column Arrow IPC stream: "no options". Fixed bytes so both
// Arrow backends produce the identical value.
const NO_OPTIONS_B64 =
  "/////zgAAAAQAAAAAAAKABAADgAHAAgACgAAAAAAAAEQAAAAAAAEAAgACAAAAAQACAAAAAQAAAAAAAAAAAAAAP////9QAAAAFAAAAAAAAAAAAAoADgAMAAcACAAKAAAAAAAAAxAAAAAEAAoAGAAMAAgABAAKAAAAFAAAABgAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD/////AAAAAA==";

/** The typed-value batch for an index with no options. */
export function emptyIndexOptions(): Uint8Array {
  return Uint8Array.from(atob(NO_OPTIONS_B64), (c) => c.charCodeAt(0));
}

export interface CanonicalIndexSpec {
  /** Schema path of the indexed table. */
  schemaPath: readonly string[];
  /** Indexed table. */
  table: string;
  /** Raw column paths, one per key, in key order. */
  columns: readonly (readonly string[])[];
  /** Provider algorithm name, e.g. "art" or "btree". */
  algorithm: string;
  /** What the index claims; DuckDB reads NONE without a constraint. Default NONE. */
  constraintType?: IndexConstraintType;
  /** The id of the PRIMARY_KEY/UNIQUE constraint record the index implements, if any. */
  associatedConstraintId?: string;
  /** Typed option values (a one-row IPC batch); default: no options. */
  options?: Uint8Array;
  /** Options the user set explicitly (names in `options`). */
  explicitOptionNames?: readonly string[];
}

/**
 * Encode an index's `IndexInfo.definition` (`IndexDefinition`) and
 * `IndexInfo.options` (`OptionState`) records.
 */
export function canonicalIndex(spec: CanonicalIndexSpec): { definition: Uint8Array; options: Uint8Array } {
  if (spec.columns.length === 0) throw new Error(`index on '${spec.table}' requires at least one key`);
  if (!spec.algorithm) throw new Error("index algorithm must be nonempty");
  const values = spec.options ?? emptyIndexOptions();
  return {
    definition: encodeASD(IndexDefinitionSchema, {
      table: encodeASD(CatalogObjectRefSchema, { kind: "table", schema_path: spec.schemaPath, name: spec.table, owner_table: null }),
      algorithm: spec.algorithm,
      keys: spec.columns.map((path) => encodeASD(IndexKeySchema, { column_path: path, expression: null })),
      options: values,
      constraint_type: spec.constraintType ?? "NONE",
      associated_constraint: spec.associatedConstraintId == null ? null
        : encodeASD(CatalogObjectRefSchema, { kind: "constraint", schema_path: spec.schemaPath, name: spec.associatedConstraintId, owner_table: spec.table }),
    }),
    options: encodeASD(OptionStateSchema, { values, explicit_names: spec.explicitOptionNames ?? [] }),
  };
}
