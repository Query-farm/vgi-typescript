// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Every wire record below is assembled from a hand-written key list against a
// schema that codegen owns. Nothing ties the two together, so a field added to
// the protocol lands in src/generated/vgi-protocol-schemas.ts and quietly
// misses the builder — and the failure that follows is remote from its cause.
//
// This is not hypothetical. `buildScanBranchesResult` supplied 7 of
// ScanBranchSchema's 9 columns because it was never updated when the two
// format_* fields were added. The missing `format_locations` is a LIST, and
// Arrow's assembler dereferences a list's `children[0]` while writing, so EVERY
// multi-branch table in the worker died with "Cannot read properties of
// undefined (reading 'slice')" — including tables that predated those fields
// entirely. `singleBranchResult`, its sibling shim, carried the identical bug.
// Java's PlanResponse marked two fields nullable where the protocol says
// non-null and the client rejected the whole response as an "out-of-date
// Apache Arrow schema".
//
// Two things go wrong, and each assertion below catches one of them:
//
//  1. The builder omits a column the schema declares. For a LIST that throws at
//     serialize; for a scalar it silently writes null and the client reads a
//     default forever. Caught by building the batch from the BUILDER's key set
//     (not the schema's) and by requiring every column of a fully-populated
//     record to come back non-null.
//  2. The builder writes against its own schema literal that has drifted from
//     the generated one — a wrong type, a flipped nullability, a reordered
//     field. Caught by comparing the round-tripped schema field-for-field.

import { describe, test, expect } from "bun:test";
import {
  type VgiSchema,
  type VgiDataType,
  schema as makeSchema,
  field as makeField,
  utf8,
  typeSignature,
  TypeId,
  batchFromColumns,
  serializeBatch,
  deserializeBatch,
} from "../arrow/index.js";
import * as generated from "../generated/vgi-protocol-schemas.js";
import {
  encodeCatalogInfo,
  encodeSchemaInfo,
  encodeTableInfo,
  encodeViewInfo,
  encodeFunctionInfo,
  encodeMacroInfo,
  encodeIndexInfo,
} from "../generated/vgi-client.js";
import {
  encodeCopyFromFormatInfo,
  encodeAttachCatalogInfo,
  encodeScanFunctionResult,
  buildScanBranchesResult,
  singleBranchResult,
} from "../catalog/interface.js";
import { scanSplitRow, SCAN_SPLIT_SCHEMA } from "../protocol/serializers/splits.js";

// --------------------------------------------------------------------------- //
// Fixtures
// --------------------------------------------------------------------------- //

/**
 * A non-null sample value for `type`.
 *
 * Deriving the fixture from the schema rather than hand-writing one per record
 * is deliberate: a field added to the protocol is then populated automatically,
 * so a failure means the BUILDER dropped it — which is the only thing this test
 * is trying to measure. A hand-written fixture would have to be updated in the
 * same edit as the builder, and then it could never catch the omission.
 */
function sampleValue(type: VgiDataType): unknown {
  const t = type as any;
  switch (type.typeId) {
    case TypeId.Bool:
      return true;
    case TypeId.Int:
      return (t.bitWidth ?? 32) === 64 ? 1n : 1;
    case TypeId.Float:
      return 1.5;
    case TypeId.Binary:
    case TypeId.LargeBinary:
      return new Uint8Array([0x01]);
    case TypeId.Utf8:
    case TypeId.LargeUtf8:
      return "x";
    case TypeId.Timestamp:
      // Canonical timestamps are integers in the type's unit, not Dates.
      return 1767322_000_000n;
    case TypeId.Dictionary:
      return sampleValue(t.dictionary as VgiDataType);
    case TypeId.List:
      return [sampleValue(t.children[0].type as VgiDataType)];
    case TypeId.Map: {
      // map<entries: struct<key, value>> — the codec takes a plain object.
      const entries = t.children[0].type.children;
      return { k: sampleValue(entries[1].type as VgiDataType) };
    }
    case TypeId.Struct:
      return Object.fromEntries(
        t.children.map((c: any) => [c.name, sampleValue(c.type as VgiDataType)]),
      );
    default:
      throw new Error(
        `sampleValue: no sample for type ${typeSignature(type)} — extend this switch ` +
          `so the record carrying it stays covered`,
      );
  }
}

/** A record with every field of `schema` populated with a non-null value. */
function sampleRow(schema: VgiSchema): Record<string, unknown> {
  return Object.fromEntries(schema.fields.map((f) => [f.name, sampleValue(f.type)]));
}

/**
 * Serialize a builder's row dict into IPC bytes, keyed on the ROW's own field
 * names.
 *
 * Production's `wrapResult` does the opposite — it iterates the schema and
 * fills `row[name] ?? null` — which is exactly why a row dict can drop a field
 * without anyone noticing. Driving off the row's keys here turns that silent
 * null into a schema mismatch the assertion below reports by name.
 */
function batchFromRowDict(row: Record<string, unknown>, reference: VgiSchema): Uint8Array {
  const fields = Object.keys(row).map(
    (name) =>
      reference.fields.find((f) => f.name === name) ??
      // Not in the reference schema: give it a placeholder so the comparison
      // reports the stray key instead of throwing here.
      makeField(name, utf8(), true),
  );
  const columns = Object.fromEntries(Object.keys(row).map((name) => [name, [row[name]]]));
  return serializeBatch(batchFromColumns(columns, makeSchema(fields)));
}

interface WireRecordCase {
  /** The dataclass name in the generated file's `// Origin: X` comment. */
  origin: string;
  /** Distinguishes several builders for one origin. */
  variant?: string;
  schema: VgiSchema;
  /** IPC bytes for one record with every field of `schema` populated. */
  build: () => Uint8Array;
  /**
   * Columns this builder writes null by construction, and why. Only for
   * builders whose input genuinely cannot carry the field — never as a way to
   * quiet a builder that simply forgot one.
   */
  allowNull?: string[];
}

/** A serialized inner arguments batch, as ScanBranch/ScanFunctionResult carry. */
const ARGUMENTS_BYTES = serializeBatch(
  batchFromColumns({ arg_0: ["s3://bucket/x.parquet"] }, makeSchema([makeField("arg_0", utf8(), true)])),
);

const CASES: WireRecordCase[] = [
  {
    origin: "CatalogInfo",
    schema: generated.CatalogInfoSchema,
    build: () => encodeCatalogInfo(sampleRow(generated.CatalogInfoSchema) as any),
  },
  {
    origin: "SchemaInfo",
    schema: generated.SchemaInfoSchema,
    build: () => encodeSchemaInfo(sampleRow(generated.SchemaInfoSchema) as any),
  },
  {
    origin: "TableInfo",
    schema: generated.TableInfoSchema,
    build: () => encodeTableInfo(sampleRow(generated.TableInfoSchema) as any),
  },
  {
    origin: "ViewInfo",
    schema: generated.ViewInfoSchema,
    build: () => encodeViewInfo(sampleRow(generated.ViewInfoSchema) as any),
  },
  {
    origin: "FunctionInfo",
    schema: generated.FunctionInfoSchema,
    build: () => encodeFunctionInfo(sampleRow(generated.FunctionInfoSchema) as any),
  },
  {
    origin: "MacroInfo",
    schema: generated.MacroInfoSchema,
    build: () => encodeMacroInfo(sampleRow(generated.MacroInfoSchema) as any),
  },
  {
    origin: "IndexInfo",
    schema: generated.IndexInfoSchema,
    build: () => encodeIndexInfo(sampleRow(generated.IndexInfoSchema) as any),
  },
  {
    // encodeCopyFromFormatInfo restates the field list in its own object
    // literal, so a field added to the schema is null until someone adds it
    // there too — the null-column assertion is what catches that.
    origin: "CopyFromFormatInfo",
    schema: generated.CopyFromFormatInfoSchema,
    build: () => encodeCopyFromFormatInfo(sampleRow(generated.CopyFromFormatInfoSchema) as any),
  },
  {
    origin: "AttachCatalogInfo",
    schema: generated.AttachCatalogInfoSchema,
    build: () => encodeAttachCatalogInfo(sampleRow(generated.AttachCatalogInfoSchema) as any),
  },
  {
    origin: "ScanFunctionResult",
    schema: generated.ScanFunctionResultSchema,
    build: () => encodeScanFunctionResult("read_parquet", ARGUMENTS_BYTES, ["parquet"]),
  },
  {
    origin: "ScanBranchesResult",
    schema: generated.ScanBranchesResultSchema,
    build: () =>
      batchFromRowDict(
        buildScanBranchesResult([{ functionName: "read_parquet" }], ["parquet"]),
        generated.ScanBranchesResultSchema,
      ),
  },
  {
    origin: "ScanBranch",
    variant: "buildScanBranchesResult",
    schema: generated.ScanBranchSchema,
    build: () =>
      buildScanBranchesResult([
        {
          functionName: "read_parquet",
          positionalArguments: [{ value: "s3://bucket/x.parquet", type: utf8() }],
          branchFilter: "ts >= '2026-01-01'",
          writable: true,
          formatName: "acme_csv",
          formatLocations: ["s3://bucket/a.csv"],
          formatOptions: new Uint8Array([1, 2, 3]),
          sourceCatalog: "lake",
          sourceSchema: "main",
          sourceTable: "events",
        },
      ]).branches[0],
  },
  {
    // The legacy single-source shim. It shipped with the identical 7-of-9
    // omission that broke buildScanBranchesResult, so it gets its own case
    // rather than riding on its sibling's coverage.
    origin: "ScanBranch",
    variant: "singleBranchResult",
    schema: generated.ScanBranchSchema,
    build: () =>
      singleBranchResult({
        function_name: "read_parquet",
        arguments: ARGUMENTS_BYTES,
        required_extensions: ["parquet"],
      }).branches[0],
    // The legacy wire result it adapts has a function name and arguments and
    // nothing else — there is no branch predicate or format to carry.
    allowNull: [
      "branch_filter",
      "source_catalog",
      "source_schema",
      "source_table",
      "format_name",
      "format_locations",
      "format_options",
    ],
  },
  {
    origin: "ScanSplit",
    schema: generated.ScanSplitSchema,
    build: () =>
      batchFromRowDict(
        scanSplitRow(
          {
            payload: new Uint8Array([0x01]),
            estimatedRows: 10,
            rowsExact: true,
            estimatedBytes: 1024,
            partitionBounds: new Uint8Array([0x02]),
            columnStatistics: new Uint8Array([0x03]),
            locationIds: [1, 2],
            startPosition: new Uint8Array([0x04]),
            endPosition: new Uint8Array([0x05]),
          },
          new Uint8Array([0x06]),
        ),
        SCAN_SPLIT_SCHEMA,
      ),
  },
];

/**
 * Records the generator emits that this SDK never builds, with the reason.
 * Keeping them here rather than ignoring unknown origins is the point of the
 * coverage guard: adding a record to the protocol forces a deliberate choice
 * between covering it and writing down why not.
 */
const NOT_BUILT_BY_TYPESCRIPT: Record<string, string> = {};

// --------------------------------------------------------------------------- //
// Tests
// --------------------------------------------------------------------------- //

describe("hand-built wire records match their generated schema", () => {
  for (const c of CASES) {
    const label = c.variant ? `${c.origin} (${c.variant})` : c.origin;
    test(label, () => {
      const batch = deserializeBatch(c.build());

      const got = batch.schema.fields.map((f) => f.name);
      const want = c.schema.fields.map((f) => f.name);
      // Report the column lists, not two whole schema dumps: a 39-field
      // FunctionInfo diff is unreadable and the only thing the reader needs is
      // which field moved.
      expect(got, `${label}: emitted columns must match the generated schema`).toEqual(want);

      for (let i = 0; i < want.length; i++) {
        const g = batch.schema.fields[i];
        const w = c.schema.fields[i];
        expect(
          typeSignature(g.type),
          `${label}: column "${w.name}" type must match the generated schema`,
        ).toBe(typeSignature(w.type));
        expect(
          g.nullable,
          `${label}: column "${w.name}" nullability must match the generated schema`,
        ).toBe(w.nullable);
      }

      expect(batch.numRows, `${label}: expected a single-row record`).toBe(1);
      for (const f of c.schema.fields) {
        if (c.allowNull?.includes(f.name)) continue;
        const col = batch.getChild(f.name);
        // The builder has a column for this field but never wrote the value
        // into it. On the wire that is indistinguishable from "the worker has
        // nothing to say", so the client reads a default and the feature
        // quietly does not exist.
        expect(
          col?.get(0),
          `${label}: column "${f.name}" is null even though the fixture populates every ` +
            `field — the builder is not wiring this field`,
        ).not.toBeNull();
      }
    });
  }
});

describe("every generated record schema is covered", () => {
  // Method-derived schemas read `// Origin: method 'x' result` and are excluded:
  // those are request/response envelopes the RPC layer assembles from a schema,
  // not records this SDK spells out by hand.
  const ORIGIN_PATTERN = /^\/\/ Origin: ([A-Za-z0-9_]+)$/gm;

  // A new record type FAILS here as unlisted rather than being picked up
  // automatically. Auto-coverage would need a generic builder per record, and
  // there isn't one — the builders take typed inputs, not schemas — so
  // "automatic" would in practice mean "skipped", which recreates the gap this
  // test exists to close.
  test("no record type is silently untested", async () => {
    const source = await Bun.file(
      new URL("../generated/vgi-protocol-schemas.ts", import.meta.url).pathname,
    ).text();
    const origins = [...source.matchAll(ORIGIN_PATTERN)].map((m) => m[1]);
    expect(
      origins.length,
      'found no "// Origin: X" markers — the generator\'s comment format changed and this ' +
        "coverage guard is now silently vacuous",
    ).toBeGreaterThan(0);

    const covered = new Set(CASES.map((c) => c.origin));
    const missing = origins
      .filter((o) => !covered.has(o) && !(o in NOT_BUILT_BY_TYPESCRIPT))
      .sort();
    expect(
      missing,
      "these appear in the generated schemas but no case builds them. Add a WireRecordCase, " +
        "or add the origin to NOT_BUILT_BY_TYPESCRIPT with the reason this SDK never " +
        "constructs it.",
    ).toEqual([]);

    // The excuse list has to stay honest too: an entry for a record the
    // generator no longer emits reads as coverage that does not exist.
    const declared = new Set(origins);
    for (const origin of Object.keys(NOT_BUILT_BY_TYPESCRIPT)) {
      expect(declared.has(origin), `NOT_BUILT_BY_TYPESCRIPT lists "${origin}", which the generated schemas no longer declare — drop the entry`).toBe(true);
      expect(covered.has(origin), `"${origin}" is both covered by a case and excused in NOT_BUILT_BY_TYPESCRIPT`).toBe(false);
    }
  });
});
