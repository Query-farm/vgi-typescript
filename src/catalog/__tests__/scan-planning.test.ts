// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Protocol 3.2 filtered scan planning: TableInfo.scan_planning validation,
// ScanBranch statistics, the tableScanBranchesPlan dispatch and mayMatch.

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import {
  CatalogInterface, buildScanBranchesResult,
  type AttachOpaqueData, type CatalogAttachResult, type SchemaInfo, type TableInfo, type TransactionOpaqueData,
} from "../interface.js";
import { ReadOnlyCatalogInterface } from "../read-only.js";
import { CompositeCatalogInterface } from "../composite.js";
import { servedValidatedTableInfo, validateTableInfoColumns } from "../column-metadata.js";
import {
  ScanPlanningFilters, filtersMayMatch, validateScanPlanning, type ScanCandidateStatistics,
} from "../scan-planning.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { buildVgiProtocol } from "../../protocol/dispatch.js";
import { decodeASD } from "../../codec/asd.js";
import { ScanBranchSchema, ScanBranchesResultSchema } from "../../generated/vgi-protocol-schemas.js";
import { dateDay, deserializeBatch, deserializeSchema, field, int64, schema, serializeSchema, utf8 } from "../../arrow/index.js";
import { serializeColumnStatistics, type ColumnStatistics } from "../../util/statistics.js";
import { defineTableFunction } from "../../functions/table.js";

const corpus = JSON.parse(readFileSync(new URL("../../../conformance/catalog-contracts.json", import.meta.url), "utf8"));
const fixture = (name: string): Uint8Array => new Uint8Array(Buffer.from(corpus.records[name], "base64"));

// WHERE event_date >= DATE '2026-10-03' AND n < 100
const COLUMNS = schema([field("event_date", dateDay(), true), field("n", int64(), true)]);
const FILTER_BYTES = fixture("scan_planning_pushdown_filters");
const day = (d: number) => new Date(Date.UTC(2026, 9, d));

function tableInfo(extra: Partial<TableInfo> = {}): TableInfo {
  return {
    comment: null, tags: {}, name: "events", schema_path: ["main"],
    columns: serializeSchema(COLUMNS), constraints: [], definition_revision: null,
    write_result_modes: {}, supports_column_statistics: false, required_filters: [],
    scan_planning: "bind", ...extra,
  } as TableInfo;
}

describe("scan_planning validation", () => {
  test("defaults to bind and accepts filtered", () => {
    expect(validateScanPlanning("t", undefined, true)).toBe("bind");
    expect(validateScanPlanning("t", "bind", true)).toBe("bind");
    expect(validateScanPlanning("t", "filtered", false)).toBe("filtered");
  });

  test("rejects unknown values and filtered + inline scan_function", () => {
    expect(() => validateScanPlanning("t", "lazy", false)).toThrow(/scan_planning must be one of/);
    expect(() => validateScanPlanning("t", "filtered", true)).toThrow(/cannot be combined with an inlined scan_function/);
  });

  test("served TableInfo defaults an absent scan_planning and validates", () => {
    const legacy = tableInfo();
    delete (legacy as any).scan_planning;
    expect(servedValidatedTableInfo(legacy).scan_planning).toBe("bind");
    expect(servedValidatedTableInfo(tableInfo({ scan_planning: "filtered" })).scan_planning).toBe("filtered");
    expect(() => servedValidatedTableInfo(tableInfo({ scan_planning: "eager" }))).toThrow(/scan_planning/);
    expect(() => servedValidatedTableInfo(tableInfo({ scan_planning: "filtered", scan_function: new Uint8Array([1]) })))
      .toThrow(/inlined scan_function/);
    expect(() => validateTableInfoColumns(tableInfo({ scan_planning: "filtered", scan_function: new Uint8Array([1]) })))
      .toThrow(/inlined scan_function/);
  });

  test("descriptor tables carry scanPlanning and refuse a filtered function table", async () => {
    const registry = new FunctionRegistry();
    const catalog = new ReadOnlyCatalogInterface({
      name: "c",
      schemas: [{ name: "main", tables: [{ name: "events", columns: COLUMNS, scanPlanning: "filtered" }, { name: "plain", columns: COLUMNS }] }],
    } as any, registry);
    const tables = await catalog.schemaContentsTables(new Uint8Array(), ["main"]);
    expect(tables.map((t) => [t.name, t.scan_planning])).toEqual([["events", "filtered"], ["plain", "bind"]]);

    const fn = defineTableFunction<Record<string, never>, null>({
      name: "seq_fn",
      description: "scan",
      onBind: () => ({ outputSchema: COLUMNS }),
      process: (_p, _s, out) => out.finish(),
    });
    expect(() => new ReadOnlyCatalogInterface({
      name: "c",
      schemas: [{ name: "main", tables: [{ name: "bad", function: fn, scanPlanning: "filtered" }] }],
    } as any, new FunctionRegistry())).toThrow(/requires columns and no function/);
    expect(() => new ReadOnlyCatalogInterface({
      name: "c",
      schemas: [{ name: "main", tables: [{ name: "bad", columns: COLUMNS, scanPlanning: "sometimes" }] }],
    } as any, new FunctionRegistry())).toThrow(/scan_planning must be one of/);
  });
});

describe("ScanBranch statistics", () => {
  const stats: ColumnStatistics[] = [{
    columnName: "event_date", arrowType: dateDay(), min: day(3), max: day(3),
    hasNull: false, hasNotNull: true, distinctCount: null, containsUnicode: null, maxStringLength: null,
  }];

  test("rowCount and columnStatistics round-trip through the branch record", () => {
    const result = buildScanBranchesResult([
      { functionName: "read_parquet", positionalArguments: [{ value: "x.parquet", type: utf8() }], rowCount: 10, columnStatistics: stats },
      { functionName: "read_parquet", positionalArguments: [{ value: "y.parquet", type: utf8() }] },
    ], ["parquet"]);
    const first = decodeASD<Record<string, any>>(ScanBranchSchema, result.branches[0]!);
    expect(Number(first.row_count)).toBe(10);
    expect(new Uint8Array(first.column_statistics)).toEqual(serializeColumnStatistics(stats));
    const batch = deserializeBatch(first.column_statistics);
    expect(batch.getChild("column_name")!.get(0)).toBe("event_date");
    const second = decodeASD<Record<string, any>>(ScanBranchSchema, result.branches[1]!);
    expect(second.row_count).toBeNull();
    expect(second.column_statistics).toBeNull();
  });

  test("validates rowCount and duplicate statistics columns", () => {
    expect(() => buildScanBranchesResult([{ functionName: "f", rowCount: -1 }])).toThrow(/rowCount must be >= 0/);
    expect(() => buildScanBranchesResult([{ functionName: "f", rowCount: 1.5 }])).toThrow(/integer/);
    expect(() => buildScanBranchesResult([{ functionName: "f", columnStatistics: [...stats, ...stats] }]))
      .toThrow(/more than once/);
  });

  test("zero branches encode", () => {
    expect(buildScanBranchesResult([], ["parquet"])).toEqual({ branches: [], required_extensions: ["parquet"] });
  });
});

describe("filtersMayMatch", () => {
  const decoded = () => new ScanPlanningFilters(FILTER_BYTES, () => COLUMNS).decode();
  const cases: [string, ScanCandidateStatistics[], boolean][] = [
    ["day before the range", [{ columnName: "event_date", min: day(1), max: day(2), hasNull: false }], false],
    ["day inside the range", [{ columnName: "event_date", min: day(1), max: day(3) }], true],
    ["n entirely >= 100", [{ columnName: "n", min: 100n, max: 500n }], false],
    ["n straddles 100", [{ columnName: "n", min: 50n, max: 500n }], true],
    ["n as number statistics", [{ columnName: "n", min: 100, max: 500 }], false],
    ["n entirely NULL", [{ columnName: "n", hasNotNull: false, hasNull: true }], false],
    ["no min/max", [{ columnName: "n", hasNull: true }], true],
    ["unknown column", [{ columnName: "other", min: 1, max: 2 }], true],
    ["no statistics", [], true],
    // date32's raw representation (days since the epoch) normalizes like a Date.
    ["raw date32 day counts before the range", [{ columnName: "event_date", min: 20727, max: 20728 }], false],
    ["raw date32 day count in the range", [{ columnName: "event_date", min: 20727, max: 20729 }], true],
    // A representation it cannot compare against the literal counts as "may match".
    ["incomparable representation", [{ columnName: "event_date", min: "a", max: "b", arrowType: utf8() }], true],
    ["string statistics on a date column", [{ columnName: "event_date", min: "2026-01-01", max: "2026-01-02" }], true],
  ];
  for (const [label, stats, expected] of cases) {
    test(label, async () => {
      expect(filtersMayMatch(await decoded(), stats)).toBe(expected);
    });
  }

  test("accepts a descriptor-style statistics record", async () => {
    expect(filtersMayMatch(await decoded(), { n: { columnName: "n", min: 100n, max: 500n } })).toBe(false);
  });

  test("decode resolves the columns once and caches", async () => {
    let calls = 0;
    const filters = new ScanPlanningFilters(FILTER_BYTES, async () => { calls++; return COLUMNS; });
    expect(filters.raw).toBe(FILTER_BYTES);
    await filters.mayMatch([]);
    await filters.decode();
    expect(calls).toBe(1);
  });

  test("an unresolvable table fails the decode", async () => {
    await expect(new ScanPlanningFilters(FILTER_BYTES, () => null).decode()).rejects.toThrow(/declared columns/);
  });
});

class PlanningCatalog extends CatalogInterface {
  seen: (ScanPlanningFilters | null)[] = [];
  tableGets = 0;
  catalogs(): string[] { return ["stub"]; }
  attach(): CatalogAttachResult { throw new Error("unused"); }
  detach(_a: AttachOpaqueData): void {}
  version(): number { return 1; }
  schemas(_attachOpaqueData: AttachOpaqueData): SchemaInfo[] {
    return [{ path: ["main"], comment: null, tags: {} }];
  }
  override async tableGet(): Promise<TableInfo | null> {
    this.tableGets++;
    return tableInfo({ scan_planning: "filtered" });
  }
  override async tableScanBranchesPlan(
    _a: AttachOpaqueData, _s: string[], _n: string, _u: string | undefined, _v: string | undefined,
    _t: TransactionOpaqueData | undefined, filters: ScanPlanningFilters | null,
  ): Promise<any> {
    this.seen.push(filters);
    const files = [1, 2, 3, 4, 5].map(day);
    const keep: Date[] = [];
    for (const d of files) {
      if (!filters || await filters.mayMatch([{ columnName: "event_date", min: d, max: d, hasNull: false }])) keep.push(d);
    }
    return buildScanBranchesResult(
      keep.map((d) => ({ functionName: "read_parquet", positionalArguments: [{ value: d.toISOString(), type: utf8() }], rowCount: 10 })),
      ["parquet"],
    );
  }
}

/** A catalog that only overrides the 3.1 hook. */
class LegacyBranchesCatalog extends PlanningCatalog {
  override tableScanBranchesPlan(...args: Parameters<CatalogInterface["tableScanBranchesPlan"]>): any {
    return CatalogInterface.prototype.tableScanBranchesPlan.apply(this, args);
  }
  override tableScanBranchesGet(): any {
    return buildScanBranchesResult([{ functionName: "legacy" }]);
  }
}

describe("catalog_table_scan_branches_get dispatch", () => {
  function handler(catalog: CatalogInterface) {
    const protocol = buildVgiProtocol({ registry: new FunctionRegistry(), catalogInterface: catalog });
    const method = (protocol as any)._methods?.get?.("catalog_table_scan_branches_get")
      ?? (protocol as any).methods?.get?.("catalog_table_scan_branches_get");
    return (params: Record<string, unknown>) => method.handler({
      attach_opaque_data: new Uint8Array([1]), schema_path: ["main"], name: "events",
      at_unit: null, at_value: null, transaction_opaque_data: null, ...params,
    }, {} as any);
  }
  const branches = (wrapped: any) => {
    const result = decodeASD<Record<string, any>>(ScanBranchesResultSchema, wrapped.result);
    return result.branches.map((b: Uint8Array) => decodeASD<Record<string, any>>(ScanBranchSchema, b));
  };

  test("passes the decoded filters to tableScanBranchesPlan", async () => {
    const catalog = new PlanningCatalog();
    const out = branches(await handler(catalog)({ pushdown_filters: FILTER_BYTES }));
    expect(out).toHaveLength(3); // 2026-10-03 .. 2026-10-05
    expect(catalog.seen[0]).toBeInstanceOf(ScanPlanningFilters);
    expect(catalog.tableGets).toBe(1);
    expect(deserializeSchema(serializeSchema(COLUMNS)).fields.map((f) => f.name)).toEqual(["event_date", "n"]);
  });

  test("null pushdown_filters reaches the hook as null, with no table_get", async () => {
    const catalog = new PlanningCatalog();
    expect(branches(await handler(catalog)({ pushdown_filters: null }))).toHaveLength(5);
    expect(catalog.seen).toEqual([null]);
    expect(catalog.tableGets).toBe(0);
  });

  test("a composite catalog routes the planning hook with its filters", async () => {
    const inner = new PlanningCatalog();
    const composite = new CompositeCatalogInterface([inner]);
    const out = branches(await handler(composite)({ attach_opaque_data: new Uint8Array([0]), pushdown_filters: FILTER_BYTES }));
    expect(out).toHaveLength(3);
    expect(inner.seen[0]).toBeInstanceOf(ScanPlanningFilters);
  });

  test("the default hook delegates to tableScanBranchesGet", async () => {
    const out = branches(await handler(new LegacyBranchesCatalog())({ pushdown_filters: FILTER_BYTES }));
    expect(out.map((b: any) => b.function_name)).toEqual(["legacy"]);
  });

  test("zero branches is a legal result", async () => {
    class Empty extends PlanningCatalog {
      override async tableScanBranchesPlan(): Promise<any> { return buildScanBranchesResult([]); }
    }
    expect(branches(await handler(new Empty())({ pushdown_filters: FILTER_BYTES }))).toEqual([]);
  });
});
