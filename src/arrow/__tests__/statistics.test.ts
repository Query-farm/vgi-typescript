// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Statistics min/max round-trip across BOTH Arrow backends. Phase 2 routed the
// sparse-union min/max column through the codec/canonical path so a date /
// timestamp / decimal stat is represented identically to ordinary column data
// and identically across arrow-js and flechette. Run both:
//
//   bun test src/arrow/__tests__/statistics.test.ts
//   bun --conditions=worker test src/arrow/__tests__/statistics.test.ts

import { describe, test, expect } from "bun:test";
import {
  buildStatisticsBatch,
  serializeBatch,
  deserializeBatch,
  readCanonicalValue,
  dateDay,
  dateMillisecond,
  timestamp,
  decimal128,
  int64,
  utf8,
  TimeUnit,
  backend,
  type ColumnStatistics,
  type VgiDataType,
} from "../index.js";

// Read one row of a deserialized stats min/max sparse-union in CANONICAL form,
// resolving the per-row child via the union typeIds/typeMap and the child's own
// canonical reader. Works on both backends (arrow-js Data vs flechette Data).
function unionCanonical(
  unionCol: any,
  rowIndex: number,
  childTypes: VgiDataType[],
): unknown {
  const data = unionCol?.data?.[0] ?? unionCol;
  const code = data.typeIds[rowIndex] as number;
  const typeMap = data.typeMap ?? data.type?.typeMap;
  const childIdx = typeMap ? typeMap[code] : code;
  const child = data.children[childIdx];
  const childType = childTypes[childIdx];
  // arrow-js child is a Data (read via { data: [child] }); flechette child Data
  // already exposes `.at`, which readCanonicalValue consumes directly.
  const colLike = typeof child?.at === "function" ? child : { data: [child] };
  return readCanonicalValue(childType, colLike, rowIndex);
}

describe(`statistics round-trip (backend=${backend.name})`, () => {
  test("date / timestamp / decimal min/max survive build -> IPC -> read", () => {
    const dateType = dateDay();
    const date64Type = dateMillisecond();
    const tsType = timestamp(TimeUnit.MICROSECOND);
    const decType = decimal128(38, 4);

    const stats: ColumnStatistics[] = [
      {
        columnName: "d32",
        arrowType: dateType,
        // rich Date min/max
        min: new Date("2020-01-01T00:00:00Z"),
        max: new Date("2021-06-15T00:00:00Z"),
        hasNull: false,
        hasNotNull: true,
        distinctCount: 3n,
        containsUnicode: null,
        maxStringLength: null,
      },
      {
        columnName: "d64",
        arrowType: date64Type,
        min: new Date("1999-12-31T00:00:00Z"),
        max: new Date("2000-01-01T00:00:00Z"),
        hasNull: false,
        hasNotNull: true,
        distinctCount: 2n,
        containsUnicode: null,
        maxStringLength: null,
      },
      {
        columnName: "ts",
        arrowType: tsType,
        // raw micros (bigint) — must NOT lose precision (the arrow-js
        // Vector.get bug this whole refactor exists to fix).
        min: 1_700_000_000_000_001n,
        max: 1_800_000_000_000_999n,
        hasNull: true,
        hasNotNull: true,
        distinctCount: null,
        containsUnicode: null,
        maxStringLength: null,
      },
      {
        columnName: "dec",
        arrowType: decType,
        // unscaled bigint
        min: 12_345n,
        max: 9_999_999_999_999n,
        hasNull: false,
        hasNotNull: true,
        distinctCount: 5n,
        containsUnicode: null,
        maxStringLength: null,
      },
    ];

    const batch = deserializeBatch(serializeBatch(buildStatisticsBatch(stats)));
    expect(batch.numRows).toBe(4);

    // Distinct child types in stable insertion order = the union's children.
    const childTypes: VgiDataType[] = [dateType, date64Type, tsType, decType];

    const minCol = batch.getChild("min");
    const maxCol = batch.getChild("max");

    // Canonical expectations: date32 -> days, date64 -> ms bigint, timestamp
    // -> micros bigint, decimal -> unscaled bigint.
    const MS_PER_DAY = 86_400_000;
    const d32min = Date.UTC(2020, 0, 1) / MS_PER_DAY;
    const d32max = Date.UTC(2021, 5, 15) / MS_PER_DAY;

    expect(unionCanonical(minCol, 0, childTypes)).toBe(d32min);
    expect(unionCanonical(maxCol, 0, childTypes)).toBe(d32max);

    expect(unionCanonical(minCol, 1, childTypes)).toBe(BigInt(Date.UTC(1999, 11, 31)));
    expect(unionCanonical(maxCol, 1, childTypes)).toBe(BigInt(Date.UTC(2000, 0, 1)));

    expect(unionCanonical(minCol, 2, childTypes)).toBe(1_700_000_000_000_001n);
    expect(unionCanonical(maxCol, 2, childTypes)).toBe(1_800_000_000_000_999n);

    expect(unionCanonical(minCol, 3, childTypes)).toBe(12_345n);
    expect(unionCanonical(maxCol, 3, childTypes)).toBe(9_999_999_999_999n);

    // Scalar side columns round-trip too (column_name, distinct_count).
    const nameCol = batch.getChild("column_name")!;
    const ddType = int64();
    const dcCol = batch.getChild("distinct_count")!;
    expect(readCanonicalValue(utf8() as unknown as VgiDataType, nameCol, 2)).toBe("ts");
    // distinct_count for row 0 is 3.
    expect(readCanonicalValue(ddType, dcCol, 0)).toBe(3n);
  });

  test("empty stats -> a readable 0-row batch", () => {
    const batch = deserializeBatch(serializeBatch(buildStatisticsBatch([])));
    expect(batch.numRows).toBe(0);
  });
});
