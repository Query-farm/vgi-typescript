// Extract ColumnStatistics by running queries against an in-process DuckDB.
//
// This mirrors vgi-python's catalog.duckdb_statistics.statistics_from_duckdb
// and is used by the example catalog to produce typed stats for tables whose
// data is known at worker startup (numbers, colors, geo_points, etc.). Stats
// for dynamically-computed/unknown data should instead be declared as literal
// ColumnStatisticsInput on the TableDescriptor.
//
// Optional dep: @duckdb/node-api is imported lazily so that workers which
// don't need DuckDB-backed stats don't pay the load cost.

import type { DataType } from "@query-farm/apache-arrow";
import { Int64, Float64, Utf8, Bool, Binary, Null } from "@query-farm/apache-arrow";
import type { ColumnStatistics } from "./statistics.js";

/**
 * Represents a column's logical type as reported by DuckDB's
 * `information_schema.columns.data_type`. We only need a small subset of the
 * DuckDB type universe to map to Arrow types for stats serialization.
 */
function duckdbTypeToArrow(duckdbType: string): DataType {
  const t = duckdbType.toUpperCase();
  if (t === "BIGINT" || t === "INTEGER" || t === "SMALLINT" || t === "TINYINT" ||
      t === "UBIGINT" || t === "UINTEGER" || t === "USMALLINT" || t === "UTINYINT") {
    return new Int64();
  }
  if (t === "DOUBLE" || t === "FLOAT" || t === "REAL" || t.startsWith("DECIMAL")) {
    return new Float64();
  }
  if (t === "VARCHAR" || t === "TEXT" || t.startsWith("VARCHAR(") || t.startsWith("CHAR(")) {
    return new Utf8();
  }
  if (t === "BOOLEAN" || t === "BOOL") {
    return new Bool();
  }
  if (t === "GEOMETRY") {
    // GEOMETRY stats use a string BOX(x1 y1, x2 y2) representation, so we
    // advertise Utf8 for min/max. The ST_Extent() value from DuckDB is
    // already a string in that form.
    return new Utf8();
  }
  if (t === "BLOB") {
    return new Binary();
  }
  // ENUM columns: stored as dictionary internally, but we return the
  // unwrapped string values. Anything starting with the enum name pattern
  // (e.g. ENUM('red','green')) maps to Utf8.
  if (t.startsWith("ENUM(") || t === "ENUM") {
    return new Utf8();
  }
  // Fallback: treat unknown types as Null so the row-level stats serializer
  // emits nulls for min/max and doesn't choke. Better than throwing.
  return new Null();
}

export interface StatisticsFromDuckDBOptions {
  /**
   * If set, run `LOAD <ext>` on the connection before extracting stats.
   * Used by the geo_points demo to install the spatial extension so
   * ST_Extent() / GEOMETRY are recognized.
   */
  loadExtensions?: string[];
}

/**
 * Connect to an in-process DuckDB, run the caller's setup SQL to build the
 * demo data, then extract per-column statistics for `tableName` and return a
 * `Record<columnName, ColumnStatistics>`.
 *
 * The setup callback receives a connection object with a `run(sql)` method
 * (Promise-returning) so callers can CREATE TABLE, LOAD extensions, etc.
 * without depending on the @duckdb/node-api surface directly.
 *
 * GEOMETRY columns are summarized via ST_Extent (requires the spatial
 * extension); min/max come back as "BOX(x1 y1, x2 y2)" strings.
 */
export async function statisticsFromDuckDB(
  tableName: string,
  setup: (conn: { run: (sql: string) => Promise<void> }) => Promise<void>,
  options?: StatisticsFromDuckDBOptions,
): Promise<Record<string, ColumnStatistics>> {
  // Lazy import so the module parse succeeds even if @duckdb/node-api is
  // absent. Throws a clear error only when callers actually invoke this.
  const duckdb = await import("@duckdb/node-api");
  const conn = await duckdb.DuckDBConnection.create();

  try {
    for (const ext of options?.loadExtensions ?? []) {
      await conn.run(`LOAD ${ext}`);
    }
    await setup({
      run: async (sql: string) => { await conn.run(sql); },
    });

    // Resolve column list + types from the DB catalog rather than Arrow —
    // DuckDB's information_schema gives us the exact type strings we need to
    // dispatch (GEOMETRY vs VARCHAR vs DECIMAL...) including ENUM expansions.
    const colResult = await conn.run(
      "SELECT column_name, data_type FROM information_schema.columns " +
      `WHERE table_name = '${tableName.replace(/'/g, "''")}' ORDER BY ordinal_position`,
    );
    const colChunks = await colResult.fetchAllChunks();
    const columns: Array<{ name: string; duckType: string }> = [];
    for (const chunk of colChunks) {
      for (const row of chunk.getRows()) {
        columns.push({ name: String(row[0]), duckType: String(row[1]) });
      }
    }

    const stats: Record<string, ColumnStatistics> = {};
    const quote = (ident: string) => `"${ident.replace(/"/g, '""')}"`;

    for (const { name, duckType } of columns) {
      const qCol = quote(name);
      const qTable = quote(tableName);
      const arrowType = duckdbTypeToArrow(duckType);
      const t = duckType.toUpperCase();

      if (t === "GEOMETRY") {
        // ST_Extent → {min_x, min_y, max_x, max_y}. Wrap in ST_AsText for a
        // portable BOX representation that matches the DuckDB repr the C++
        // side uses when formatting spatial stats.
        const r = await conn.run(
          `SELECT ST_AsText(ST_Extent_Agg(${qCol})), ` +
          `  bool_or(${qCol} IS NULL), bool_or(${qCol} IS NOT NULL), ` +
          `  count(DISTINCT ${qCol}) FROM ${qTable}`,
        );
        const chunk = (await r.fetchAllChunks())[0];
        const row = chunk?.getRows()[0] ?? [null, true, true, null];
        stats[name] = {
          columnName: name,
          arrowType,
          min: row[0] ?? null,
          max: row[0] ?? null,  // single BOX covers both ends for ST_Extent
          hasNull: Boolean(row[1]),
          hasNotNull: Boolean(row[2]),
          distinctCount: toBigIntOrNull(row[3]),
          containsUnicode: null,
          maxStringLength: null,
        };
        continue;
      }

      // For everything else, run a single multi-aggregate. VARCHAR columns
      // also collect contains_unicode and max_string_length.
      const isString = arrowType instanceof Utf8;
      const isEnum = t.startsWith("ENUM(") || t === "ENUM";
      const selectCol = isEnum ? `CAST(${qCol} AS VARCHAR)` : qCol;

      const cols = [
        `min(${selectCol})`,
        `max(${selectCol})`,
        `bool_or(${qCol} IS NULL)`,
        `bool_or(${qCol} IS NOT NULL)`,
        `count(DISTINCT ${qCol})`,
      ];
      if (isString || isEnum) {
        cols.push(
          `bool_or(regexp_matches(CAST(${qCol} AS VARCHAR), '[^\\x00-\\x7F]'))`,
          `max(length(CAST(${qCol} AS VARCHAR)))`,
        );
      }

      const r = await conn.run(`SELECT ${cols.join(", ")} FROM ${qTable}`);
      const chunk = (await r.fetchAllChunks())[0];
      const row = chunk?.getRows()[0] ?? [];

      stats[name] = {
        columnName: name,
        arrowType,
        min: coerceForArrow(row[0], arrowType),
        max: coerceForArrow(row[1], arrowType),
        hasNull: Boolean(row[2]),
        hasNotNull: Boolean(row[3]),
        distinctCount: toBigIntOrNull(row[4]),
        containsUnicode: isString || isEnum ? Boolean(row[5] ?? false) : null,
        maxStringLength:
          isString || isEnum ? toBigIntOrNull(row[6]) : null,
      };
    }

    return stats;
  } finally {
    await conn.disconnectSync();
  }
}

function toBigIntOrNull(v: any): bigint | null {
  if (v == null) return null;
  if (typeof v === "bigint") return v;
  if (typeof v === "number" && Number.isFinite(v)) return BigInt(v);
  return null;
}

// Normalize DuckDB's row values (typically BigInt for integers, number for
// doubles, string for VARCHAR) to the JS shape our Arrow vectorFromArray
// expects when the column is later serialized.
function coerceForArrow(v: any, arrowType: DataType): any {
  if (v == null) return null;
  if (arrowType instanceof Int64) {
    return typeof v === "bigint" ? v : BigInt(v);
  }
  if (arrowType instanceof Float64) {
    return typeof v === "number" ? v : Number(v);
  }
  if (arrowType instanceof Utf8) {
    return typeof v === "string" ? v : String(v);
  }
  return v;
}
