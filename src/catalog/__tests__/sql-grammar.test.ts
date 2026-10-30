import { expect, test } from "bun:test";
import { validateColumnExpressionSql, validateNumericCheckSql } from "../sql-grammar.js";
import { canonicalIndex } from "../indexes.js";
import { decodeIndexDefinition } from "../../generated/vgi-protocol-types.js";

const generated = (sql: string) => validateColumnExpressionSql(sql, { usingColumns: true, generated: true });
const defaults = (sql: string) => validateColumnExpressionSql(sql, { usingColumns: false, generated: false });

test("generated grammar mirrors vgi_column_expression.cpp", () => {
  expect(generated("a + b * 2")).toEqual(["a", "b"]);
  expect(generated('upper("Name") || \'x\'')).toEqual(["Name"]);
  expect(generated("COALESCE(a, 0) > 1 AND NOT b IS NULL")).toEqual(["a", "b"]);
  expect(generated("CAST(a AS VARCHAR)")).toEqual(["a"]);
  expect(generated("a::BIGINT % -3")).toEqual(["a"]);
  for (const bad of ["a / 2", "a + 1.5", "a::DOUBLE", "now()", "s.a", "CASE WHEN a THEN 1 END", "a IN (1, 2)",
    "a LIKE 'x'", "TRY_CAST(a AS INT)", "main.abs(a)", "99999999999999999999 + a", "a BETWEEN 1 AND 2", "(a, b)", "DATE '2020-01-01'"])
    expect(() => generated(bad)).toThrow();
});

test("default grammar: constants only, floating literals allowed, no '/'", () => {
  expect(defaults("1.5 * 2")).toEqual([]);
  expect(() => defaults("1 / 2")).toThrow(/unsupported scalar function/);
  // '/' is accepted only for a USING expression (column references, not generated).
  expect(validateColumnExpressionSql("a / 2", { usingColumns: true, generated: false })).toEqual(["a"]);
  expect(defaults("-5")).toEqual([]);
  expect(defaults("'x' || 'y'".replace("||", "+"))).toEqual([]);
  expect(() => defaults("a + 1")).toThrow(/column references/);
  expect(() => defaults("'a' || 'b'")).toThrow(/unsupported scalar function/);
});

test("CHECK grammar mirrors ParseNumericCheck", () => {
  const seen: string[] = [];
  validateNumericCheckSql("price >= -1.5", (n) => seen.push(n));
  validateNumericCheckSql("(lo < hi)", (n) => seen.push(n));
  expect(seen).toEqual(["price", "lo", "hi"]);
  for (const bad of ["a > 0 AND b > 0", "a + 1 > 0", "a > 'x'", "a > NULL", "a IS NULL", "a BETWEEN 1 AND 2", "t.a > 0", "a"])
    expect(() => validateNumericCheckSql(bad, () => {})).toThrow();
});

test("canonicalIndex encodes the table, keys and constraint claim", () => {
  const { definition, options } = canonicalIndex({ schemaPath: ["main"], table: "t", columns: [["a"]], algorithm: "art" });
  const def = decodeIndexDefinition(definition);
  expect(def.algorithm).toBe("art");
  expect(def.constraint_type).toBe("NONE");
  expect(def.keys.length).toBe(1);
  expect(options.length).toBeGreaterThan(0);
});
