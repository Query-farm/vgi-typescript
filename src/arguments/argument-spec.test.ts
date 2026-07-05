// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

import { describe, test, expect } from "bun:test";
import { int64, utf8 } from "../arrow/index.js";
import {
  VGI_DOC_KEY,
  VGI_DEFAULT_KEY,
  VGI_CHOICES_KEY,
  VGI_RANGE_KEY,
  VGI_PATTERN_KEY,
} from "../types.js";
import {
  type ArgumentSpec,
  argumentSpecsToSchema,
  schemaToArgumentSpecs,
  formatRange,
  constraintSpecFields,
} from "./argument-spec.js";

describe("ArgumentSpec vgi_doc", () => {
  test("vgi_doc round-trips through argumentSpecsToSchema/schemaToArgumentSpecs", () => {
    // Unicode doc exercises UTF-8 metadata encoding.
    const documentedDoc = "µ ≥ value — note";

    const specs: ArgumentSpec[] = [
      { name: "documented", position: 0, arrowType: int64(), doc: documentedDoc },
      { name: "plain", position: 1, arrowType: utf8() },
    ];

    const schema = argumentSpecsToSchema(specs);
    const decoded = schemaToArgumentSpecs(schema);

    const decodedByName = new Map(decoded.map((s) => [s.name, s]));

    // The documented arg's doc survives equal to the original.
    expect(decodedByName.get("documented")?.doc).toBe(documentedDoc);

    // Presence-only semantics: the field for the documented arg carries the
    // vgi_doc metadata key; the no-doc arg's field does NOT (matches
    // Python/Rust/Go/Java, which omit the key when empty).
    const fieldByName = new Map(schema.fields.map((f) => [f.name, f]));

    const documentedField = fieldByName.get("documented");
    const plainField = fieldByName.get("plain");

    expect(documentedField?.metadata.has(VGI_DOC_KEY)).toBe(true);
    expect(documentedField?.metadata.get(VGI_DOC_KEY)).toBe(documentedDoc);

    expect(plainField?.metadata.has(VGI_DOC_KEY)).toBe(false);
  });
});

describe("formatRange (interval notation)", () => {
  test("both inclusive bounds -> [low, high]", () => {
    expect(formatRange(0, 10, undefined, undefined)).toBe("[0, 10]");
  });

  test("exclusive lower, no upper -> (low, +inf)", () => {
    expect(formatRange(undefined, undefined, 0, undefined)).toBe("(0, +inf)");
  });

  test("inclusive lower, exclusive upper -> [low, high)", () => {
    expect(formatRange(1, undefined, undefined, 10)).toBe("[1, 10)");
  });

  test("exclusive lower, inclusive upper -> (low, high]", () => {
    expect(formatRange(undefined, 10, 0, undefined)).toBe("(0, 10]");
  });

  test("both exclusive -> (low, high)", () => {
    expect(formatRange(undefined, undefined, 0, 10)).toBe("(0, 10)");
  });

  test("open lower side -> (-inf, high]", () => {
    expect(formatRange(undefined, 100, undefined, undefined)).toBe("(-inf, 100]");
  });

  test("open lower side, exclusive upper -> (-inf, high)", () => {
    expect(formatRange(undefined, undefined, undefined, 100)).toBe("(-inf, 100)");
  });

  test("open upper side (inclusive lower) -> [low, +inf)", () => {
    expect(formatRange(5, undefined, undefined, undefined)).toBe("[5, +inf)");
  });

  test("gt wins over ge on the low side", () => {
    expect(formatRange(0, undefined, 1, undefined)).toBe("(1, +inf)");
  });

  test("lt wins over le on the high side", () => {
    expect(formatRange(undefined, 10, undefined, 9)).toBe("(-inf, 9)");
  });

  test("no bounds -> undefined (key omitted)", () => {
    expect(formatRange(undefined, undefined, undefined, undefined)).toBeUndefined();
  });

  test("integer bounds print without a trailing .0", () => {
    expect(formatRange(0, 10, undefined, undefined)).toBe("[0, 10]");
    expect(formatRange(0.5, 2.5, undefined, undefined)).toBe("[0.5, 2.5]");
  });
});

describe("constraintSpecFields (raw -> encoded)", () => {
  test("undefined constraints -> empty", () => {
    expect(constraintSpecFields(undefined)).toEqual({});
  });

  test("number default is JSON-encoded", () => {
    expect(constraintSpecFields({ default: 5 })).toEqual({ defaultJson: "5" });
  });

  test("string default is JSON-encoded (quoted)", () => {
    expect(constraintSpecFields({ default: "x" })).toEqual({ defaultJson: '"x"' });
  });

  test("boolean default is JSON-encoded", () => {
    expect(constraintSpecFields({ default: true })).toEqual({ defaultJson: "true" });
  });

  test("undefined default is omitted (required arg)", () => {
    expect(constraintSpecFields({}).defaultJson).toBeUndefined();
  });

  test("numeric choices -> JSON array", () => {
    expect(constraintSpecFields({ choices: [1, 2, 3] }).choicesJson).toBe("[1,2,3]");
  });

  test("string choices -> JSON array", () => {
    expect(constraintSpecFields({ choices: ["a", "b"] }).choicesJson).toBe('["a","b"]');
  });

  test("bounds collapse to a single range notation", () => {
    expect(constraintSpecFields({ ge: 0, le: 10 }).rangeNotation).toBe("[0, 10]");
  });

  test("pattern passes through as-is", () => {
    expect(constraintSpecFields({ pattern: "^[a-z]+$" }).pattern).toBe("^[a-z]+$");
  });

  test("all constraints together", () => {
    expect(
      constraintSpecFields({
        default: 3,
        choices: [1, 2, 3],
        ge: 1,
        lt: 10,
        pattern: "\\d+",
      }),
    ).toEqual({
      defaultJson: "3",
      choicesJson: "[1,2,3]",
      rangeNotation: "[1, 10)",
      pattern: "\\d+",
    });
  });
});

describe("ArgumentSpec constraint metadata emission + round-trip", () => {
  test("presence-only: only declared keys are emitted", () => {
    const specs: ArgumentSpec[] = [
      {
        name: "constrained",
        position: 0,
        arrowType: int64(),
        defaultJson: "5",
        choicesJson: "[1,2,3]",
        rangeNotation: "[0, 10]",
        pattern: "\\d+",
      },
      { name: "plain", position: 1, arrowType: utf8() },
    ];

    const schema = argumentSpecsToSchema(specs);
    const byName = new Map(schema.fields.map((f) => [f.name, f]));

    const constrained = byName.get("constrained")!;
    expect(constrained.metadata.get(VGI_DEFAULT_KEY)).toBe("5");
    expect(constrained.metadata.get(VGI_CHOICES_KEY)).toBe("[1,2,3]");
    expect(constrained.metadata.get(VGI_RANGE_KEY)).toBe("[0, 10]");
    expect(constrained.metadata.get(VGI_PATTERN_KEY)).toBe("\\d+");

    const plain = byName.get("plain")!;
    expect(plain.metadata.has(VGI_DEFAULT_KEY)).toBe(false);
    expect(plain.metadata.has(VGI_CHOICES_KEY)).toBe(false);
    expect(plain.metadata.has(VGI_RANGE_KEY)).toBe(false);
    expect(plain.metadata.has(VGI_PATTERN_KEY)).toBe(false);
  });

  test("partial constraints omit the absent keys", () => {
    const specs: ArgumentSpec[] = [
      { name: "onlyrange", position: 0, arrowType: int64(), rangeNotation: "(0, +inf)" },
    ];
    const field = argumentSpecsToSchema(specs).fields[0]!;
    expect(field.metadata.get(VGI_RANGE_KEY)).toBe("(0, +inf)");
    expect(field.metadata.has(VGI_DEFAULT_KEY)).toBe(false);
    expect(field.metadata.has(VGI_CHOICES_KEY)).toBe(false);
    expect(field.metadata.has(VGI_PATTERN_KEY)).toBe(false);
  });

  test("round-trips through argumentSpecsToSchema/schemaToArgumentSpecs", () => {
    const specs: ArgumentSpec[] = [
      {
        name: "constrained",
        position: 0,
        arrowType: int64(),
        defaultJson: "5",
        choicesJson: '["a","b"]',
        rangeNotation: "[1, 10)",
        pattern: "^x$",
      },
      { name: "plain", position: 1, arrowType: utf8() },
    ];

    const decoded = schemaToArgumentSpecs(argumentSpecsToSchema(specs));
    const byName = new Map(decoded.map((s) => [s.name, s]));

    const c = byName.get("constrained")!;
    expect(c.defaultJson).toBe("5");
    expect(c.choicesJson).toBe('["a","b"]');
    expect(c.rangeNotation).toBe("[1, 10)");
    expect(c.pattern).toBe("^x$");

    const p = byName.get("plain")!;
    expect(p.defaultJson).toBeUndefined();
    expect(p.choicesJson).toBeUndefined();
    expect(p.rangeNotation).toBeUndefined();
    expect(p.pattern).toBeUndefined();
  });

  test("end-to-end: scalar-style parameter with ge/le surfaces vgi_range", () => {
    // Mirrors the Python fixture's format_number(precision, ge=0, le=10).
    const specs: ArgumentSpec[] = [
      { name: "value", position: 0, arrowType: utf8() },
      {
        name: "precision",
        position: 1,
        arrowType: int64(),
        isConst: true,
        ...constraintSpecFields({ ge: 0, le: 10 }),
      },
    ];
    const field = argumentSpecsToSchema(specs).fields.find((f) => f.name === "precision")!;
    expect(field.metadata.get(VGI_RANGE_KEY)).toBe("[0, 10]");
  });
});
