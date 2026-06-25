// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

import { describe, test, expect } from "bun:test";
import { int64, utf8 } from "../arrow/index.js";
import { VGI_DOC_KEY } from "../types.js";
import {
  type ArgumentSpec,
  argumentSpecsToSchema,
  schemaToArgumentSpecs,
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
