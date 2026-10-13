// Copyright 2025, 2026 Query Farm LLC - https://query.farm

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import {
  decodeCatalogCapabilities, encodeCatalogCapabilities,
  decodeOptionDefinitions, encodeOptionDefinitions,
  decodeOptionChanges, encodeOptionChanges,
  decodeOptionState, encodeOptionState,
  decodeRequiredFilterPaths, encodeRequiredFilterPaths,
} from "../vgi-protocol-types.js";

const corpus = JSON.parse(readFileSync(new URL("../../../conformance/catalog-contracts.json", import.meta.url), "utf8"));
const fixture = (name: string): Uint8Array => new Uint8Array(Buffer.from(corpus.records[name], "base64"));

describe("Python catalog contract corpus", () => {
  test("option changes preserve opaque Arrow metadata and distinguish absence", () => {
    for (const name of ["changes", "reset_only"]) {
      const first = decodeOptionChanges(fixture(name));
      const restored = decodeOptionChanges(encodeOptionChanges(first));
      expect(restored.reset_names).toEqual(["reset.me"]);
      expect(restored.set_values).toEqual(name === "changes" ? fixture("values") : null);
    }
  });

  test("readback retains explicit NULL overrides and empty one-row payloads", () => {
    for (const name of ["state", "empty_state"]) {
      const first = decodeOptionState(fixture(name));
      const restored = decodeOptionState(encodeOptionState(first));
      expect(restored.values).toEqual(fixture(name === "state" ? "values" : "empty_values"));
      expect(restored.explicit_names).toEqual(name === "state" ? ["optional"] : []);
    }
  });

  test("three list levels retain dotted names, nested paths, and OR groups", () => {
    for (const name of ["paths", "empty_paths"]) {
      const first = decodeRequiredFilterPaths(fixture(name));
      const restored = decodeRequiredFilterPaths(encodeRequiredFilterPaths(first));
      expect(restored.groups).toEqual(name === "paths"
        ? [[["a.b"], ["a", "b"]], [["record", 'literal."name']]] : []);
    }
  });
});

test("capability profiles and option definitions preserve populated and empty fields", () => {
  const capabilities = decodeCatalogCapabilities(encodeCatalogCapabilities(decodeCatalogCapabilities(fixture("capabilities"))));
  expect(capabilities.read).toEqual(["vgi.schema.options.read.v1"]);
  expect(capabilities.ddl).toEqual(["vgi.schema.options.create.v1"]);
  expect(decodeCatalogCapabilities(fixture("empty_capabilities"))).toEqual({ read: [], ddl: [], write: [], transaction: [] });
  const definitions = decodeOptionDefinitions(encodeOptionDefinitions(decodeOptionDefinitions(fixture("definitions"))));
  expect(definitions.defaults).toEqual(fixture("values"));
  expect(definitions.create_names).toEqual(["optional", "record"]);
  expect(definitions.alter_names).toEqual(["optional"]);
});
