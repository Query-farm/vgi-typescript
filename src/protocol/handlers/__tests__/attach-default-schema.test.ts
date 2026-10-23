// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import type { RpcClient } from "@query-farm/vgi-rpc";
import { ReadOnlyCatalogInterface } from "../../../catalog/read-only.js";
import { VgiClient } from "../../../client/client.js";
import { decodeASD, encodeASD } from "../../../codec/asd.js";
import { FunctionRegistry } from "../../../functions/registry.js";
import { CatalogAttachResultSchema } from "../../../generated/vgi-protocol-schemas.js";
import { buildCatalogAttachResult } from "../../../generated/vgi-protocol-types.js";
import type { CatalogAttachResult } from "../../../catalog/interface.js";

const corpus = JSON.parse(readFileSync(new URL("../../../../conformance/catalog-contracts.json", import.meta.url), "utf8"));

test("attach codec and public client preserve null and raw default schema names", async () => {
  for (const [suffix, expected] of [["none", null], ["main", "main"], ["dotted", "sales.archive"]] as const) {
    const bytes = new Uint8Array(Buffer.from(corpus.records[`attach_default_schema_${suffix}`], "base64"));
    const decoded = decodeASD<CatalogAttachResult>(CatalogAttachResultSchema, bytes);
    expect(decoded.default_schema).toBe(expected);
    const encoded = encodeASD(CatalogAttachResultSchema, buildCatalogAttachResult(decoded));
    expect(decodeASD<CatalogAttachResult>(CatalogAttachResultSchema, encoded).default_schema).toBe(expected);
    const client = new VgiClient({
      async call(method: string) {
        expect(method).toBe("catalog_attach");
        return { result: encoded };
      },
    } as unknown as RpcClient);
    expect((await client.catalogAttach("example")).default_schema).toBe(expected);
  }
});

test("declarative defaults distinguish omitted, null and dotted names", async () => {
  for (const [value, expected] of [[undefined, "main"], [null, null], ["sales.archive", "sales.archive"]] as const) {
    const provider = new ReadOnlyCatalogInterface({ name: "example", schemas: [], defaultSchema: value }, new FunctionRegistry());
    const result = await provider.attach("example");
    expect(result.default_schema).toBe(expected);
    expect(decodeASD<CatalogAttachResult>(CatalogAttachResultSchema,
      encodeASD(CatalogAttachResultSchema, result)).default_schema).toBe(expected);
  }
  for (const value of ["", "bad\0name"]) {
    expect(() => new ReadOnlyCatalogInterface({ name: "example", schemas: [], defaultSchema: value }, new FunctionRegistry()))
      .toThrow("raw root identifier");
  }
});
