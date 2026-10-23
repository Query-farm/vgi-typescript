// Copyright 2026 Query Farm LLC - https://query.farm
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import type { RpcClient } from "@query-farm/vgi-rpc";
import { CatalogInterface } from "../../../catalog/interface.js";
import { VgiClient } from "../../../client/client.js";
import { encodeASD, decodeASD } from "../../../codec/asd.js";
import * as schemas from "../../../generated/vgi-protocol-schemas.js";
import { batchToScalarDict, deserializeBatch } from "../../../util/arrow/index.js";
import { catalogMacroHandlers } from "../catalog/macro.js";
import type { MacroCreateRequest } from "../../../generated/vgi-client.js";

const corpus = JSON.parse(readFileSync(new URL("../../../../conformance/catalog-contracts.json", import.meta.url), "utf8"));
test("macro create client and handler transport the complete canonical family", async () => {
  for (const id of ["macro_create_single", "macro_create"]) {
    const request = decodeASD<MacroCreateRequest>(schemas.MacroCreateRequestSchema,
      new Uint8Array(Buffer.from(corpus.records[id], "base64")));
    request.attach_opaque_data = new Uint8Array([1]);
    const calls: unknown[][] = [];
    const provider = Object.create(CatalogInterface.prototype) as CatalogInterface;
    provider.macroCreate = async (...args) => { calls.push(args); };
    const handler = catalogMacroHandlers(() => provider).catalogMacroCreate!;
    const client = new VgiClient({ async call(method: string, values: Record<string, unknown>) {
      expect(method).toBe("catalog_macro_create");
      const wire = batchToScalarDict(deserializeBatch(encodeASD(schemas.CatalogMacroCreateParamsSchema, values)));
      return await (handler as any)(wire, {});
    } } as unknown as RpcClient);
    await client.macroCreate(request.attach_opaque_data, request.schema_path, request.name,
      request.macro_type as any, request.overloads, request.on_conflict, request.transaction_opaque_data ?? undefined);
    expect(calls).toEqual([[request.attach_opaque_data, request.schema_path, request.name, request.macro_type,
      request.overloads, request.on_conflict, request.transaction_opaque_data ?? undefined]]);
  }
});
