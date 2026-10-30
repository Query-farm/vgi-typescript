import { expect, test } from "bun:test";
import { buildVgiProtocol } from "../../dispatch.js";
import { wrapRequest } from "../../../client/protocol.js";
import { batchFromColumns } from "../../../util/arrow/index.js";
import { binary, field, int64, schema, utf8 } from "../../../arrow/index.js";
import { decodeASD, encodeASD } from "../../../codec/asd.js";
import { CatalogAttachResultSchema, ClientCapabilitiesSchema } from "../../../generated/vgi-protocol-schemas.js";
import { decodeCatalogCapabilities, encodeCatalogCapabilities } from "../../../generated/vgi-protocol-types.js";
import { FunctionRegistry } from "../../../functions/registry.js";
import { ReadOnlyCatalogInterface } from "../../../catalog/read-only.js";
import { constraintRecord, CONSTRAINT_RECORDS_READ } from "../../../catalog/constraints.js";
import type { CatalogAttachResult } from "../../../catalog/interface.js";

/** A pre-v3 override: forwards four arguments and silently drops clientCapabilities. */
class LegacyAttachCatalog extends ReadOnlyCatalogInterface {
  override async attach(name: string, options?: Record<string, unknown>,
    dataVersionSpec?: string | null, implementationVersion?: string | null): Promise<CatalogAttachResult> {
    return { ...(await super.attach(name, options, dataVersionSpec, implementationVersion)),
      resolved_data_version: "1" };
  }
}

test("catalog_attach negotiates from the request even when attach() drops clientCapabilities", async () => {
  const cat = new LegacyAttachCatalog({ name: "c", schemas: [{ name: "main", tables: [{
    name: "t", columns: schema([field("id", int64(), false)]),
    constraints: [constraintRecord({ schemaPath: ["main"], table: "t", id: "t.pk", kind: "PRIMARY_KEY",
      columns: [["id"]], enforcement: "ENFORCED", validation: "ALL_ROWS" })],
  }] }] }, new FunctionRegistry());
  const protocol = buildVgiProtocol({ registry: new FunctionRegistry(), catalogInterface: cat });
  const method = ((protocol as any)._methods?.get?.("catalog_attach") ?? (protocol as any).methods?.get?.("catalog_attach"));
  const offer = encodeASD(ClientCapabilitiesSchema, { engine: "test", native_formats: [], catalogs: [], can_stream: true,
    filter_encodings: [], query_pushdown: null,
    catalog: encodeCatalogCapabilities({ read: [CONSTRAINT_RECORDS_READ], ddl: [], write: [], transaction: [] }) });
  const request = wrapRequest(batchFromColumns(
    { name: ["c"], options: [null], data_version_spec: [null], implementation_version: [null], client_capabilities: [offer] },
    schema([field("name", utf8(), false), field("options", binary(), true), field("data_version_spec", utf8(), true),
      field("implementation_version", utf8(), true), field("client_capabilities", binary(), true)]),
  ));
  const result = decodeASD<CatalogAttachResult>(CatalogAttachResultSchema, (await method.handler(request, {} as any)).result);
  expect(result.resolved_data_version).toBe("1");
  expect(decodeCatalogCapabilities(result.selected_capabilities!).read).toEqual([CONSTRAINT_RECORDS_READ]);
});
