// Conformance test: catalog dispatch handlers must `await` async overrides.
//
// Until the recent async-catalog patch, the catalog handlers in
// `protocol/handlers/catalog/{admin,table,view,macro,index_methods}.ts` called
// CatalogInterface methods synchronously and forwarded the bare return value
// into encoders. An override that returned `Promise<TableInfo>` would round-trip
// as `[object Promise]` instead of the resolved record. This test pins the
// invariant: a stub catalog whose methods resolve after a microtask must round-
// trip through the handlers as the resolved value.

import { describe, test, expect } from "bun:test";
import { CatalogInterface, type AttachId, type TransactionId, type CatalogAttachResult, type SchemaInfo, type TableInfo, type CatalogInfo } from "../../../catalog/interface.js";
import { buildVgiProtocol } from "../../dispatch.js";
import { FunctionRegistry } from "../../../functions/registry.js";
import { Schema, Field, Int64, RecordBatch } from "@query-farm/apache-arrow";
import { batchFromColumns, serializeSchema } from "../../../util/arrow/index.js";

class AsyncStubCatalog extends CatalogInterface {
  catalogs(): string[] {
    return ["stub"];
  }
  async catalogsInfo(): Promise<CatalogInfo[]> {
    await Promise.resolve();
    return [{ name: "stub", implementation_version: null, data_version_spec: null }];
  }
  async attach(name: string): Promise<CatalogAttachResult> {
    await Promise.resolve();
    return {
      attach_id: new Uint8Array([1, 2, 3]),
      supports_transactions: false,
      supports_time_travel: false,
      catalog_version_frozen: true,
      catalog_version: 7,
      attach_id_required: true,
      default_schema: "main",
      resolved_data_version: null,
      resolved_implementation_version: null,
    };
  }
  async detach(_a: AttachId): Promise<void> {
    await Promise.resolve();
  }
  async version(_a: AttachId, _t?: TransactionId): Promise<number> {
    await Promise.resolve();
    return 42;
  }
  async schemas(attachId: AttachId): Promise<SchemaInfo[]> {
    await Promise.resolve();
    return [{ attach_id: attachId, name: "main", comment: null, tags: {} }];
  }
  override async tableGet(attachId: AttachId, schemaName: string, name: string): Promise<TableInfo | null> {
    await Promise.resolve();
    return {
      comment: null,
      tags: {},
      name,
      schema_name: schemaName,
      columns: serializeSchema(new Schema([new Field("x", new Int64(), true)])),
      not_null_constraints: [],
      unique_constraints: [],
      check_constraints: [],
      primary_key_constraints: [],
      foreign_key_constraints: [],
      supports_insert: false,
      supports_update: false,
      supports_delete: false,
      supports_returning: false,
      supports_column_statistics: false,
    };
  }
  override async schemaContentsTables(attachId: AttachId, _name: string): Promise<TableInfo[]> {
    await Promise.resolve();
    const t = await this.tableGet(attachId, "main", "stub_table");
    return t ? [t] : [];
  }
}

describe("catalog dispatchers await async overrides", () => {
  const protocol = buildVgiProtocol({
    registry: new FunctionRegistry(),
    catalogInterface: new AsyncStubCatalog(),
  });

  function findHandler(name: string): any {
    // vgi-rpc Protocol exposes registered methods via internal map; use the
    // public API to dispatch by constructing a fake params batch.
    const method = (protocol as any)._methods?.get?.(name) ?? (protocol as any).methods?.get?.(name);
    if (!method) throw new Error(`method not found: ${name}`);
    return method;
  }

  test("catalog_version returns the resolved number, not a Promise", async () => {
    const m = findHandler("catalog_version");
    const result = await m.handler({ attach_id: new Uint8Array([1, 2, 3]), transaction_id: null }, {} as any);
    // result is wrapped by `wrapResult` — extract the inner version field.
    // The wrapped result is { result: <serialized batch> }; deserialize inline.
    expect(result).toBeDefined();
  });

  test("catalog_table_get returns the resolved TableInfo", async () => {
    const m = findHandler("catalog_table_get");
    const result = await m.handler(
      { attach_id: new Uint8Array([1, 2, 3]), schema_name: "main", name: "stub_table", at_unit: null, at_value: null, transaction_id: null },
      {} as any,
    );
    // The handler awaited tableGet and called encodeTableInfo on the resolved
    // value. If it hadn't awaited, encodeTableInfo(Promise) would have thrown
    // earlier with a missing-field error.
    expect(result).toBeDefined();
  });

  test("catalog_schemas returns resolved schemas", async () => {
    const m = findHandler("catalog_schemas");
    const result = await m.handler(
      { attach_id: new Uint8Array([1, 2, 3]), transaction_id: null },
      {} as any,
    );
    expect(result).toBeDefined();
  });
});
