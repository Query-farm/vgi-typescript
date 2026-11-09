/**
 * A ReadOnlyCatalogInterface subclass may discover schemas and views from a
 * remote service: `schemas`, `schemaGet`, `schemaContentsViews` and `viewGet`
 * accept async overrides (the dispatcher awaits them), and the base `viewGet`
 * awaits an async `schemaContentsViews`.
 *
 * Before, these were typed synchronous, so an async override failed to compile
 * and had to be typed `any`; and the base `viewGet` called `.find` on the
 * Promise an async `schemaContentsViews` returned.
 */

import { describe, expect, test } from "bun:test";
import { FunctionRegistry } from "../../functions/registry.js";
import type { AttachOpaqueData, SchemaInfo, TransactionOpaqueData, ViewInfo } from "../interface.js";
import { ReadOnlyCatalogInterface } from "../read-only.js";

const ATTACH = new Uint8Array([1]);

class RemoteCatalog extends ReadOnlyCatalogInterface {
  constructor() {
    super({ name: "remote", schemas: [{ name: "main" }] }, new FunctionRegistry());
  }

  // No `any`: these must type-check as async overrides.
  override async schemas(attach: AttachOpaqueData, txn?: TransactionOpaqueData): Promise<SchemaInfo[]> {
    const base = await super.schemas(attach, txn);
    await Promise.resolve(); // stands in for a remote lookup
    return [...base, { ...base[0]!, path: ["discovered"] }];
  }

  override async schemaContentsViews(attach: AttachOpaqueData, path: string[]): Promise<ViewInfo[]> {
    await Promise.resolve();
    if (path[0] !== "discovered") return [];
    return [
      {
        name: "v",
        schema_path: path,
        comment: null,
        tags: {},
        definition: "SELECT 1 AS x",
        column_comments: {},
        aliases: [],
        security: "regular",
      } as ViewInfo,
    ];
  }
}

describe("ReadOnlyCatalogInterface async overrides", () => {
  test("async schemas() override is awaited by callers", async () => {
    const schemas = await new RemoteCatalog().schemas(ATTACH);
    expect(schemas.map((s) => s.path)).toEqual([["main"], ["discovered"]]);
  });

  test("base viewGet() awaits an async schemaContentsViews()", async () => {
    const catalog = new RemoteCatalog();
    expect((await catalog.viewGet(ATTACH, ["discovered"], "v"))?.definition).toBe("SELECT 1 AS x");
    expect(await catalog.viewGet(ATTACH, ["discovered"], "missing")).toBeNull();
    expect(await catalog.viewGet(ATTACH, ["main"], "v")).toBeNull();
  });
});
