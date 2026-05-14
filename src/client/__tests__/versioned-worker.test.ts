// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Unit-level test of the VersionedCatalog attach/catalogs semantics. We
// exercise the CatalogInterface directly rather than through a real
// subprocess (bun:test interacts poorly with Bun.spawn stdin/stdout
// pipes — the end-to-end behaviour is covered by the DuckDB integration
// tests in ~/Development/vgi/test/sql/integration/attach/versioning*.test,
// wired into `make test` via VGI_VERSIONED_WORKER).

import { describe, test, expect } from "bun:test";
import { CatalogInterface, type CatalogInfo, type CatalogAttachResult, type AttachOpaqueData, type TransactionOpaqueData, type SchemaInfo } from "../../index.js";

const IMPLEMENTATION_VERSION = "1.0.0";
const DATA_VERSION_SPEC = ">=1.0.0,<2.0.0";
const SUPPORTED_DATA_VERSIONS = new Set(["1.0.0", "1.1.0", "1.2.0"]);
const DEFAULT_DATA_VERSION = "1.2.0";

// Inline copy of the VersionedCatalog logic so this test exercises the
// public API surface a user would build against, not a private import.
class VersionedCatalog extends CatalogInterface {
  catalogs(): string[] { return ["versioned"]; }
  catalogsInfo(): CatalogInfo[] {
    return [{
      name: "versioned",
      implementation_version: IMPLEMENTATION_VERSION,
      data_version_spec: DATA_VERSION_SPEC,
    }];
  }
  attach(
    name: string,
    _options?: any,
    dataVersionSpec?: string | null,
    implementationVersion?: string | null,
  ): CatalogAttachResult {
    if (name !== "versioned") throw new Error(`Unknown catalog: '${name}'`);
    if (implementationVersion != null && implementationVersion !== IMPLEMENTATION_VERSION) {
      throw new Error(`Unsupported implementation_version '${implementationVersion}'; this worker serves '${IMPLEMENTATION_VERSION}'`);
    }
    if (dataVersionSpec != null && !SUPPORTED_DATA_VERSIONS.has(dataVersionSpec)) {
      throw new Error(`Unsupported data_version_spec '${dataVersionSpec}'`);
    }
    const attachOpaqueData = new Uint8Array(16);
    crypto.getRandomValues(attachOpaqueData);
    return {
      attach_opaque_data: attachOpaqueData,
      supports_transactions: false,
      supports_time_travel: false,
      catalog_version_frozen: true,
      catalog_version: 1,
      attach_opaque_data_required: false,
      default_schema: "main",
      resolved_data_version: dataVersionSpec ?? DEFAULT_DATA_VERSION,
      resolved_implementation_version: IMPLEMENTATION_VERSION,
    };
  }
  detach(_attachOpaqueData: AttachOpaqueData): void {}
  version(_attachOpaqueData: AttachOpaqueData, _transactionOpaqueData?: TransactionOpaqueData): number { return 1; }
  schemas(_attachOpaqueData: AttachOpaqueData, _transactionOpaqueData?: TransactionOpaqueData): SchemaInfo[] { return []; }
}

describe("VersionedCatalog — attach protocol", () => {
  const cat = new VersionedCatalog();

  test("catalogsInfo() advertises implementation_version + data_version_spec", () => {
    const infos = cat.catalogsInfo();
    expect(infos).toHaveLength(1);
    expect(infos[0].name).toBe("versioned");
    expect(infos[0].implementation_version).toBe("1.0.0");
    expect(infos[0].data_version_spec).toBe(">=1.0.0,<2.0.0");
  });

  test("attach without versions resolves to defaults", () => {
    const result = cat.attach("versioned", undefined, null, null);
    expect(result.resolved_implementation_version).toBe("1.0.0");
    expect(result.resolved_data_version).toBe("1.2.0");
  });

  test("attach with matching versions echoes resolved values", () => {
    const result = cat.attach("versioned", undefined, "1.1.0", "1.0.0");
    expect(result.resolved_data_version).toBe("1.1.0");
    expect(result.resolved_implementation_version).toBe("1.0.0");
  });

  test("attach with unsatisfiable data_version_spec rejects", () => {
    expect(() => cat.attach("versioned", undefined, "9.9.9", null))
      .toThrow(/Unsupported data_version_spec/);
  });

  test("attach with unsatisfiable implementation_version rejects", () => {
    expect(() => cat.attach("versioned", undefined, null, "9.9.9"))
      .toThrow(/Unsupported implementation_version/);
  });

  test("two attaches at different data_version_spec keep distinct resolved values", () => {
    const a = cat.attach("versioned", undefined, "1.0.0", null);
    const b = cat.attach("versioned", undefined, "1.1.0", null);
    expect(a.resolved_data_version).toBe("1.0.0");
    expect(b.resolved_data_version).toBe("1.1.0");
    // Distinct attach_opaque_data values
    expect(a.attach_opaque_data).not.toEqual(b.attach_opaque_data);
  });
});
