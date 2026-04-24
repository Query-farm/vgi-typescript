// Versioned VGI example worker — TS port of vgi-python/vgi/examples/versioned.py.
//
// Advertises per-catalog implementation_version + data_version_spec via
// catalogsInfo() and validates the client's requested versions at ATTACH.
// Mirrors the Python side so vgi's integration/attach/versioning.test can
// run against either worker and observe identical behavior.
//
// Registered via bin/vgi-example-versioned-worker.

import {
  CatalogInterface,
  ReadOnlyCatalogInterface,
  Worker,
  type AttachId,
  type TransactionId,
  type CatalogAttachResult,
} from "../src/index.js";
import type { CatalogInfo } from "../src/index.js";
import type { SchemaInfo } from "../src/index.js";

const IMPLEMENTATION_VERSION = "1.0.0";
const DATA_VERSION_SPEC = ">=1.0.0,<2.0.0";
const SUPPORTED_DATA_VERSIONS = new Set(["1.0.0", "1.1.0", "1.2.0"]);
const DEFAULT_DATA_VERSION = "1.2.0";
const CATALOG_NAME = "versioned";

function randomAttachId(): Uint8Array {
  const buf = new Uint8Array(16);
  crypto.getRandomValues(buf);
  return buf;
}

class VersionedCatalog extends CatalogInterface {
  catalogs(): string[] {
    return [CATALOG_NAME];
  }

  catalogsInfo(): CatalogInfo[] {
    return [{
      name: CATALOG_NAME,
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
    if (name !== CATALOG_NAME) {
      throw new Error(`Unknown catalog: '${name}'. Available: ${CATALOG_NAME}`);
    }
    if (implementationVersion != null && implementationVersion !== IMPLEMENTATION_VERSION) {
      throw new Error(
        `Unsupported implementation_version '${implementationVersion}'; this worker serves '${IMPLEMENTATION_VERSION}'`,
      );
    }
    if (dataVersionSpec != null && !SUPPORTED_DATA_VERSIONS.has(dataVersionSpec)) {
      // Exact-match only (matches Python side). Real workers would parse a range.
      const supported = [...SUPPORTED_DATA_VERSIONS].sort().map((v) => `'${v}'`).join(", ");
      throw new Error(
        `Unsupported data_version_spec '${dataVersionSpec}'; this worker serves one of [${supported}]`,
      );
    }
    const resolvedDataVersion = dataVersionSpec ?? DEFAULT_DATA_VERSION;

    return {
      attach_id: randomAttachId(),
      supports_transactions: false,
      supports_time_travel: false,
      catalog_version_frozen: true,
      catalog_version: 1,
      attach_id_required: false,
      default_schema: "main",
      comment: "Example catalog demonstrating data_version_spec validation and cookie stickiness",
      tags: {},
      resolved_data_version: resolvedDataVersion,
      resolved_implementation_version: IMPLEMENTATION_VERSION,
    };
  }

  detach(_attachId: AttachId): void { /* no-op */ }

  version(_attachId: AttachId, _transactionId?: TransactionId): number {
    return 1;
  }

  schemas(_attachId: AttachId, _transactionId?: TransactionId): SchemaInfo[] {
    return [{
      attach_id: new Uint8Array(0),
      name: "main",
      comment: null,
      tags: {},
    }];
  }
}

// The CatalogInterface abstract class drives registration through its own
// default methods; no CatalogDescriptor needed since this worker has no
// tables/views/functions to advertise.
const worker = new Worker({
  catalogInterface: new VersionedCatalog(),
  catalogName: CATALOG_NAME,
});

worker.run();
