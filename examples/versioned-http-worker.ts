// HTTP variant of the versioned example worker — TS port of
// `vgi-example-versioned-worker --http --port 0` from vgi-python.
// Prints PORT:<n> to stdout for test discovery.

import { createHttpHandler, unpackStateToken } from "vgi-rpc";
import { arrowStateSerializer } from "../src/protocol/state-serializer.js";
import { FunctionRegistry } from "../src/functions/registry.js";
import { buildVgiProtocol } from "../src/protocol/dispatch.js";
import {
  CatalogInterface,
  type AttachId,
  type TransactionId,
  type CatalogAttachResult,
} from "../src/index.js";
import type { CatalogInfo, SchemaInfo } from "../src/index.js";

const IMPLEMENTATION_VERSION = "1.0.0";
const DATA_VERSION_SPEC = ">=1.0.0,<2.0.0";
const SUPPORTED_DATA_VERSIONS = new Set(["1.0.0", "1.1.0", "1.2.0"]);
const DEFAULT_DATA_VERSION = "1.2.0";
const CATALOG_NAME = "versioned";

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
      const supported = [...SUPPORTED_DATA_VERSIONS].sort().map((v) => `'${v}'`).join(", ");
      throw new Error(
        `Unsupported data_version_spec '${dataVersionSpec}'; this worker serves one of [${supported}]`,
      );
    }
    const resolvedDataVersion = dataVersionSpec ?? DEFAULT_DATA_VERSION;
    const attachId = new Uint8Array(16);
    crypto.getRandomValues(attachId);
    return {
      attach_id: attachId,
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
    return [{ attach_id: new Uint8Array(0), name: "main", comment: null, tags: {} }];
  }
}

const registry = new FunctionRegistry();
const signingKey = crypto.getRandomValues(new Uint8Array(32));
const tokenTtl = 3600;

const protocol = buildVgiProtocol({
  registry,
  catalogInterface: new VersionedCatalog(),
  catalogName: CATALOG_NAME,
  recoverExchangeState: (opaqueData: Uint8Array) => {
    const tokenString = new TextDecoder().decode(opaqueData);
    const unpacked = unpackStateToken(tokenString, signingKey, tokenTtl);
    return arrowStateSerializer.deserialize(unpacked.stateBytes);
  },
});

const handler = createHttpHandler(protocol, {
  prefix: "/vgi",
  serverId: "vgi-example-versioned-http",
  signingKey,
  tokenTtl,
  stateSerializer: arrowStateSerializer,
});

const server = Bun.serve({ port: 0, fetch: handler });
console.log(`PORT:${server.port}`);
