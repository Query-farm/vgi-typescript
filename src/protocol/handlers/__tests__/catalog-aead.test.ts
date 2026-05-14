// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Worker-path test for catalog opaque-data AEAD sealing. Confirms that when a
// signing key is configured, the catalogUnary wrapper unwraps the
// attach_opaque_data envelope before the handler body runs — and rejects an
// envelope sealed for a different principal.

import { describe, test, expect } from "bun:test";
import type { AuthContext } from "vgi-rpc";
import {
  CatalogInterface,
  type AttachOpaqueData,
  type TransactionOpaqueData,
  type CatalogAttachResult,
  type SchemaInfo,
} from "../../../catalog/interface.js";
import { buildVgiProtocol } from "../../dispatch.js";
import { FunctionRegistry } from "../../../functions/registry.js";
import { sealBytes, attachAad, ATTACH_ENVELOPE_VERSION, OpaqueDataRejectedError } from "../../../crypto.js";

const SIGNING_KEY = new TextEncoder().encode("a-worker-signing-key-for-tests!!");
const PLAINTEXT_ATTACH = new TextEncoder().encode("stub-catalog");

function authCtx(domain: string, principal: string): { auth: AuthContext } {
  return { auth: { domain, principal, authenticated: true } as AuthContext };
}

// Records the attach_opaque_data the handler actually passed through.
class RecordingCatalog extends CatalogInterface {
  lastDetachArg: Uint8Array | null = null;
  catalogs(): string[] {
    return ["stub"];
  }
  async attach(): Promise<CatalogAttachResult> {
    return {
      attach_opaque_data: PLAINTEXT_ATTACH,
      supports_transactions: false,
      supports_time_travel: false,
      catalog_version_frozen: true,
      catalog_version: 1,
      attach_opaque_data_required: true,
      default_schema: "main",
      settings: [],
      secret_types: [],
      tags: {},
      supports_column_statistics: false,
      resolved_data_version: null,
      resolved_implementation_version: null,
    };
  }
  async detach(a: AttachOpaqueData): Promise<void> {
    this.lastDetachArg = a;
  }
  async version(_a: AttachOpaqueData, _t?: TransactionOpaqueData): Promise<number> {
    return 1;
  }
  async schemas(_a: AttachOpaqueData): Promise<SchemaInfo[]> {
    return [];
  }
}

describe("catalog opaque-data AEAD (worker path)", () => {
  test("catalogUnary unwraps the sealed envelope before the handler runs", async () => {
    const catalog = new RecordingCatalog();
    const protocol = buildVgiProtocol({
      registry: new FunctionRegistry(),
      catalogInterface: catalog,
      signingKey: SIGNING_KEY,
    });
    const detach = (protocol as any)._methods.get("catalog_detach");

    // Seal a value for alice, then present it as alice — the handler must
    // receive the *plaintext*.
    const alice = authCtx("test", "alice");
    const sealed = await sealBytes(
      PLAINTEXT_ATTACH,
      SIGNING_KEY,
      attachAad(alice.auth),
      ATTACH_ENVELOPE_VERSION,
    );
    await detach.handler({ attach_opaque_data: sealed }, alice);
    expect(catalog.lastDetachArg).not.toBeNull();
    expect(new TextDecoder().decode(catalog.lastDetachArg!)).toBe("stub-catalog");

    // A different principal cannot open alice's envelope.
    catalog.lastDetachArg = null;
    await expect(
      detach.handler({ attach_opaque_data: sealed }, authCtx("test", "bob")),
    ).rejects.toBeInstanceOf(OpaqueDataRejectedError);
    expect(catalog.lastDetachArg).toBeNull(); // handler body never ran
  });

  test("without a signing key, the envelope is passed through unsealed", async () => {
    const catalog = new RecordingCatalog();
    const protocol = buildVgiProtocol({
      registry: new FunctionRegistry(),
      catalogInterface: catalog,
      // no signingKey — subprocess / unix transport
    });
    const detach = (protocol as any)._methods.get("catalog_detach");
    await detach.handler({ attach_opaque_data: PLAINTEXT_ATTACH }, authCtx("test", "alice"));
    expect(new TextDecoder().decode(catalog.lastDetachArg!)).toBe("stub-catalog");
  });
});
