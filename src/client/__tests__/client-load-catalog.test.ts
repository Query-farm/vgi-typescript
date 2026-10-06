// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// VgiClient.loadCatalog honors the catalog_contents capability: it uses
// catalog_contents when the attach result advertises it, falls back to the
// per-schema RPCs when it fails, never sends it when not advertised, and
// revalidates with if_none_match / not_modified. Runs the client over HTTP
// against the example worker's contents_* fixture catalogs and asserts which
// RPCs went on the wire.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { httpConnect, type RpcClient } from "@query-farm/vgi-rpc";
import { allFunctions } from "../../../examples/common.js";
import {
  BROKEN_MESSAGE,
  CATALOG_BROKEN,
  CATALOG_LEGACY,
  CATALOG_PROBE,
  CATALOG_REVAL,
  createCatalogContentsCatalogs,
} from "../../../examples/catalog_contents.js";
import { CompositeCatalogInterface } from "../../catalog/composite.js";
import { FunctionRegistry } from "../../functions/registry.js";
import { SIGNING_KEY_BYTES, serveVgiWorker } from "../../serve-entry.js";
import { VgiClient } from "../client.js";
import type { CatalogSnapshot } from "../types.js";

let server: ReturnType<typeof serveVgiWorker>;
let calls: string[] = [];

beforeAll(() => {
  const registry = new FunctionRegistry();
  for (const fn of allFunctions) registry.register(fn);
  server = serveVgiWorker({
    name: "contents",
    doc: "loadCatalog test worker.",
    version: "0.0.1",
    registry,
    catalogInterface: new CompositeCatalogInterface(createCatalogContentsCatalogs(registry)),
    prefix: "",
    port: 0,
    signingKey: new Uint8Array(SIGNING_KEY_BYTES).fill(7),
    quiet: true,
    env: {},
  });
});

afterAll(() => {
  server?.stop(true);
});

/** A client whose RpcClient records every method name it sends. */
function recordingClient(): VgiClient {
  const rpc = httpConnect(`http://localhost:${server.port}`, { prefix: "" });
  const recording = new Proxy(rpc, {
    get(target, prop, receiver) {
      const value = Reflect.get(target, prop, receiver);
      if (prop === "call" && typeof value === "function") {
        return (method: string, ...rest: unknown[]) => {
          calls.push(method);
          return (value as (...a: unknown[]) => unknown).call(target, method, ...rest);
        };
      }
      return typeof value === "function" ? value.bind(target) : value;
    },
  }) as RpcClient;
  return new VgiClient(recording);
}

async function attach(client: VgiClient, name: string) {
  const result = await client.catalogAttach(name);
  calls = [];
  return result;
}

function names(snapshot: CatalogSnapshot) {
  const main = snapshot.schemas.find((s) => s.schema.path.join(".") === "main")!;
  return {
    schemas: snapshot.schemas.map((s) => s.schema.path.join(".")).sort(),
    tables: main.tables.map((t) => t.name).sort(),
    views: main.views.map((v) => v.name),
    scalar: main.scalar_functions.map((f) => f.name),
    aggregate: main.aggregate_functions.map((f) => f.name),
    table: main.table_functions.map((f) => f.name),
    scalarMacros: main.scalar_macros.map((m) => m.name),
    tableMacros: main.table_macros.map((m) => m.name),
    indexes: main.indexes.length,
  };
}

const PER_SCHEMA = new Set([
  "catalog_schemas",
  "catalog_schema_contents_tables",
  "catalog_schema_contents_views",
  "catalog_schema_contents_functions",
  "catalog_schema_contents_macros",
  "catalog_schema_contents_indexes",
]);

describe("VgiClient.loadCatalog", () => {
  test("probe: one catalog_contents call decodes every kind", async () => {
    const client = recordingClient();
    const attached = await attach(client, CATALOG_PROBE);
    expect(attached.supports_catalog_contents).toBe(true);
    const snapshot = await client.loadCatalog(attached);
    expect(calls).toEqual(["catalog_contents"]);
    expect(snapshot.source).toBe("catalog_contents");
    expect(snapshot.fallback_error).toBeNull();
    expect(snapshot.etag).toBeNull();
    const n = names(snapshot);
    expect(n.schemas).toEqual(["extra", "main"]);
    expect(n.tables).toEqual(["ten"]);
    expect(n.views).toEqual(["answer"]);
    expect(n.scalar).toContain("double");
    expect(n.aggregate).toContain("vgi_sum");
    expect(n.table).toContain("sequence");
    expect(n.scalarMacros).toEqual(["contents_triple"]);
    expect(n.tableMacros).toEqual(["contents_range"]);
    // No etag: passing the snapshot back is an unconditional reload.
    calls = [];
    const again = await client.loadCatalog(attached, { previous: snapshot });
    expect(calls).toEqual(["catalog_contents"]);
    expect(again.not_modified).toBe(false);
  });

  test("probe: per-schema answers equal the catalog_contents answer", async () => {
    const client = recordingClient();
    const attached = await attach(client, CATALOG_PROBE);
    const bulk = await client.loadCatalog(attached);
    calls = [];
    const lazy = await client.loadCatalog(attached, { useCatalogContents: false });
    expect(calls).not.toContain("catalog_contents");
    expect(calls[0]).toBe("catalog_schemas");
    expect(lazy.source).toBe("per_schema");
    expect(lazy.schemas).toEqual(bulk.schemas);
  });

  test("legacy: not advertised, so catalog_contents is never sent", async () => {
    const client = recordingClient();
    const attached = await attach(client, CATALOG_LEGACY);
    expect(attached.supports_catalog_contents).toBe(false);
    const snapshot = await client.loadCatalog(attached);
    expect(calls).not.toContain("catalog_contents");
    expect(calls[0]).toBe("catalog_schemas");
    expect(calls.every((c) => PER_SCHEMA.has(c))).toBe(true);
    // estimated_object_count: "extra" has only tables, so its other kinds are skipped.
    expect(calls.filter((c) => c === "catalog_schema_contents_tables")).toHaveLength(2);
    expect(calls.filter((c) => c === "catalog_schema_contents_views")).toHaveLength(1);
    expect(snapshot.source).toBe("per_schema");
    expect(snapshot.fallback_error).toBeNull();
    const n = names(snapshot);
    expect(n.tables).toEqual(["ten"]);
    expect(n.scalarMacros).toEqual(["contents_triple"]);
    expect(n.tableMacros).toEqual(["contents_range"]);
    expect(n.aggregate).toContain("vgi_sum");
  });

  test("broken: catalog_contents fails, falls back to the per-schema RPCs", async () => {
    const client = recordingClient();
    const attached = await attach(client, CATALOG_BROKEN);
    expect(attached.supports_catalog_contents).toBe(true);
    const snapshot = await client.loadCatalog(attached);
    expect(calls[0]).toBe("catalog_contents");
    expect(calls[1]).toBe("catalog_schemas");
    expect(calls.slice(1).every((c) => PER_SCHEMA.has(c))).toBe(true);
    expect(snapshot.source).toBe("per_schema");
    expect(snapshot.fallback_error).toContain(BROKEN_MESSAGE);
    expect(names(snapshot).schemas).toEqual(["extra", "main"]);
    expect(names(snapshot).tables).toEqual(["ten"]);
  });

  test("reval: if_none_match -> not_modified keeps the snapshot; DDL gives a full answer", async () => {
    const client = recordingClient();
    const attached = await attach(client, CATALOG_REVAL);
    const first = await client.loadCatalog(attached);
    expect(calls).toEqual(["catalog_contents"]);
    expect(first.etag).toBe("gen-1");
    expect(first.catalog_version).toBe(1);

    calls = [];
    const same = await client.loadCatalog(attached, { previous: first });
    expect(calls).toEqual(["catalog_contents"]);
    expect(same.not_modified).toBe(true);
    expect(same.etag).toBe("gen-1");
    expect(same.schemas).toBe(first.schemas);

    await client.schemaCreate(attached.attach_opaque_data, ["added"]);
    calls = [];
    const changed = await client.loadCatalog(attached, { previous: same });
    expect(calls).toEqual(["catalog_contents"]);
    expect(changed.not_modified).toBe(false);
    expect(changed.etag).toBe("gen-2");
    expect(changed.catalog_version).toBe(2);
    expect(changed.schemas.map((s) => s.schema.path.join("."))).toContain("added");
  });

  test("a transaction-scoped load uses the per-schema RPCs", async () => {
    const client = recordingClient();
    const attached = await attach(client, CATALOG_PROBE);
    // catalog_contents takes no transaction, so a transaction-scoped load must
    // go per-schema. The read-only fixture has no transactions, so a made-up
    // id is rejected by the worker -- but only after the client chose the
    // per-schema path, which is what this asserts.
    await expect(
      client.loadCatalog(attached, { transactionOpaqueData: new Uint8Array([1]) }),
    ).rejects.toThrow("transaction_opaque_data");
    expect(calls).toEqual(["catalog_schemas"]);
  });
});
