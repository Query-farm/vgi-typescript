// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// End-to-end tests of the ATTACH-time versioning protocol against
// vgi-python's `vgi-example-versioned-worker` (HTTP variant). Exercises
// the catalog_catalogs + catalog_attach wire paths — same contract the
// DuckDB C++ extension's integration/attach/versioning_http.test uses,
// but driven through VgiClient.
//
// Skipped automatically when VGI_PYTHON_VERSIONED_HTTP_WORKER isn't set.

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import {
  pythonVersionedHttpWorkerAvailable,
  startPythonVersionedHttpWorker,
  type PythonHttpWorkerHandle,
} from "./helpers/python-http-worker.js";
import type { VgiClient } from "../../index.js";

const skip = !pythonVersionedHttpWorkerAvailable();

let handle: PythonHttpWorkerHandle;
let client: VgiClient;

beforeAll(async () => {
  if (skip) return;
  handle = await startPythonVersionedHttpWorker();
  client = handle.client;
}, 10_000);

afterAll(async () => {
  if (skip) return;
  await handle?.stop();
});

// ============================================================================
// Discovery: catalogsInfo() advertises versions
// ============================================================================

describe.skipIf(skip)("VgiClient — versioned worker discovery", () => {
  test("catalogsInfo() advertises implementation_version + data_version_spec", async () => {
    const infos = await client.catalogsInfo();
    const versioned = infos.find((i) => i.name === "versioned");
    expect(versioned).toBeDefined();
    expect(versioned!.implementation_version).toBe("1.0.0");
    expect(versioned!.data_version_spec).toBe(">=1.0.0,<2.0.0");
  });

  test("catalogs() returns just the versioned catalog name", async () => {
    const names = await client.catalogs();
    expect(names).toContain("versioned");
  });
});

// ============================================================================
// Attach: default resolution, matching versions, rejection paths
// ============================================================================

describe.skipIf(skip)("VgiClient — versioned attach", () => {
  test("attach without versions resolves to defaults", async () => {
    const r = await client.catalogAttach("versioned");
    try {
      expect(r.resolved_implementation_version).toBe("1.0.0");
      expect(r.resolved_data_version).toBe("1.2.0");
      // Catalog metadata round-trip — comment was set on the Python side.
      expect(r.comment).toBe(
        "Example catalog demonstrating data_version_spec validation and cookie stickiness",
      );
      // No tags set on the versioned catalog — must be {}.
      expect(r.tags).toEqual({});
    } finally {
      await client.catalogDetach(r.attach_id);
    }
  });

  test("attach with matching data_version_spec echoes resolved value", async () => {
    const r = await client.catalogAttach("versioned", undefined, {
      dataVersionSpec: "1.1.0",
      implementationVersion: "1.0.0",
    });
    try {
      expect(r.resolved_data_version).toBe("1.1.0");
      expect(r.resolved_implementation_version).toBe("1.0.0");
    } finally {
      await client.catalogDetach(r.attach_id);
    }
  });

  test("attach with each supported data_version_spec echoes that value", async () => {
    for (const v of ["1.0.0", "1.1.0", "1.2.0"]) {
      const r = await client.catalogAttach("versioned", undefined, { dataVersionSpec: v });
      try {
        expect(r.resolved_data_version).toBe(v);
      } finally {
        await client.catalogDetach(r.attach_id);
      }
    }
  });

  test("attach with unsatisfiable data_version_spec rejects with server message", async () => {
    await expect(
      client.catalogAttach("versioned", undefined, { dataVersionSpec: "9.9.9" }),
    ).rejects.toThrow(/Unsupported data_version_spec/);
  });

  test("attach with unsatisfiable implementation_version rejects with server message", async () => {
    await expect(
      client.catalogAttach("versioned", undefined, { implementationVersion: "9.9.9" }),
    ).rejects.toThrow(/Unsupported implementation_version/);
  });

  test("attach with unknown catalog name rejects", async () => {
    await expect(client.catalogAttach("not-a-real-catalog")).rejects.toThrow();
  });

  test("two concurrent attach IDs are distinct", async () => {
    // Each attach allocates a fresh UUID-backed attach_id.
    const a = await client.catalogAttach("versioned");
    const b = await client.catalogAttach("versioned");
    try {
      // Compare bytes — two Uint8Arrays aren't === but content should differ.
      const sa = Array.from(a.attach_id).join(",");
      const sb = Array.from(b.attach_id).join(",");
      expect(sa).not.toBe(sb);
    } finally {
      await client.catalogDetach(a.attach_id);
      await client.catalogDetach(b.attach_id);
    }
  });

  test("attach with zero-byte options is coerced to null (no server crash)", async () => {
    // See client-catalog.test.ts for the regression rationale — zero-byte
    // options is not a valid IPC batch; the client coerces to null so
    // callers can pass `new Uint8Array(0)` as a natural "no options".
    const r = await client.catalogAttach("versioned", new Uint8Array(0), {
      dataVersionSpec: "1.0.0",
    });
    try {
      expect(r.resolved_data_version).toBe("1.0.0");
    } finally {
      await client.catalogDetach(r.attach_id);
    }
  });
});

// ============================================================================
// Schemas on versioned worker — it has a single "main" schema with no tables
// ============================================================================

describe.skipIf(skip)("VgiClient — versioned worker schemas", () => {
  test("schemas() returns the default 'main' schema", async () => {
    const r = await client.catalogAttach("versioned");
    try {
      const schemas = await client.schemas(r.attach_id);
      expect(schemas.map((s) => s.name)).toContain("main");
    } finally {
      await client.catalogDetach(r.attach_id);
    }
  });
});
