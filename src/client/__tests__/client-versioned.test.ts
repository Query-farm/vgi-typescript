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
import { RpcError } from "vgi-rpc";
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

  test("attach with unsatisfiable data_version_spec rejects with actionable message", async () => {
    // Assert on the full error shape, not just a regex match:
    //  - surface is RpcError (so callers can programmatically distinguish
    //    server-side rejection from transport errors);
    //  - errorType is Python's ValueError, propagated untouched;
    //  - errorMessage identifies WHICH version was rejected AND lists the
    //    supported ones — users need both to self-correct.
    let caught: unknown;
    try {
      await client.catalogAttach("versioned", undefined, { dataVersionSpec: "9.9.9" });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(RpcError);
    const err = caught as RpcError;
    expect(err.errorType).toBe("ValueError");
    expect(err.errorMessage).toContain("Unsupported data_version_spec");
    expect(err.errorMessage).toContain("'9.9.9'");
    // Actionable: must mention what the worker DOES support.
    expect(err.errorMessage).toMatch(/1\.0\.0.*1\.1\.0.*1\.2\.0/);
  });

  test("attach with unsatisfiable implementation_version rejects with actionable message", async () => {
    let caught: unknown;
    try {
      await client.catalogAttach("versioned", undefined, { implementationVersion: "9.9.9" });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(RpcError);
    const err = caught as RpcError;
    expect(err.errorType).toBe("ValueError");
    expect(err.errorMessage).toContain("Unsupported implementation_version");
    expect(err.errorMessage).toContain("'9.9.9'");
    // The worker serves exactly one implementation version — the message
    // should say which one so users can downgrade their request.
    expect(err.errorMessage).toContain("'1.0.0'");
  });

  test("when BOTH data and implementation are bad, implementation error fires first", async () => {
    // The Python worker validates implementation_version BEFORE
    // data_version_spec. Pin that ordering so a future refactor doesn't
    // silently change the first error users see.
    let caught: unknown;
    try {
      await client.catalogAttach("versioned", undefined, {
        dataVersionSpec: "8.8.8",
        implementationVersion: "9.9.9",
      });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(RpcError);
    expect((caught as RpcError).errorMessage).toContain("Unsupported implementation_version");
    expect((caught as RpcError).errorMessage).not.toContain("data_version_spec");
  });

  test("version rejection errors carry a Python traceback for debuggability", async () => {
    // When production users hit a version mismatch we want the Python
    // traceback to come through — it pinpoints exactly where validation
    // fired and is invaluable during integration work.
    let caught: unknown;
    try {
      await client.catalogAttach("versioned", undefined, { dataVersionSpec: "9.9.9" });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(RpcError);
    const err = caught as RpcError;
    expect(err.remoteTraceback).toContain("Traceback");
    expect(err.remoteTraceback).toContain("Unsupported data_version_spec");
  });

  test("empty-string data_version_spec is rejected (not treated as null)", async () => {
    // ''-as-spec is a plausible mistake (string concat of an undefined
    // variable, falsy-but-truthy JS confusion, etc). It must reject
    // explicitly — we do NOT want the server to silently coerce to null
    // and accept with default resolved values.
    let caught: unknown;
    try {
      await client.catalogAttach("versioned", undefined, { dataVersionSpec: "" });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(RpcError);
    expect((caught as RpcError).errorMessage).toContain("Unsupported data_version_spec");
    expect((caught as RpcError).errorMessage).toContain("''");
  });

  test("whitespace-only data_version_spec is rejected", async () => {
    // Similar guard: a padded version from a config file must fail loudly
    // rather than silently falling through to the default.
    let caught: unknown;
    try {
      await client.catalogAttach("versioned", undefined, { dataVersionSpec: "   " });
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(RpcError);
    expect((caught as RpcError).errorMessage).toContain("Unsupported data_version_spec");
  });

  test("data_version_spec matching is case-sensitive and exact", async () => {
    // '1.0.0' is supported; '1.0.0 ' (trailing space) and 'v1.0.0' are not.
    // Pin the exact-match semantics — a forgiving matcher would be
    // user-friendly but would hide bugs where callers send stray data.
    for (const bad of ["1.0.0 ", " 1.0.0", "v1.0.0", "1.0", "1.0.0.0", "1,0,0"]) {
      let caught: unknown;
      try {
        await client.catalogAttach("versioned", undefined, { dataVersionSpec: bad });
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(RpcError);
      expect((caught as RpcError).errorMessage).toContain("Unsupported data_version_spec");
    }
  });

  test("implementation_version matching is case-sensitive and exact", async () => {
    for (const bad of ["1.0.0 ", " 1.0.0", "v1.0.0", "1.0", "1.0.1", "1.0.0.0"]) {
      let caught: unknown;
      try {
        await client.catalogAttach("versioned", undefined, { implementationVersion: bad });
      } catch (err) {
        caught = err;
      }
      expect(caught).toBeInstanceOf(RpcError);
      expect((caught as RpcError).errorMessage).toContain("Unsupported implementation_version");
    }
  });

  test("attach with unknown catalog name rejects with actionable message", async () => {
    let caught: unknown;
    try {
      await client.catalogAttach("not-a-real-catalog");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(RpcError);
    const err = caught as RpcError;
    // The versioned worker's catalog_attach starts with a name check that
    // raises ValueError with the requested name + the valid one.
    expect(err.errorType).toBe("ValueError");
    expect(err.errorMessage).toContain("not-a-real-catalog");
    expect(err.errorMessage).toContain("versioned");
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
