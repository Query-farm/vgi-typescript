// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// End-to-end test of VgiClient.catalogAttach({ options }) against
// vgi-python's vgi-example-attach-options-worker. Proves the
// TS client's options-bag serialization is wire-compatible with the
// reference Python implementation.
//
// The Python worker advertises attach_option_specs for ~20 Arrow types
// and echoes received options back via the `echo_attach_options` table
// function. We attach with specific overrides, call the echo function,
// and assert the values round-trip exactly.
//
// Skipped automatically when VGI_PYTHON_ATTACH_OPTIONS_HTTP_WORKER is unset.

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import {
  pythonAttachOptionsHttpWorkerAvailable,
  startPythonAttachOptionsHttpWorker,
  type PythonHttpWorkerHandle,
} from "./helpers/python-http-worker.js";
import type { VgiClient } from "../../index.js";

const skip = !pythonAttachOptionsHttpWorkerAvailable();

let handle: PythonHttpWorkerHandle;
let client: VgiClient;

beforeAll(async () => {
  if (skip) return;
  handle = await startPythonAttachOptionsHttpWorker();
  client = handle.client;
}, 10_000);

afterAll(async () => {
  if (skip) return;
  await handle?.stop();
});

// ============================================================================
// Pre-attach discovery: attach_option_specs is surfaced
// ============================================================================

describe.skipIf(skip)("VgiClient — attach-options worker discovery", () => {
  test("catalogsInfo() lists the attach_options catalog with spec bytes", async () => {
    const infos = await client.catalogsInfo();
    const ao = infos.find((i) => i.name === "attach_options");
    expect(ao).toBeDefined();
    // attach_option_specs is a list of IPC-serialized AttachOptionSpec
    // batches. We don't decode them here — just assert they're present.
    expect(ao!.attach_option_specs).toBeDefined();
    expect(ao!.attach_option_specs!.length).toBeGreaterThan(0);
    // At least the core primitive options must be advertised. The Python
    // worker declares ~20 specs covering int/uint widths, floats, string,
    // blob, temporal, decimal, list, struct.
    expect(ao!.attach_option_specs!.length).toBeGreaterThanOrEqual(15);
  });
});

// ============================================================================
// Round-trip: pass options, call echo_attach_options, verify values match
// ============================================================================

describe.skipIf(skip)("VgiClient — attach-options round-trip", () => {
  test("attach without overrides → echo returns declared defaults", async () => {
    const r = await client.catalogAttach("attach_options");
    try {
      const rows: Record<string, any>[] = [];
      for await (const batch of client.tableFunctionRows({
        functionName: "echo_attach_options",
        attachId: r.attach_id,
      })) {
        rows.push(...batch);
      }
      expect(rows).toHaveLength(1);
      const row = rows[0];
      // Defaults declared on the Python side.
      expect(row.opt_bool).toBe(true);
      expect(row.opt_string).toBe("hello");
      expect(Number(row.opt_int32)).toBe(-32);
      expect(Number(row.opt_int64)).toBe(-64);
    } finally {
      await client.catalogDetach(r.attach_id);
    }
  });

  test("attach with overrides → echo returns the overridden values", async () => {
    const r = await client.catalogAttach("attach_options", {
      options: {
        opt_bool: false,
        opt_int32: 7,
        opt_int64: 9_999_999_999n,
        opt_string: "world",
        opt_float64: 3.25,
      },
    });
    try {
      const rows: Record<string, any>[] = [];
      for await (const batch of client.tableFunctionRows({
        functionName: "echo_attach_options",
        attachId: r.attach_id,
      })) {
        rows.push(...batch);
      }
      expect(rows).toHaveLength(1);
      const row = rows[0];
      expect(row.opt_bool).toBe(false);
      expect(Number(row.opt_int32)).toBe(7);
      expect(BigInt(row.opt_int64)).toBe(9_999_999_999n);
      expect(row.opt_string).toBe("world");
      expect(row.opt_float64).toBe(3.25);
    } finally {
      await client.catalogDetach(r.attach_id);
    }
  });

  test("two attaches carry independent options — no cross-contamination", async () => {
    const a = await client.catalogAttach("attach_options", {
      options: { opt_int32: 11, opt_string: "a" },
    });
    const b = await client.catalogAttach("attach_options", {
      options: { opt_int32: 22, opt_string: "b" },
    });
    try {
      const echo = async (attachId: Uint8Array) => {
        const rows: Record<string, any>[] = [];
        for await (const batch of client.tableFunctionRows({
          functionName: "echo_attach_options",
          attachId,
        })) {
          rows.push(...batch);
        }
        return rows[0];
      };
      const ra = await echo(a.attach_id);
      const rb = await echo(b.attach_id);
      expect(Number(ra.opt_int32)).toBe(11);
      expect(ra.opt_string).toBe("a");
      expect(Number(rb.opt_int32)).toBe(22);
      expect(rb.opt_string).toBe("b");
    } finally {
      await client.catalogDetach(a.attach_id);
      await client.catalogDetach(b.attach_id);
    }
  });
});
