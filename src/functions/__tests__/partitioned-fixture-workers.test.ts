// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// The work-queue fixtures declare no more readers than they have work items.
//
// `partitioned_sequence` and its three order-mode clones enqueue one work item
// per 1000 rows at init and used to answer `max_workers: 99999`. The client
// caps that at DuckDB's thread count, so a 48-core host opened 48 streams for a
// 10-item scan, each extra one an init plus an empty drain -- several times the
// worker CPU the scan needs. They now answer `max(1, work items)`, as
// vgi-python's fixture does (6fbffc0).
//
// The request is hand-built (a primary init, as the client sends it); the
// answer is the real fixture's own `globalInit`.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { tableFunctions } from "../../../examples/table.js";
import { field, int64, schema } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import type { InitRequest } from "../../protocol/types.js";
import { FunctionType } from "../../types.js";
import { FunctionStorageSqlite, resolveStorageFromEnv, setStorage } from "../storage.js";

let dir: string;

beforeAll(() => {
  dir = mkdtempSync(join(tmpdir(), "vgi-partitioned-workers-"));
  setStorage(new FunctionStorageSqlite(join(dir, "state.db")));
});

afterAll(() => {
  setStorage(resolveStorageFromEnv());
  rmSync(dir, { recursive: true, force: true });
});

function primaryInit(name: string, count: number): InitRequest {
  return {
    bind_call: {
      function_name: name,
      arguments: new Arguments([count]),
      function_type: FunctionType.TABLE,
      input_schema: null,
      settings: null,
      secrets: null,
      attach_opaque_data: null,
      transaction_opaque_data: null,
      resolved_secrets_provided: false,
    },
    output_schema: schema([field("n", int64(), true)]),
    bind_opaque_data: null,
    split_tokens: null,
    split_payloads: null,
    row_limit: null,
    projection_ids: null,
    pushdown_filters: null,
    join_keys: [],
    phase: null,
    finalize_state_id: null,
    order_by_column_name: null,
    order_by_direction: null,
    order_by_null_order: null,
    order_by_limit: null,
    tablesample_percentage: null,
    tablesample_seed: null,
    execution_id: null,
    init_opaque_data: null,
    substream_id: null,
  };
}

describe("work-queue fixtures bound max_workers by their work items", () => {
  for (const name of [
    "partitioned_sequence",
    "partitioned_preserves_order",
    "partitioned_no_order_guarantee",
    "partitioned_fixed_order",
  ]) {
    test(name, async () => {
      const fn = tableFunctions.find((f) => f.meta.name === name)!;
      expect(fn).toBeDefined();
      // One work item per 1000 rows (the fixtures' CHUNK_SIZE).
      expect(Number((await fn.globalInit(primaryInit(name, 10_000))).max_workers)).toBe(10);
      expect(Number((await fn.globalInit(primaryInit(name, 2_500))).max_workers)).toBe(3);
      expect(Number((await fn.globalInit(primaryInit(name, 5))).max_workers)).toBe(1);
      // An empty scan still needs its one reader to drain the (empty) queue.
      expect(Number((await fn.globalInit(primaryInit(name, 0))).max_workers)).toBe(1);
    });
  }
});
