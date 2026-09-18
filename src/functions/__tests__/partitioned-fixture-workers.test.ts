// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// The work-queue fixtures declare no more readers than they have work items.
//
// Every fixture that hands its work out through the shared queue enqueues its
// items at init and used to answer `max_workers: 99999`. The client caps that
// at DuckDB's thread count, so a 48-core host opened 48 streams for a 10-item
// scan, each extra one an init plus an empty drain -- several times the worker
// CPU the scan needs. They now answer `max(1, work items)`, as vgi-python's
// partitioned_sequence does (6fbffc0).
//
// The request is hand-built (a primary init, as the client sends it); the
// answer is the real fixture's own `globalInit`, and the work items it is
// measured against are the ones it actually put on the queue.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { cacheTableFunctions } from "../../../examples/cache.js";
import { cachePartitionScopeTableFunctions } from "../../../examples/cache_partition_scope.js";
import { tableFunctions } from "../../../examples/table.js";
import { partitionTableFunctions } from "../../../examples/table_partition.js";
import { field, int64, schema } from "../../arrow/index.js";
import { Arguments } from "../../arguments/arguments.js";
import type { InitRequest } from "../../protocol/types.js";
import { FunctionType } from "../../types.js";
import { BoundStorage, FunctionStorageSqlite, resolveStorageFromEnv, setStorage, storage } from "../storage.js";
import { DEFAULT_MAX_WORKERS } from "../../types.js";

let dir: string;

beforeAll(() => {
  dir = mkdtempSync(join(tmpdir(), "vgi-partitioned-workers-"));
  setStorage(new FunctionStorageSqlite(join(dir, "state.db")));
});

afterAll(() => {
  setStorage(resolveStorageFromEnv());
  rmSync(dir, { recursive: true, force: true });
});

function primaryInit(name: string, positional: number | number[], named: Record<string, number> = {}): InitRequest {
  return {
    bind_call: {
      function_name: name,
      arguments: new Arguments(Array.isArray(positional) ? positional : [positional], new Map(Object.entries(named))),
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

/** Drain an execution's work queue: how many items the init actually enqueued. */
async function enqueued(executionId: Uint8Array): Promise<number> {
  const queue = new BoundStorage(storage, executionId);
  let n = 0;
  while ((await queue.queuePop()) !== null) n++;
  return n;
}

const ALL = [...tableFunctions, ...partitionTableFunctions, ...cacheTableFunctions, ...cachePartitionScopeTableFunctions];

describe("every work-queue fixture declares max(1, the items it enqueued)", () => {
  // [fixture, positional arguments, named arguments, the item count they produce]
  const cases: Array<[string, number[], Record<string, number>, number]> = [
    ["partitioned_batch_index", [2500], {}, 3],
    ["partitioned_batch_index_marked", [1000], { chunk_size: 100 }, 10],
    ["country_partitioned_sales", [10], {}, 5],
    ["region_year_partitioned", [10], {}, 6],
    ["partitioned_with_explicit_override", [5], {}, 3],
    ["disjoint_range_partitioned", [3], {}, 3],
    ["overlapping_range_partitioned", [4], {}, 4],
    ["cache_parallel", [1000], {}, 24],
    ["cache_ordered", [], { rows: 5000, chunk_size: 1000 }, 5],
    ["cache_partition_parallel", [3], {}, 4],
  ];
  for (const [name, args, named, items] of cases) {
    test(name, async () => {
      const fn = ALL.find((f) => f.meta.name === name)!;
      expect(fn).toBeDefined();
      const response = await fn.globalInit(primaryInit(name, args, named));
      expect(await enqueued(response.execution_id)).toBe(items);
      expect(Number(response.max_workers)).toBe(items);
      expect(Number(response.max_workers)).toBeLessThan(DEFAULT_MAX_WORKERS);
    });
  }

  test("an init that enqueues nothing still declares one reader", async () => {
    for (const [name, args] of [["partitioned_batch_index", [0]], ["cache_parallel", [0]]] as const) {
      const fn = ALL.find((f) => f.meta.name === name)!;
      const response = await fn.globalInit(primaryInit(name, [...args]));
      expect(await enqueued(response.execution_id)).toBe(0);
      expect(Number(response.max_workers)).toBe(1);
    }
  });
});
