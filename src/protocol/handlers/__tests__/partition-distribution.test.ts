// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

import { describe, expect, test } from "bun:test";

import {
  batchFromColumns,
  field,
  int64,
  schema,
  serializeBatch,
  utf8,
} from "../../../arrow/index.js";
import { Arguments } from "../../../arguments/arguments.js";
import { defineTableFunction } from "../../../functions/table.js";
import { FunctionRegistry } from "../../../functions/registry.js";
import {
  ScanSplitSchema,
  TableFunctionPlanRequestSchema,
} from "../../../generated/vgi-protocol-schemas.js";
import {
  decodeKeyGrouping,
  decodeRangePartitioning,
  encodeFieldPath,
  encodeKeyGrouping,
  encodeRangeKey,
  encodeRangePartitioning,
  encodeTransformSpec,
} from "../../../generated/vgi-protocol-types.js";
import { batchToScalarDict, deserializeBatch } from "../../../util/arrow/index.js";
import { toUint8Array } from "../../../util/bytes.js";
import { buildVgiProtocol } from "../../dispatch.js";
import { serializeBindRequest } from "../../serializers/bind.js";
import type { TableFunctionPlanRequest } from "../../serializers/splits.js";
import { FunctionType } from "../../../types.js";

const bytes = (...values: number[]): Uint8Array => new Uint8Array(values);

function values<T>(value: unknown): T[] {
  if (value == null) return [];
  return Array.isArray(value) ? value as T[] : [...value as Iterable<T>];
}

describe("Protocol 3.1 partition distribution producer path", () => {
  test("the plan hook receives the complete request and emits exact distribution metadata", async () => {
    const outputSchema = schema([
      field("partition", utf8(), false),
      field("value", int64(), false),
    ]);
    const keyValue = serializeBatch(batchFromColumns({ partition: ["a"] }, schema([
      field("partition", utf8(), false),
    ])));
    const columnBounds = serializeBatch(batchFromColumns({ value: [0n, 99n] }, schema([
      field("value", int64(), false),
    ])));
    const splitPoints = serializeBatch(batchFromColumns({ partition: ["m"] }, schema([
      field("partition", utf8(), false),
    ])));

    const path = encodeFieldPath({ components: ["partition"] });
    const transform = encodeTransformSpec({
      id: "vgi.identity/1",
      inputs: [path],
      parameters: null,
    });
    const keyGrouping = encodeKeyGrouping({ keys: [transform] });
    const rangeKey = encodeRangeKey({
      transform,
      direction: "ASC",
      null_order: "NULLS_LAST",
    });
    const rangePartitioning = encodeRangePartitioning({
      keys: [rangeKey],
      split_points: splitPoints,
    });

    let requestSeen: TableFunctionPlanRequest | undefined;
    const fn = defineTableFunction<Record<string, never>, null>({
      name: "partition_distribution",
      onBind: () => ({ outputSchema }),
      supportsSplits: true,
      plan: (_params, request) => {
        requestSeen = request;
        return {
          splits: [{
            payload: bytes(0x10),
            estimatedRows: 100,
            rowsExact: true,
            estimatedBytes: 800,
            keyValue,
            rangeIndex: 0,
            columnBounds,
            locationIds: [1],
            startPosition: bytes(0x11),
            endPosition: bytes(0x12),
          }],
          maxWorkers: 2,
          estimatedTotalSplits: 1,
          estimatedTotalRows: 100,
          estimatedTotalBytes: 800,
          catalogVersion: 4,
          locations: ["worker-a", "worker-b"],
          keyGrouping,
          rangePartitioning,
          cacheMaxAgeSeconds: 30,
          startPosition: bytes(0x13),
          endPosition: bytes(0x14),
        };
      },
      onSplit: () => undefined,
      initialState: () => null,
      process: (_params, _state, out) => out.finish(),
    });
    const registry = new FunctionRegistry();
    registry.register(fn);

    const bind = serializeBatch(serializeBindRequest({
      function_name: "partition_distribution",
      arguments: new Arguments(),
      function_type: FunctionType.TABLE,
      input_schema: null,
      settings: null,
      secrets: null,
      attach_opaque_data: null,
      transaction_opaque_data: null,
      resolved_secrets_provided: false,
      schema_path: ["main"],
    }));
    const requestValues: Record<string, unknown> = {
      bind_call: bind,
      bind_opaque_data: bytes(0x20),
      projection_ids: [0n, 1n],
      pushdown_filters: bytes(0x21),
      join_keys: [bytes(0x22), bytes(0x23)],
      row_limit: 10n,
      target_split_bytes: 1024n,
      min_splits: 2n,
      max_splits_per_response: 3n,
      cursor: bytes(0x24),
      refined_filters: bytes(0x25),
      filters_complete: false,
      start_position: bytes(0x26),
      end_position: bytes(0x27),
      order_by_column_name: "value",
      order_by_direction: "DESC",
      order_by_null_order: "NULLS_FIRST",
      order_by_limit: 9n,
      tablesample_percentage: 12.5,
      tablesample_seed: 42n,
    };
    const requestBatch = batchFromColumns(
      Object.fromEntries(TableFunctionPlanRequestSchema.fields.map((f) => [f.name, [requestValues[f.name] ?? null]])),
      TableFunctionPlanRequestSchema,
    );

    const protocol = buildVgiProtocol({ registry });
    const method = (protocol as any)._methods?.get?.("table_function_plan") ??
      (protocol as any).methods?.get?.("table_function_plan");
    expect(method).toBeDefined();
    const response = await method.handler({ request: serializeBatch(requestBatch) }, {} as any);

    expect(requestSeen).toMatchObject({
      projection_ids: [0, 1],
      row_limit: 10n,
      target_split_bytes: 1024n,
      min_splits: 2n,
      max_splits_per_response: 3n,
      filters_complete: false,
      order_by_column_name: "value",
      order_by_direction: "DESC",
      order_by_null_order: "NULLS_FIRST",
      order_by_limit: 9n,
      tablesample_percentage: 12.5,
      tablesample_seed: 42n,
    });
    expect(requestSeen?.join_keys).toEqual([bytes(0x22), bytes(0x23)]);
    expect(requestSeen?.refined_filters).toEqual(bytes(0x25));
    expect(requestSeen?.start_position).toEqual(bytes(0x26));
    expect(requestSeen?.end_position).toEqual(bytes(0x27));

    const plan = batchToScalarDict(deserializeBatch(response.result));
    expect(values<string>(plan.locations)).toEqual(["worker-a", "worker-b"]);
    expect(decodeKeyGrouping(toUint8Array(plan.key_grouping))).toEqual({ keys: [transform] });
    expect(decodeRangePartitioning(toUint8Array(plan.range_partitioning))).toEqual({
      keys: [rangeKey],
      split_points: splitPoints,
    });
    expect(plan.start_position).toEqual(bytes(0x13));
    expect(plan.end_position).toEqual(bytes(0x14));

    const splitBlob = toUint8Array(values<unknown>(plan.splits)[0]);
    const split = batchToScalarDict(deserializeBatch(splitBlob));
    expect(deserializeBatch(splitBlob).schema.fields.map((f) => f.name)).toEqual(
      ScanSplitSchema.fields.map((f) => f.name),
    );
    expect(split.key_value).toEqual(keyValue);
    expect(split.range_index).toBe(0n);
    expect(split.column_bounds).toEqual(columnBounds);
    expect(values<number | bigint>(split.location_ids).map(Number)).toEqual([1]);
    expect(split.start_position).toEqual(bytes(0x11));
    expect(split.end_position).toEqual(bytes(0x12));
  });
});
