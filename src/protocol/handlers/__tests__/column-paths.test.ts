// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import type { RpcClient } from "@query-farm/vgi-rpc";
import { CatalogInterface } from "../../../catalog/interface.js";
import { VgiClient } from "../../../client/client.js";
import { encodeASD, decodeASD } from "../../../codec/asd.js";
import * as schemas from "../../../generated/vgi-protocol-schemas.js";
import { batchToScalarDict, deserializeBatch } from "../../../util/arrow/index.js";
import { catalogTableHandlers } from "../catalog/table.js";

const corpus = JSON.parse(readFileSync(new URL("../../../../conformance/catalog-contracts.json", import.meta.url), "utf8"));
const cases = [
  ["column_add", "ColumnAdd", ["column_definition", "ignore_not_found", "if_column_not_exists"]],
  ["column_drop", "ColumnDrop", ["ignore_not_found", "if_column_exists", "cascade"]],
  ["column_rename", "ColumnRename", ["new_column_name", "ignore_not_found"]],
  ["column_comment_set", "ColumnCommentSet", ["comment", "ignore_not_found"]],
  ["column_type_change", "ColumnTypeChange", ["column_definition", "expression", "ignore_not_found"]],
  ["column_default_set", "ColumnDefaultSet", ["expression", "ignore_not_found"]],
  ["column_default_drop", "ColumnDefaultDrop", ["ignore_not_found"]],
  ["not_null_set", "NotNullSet", ["ignore_not_found"]],
  ["not_null_drop", "NotNullDrop", ["ignore_not_found"]],
] as const;

test("column clients and handlers preserve definitions, raw paths and all flags", async () => {
  for (const [operation, suffix, tail] of cases) {
    const paramsSchema = (schemas as any)[`CatalogTable${suffix}ParamsSchema`];
    const params = decodeASD<Record<string, any>>(paramsSchema,
      new Uint8Array(Buffer.from(corpus.records[operation + "_literal"], "base64")));
    for (const enabled of [false, true]) {
      for (const key of tail) if (typeof params[key] === "boolean") params[key] = enabled;
      params.transaction_opaque_data = enabled ? new Uint8Array([9, 8]) : null;
      if (operation === "column_comment_set") params.comment = enabled ? "kept" : null;
      const calls: unknown[][] = [];
      const provider = Object.create(CatalogInterface.prototype) as CatalogInterface;
      (provider as any)[`table${suffix}`] = async (...args: unknown[]) => { calls.push(args); };
      const handlers = catalogTableHandlers(() => provider);
      const client = new VgiClient({
        async call(method: string, values: Record<string, unknown>) {
          expect(method).toBe(`catalog_table_${operation}`);
          const bytes = encodeASD(paramsSchema, values);
          expect(decodeASD(paramsSchema, bytes)).toEqual(params);
          // Match the RPC server boundary: Arrow list scalars may be vectors,
          // while the ASD decoder deliberately returns plain arrays.
          const wireParams = batchToScalarDict(deserializeBatch(bytes));
          return await (handlers as any)[`catalogTable${suffix}`](wireParams, {});
        },
      } as unknown as RpcClient);
      const args = [params.attach_opaque_data, params.schema_path, params.name, params.column_path,
        ...tail.map(key => params[key]), params.transaction_opaque_data ?? undefined];
      await (client as any)[`table${suffix}`](...args);
      expect(calls).toEqual([args]);
      expect(calls[0][3]).toEqual(["a.b"]);
    }
  }
});

test("unnegotiated nested targets are rejected before the provider is called", async () => {
  for (const [operation, suffix] of cases) {
    const paramsSchema = (schemas as any)[`CatalogTable${suffix}ParamsSchema`];
    const params = decodeASD(paramsSchema,
      new Uint8Array(Buffer.from(corpus.records[operation + "_nested"], "base64")));
    let dispatched = false;
    const handlers = catalogTableHandlers(() => { dispatched = true; throw new Error("provider called"); });
    await expect((handlers as any)[`catalogTable${suffix}`](params, {})).rejects.toThrow("negotiated capability");
    expect(dispatched).toBe(false);
  }
});

test("column expression clients preserve canonical bytes and absent USING", async () => {
  const context = {
    timezone: "UTC", collation: "binary", default_order: "desc",
    default_null_order: "nulls_first", preserve_insertion_order: false,
  };
  const expression = encodeASD(schemas.SqlExpressionSchema, {
    sql: "CAST(NULL AS BIGINT)", profile: "vgi.expression.duckdb.scalar.v1",
    schema_path: ["main"], evaluation_context: context,
  });
  const params = decodeASD<Record<string, any>>(schemas.CatalogTableColumnTypeChangeParamsSchema,
    new Uint8Array(Buffer.from(corpus.records.column_type_change_literal, "base64")));
  const received: (Uint8Array | null)[] = [];
  const provider = Object.create(CatalogInterface.prototype) as CatalogInterface;
  provider.tableColumnDefaultSet = async (_attach, _schema, _name, _path, value) => {
    received.push(value);
  };
  provider.tableColumnTypeChange = async (_attach, _schema, _name, _path, _definition, value) => {
    received.push(value ?? null);
  };
  const handlers = catalogTableHandlers(() => provider);
  const client = new VgiClient({
    async call(method: string, values: Record<string, unknown>) {
      const isDefault = method === "catalog_table_column_default_set";
      const schema = isDefault ? schemas.CatalogTableColumnDefaultSetParamsSchema
        : schemas.CatalogTableColumnTypeChangeParamsSchema;
      const handler = isDefault ? handlers.catalogTableColumnDefaultSet!
        : handlers.catalogTableColumnTypeChange!;
      const wire = batchToScalarDict(deserializeBatch(encodeASD(schema, values)));
      return await (handler as any)(wire, {});
    },
  } as unknown as RpcClient);
  const attach = new Uint8Array([1, 0, 255]);
  await client.tableColumnDefaultSet(attach, ["main"], "t", ["a.b"], expression);
  await client.tableColumnTypeChange(attach, ["main"], "t", ["a.b"], params.column_definition, expression);
  await client.tableColumnTypeChange(attach, ["main"], "t", ["a.b"], params.column_definition);
  expect(received).toEqual([expression, expression, null]);
  for (const value of received.slice(0, 2)) {
    const decoded = decodeASD<Record<string, any>>(schemas.SqlExpressionSchema, value!);
    expect(decoded.sql).toBe("CAST(NULL AS BIGINT)");
    expect(decoded.schema_path).toEqual(["main"]);
    expect(decoded.evaluation_context).toEqual(context);
  }
});
