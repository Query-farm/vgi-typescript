// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import type { RpcClient } from "@query-farm/vgi-rpc";
import { CatalogInterface } from "../../../catalog/interface.js";
import { ReadOnlyCatalogInterface } from "../../../catalog/read-only.js";
import { FunctionRegistry } from "../../../functions/registry.js";
import { VgiClient } from "../../../client/client.js";
import { encodeASD } from "../../../codec/asd.js";
import * as schemas from "../../../generated/vgi-protocol-schemas.js";
import {
  batchToScalarDict,
  deserializeBatch,
} from "../../../util/arrow/index.js";
import { deserializeSchema, serializeSchema } from "../../../arrow/index.js";
import { validateColumnMetadataIpc } from "../../../catalog/column-metadata.js";
import { catalogTableHandlers } from "../catalog/table.js";
const vectors = JSON.parse(
  readFileSync(
    new URL("../../../../conformance/catalog-contracts.json", import.meta.url),
    "utf8",
  ),
).column_metadata_schemas;
const bytes = (value: string) => new Uint8Array(Buffer.from(value, "base64"));

test("CREATE, ADD and discovery preserve raw metadata through client and provider boundaries", async () => {
  const stored = new Map<string, Uint8Array>();
  const added: Uint8Array[] = [];
  const provider = Object.create(
    CatalogInterface.prototype,
  ) as CatalogInterface;
  provider.tableCreate = async (_a, _s, name, columns) => {
    stored.set(name, columns);
  };
  provider.tableColumnAdd = async (_a, _s, _n, _p, definition) => {
    added.push(definition);
  };
  provider.tableGet = async (_a, s, name) => ({
    name,
    schema_path: s,
    comment: null,
    tags: {},
    columns: stored.get(name)!,
    constraints: [],
    definition_revision: null,
    write_result_modes: {},
    supports_column_statistics: false,
    scan_function: null,
    insert_function: null,
    update_function: null,
    delete_function: null,
    cardinality_estimate: null,
    cardinality_max: null,
    column_statistics: null,
    bind_result: null,
    required_filters: [],
    options: null,
    layout: null,
  });
  const handlers = catalogTableHandlers(() => provider);
  const client = new VgiClient({
    async call(method: string, values: Record<string, unknown>) {
      const suffix =
        method === "catalog_table_create"
          ? "Create"
          : method === "catalog_table_column_add"
            ? "ColumnAdd"
            : "Get";
      const wire = batchToScalarDict(
        deserializeBatch(
          encodeASD(
            (schemas as any)[`CatalogTable${suffix}ParamsSchema`],
            values,
          ),
        ),
      );
      return await (handlers as any)[`catalogTable${suffix}`](wire, {});
    },
  } as unknown as RpcClient);
  for (const [name, value] of Object.entries(vectors.valid)) {
    const columns = bytes(value as string);
    await client.tableCreate(
      new Uint8Array([1]),
      ["main"],
      name,
      columns,
      "ERROR",
    );
    expect(stored.get(name)).toEqual(columns);
    expect(
      (await client.tableGet(new Uint8Array([1]), ["main"], name))!.columns,
    ).toEqual(columns);
    await client.tableColumnAdd(
      new Uint8Array([1]),
      ["main"],
      name,
      ["a.b"],
      columns,
    );
    expect(added.at(-1)).toEqual(columns);
    const source = deserializeSchema(columns);
    const readonly = new ReadOnlyCatalogInterface(
      {
        name: "meta",
        schemas: [{ name: "main", tables: [{ name, columns: source }] }],
      },
      new FunctionRegistry(),
    );
    const listed = await readonly.schemaContentsTables(new Uint8Array([1]), [
      "main",
    ]);
    expect(listed[0].columns).toEqual(serializeSchema(source));
  }
  for (const [name, value] of Object.entries(vectors.invalid)) {
    const columns = bytes(value as string);
    expect(() => validateColumnMetadataIpc(columns)).toThrow();
    await expect(
      client.tableCreate(new Uint8Array([1]), ["main"], name, columns, "ERROR"),
    ).rejects.toThrow();
    await expect(
      client.tableColumnAdd(
        new Uint8Array([1]),
        ["main"],
        name,
        ["a.b"],
        columns,
      ),
    ).rejects.toThrow();
    expect(stored.has(name)).toBe(false);
    stored.set("bad_discovery", columns);
    await expect(
      client.tableGet(new Uint8Array([1]), ["main"], "bad_discovery"),
    ).rejects.toThrow();
    stored.delete("bad_discovery");
  }
  expect(stored.size).toBe(4);
  expect(added.length).toBe(4);
});

test("descriptor empty comments override existing values and retain unrelated metadata", async () => {
  const source = deserializeSchema(bytes(vectors.valid.literal));
  const catalog = new ReadOnlyCatalogInterface(
    {
      name: "meta",
      schemas: [
        {
          name: "main",
          tables: [
            { name: "t", columns: source, columnComments: { "a.b": "" } },
          ],
        },
      ],
    },
    new FunctionRegistry(),
  );
  const info = (
    await catalog.schemaContentsTables(new Uint8Array([1]), ["main"])
  )[0];
  const restored = deserializeSchema(info.columns);
  expect(restored.fields[0].metadata.get("comment")).toBe("");
  expect(restored.fields[0].metadata.get("tags")).toBe(
    source.fields[0].metadata.get("tags"),
  );
  expect(restored.fields[0].metadata.get("provider.note")).toBe(
    source.fields[0].metadata.get("provider.note"),
  );
  expect(restored.metadata).toEqual(source.metadata);
});
