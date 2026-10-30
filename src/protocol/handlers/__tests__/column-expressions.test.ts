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
import { validateColumnExpressionsIpc } from "../../../catalog/column-expressions.js";
import { catalogTableHandlers } from "../catalog/table.js";
const vectors = JSON.parse(
  readFileSync(
    new URL("../../../../conformance/catalog-contracts.json", import.meta.url),
    "utf8",
  ),
).generated_column_schemas;
const bytes = (value: string) => new Uint8Array(Buffer.from(value, "base64"));

test("canonical generated CREATE, ADD and discovery preserve expressions through custom provider boundaries", async () => {
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
    expect(() => validateColumnExpressionsIpc(columns, ["main"])).toThrow();
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
    if (name === "legacy_generated_only") {
      // Served tables translate the unambiguous legacy key to canonical form.
      const served = await client.tableGet(new Uint8Array([1]), ["main"], "bad_discovery");
      const field = deserializeSchema(served!.columns).fields.find((f) => f.metadata.has("vgi.column.generated.v1"));
      expect(field?.metadata.get("vgi.column.generation.v1")).toBe("virtual");
      expect(field?.metadata.has("generated_expression")).toBe(false);
    } else {
      await expect(
        client.tableGet(new Uint8Array([1]), ["main"], "bad_discovery"),
      ).rejects.toThrow();
    }
    stored.delete("bad_discovery");
  }
  expect(stored.size).toBe(3);
  expect(added.length).toBe(3);
});

test("builtin mutable catalogs refuse generated replacement before effects", async () => {
  const { InMemoryCatalog } = await import("../../../../examples/catalog.js");
  const { ContentsRevalCatalog } = await import(
    "../../../../examples/catalog_contents.js"
  );
  for (const catalog of [new InMemoryCatalog(), new ContentsRevalCatalog()]) {
    const attach = catalog.attach(catalog.catalogs()[0]).attach_opaque_data;
    const ordinary = bytes(vectors.valid.ordinary);
    catalog.tableCreate(attach, ["main"], "keep", ordinary, "error", []);
    const before = catalog.version(attach);
    for (const [name, encoded] of Object.entries({
      ...vectors.invalid,
      ...vectors.valid,
    })) {
      if (name === "ordinary") continue;
      expect(() =>
        catalog.tableCreate(
          attach,
          ["main"],
          "keep",
          bytes(encoded as string),
          "replace",
          [],
        ),
      ).toThrow();
      expect(catalog.version(attach)).toBe(before);
      expect(catalog.tableGet(attach, ["main"], "keep")!.columns).toEqual(
        ordinary,
      );
    }
  }
});

test("generated authoring preserves metadata and literal dotted owner", async () => {
  const { schema, field, int64 } = await import("../../../arrow/index.js");
  const { decodeSqlExpression } = await import(
    "../../../generated/vgi-protocol-types.js"
  );
  const source = schema(
    [
      field("n", int64(), true),
      field(
        "doubled",
        int64(),
        true,
        new Map([
          ["comment", ""],
          ["tags", "{}"],
          ["provider.note", "keep"],
        ]),
      ),
    ],
    new Map([["provider", "retained"]]),
  );
  const catalog = new ReadOnlyCatalogInterface(
    {
      name: "generated",
      schemas: [
        {
          name: "raw.schema",
          tables: [
            {
              name: "t",
              columns: source,
              generatedColumns: { doubled: "n * 2" },
            },
          ],
        },
      ],
    },
    new FunctionRegistry(),
  );
  const info = (
    await catalog.schemaContentsTables(new Uint8Array([1]), ["raw.schema"])
  )[0];
  const restored = deserializeSchema(info.columns);
  const metadata = restored.fields[1].metadata;
  expect(metadata.get("vgi.column.generation.v1")).toBe("virtual");
  expect(metadata.has("generated_expression")).toBe(false);
  expect(metadata.get("comment")).toBe("");
  expect(metadata.get("tags")).toBe("{}");
  expect(metadata.get("provider.note")).toBe("keep");
  expect(restored.metadata).toEqual(source.metadata);
  const expression = decodeSqlExpression(
    bytes(metadata.get("vgi.column.generated.v1")!),
  );
  expect(expression.schema_path).toEqual(["raw.schema"]);
  expect(expression.sql).toBe("n * 2");
  expect(expression.evaluation_context.timezone).toBe("UTC");
});
