// CatalogInterface abstract class with ~35 methods.
// Matching Python's vgi/catalog/catalog_interface.py

import {
  Schema,
  Field,
  RecordBatch,
  Binary,
  Utf8,
  Int32,
  Int64,
  Bool,
  List,
  Map_,
  RecordBatchStreamWriter,
  RecordBatchReader,
  Struct,
  makeData,
  vectorFromArray,
} from "apache-arrow";
import { CatalogReadOnlyError, CatalogNotFoundError } from "../errors.js";
import { serializeSchema, serializeBatch } from "../util/arrow.js";

// Helper to create a Map_ type properly
function mapType(keyType: any, valueType: any): Map_ {
  const entriesStruct = new Struct([
    new Field("key", keyType, false),
    new Field("value", valueType, true),
  ]);
  return new Map_(new Field("entries", entriesStruct, false));
}

export type AttachId = Uint8Array;
export type TransactionId = Uint8Array;

export interface CatalogAttachResult {
  attachId: AttachId;
  supportsTransactions?: boolean;
  supportsTimeTravel?: boolean;
  catalogVersionFrozen?: boolean;
  catalogVersion?: number;
  attachIdRequired?: boolean;
  defaultSchema?: string;
  settings?: Uint8Array[];
}

// ============================================================================
// Serializable info types
// ============================================================================

// These serialize themselves to Arrow IPC bytes for catalog responses

const SCHEMA_INFO_SCHEMA = new Schema([
  new Field("attach_id", new Binary(), false),
  new Field("name", new Utf8(), false),
  new Field("comment", new Utf8(), true),
  new Field("tags", mapType(new Utf8(), new Utf8()), true),
]);

export class SchemaInfo {
  constructor(
    public readonly attachId: AttachId,
    public readonly name: string,
    public readonly comment: string | null = null,
    public readonly tags: Record<string, string> = {}
  ) {}

  serialize(): Uint8Array {
    return serializeInfoBatch(SCHEMA_INFO_SCHEMA, {
      attach_id: this.attachId,
      name: this.name,
      comment: this.comment,
      tags: Object.entries(this.tags).map(([k, v]) => [k, v]),
    });
  }
}

const TABLE_INFO_SCHEMA = new Schema([
  new Field("name", new Utf8(), false),
  new Field("schema_name", new Utf8(), false),
  new Field("columns", new Binary(), false),
  new Field("not_null_constraints", new List(new Field("item", new Int32(), false)), false),
  new Field("unique_constraints", new List(new Field("item", new List(new Field("item", new Int32(), false)), false)), false),
  new Field("check_constraints", new List(new Field("item", new Utf8(), false)), false),
  new Field("comment", new Utf8(), true),
  new Field("tags", mapType(new Utf8(), new Utf8()), true),
]);

export class TableInfo {
  constructor(
    public readonly name: string,
    public readonly schemaName: string,
    public readonly columns: Uint8Array,
    public readonly notNullConstraints: number[] = [],
    public readonly uniqueConstraints: number[][] = [],
    public readonly checkConstraints: string[] = [],
    public readonly comment: string | null = null,
    public readonly tags: Record<string, string> = {}
  ) {}

  serialize(): Uint8Array {
    return serializeInfoBatch(TABLE_INFO_SCHEMA, {
      name: this.name,
      schema_name: this.schemaName,
      columns: this.columns,
      not_null_constraints: this.notNullConstraints,
      unique_constraints: this.uniqueConstraints,
      check_constraints: this.checkConstraints,
      comment: this.comment,
      tags: Object.entries(this.tags).map(([k, v]) => [k, v]),
    });
  }
}

const VIEW_INFO_SCHEMA = new Schema([
  new Field("name", new Utf8(), false),
  new Field("schema_name", new Utf8(), false),
  new Field("definition", new Utf8(), false),
  new Field("comment", new Utf8(), true),
  new Field("tags", mapType(new Utf8(), new Utf8()), true),
]);

export class ViewInfo {
  constructor(
    public readonly name: string,
    public readonly schemaName: string,
    public readonly definition: string,
    public readonly comment: string | null = null,
    public readonly tags: Record<string, string> = {}
  ) {}

  serialize(): Uint8Array {
    return serializeInfoBatch(VIEW_INFO_SCHEMA, {
      name: this.name,
      schema_name: this.schemaName,
      definition: this.definition,
      comment: this.comment,
      tags: Object.entries(this.tags).map(([k, v]) => [k, v]),
    });
  }
}

const FUNCTION_INFO_SCHEMA = new Schema([
  new Field("name", new Utf8(), false),
  new Field("schema_name", new Utf8(), false),
  new Field("function_type", new Utf8(), false),
  new Field("arguments", new Binary(), false),
  new Field("output_schema", new Binary(), false),
  new Field("stability", new Utf8(), true),
  new Field("null_handling", new Utf8(), true),
  new Field("description", new Utf8(), false),
  new Field("examples", new List(new Field("item", new Struct([
    new Field("sql", new Utf8(), false),
    new Field("description", new Utf8(), false),
  ]), true)), false),
  new Field("categories", new List(new Field("item", new Utf8(), true)), false),
  new Field("projection_pushdown", new Bool(), true),
  new Field("filter_pushdown", new Bool(), true),
  new Field("order_preservation", new Utf8(), true),
  new Field("max_workers", new Int32(), true),
  new Field("order_dependent", new Utf8(), false),
  new Field("distinct_dependent", new Utf8(), false),
  new Field("required_settings", new List(new Field("item", new Utf8(), true)), false),
  new Field("comment", new Utf8(), true),
  new Field("tags", mapType(new Utf8(), new Utf8()), true),
]);

export class FunctionInfo {
  constructor(
    public readonly name: string,
    public readonly schemaName: string,
    public readonly functionType: string,
    public readonly functionArguments: Uint8Array,
    public readonly outputSchema: Uint8Array,
    public readonly stability: string | null = null,
    public readonly nullHandling: string | null = null,
    public readonly description: string = "",
    public readonly examples: { sql: string; description: string }[] = [],
    public readonly categories: string[] = [],
    public readonly projectionPushdown: boolean | null = null,
    public readonly filterPushdown: boolean | null = null,
    public readonly orderPreservation: string | null = null,
    public readonly maxWorkers: number | null = null,
    public readonly orderDependent: string = "NOT_ORDER_DEPENDENT",
    public readonly distinctDependent: string = "NOT_DISTINCT_DEPENDENT",
    public readonly requiredSettings: string[] = [],
    public readonly comment: string | null = null,
    public readonly tags: Record<string, string> = {}
  ) {}

  serialize(): Uint8Array {
    return serializeInfoBatch(FUNCTION_INFO_SCHEMA, {
      name: this.name,
      schema_name: this.schemaName,
      function_type: this.functionType,
      arguments: this.functionArguments,
      output_schema: this.outputSchema,
      stability: this.stability,
      null_handling: this.nullHandling,
      description: this.description,
      examples: this.examples.map((e) => ({
        sql: e.sql,
        description: e.description,
      })),
      categories: this.categories,
      projection_pushdown: this.projectionPushdown,
      filter_pushdown: this.filterPushdown,
      order_preservation: this.orderPreservation,
      max_workers: this.maxWorkers,
      order_dependent: this.orderDependent,
      distinct_dependent: this.distinctDependent,
      required_settings: this.requiredSettings,
      comment: this.comment,
      tags: Object.entries(this.tags).map(([k, v]) => [k, v]),
    });
  }
}

// ============================================================================
// CatalogInterface abstract class
// ============================================================================

export abstract class CatalogInterface {
  // Required overrides
  abstract catalogs(): string[];
  abstract attach(
    name: string,
    options?: any
  ): CatalogAttachResult;
  abstract detach(attachId: AttachId): void;
  abstract version(
    attachId: AttachId,
    transactionId?: TransactionId
  ): number;
  abstract schemas(
    attachId: AttachId,
    transactionId?: TransactionId
  ): SchemaInfo[];

  // Default implementations (throw CatalogReadOnlyError)
  create(name: string, onConflict: string, options?: any): void {
    throw new CatalogReadOnlyError("create");
  }
  drop(name: string): void {
    throw new CatalogReadOnlyError("drop");
  }
  schemaGet(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): SchemaInfo | null {
    const all = this.schemas(attachId, transactionId);
    return all.find((s) => s.name === name) ?? null;
  }
  schemaCreate(
    attachId: AttachId,
    name: string,
    comment?: string | null,
    tags?: any,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("schema_create");
  }
  schemaDrop(
    attachId: AttachId,
    name: string,
    ignoreNotFound?: boolean,
    cascade?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("schema_drop");
  }
  schemaContentsTables(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): TableInfo[] {
    return [];
  }
  schemaContentsViews(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): ViewInfo[] {
    return [];
  }
  schemaContentsFunctions(
    attachId: AttachId,
    name: string,
    type: string,
    transactionId?: TransactionId
  ): FunctionInfo[] {
    return [];
  }
  tableGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    transactionId?: TransactionId
  ): TableInfo | null {
    return null;
  }
  tableCreate(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columns: Uint8Array,
    onConflict: string,
    notNullConstraints?: number[],
    uniqueConstraints?: number[][],
    checkConstraints?: string[],
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_create");
  }
  tableDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_drop");
  }
  tableScanFunctionGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    atUnit?: string,
    atValue?: string,
    transactionId?: TransactionId
  ): any {
    throw new CatalogReadOnlyError("table_scan_function_get");
  }
  tableCommentSet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_comment_set");
  }
  tableRename(
    attachId: AttachId,
    schemaName: string,
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_rename");
  }
  tableColumnAdd(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    columnType: string,
    defaultValue?: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_column_add");
  }
  tableColumnDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_column_drop");
  }
  tableColumnRename(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_column_rename");
  }
  tableColumnDefaultSet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    defaultValue: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_column_default_set");
  }
  tableColumnDefaultDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_column_default_drop");
  }
  tableColumnTypeChange(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    newType: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_column_type_change");
  }
  tableNotNullSet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_not_null_set");
  }
  tableNotNullDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    columnName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("table_not_null_drop");
  }
  viewGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    transactionId?: TransactionId
  ): ViewInfo | null {
    return null;
  }
  viewCreate(
    attachId: AttachId,
    schemaName: string,
    name: string,
    definition: string,
    onConflict: string,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("view_create");
  }
  viewDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("view_drop");
  }
  viewRename(
    attachId: AttachId,
    schemaName: string,
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("view_rename");
  }
  viewCommentSet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("view_comment_set");
  }
  transactionBegin(attachId: AttachId): Uint8Array | null {
    return null;
  }
  transactionCommit(
    attachId: AttachId,
    transactionId: TransactionId
  ): void {}
  transactionRollback(
    attachId: AttachId,
    transactionId: TransactionId
  ): void {}
}

// Helper to serialize a single-row info batch to IPC bytes
function serializeInfoBatch(
  schema: Schema,
  values: Record<string, any>
): Uint8Array {
  const children = schema.fields.map((f: Field) => {
    let val = values[f.name];
    if (val === undefined) val = null;
    // Coerce int64
    if (val != null && f.type instanceof Int64 && typeof val === "number") {
      val = BigInt(val);
    }
    const arr = vectorFromArray([val], f.type);
    return arr.data[0];
  });

  const structType = new Struct(schema.fields);
  const data = makeData({
    type: structType,
    length: 1,
    children,
    nullCount: 0,
  });

  const batch = new RecordBatch(schema, data);
  const writer = new RecordBatchStreamWriter();
  writer.reset(undefined, schema);
  // Use _writeRecordBatch to bypass schema comparison bug
  // (public write() silently drops batches on nullability mismatch)
  (writer as any)._writeRecordBatch(batch);
  writer.close();
  return writer.toUint8Array(true);
}
