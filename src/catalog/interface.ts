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
} from "@query-farm/apache-arrow";
import { CatalogReadOnlyError, CatalogNotFoundError } from "../errors.js";
import { serializeSchema, serializeBatch, deserializeBatch, batchToScalarDict, deserializeSchema } from "../util/arrow.js";
import {
  SchemaInfoSchema as SCHEMA_INFO_SCHEMA,
  TableInfoSchema as TABLE_INFO_SCHEMA,
  ViewInfoSchema as VIEW_INFO_SCHEMA,
  FunctionInfoSchema as FUNCTION_INFO_SCHEMA,
  MacroInfoSchema as MACRO_INFO_SCHEMA,
} from "../generated/vgi-protocol-schemas.js";

export type AttachId = Uint8Array;
export type TransactionId = Uint8Array;

export enum MacroType {
  SCALAR = "scalar",
  TABLE = "table",
}

export interface CatalogAttachResult {
  attachId: AttachId;
  supportsTransactions?: boolean;
  supportsTimeTravel?: boolean;
  catalogVersionFrozen?: boolean;
  catalogVersion?: number;
  attachIdRequired?: boolean;
  defaultSchema?: string;
  settings?: Uint8Array[];
  secretTypes?: Uint8Array[];
  comment?: string | null;
  tags?: Record<string, string>;
}

// ============================================================================
// Serializable info types
// ============================================================================

// These serialize themselves to Arrow IPC bytes for catalog responses

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

  static deserialize(bytes: Uint8Array): SchemaInfo {
    const d = deserializeInfoRow(bytes);
    const tags: Record<string, string> = {};
    if (d.tags) {
      for (const [k, v] of iterMapEntries(d.tags)) {
        tags[k] = v;
      }
    }
    return new SchemaInfo(
      toU8(d.attach_id),
      d.name,
      d.comment ?? null,
      tags,
    );
  }
}

export class TableInfo {
  constructor(
    public readonly name: string,
    public readonly schemaName: string,
    public readonly columns: Uint8Array,
    public readonly notNullConstraints: number[] = [],
    public readonly uniqueConstraints: number[][] = [],
    public readonly checkConstraints: string[] = [],
    public readonly primaryKeyConstraints: number[][] = [],
    public readonly foreignKeyConstraints: Uint8Array[] = [],
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
      primary_key_constraints: this.primaryKeyConstraints,
      foreign_key_constraints: this.foreignKeyConstraints,
      comment: this.comment,
      tags: Object.entries(this.tags).map(([k, v]) => [k, v]),
    });
  }

  static deserialize(bytes: Uint8Array): TableInfo {
    const d = deserializeInfoRow(bytes);
    const tags: Record<string, string> = {};
    if (d.tags) {
      for (const [k, v] of iterMapEntries(d.tags)) {
        tags[k] = v;
      }
    }
    return new TableInfo(
      d.name,
      d.schema_name,
      toU8(d.columns),
      toNumberArray(d.not_null_constraints),
      toNestedNumberArray(d.unique_constraints),
      toStringArray(d.check_constraints),
      toNestedNumberArray(d.primary_key_constraints),
      toBinaryArray(d.foreign_key_constraints),
      d.comment ?? null,
      tags,
    );
  }
}

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

  static deserialize(bytes: Uint8Array): ViewInfo {
    const d = deserializeInfoRow(bytes);
    const tags: Record<string, string> = {};
    if (d.tags) {
      for (const [k, v] of iterMapEntries(d.tags)) {
        tags[k] = v;
      }
    }
    return new ViewInfo(
      d.name,
      d.schema_name,
      d.definition,
      d.comment ?? null,
      tags,
    );
  }
}

export interface FunctionInfoOptions {
  name: string;
  schemaName: string;
  functionType: string;
  functionArguments: Uint8Array;
  outputSchema: Uint8Array;
  stability?: string | null;
  nullHandling?: string | null;
  description?: string;
  examples?: { sql: string; description: string }[];
  categories?: string[];
  projectionPushdown?: boolean | null;
  filterPushdown?: boolean | null;
  orderPreservation?: string | null;
  maxWorkers?: number | null;
  orderDependent?: string;
  distinctDependent?: string;
  requiredSettings?: string[];
  requiredSecrets?: Array<{secret_type: string, scope: string | null, secret_name: string | null}>;
  comment?: string | null;
  tags?: Record<string, string>;
}

export class FunctionInfo {
  readonly name: string;
  readonly schemaName: string;
  readonly functionType: string;
  readonly functionArguments: Uint8Array;
  readonly outputSchema: Uint8Array;
  readonly stability: string | null;
  readonly nullHandling: string | null;
  readonly description: string;
  readonly examples: { sql: string; description: string }[];
  readonly categories: string[];
  readonly projectionPushdown: boolean | null;
  readonly filterPushdown: boolean | null;
  readonly orderPreservation: string | null;
  readonly maxWorkers: number | null;
  readonly orderDependent: string;
  readonly distinctDependent: string;
  readonly requiredSettings: string[];
  readonly requiredSecrets: Array<{secret_type: string, scope: string | null, secret_name: string | null}>;
  readonly comment: string | null;
  readonly tags: Record<string, string>;

  constructor(opts: FunctionInfoOptions) {
    this.name = opts.name;
    this.schemaName = opts.schemaName;
    this.functionType = opts.functionType;
    this.functionArguments = opts.functionArguments;
    this.outputSchema = opts.outputSchema;
    this.stability = opts.stability ?? null;
    this.nullHandling = opts.nullHandling ?? null;
    this.description = opts.description ?? "";
    this.examples = opts.examples ?? [];
    this.categories = opts.categories ?? [];
    this.projectionPushdown = opts.projectionPushdown ?? null;
    this.filterPushdown = opts.filterPushdown ?? null;
    this.orderPreservation = opts.orderPreservation ?? null;
    this.maxWorkers = opts.maxWorkers ?? null;
    this.orderDependent = opts.orderDependent ?? "NOT_ORDER_DEPENDENT";
    this.distinctDependent = opts.distinctDependent ?? "NOT_DISTINCT_DEPENDENT";
    this.requiredSettings = opts.requiredSettings ?? [];
    this.requiredSecrets = opts.requiredSecrets ?? [];
    this.comment = opts.comment ?? null;
    this.tags = opts.tags ?? {};
  }

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
      required_secrets: this.requiredSecrets.map(s => ({
        secret_type: s.secret_type,
        scope: s.scope,
        secret_name: s.secret_name,
      })),
      comment: this.comment,
      tags: Object.entries(this.tags).map(([k, v]) => [k, v]),
    });
  }

  static deserialize(bytes: Uint8Array): FunctionInfo {
    const d = deserializeInfoRow(bytes);
    const tags: Record<string, string> = {};
    if (d.tags) {
      for (const [k, v] of iterMapEntries(d.tags)) {
        tags[k] = v;
      }
    }
    const examples: { sql: string; description: string }[] = [];
    if (d.examples) {
      const exArr = Array.isArray(d.examples) ? d.examples : [...d.examples];
      for (const ex of exArr) {
        if (ex) {
          examples.push({
            sql: ex.sql ?? ex.get?.("sql") ?? "",
            description: ex.description ?? ex.get?.("description") ?? "",
          });
        }
      }
    }
    const requiredSecrets: Array<{secret_type: string, scope: string | null, secret_name: string | null}> = [];
    if (d.required_secrets) {
      const secArr = Array.isArray(d.required_secrets) ? d.required_secrets : [...d.required_secrets];
      for (const entry of secArr) {
        if (entry) {
          requiredSecrets.push({
            secret_type: entry.secret_type ?? entry.get?.("secret_type") ?? "",
            scope: entry.scope ?? entry.get?.("scope") ?? null,
            secret_name: entry.secret_name ?? entry.get?.("secret_name") ?? null,
          });
        }
      }
    }
    return new FunctionInfo({
      name: d.name,
      schemaName: d.schema_name,
      functionType: d.function_type,
      functionArguments: toU8(d.arguments),
      outputSchema: toU8(d.output_schema),
      stability: d.stability ?? null,
      nullHandling: d.null_handling ?? null,
      description: d.description ?? "",
      examples,
      categories: toStringArray(d.categories),
      projectionPushdown: d.projection_pushdown ?? null,
      filterPushdown: d.filter_pushdown ?? null,
      orderPreservation: d.order_preservation ?? null,
      maxWorkers: d.max_workers != null ? Number(d.max_workers) : null,
      orderDependent: d.order_dependent ?? "NOT_ORDER_DEPENDENT",
      distinctDependent: d.distinct_dependent ?? "NOT_DISTINCT_DEPENDENT",
      requiredSettings: toStringArray(d.required_settings),
      requiredSecrets,
      comment: d.comment ?? null,
      tags,
    });
  }
}

export class MacroInfo {
  constructor(
    public readonly name: string,
    public readonly schemaName: string,
    public readonly macroType: MacroType,
    public readonly parameters: string[],
    public readonly parameterDefaultValues: Uint8Array | null,
    public readonly definition: string,
    public readonly comment: string | null = null,
    public readonly tags: Record<string, string> = {}
  ) {}

  serialize(): Uint8Array {
    return serializeInfoBatch(MACRO_INFO_SCHEMA, {
      name: this.name,
      schema_name: this.schemaName,
      macro_type: this.macroType,
      parameters: this.parameters,
      parameter_default_values: this.parameterDefaultValues,
      definition: this.definition,
      comment: this.comment,
      tags: Object.entries(this.tags).map(([k, v]) => [k, v]),
    });
  }

  static deserialize(bytes: Uint8Array): MacroInfo {
    const d = deserializeInfoRow(bytes);
    const tags: Record<string, string> = {};
    if (d.tags) {
      for (const [k, v] of iterMapEntries(d.tags)) {
        tags[k] = v;
      }
    }
    return new MacroInfo(
      d.name,
      d.schema_name,
      d.macro_type as MacroType,
      toStringArray(d.parameters),
      d.parameter_default_values ? toU8(d.parameter_default_values) : null,
      d.definition,
      d.comment ?? null,
      tags,
    );
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
    atUnit?: string,
    atValue?: string,
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
  macroGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    transactionId?: TransactionId
  ): MacroInfo | null {
    return null;
  }
  macroCreate(
    attachId: AttachId,
    schemaName: string,
    name: string,
    macroType: MacroType,
    parameters: string[],
    definition: string,
    onConflict: string,
    parameterDefaultValues?: Uint8Array | null,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("macro_create");
  }
  macroDrop(
    attachId: AttachId,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    throw new CatalogReadOnlyError("macro_drop");
  }
  schemaContentsMacros(
    attachId: AttachId,
    name: string,
    type: string,
    transactionId?: TransactionId
  ): MacroInfo[] {
    return [];
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

// Helper to deserialize a single-row info batch from IPC bytes
function deserializeInfoRow(bytes: Uint8Array): Record<string, any> {
  const batch = deserializeBatch(bytes);
  return batchToScalarDict(batch);
}

// Helper to convert Arrow value to Uint8Array
function toU8(val: any): Uint8Array {
  if (val instanceof Uint8Array) return val;
  if (val instanceof ArrayBuffer) return new Uint8Array(val);
  if (val && val.buffer instanceof ArrayBuffer) {
    return new Uint8Array(val.buffer, val.byteOffset, val.byteLength);
  }
  return new Uint8Array(0);
}

// Helper to iterate Map entries (Arrow MapRow or JS iterable)
function* iterMapEntries(mapVal: any): Generator<[string, string]> {
  if (!mapVal) return;
  if (typeof mapVal[Symbol.iterator] === "function") {
    for (const entry of mapVal) {
      if (Array.isArray(entry)) {
        yield [String(entry[0]), String(entry[1])];
      } else if (entry && typeof entry === "object") {
        yield [String(entry.key ?? entry[0] ?? ""), String(entry.value ?? entry[1] ?? "")];
      }
    }
  }
}

// Helper to convert iterable to number[]
function toNumberArray(val: any): number[] {
  if (!val) return [];
  const arr = Array.isArray(val) ? val : [...val];
  return arr.map(Number);
}

// Helper to convert iterable of iterables to number[][]
function toNestedNumberArray(val: any): number[][] {
  if (!val) return [];
  const arr = Array.isArray(val) ? val : [...val];
  return arr.map((inner: any) => {
    if (!inner) return [];
    const innerArr = Array.isArray(inner) ? inner : [...inner];
    return innerArr.map(Number);
  });
}

// Helper to convert iterable to string[]
function toStringArray(val: any): string[] {
  if (!val) return [];
  const arr = Array.isArray(val) ? val : [...val];
  return arr.filter((v: any) => v != null).map(String);
}

// Helper to convert iterable to Uint8Array[]
function toBinaryArray(val: any): Uint8Array[] {
  if (!val) return [];
  const arr = Array.isArray(val) ? val : [...val];
  return arr.filter((v: any) => v != null).map(toU8);
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
