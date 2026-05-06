// ReadOnlyCatalogInterface: derives catalog from registered functions + descriptors.

import { Schema, Field, Int32, Int64, DataType, Utf8, Binary, Bool, Null, List } from "@query-farm/apache-arrow";
import {
  CatalogInterface,
  type AttachId,
  type TransactionId,
  type CatalogAttachResult,
  type SchemaInfo,
  type TableInfo,
  type ViewInfo,
  type FunctionInfo,
  type MacroInfo,
  type IndexInfo,
  type IndexConstraintType,
} from "./interface.js";
import type { CatalogDescriptor, SchemaDescriptor, TableDescriptor, ViewDescriptor, MacroDescriptor, SettingDescriptor, SecretTypeDescriptor, ForeignKeyDef, DefaultValue } from "./descriptors.js";
import { serializeColumnStatistics } from "../util/statistics.js";
import type { VgiFunction } from "../functions/types.js";
import type { FunctionRegistry } from "../functions/registry.js";
import { argumentSpecsToSchema } from "../arguments/argument-spec.js";
import { serializeSchema, serializeBatch, emptyBatch, batchFromColumns } from "../util/arrow/index.js";
import { resolveMetadata } from "../metadata/resolve.js";
import { FunctionStability, NullHandling, OrderPreservation, DEFAULT_MAX_WORKERS } from "../types.js";
import { Arguments } from "../arguments/arguments.js";

export class ReadOnlyCatalogInterface extends CatalogInterface {
  private _descriptor: CatalogDescriptor;
  private _registry: FunctionRegistry;
  private _attachments = new Map<string, AttachId>();
  private _version = 1;

  constructor(descriptor: CatalogDescriptor, registry: FunctionRegistry) {
    super();
    this._descriptor = descriptor;
    this._registry = registry;
  }

  catalogs(): string[] {
    return [this._descriptor.name];
  }

  attach(
    name: string,
    options?: Record<string, unknown>,
    dataVersionSpec?: string | null,
    implementationVersion?: string | null,
  ): CatalogAttachResult {
    if (!this.catalogs().includes(name)) {
      throw new Error(`No worker handles catalog '${name}'`);
    }

    const attachId = new Uint8Array(16);
    crypto.getRandomValues(attachId);
    this._attachments.set(name, attachId);

    const settings = (this._descriptor.settings ?? []).map((s) =>
      serializeSetting(s)
    );

    const secretTypes = (this._descriptor.secretTypes ?? []).map((s) =>
      serializeSecretType(s)
    );

    // Auto-derive supportsTimeTravel from table descriptors
    const hasTimeTravel = this._descriptor.schemas.some(
      (s) => s.tables?.some((t) => t.supportsTimeTravel) ?? false
    );

    return {
      attach_id: attachId,
      supports_transactions: false,
      supports_time_travel: hasTimeTravel,
      catalog_version_frozen: true,
      catalog_version: this._version,
      attach_id_required: true,
      default_schema: this._descriptor.defaultSchema ?? "main",
      settings,
      secret_types: secretTypes,
      comment: this._descriptor.comment ?? null,
      tags: this._descriptor.tags ?? {},
      resolved_data_version: null,
      resolved_implementation_version: null,
    };
  }

  detach(attachId: AttachId): void {
    // Remove attachment
    for (const [name, id] of this._attachments) {
      if (bufferEquals(id, attachId)) {
        this._attachments.delete(name);
        break;
      }
    }
  }

  version(attachId: AttachId, transactionId?: TransactionId): number {
    return this._version;
  }

  schemas(attachId: AttachId, transactionId?: TransactionId): SchemaInfo[] {
    return this._descriptor.schemas.map((s) => ({
      attach_id: attachId,
      name: s.name,
      comment: s.comment ?? null,
      tags: s.tags ?? {},
      estimated_object_count: this._estimatedObjectCount(s),
    }));
  }

  // Compute per-kind object counts so the C++ extension's eager-load gate
  // can skip bulk RPCs for empty kinds. Zero is load-bearing — absence
  // would read as "unknown=1" and suppress the bypass. Function counts are
  // partitioned by kind to match python's behavior.
  private _estimatedObjectCount(s: SchemaDescriptor): Record<string, number> {
    let scalar = 0, aggregate = 0, table = 0;
    for (const f of s.functions ?? []) {
      if (f.kind === "scalar") scalar++;
      else if ((f.kind as string) === "aggregate") aggregate++;
      else table++; // "table" and "table_in_out"
    }
    return {
      table: s.tables?.length ?? 0,
      view: s.views?.length ?? 0,
      scalar_function: scalar,
      aggregate_function: aggregate,
      table_function: table,
      macro: s.macros?.length ?? 0,
      index: s.indexes?.length ?? 0,
    };
  }

  override schemaGet(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): SchemaInfo | null {
    const desc = this._descriptor.schemas.find((s) => s.name === name);
    if (!desc) return null;
    return {
      attach_id: attachId,
      name: desc.name,
      comment: desc.comment ?? null,
      tags: desc.tags ?? {},
      estimated_object_count: this._estimatedObjectCount(desc),
    };
  }

  override async schemaContentsTables(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): Promise<TableInfo[]> {
    const schema = this._descriptor.schemas.find((s) => s.name === name);
    if (!schema || !schema.tables) return [];

    return await Promise.all(schema.tables.map(async (t) => {
      // Resolve the column schema
      let colSchema: Schema;
      if (t.columns) {
        colSchema = t.columns;
      } else if (t.function) {
        const func = t.function;
        const dummyArgs = t.arguments ?? new Arguments();
        try {
          const bindRequest = {
            function_name: func.meta.name,
            arguments: dummyArgs,
            function_type: "table" as any,
            input_schema: null,
            settings: null,
            secrets: null,
            attach_id: attachId,
            transaction_id: null,
            resolved_secrets_provided: false,
          };
          const response = await func.bind(bindRequest);
          colSchema = response.output_schema;
        } catch {
          colSchema = new Schema([]);
        }
      } else {
        colSchema = new Schema([]);
      }

      // Apply defaults as Arrow field metadata
      if (t.defaults) {
        colSchema = applyDefaults(colSchema, t.defaults);
      }
      // Apply generated-column expressions as Arrow field metadata
      // (`generated_expression`). DuckDB registers these columns as GENERATED
      // and evaluates the SQL expression client-side — the scan function
      // should not return values for them.
      if (t.generatedColumns) {
        colSchema = applyGeneratedColumns(colSchema, t.generatedColumns);
      }
      // Apply per-column comments as Arrow field metadata (matches Python's
      // `comment` metadata key on columns).
      if (t.columnComments) {
        colSchema = applyColumnComments(colSchema, t.columnComments);
      }

      const columns = serializeSchema(colSchema);

      // Resolve constraint column names → indices
      const notNullIndices = resolveIndices(colSchema, t.notNull);
      const uniqueIndices = resolveIndexGroups(colSchema, t.unique);
      const pkIndices = resolveIndexGroups(colSchema, t.primaryKey);
      const fkBytes = serializeForeignKeys(t.foreignKey, name);

      return {
        comment: t.comment ?? null,
        tags: t.tags ?? {},
        name: t.name,
        schema_name: name,
        columns,
        not_null_constraints: notNullIndices,
        unique_constraints: uniqueIndices,
        check_constraints: t.check ?? [],
        primary_key_constraints: pkIndices,
        foreign_key_constraints: fkBytes,
        supports_insert: false,
        supports_update: false,
        supports_delete: false,
        supports_returning: false,
        supports_column_statistics: t.statistics != null && Object.keys(t.statistics).length > 0,
        // Inline scan_function only for purely function-backed tables.
        // When explicit `columns` are also declared, the table is on the
        // legacy catalog_table_scan_function_get path; inlining would
        // shadow the per-call dispatch and prevent workers from varying
        // the scan function (e.g. version-resolved time-travel).
        scan_function: t.function && !t.columns
          ? inlineScanFunction(t.function, t.arguments)
          : null,
        insert_function: null,
        update_function: null,
        delete_function: null,
        cardinality_estimate: t.inlinedCardinality
          ? Number(t.inlinedCardinality.estimate)
          : null,
        cardinality_max: t.inlinedCardinality
          ? Number(t.inlinedCardinality.max)
          : null,
      };
    }));
  }

  override tableColumnStatisticsGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    transactionId?: TransactionId,
  ): { bytes: Uint8Array; cacheMaxAgeSeconds: number | null } | null {
    const schema = this._descriptor.schemas.find((s) => s.name === schemaName);
    if (!schema || !schema.tables) return null;
    const table = schema.tables.find((t) => t.name === name);
    if (!table || !table.statistics) return null;
    const stats = Object.values(table.statistics);
    if (stats.length === 0) return null;
    return {
      bytes: serializeColumnStatistics(stats),
      cacheMaxAgeSeconds: table.statisticsCacheMaxAgeSeconds ?? null,
    };
  }

  override schemaContentsViews(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): ViewInfo[] {
    const schema = this._descriptor.schemas.find((s) => s.name === name);
    if (!schema || !schema.views) return [];

    return schema.views.map((v) => ({
      comment: v.comment ?? null,
      tags: v.tags ?? {},
      name: v.name,
      schema_name: name,
      definition: v.definition,
    }));
  }

  override schemaContentsFunctions(
    attachId: AttachId,
    name: string,
    type: string,
    transactionId?: TransactionId
  ): FunctionInfo[] {
    const schema = this._descriptor.schemas.find((s) => s.name === name);
    if (!schema || !schema.functions) return [];

    return schema.functions
      .filter((f) => {
        // Filter by SchemaObjectType (lowercase on the wire: scalar_function,
        // table_function, aggregate_function). Unknown filter values pass
        // everything through. Defensive String() coerces null/undefined.
        const t = String(type ?? "").toUpperCase();
        if (t === "SCALAR_FUNCTION") return f.kind === "scalar";
        if (t === "TABLE_FUNCTION") return f.kind === "table" || f.kind === "table_in_out";
        if (t === "AGGREGATE_FUNCTION") return (f.kind as string) === "aggregate";
        return true;
      })
      .map((f) => {
        const meta = resolveMetadata(f);
        const argSchema = argumentSpecsToSchema(f.argumentSpecs);
        const argBytes = serializeSchema(argSchema);

        // Use the function's default output schema if available,
        // otherwise empty schema (table functions determine output at bind time)
        const outputSchemaBytes = serializeSchema(
          f.defaultOutputSchema ?? new Schema([])
        );

        const requiredSecrets = (meta.requiredSecrets ?? []).map(
          (secretType: string) => ({ secret_type: secretType, scope: null, secret_name: null })
        );

        return {
          comment: null,
          tags: {},
          name: f.meta.name,
          schema_name: name,
          function_type: meta.functionType.toUpperCase() as "SCALAR" | "TABLE" | "AGGREGATE",
          arguments: argBytes,
          output_schema: outputSchemaBytes,
          stability: meta.stability as any,
          null_handling: meta.nullHandling as any,
          description: meta.description,
          examples: meta.examples.map((e) => ({
            sql: e.sql,
            description: e.description,
            expected_output: null,
          })),
          categories: meta.categories,
          projection_pushdown: meta.projectionPushdown,
          filter_pushdown: meta.filterPushdown,
          sampling_pushdown: meta.samplingPushdown,
          supported_expression_filters: meta.supportedExpressionFilters,
          order_preservation: meta.preservesOrder as any,
          max_workers: meta.maxWorkers ?? DEFAULT_MAX_WORKERS,
          order_dependent: meta.orderDependent as any,
          distinct_dependent: meta.distinctDependent as any,
          supports_window: false,
          streaming_partitioned: false,
          // Only table_in_out functions that wired up a finalize callback get
          // the FINALIZE init() phase. Advertising has_finalize=true on a
          // function without it makes DuckDB call FinalExecute and crash
          // with 'FinalExecute not supported for project_input'.
          has_finalize: f.kind === "table_in_out" && f.meta.hasFinalize === true,
          required_settings: meta.requiredSettings,
          required_secrets: requiredSecrets.map((s) => ({
            secret_type: s.secret_type,
            scope: s.scope,
            secret_name: s.secret_name,
          })),
        };
      });
  }

  override schemaContentsMacros(
    attachId: AttachId,
    name: string,
    type: string,
    transactionId?: TransactionId
  ): MacroInfo[] {
    const schema = this._descriptor.schemas.find((s) => s.name === name);
    if (!schema || !schema.macros) return [];

    return schema.macros
      .filter((m) => {
        // Defensive String() — see schemaContentsFunctions for rationale.
        const t = String(type ?? "").toLowerCase();
        if (t === "scalar_macro" && m.macroType !== "scalar") return false;
        if (t === "table_macro" && m.macroType !== "table") return false;
        return true;
      })
      .map((m) => ({
        comment: m.comment ?? null,
        tags: m.tags ?? {},
        name: m.name,
        schema_name: name,
        macro_type: (m.macroType === "scalar" ? "SCALAR" : "TABLE") as "SCALAR" | "TABLE",
        parameters: m.parameters,
        parameter_default_values: m.parameterDefaultValues ?? new Uint8Array(0),
        definition: m.definition,
      }));
  }

  override schemaContentsIndexes(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): IndexInfo[] {
    const schema = this._descriptor.schemas.find((s) => s.name === name);
    if (!schema || !schema.indexes) return [];
    return schema.indexes.map((idx) => ({
      comment: idx.comment ?? null,
      tags: idx.tags ?? {},
      name: idx.name,
      schema_name: name,
      table_name: idx.tableName,
      index_type: idx.indexType ?? "ART",
      constraint_type: (idx.constraintType ?? "NONE") as IndexConstraintType,
      expressions: idx.expressions,
      options: idx.options ?? {},
    }));
  }

  override indexGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    transactionId?: TransactionId
  ): IndexInfo | null {
    const all = this.schemaContentsIndexes(attachId, schemaName, transactionId);
    return all.find((i) => i.name === name) ?? null;
  }

  override macroGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    transactionId?: TransactionId
  ): MacroInfo | null {
    const macros = this.schemaContentsMacros(attachId, schemaName, "SCALAR_MACRO", transactionId);
    const tableMacros = this.schemaContentsMacros(attachId, schemaName, "TABLE_MACRO", transactionId);
    const all = [...macros, ...tableMacros];
    return all.find((m) => m.name.toLowerCase() === name.toLowerCase()) ?? null;
  }

  override tableScanFunctionGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    atUnit?: string,
    atValue?: string,
    transactionId?: TransactionId
  ): any {
    validateAtParams(atUnit, atValue);

    // Find the table descriptor
    const schema = this._descriptor.schemas.find((s) => s.name === schemaName);
    if (!schema || !schema.tables) {
      throw new Error(`Table '${name}' not found in schema '${schemaName}'`);
    }
    const table = schema.tables.find((t) => t.name === name);
    if (!table) {
      throw new Error(`Table '${name}' not found in schema '${schemaName}'`);
    }

    // Reject AT clause on tables that don't support time travel
    if (atUnit && !table.supportsTimeTravel) {
      throw new Error(`Table '${schemaName}.${name}' does not support time travel queries`);
    }

    if (table.function) {
      // Function-backed table
      const args = table.arguments ?? new Arguments();
      const argSchema = argumentSpecsToSchema(table.function.argumentSpecs);
      const argBytes = serializeArgsBatch(args, argSchema);
      return {
        function_name: table.function.meta.name,
        arguments: argBytes,
        required_extensions: [],
      };
    }

    throw new Error(`Table '${name}' is not function-backed`);
  }

  override async tableGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    atUnit?: string,
    atValue?: string,
    transactionId?: TransactionId
  ): Promise<TableInfo | null> {
    validateAtParams(atUnit, atValue);

    // Find the table descriptor to check supports_time_travel
    const schema = this._descriptor.schemas.find((s) => s.name === schemaName);
    if (schema?.tables) {
      const tableDesc = schema.tables.find((t) => t.name === name);
      if (tableDesc && atUnit && !tableDesc.supportsTimeTravel) {
        throw new Error(`Table '${schemaName}.${name}' does not support time travel queries`);
      }
    }

    const tables = await this.schemaContentsTables(attachId, schemaName, transactionId);
    return tables.find((t) => t.name === name) ?? null;
  }

  override viewGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    transactionId?: TransactionId
  ): ViewInfo | null {
    const views = this.schemaContentsViews(attachId, schemaName, transactionId);
    return views.find((v) => v.name === name) ?? null;
  }
}

// ============================================================================
// Constraint resolution helpers
// ============================================================================

function resolveIndices(schema: Schema, names?: string[]): number[] {
  if (!names || names.length === 0) return [];
  return names.map((n) => {
    const idx = schema.fields.findIndex((f) => f.name === n);
    if (idx < 0) throw new Error(`Column '${n}' not found in schema`);
    return idx;
  });
}

function resolveIndexGroups(schema: Schema, groups?: string[][]): number[][] {
  if (!groups || groups.length === 0) return [];
  return groups.map((group) => resolveIndices(schema, group));
}

const FK_SCHEMA = new Schema([
  new Field("fk_columns", new List(new Field("item", new Utf8(), true)), false),
  new Field("pk_columns", new List(new Field("item", new Utf8(), true)), false),
  new Field("referenced_table", new Utf8(), false),
  new Field("referenced_schema", new Utf8(), false),
]);

function serializeForeignKeys(fks?: ForeignKeyDef[], schemaName?: string): Uint8Array[] {
  if (!fks || fks.length === 0) return [];
  return fks.map((fk) =>
    serializeBatch(
      batchFromColumns(
        {
          fk_columns: [fk.columns],
          pk_columns: [fk.referencedColumns],
          referenced_table: [fk.referencedTable],
          referenced_schema: [fk.referencedSchema ?? schemaName ?? "main"],
        },
        FK_SCHEMA
      )
    )
  );
}

function defaultToSql(value: DefaultValue): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") return String(value);
  // string — quote as SQL string literal
  const escaped = value.replace(/'/g, "''");
  return `'${escaped}'`;
}

function applyDefaults(schema: Schema, defaults: Record<string, DefaultValue>): Schema {
  let result = schema;
  for (const [colName, value] of Object.entries(defaults)) {
    const idx = result.fields.findIndex((f) => f.name === colName);
    if (idx < 0) continue;
    const f = result.fields[idx];
    const existingMeta = f.metadata ? new Map(f.metadata) : new Map<string, string>();
    existingMeta.set("default", defaultToSql(value));
    const newField = new Field(f.name, f.type, f.nullable, existingMeta);
    const fields = [...result.fields];
    fields[idx] = newField;
    result = new Schema(fields);
  }
  return result;
}

function applyGeneratedColumns(schema: Schema, generated: Record<string, string>): Schema {
  let result = schema;
  for (const [colName, expression] of Object.entries(generated)) {
    const idx = result.fields.findIndex((f) => f.name === colName);
    if (idx < 0) continue;
    const f = result.fields[idx];
    const existingMeta = f.metadata ? new Map(f.metadata) : new Map<string, string>();
    existingMeta.set("generated_expression", expression);
    const newField = new Field(f.name, f.type, f.nullable, existingMeta);
    const fields = [...result.fields];
    fields[idx] = newField;
    result = new Schema(fields);
  }
  return result;
}

function applyColumnComments(schema: Schema, comments: Record<string, string>): Schema {
  let result = schema;
  for (const [colName, comment] of Object.entries(comments)) {
    if (!comment) continue;
    const idx = result.fields.findIndex((f) => f.name === colName);
    if (idx < 0) continue;
    const f = result.fields[idx];
    const existingMeta = f.metadata ? new Map(f.metadata) : new Map<string, string>();
    existingMeta.set("comment", comment);
    const newField = new Field(f.name, f.type, f.nullable, existingMeta);
    const fields = [...result.fields];
    fields[idx] = newField;
    result = new Schema(fields);
  }
  return result;
}

// ============================================================================
// Setting serialization (matches Python SettingSpec.serialize() format)
// ============================================================================

const SETTING_SCHEMA = new Schema([
  new Field("name", new Utf8(), false),
  new Field("description", new Utf8(), false),
  new Field("type", new Binary(), false),
  new Field("default_value", new Binary(), true),
]);

function serializeSetting(setting: SettingDescriptor): Uint8Array {
  // Serialize the Arrow type as a schema with a single "value" field
  const typeSchema = new Schema([new Field("value", setting.type, true)]);
  const typeBytes = serializeSchema(typeSchema);

  // Serialize the default value as a single-row batch, or null
  let defaultBytes: Uint8Array | null = null;
  if (setting.defaultValue != null) {
    let val: any = setting.defaultValue;
    // Coerce JS numbers to BigInt for 64-bit integer Arrow types
    if (DataType.isInt(setting.type) && (setting.type as any).bitWidth === 64) {
      if (typeof val === "number") val = BigInt(val);
    }
    // Coerce BigInt to Number for 32-bit integer Arrow types
    if (DataType.isInt(setting.type) && (setting.type as any).bitWidth <= 32) {
      if (typeof val === "bigint") val = Number(val);
    }
    defaultBytes = serializeBatch(
      batchFromColumns({ value: [val] }, typeSchema)
    );
  }

  // Serialize the outer setting spec batch
  return serializeBatch(
    batchFromColumns(
      {
        name: [setting.name],
        description: [setting.description],
        type: [typeBytes],
        default_value: [defaultBytes],
      },
      SETTING_SCHEMA
    )
  );
}

// ============================================================================
// Secret type serialization (matches Python SecretTypeSpec.serialize() format)
// ============================================================================

const SECRET_TYPE_SCHEMA = new Schema([
  new Field("name", new Utf8(), false),
  new Field("description", new Utf8(), false),
  new Field("parameters_schema", new Binary(), false),
]);

function serializeSecretType(spec: SecretTypeDescriptor): Uint8Array {
  const parametersSchemaBytes = serializeSchema(spec.schema);

  return serializeBatch(
    batchFromColumns(
      {
        name: [spec.name],
        description: [spec.description],
        parameters_schema: [parametersSchemaBytes],
      },
      SECRET_TYPE_SCHEMA
    )
  );
}

function bufferEquals(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function serializeArgsBatch(args: Arguments, argSpecSchema: Schema): Uint8Array {
  // Serialize arguments as a FLAT batch using the argument spec field names.
  // DuckDB's VGI extension reads each column as a named argument and wraps it
  // with "named_" prefix when sending the bind request. The function extractArgs
  // will look up these by name as a fallback.
  const fields: Field[] = [];
  const values: Record<string, any[]> = {};

  for (let i = 0; i < args.positional.length; i++) {
    const val = args.positional[i];
    const specField = argSpecSchema.fields[i];
    const fieldName = specField ? specField.name : `positional_${i}`;
    const fieldType = specField ? specField.type : inferScalarType(val);
    fields.push(new Field(fieldName, fieldType, true));
    let coerced = val;
    if (DataType.isInt(fieldType) && (fieldType as any).bitWidth === 64) {
      coerced = typeof val === "number" ? BigInt(val) : val;
    }
    values[fieldName] = [coerced];
  }

  for (const [name, val] of args.named) {
    fields.push(new Field(name, inferScalarType(val), true));
    values[name] = [val];
  }

  const batchSchema = new Schema(fields);

  if (fields.length === 0) {
    return serializeBatch(emptyBatch(batchSchema));
  }

  return serializeBatch(batchFromColumns(values, batchSchema));
}

function validateAtParams(atUnit?: string, atValue?: string): void {
  if (Boolean(atUnit) !== Boolean(atValue)) {
    throw new Error("at_unit and at_value must both be provided or both be absent");
  }
}

function inferScalarType(val: any): DataType {
  if (val === null || val === undefined) return new Null();
  if (typeof val === "string") return new Utf8();
  if (typeof val === "boolean") return new Bool();
  if (typeof val === "number") return new Int64();
  if (typeof val === "bigint") return new Int64();
  if (val instanceof Uint8Array || val instanceof ArrayBuffer) return new Binary();
  return new Utf8();
}

// Build the inlined `scan_function` payload for a function-backed table.
// Mirrors python's _inline_function_result: a serialized one-row batch of
// ScanFunctionResultSchema (function_name, arguments, required_extensions).
// The C++ extension uses these bytes verbatim and skips the
// catalog_table_scan_function_get RPC.
function inlineScanFunction(
  func: VgiFunction,
  args: Arguments | undefined,
): Uint8Array {
  const argSchema = argumentSpecsToSchema(func.argumentSpecs);
  const argBytes = serializeArgsBatch(args ?? new Arguments(), argSchema);
  const scanSchema = new Schema([
    new Field("function_name", new Utf8(), false),
    new Field("arguments", new Binary(), false),
    new Field(
      "required_extensions",
      new List(new Field("item", new Utf8(), true)),
      false,
    ),
  ]);
  const batch = batchFromColumns(
    {
      function_name: [func.meta.name],
      arguments: [argBytes],
      required_extensions: [[] as string[]],
    },
    scanSchema,
  );
  return serializeBatch(batch);
}
