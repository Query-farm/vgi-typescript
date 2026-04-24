// ReadOnlyCatalogInterface: derives catalog from registered functions + descriptors.

import { Schema, Field, Int32, Int64, DataType, Utf8, Binary, Bool, Null, List } from "@query-farm/apache-arrow";
import {
  CatalogInterface,
  type AttachId,
  type TransactionId,
  type CatalogAttachResult,
  SchemaInfo,
  TableInfo,
  ViewInfo,
  FunctionInfo,
  MacroInfo,
  MacroType,
} from "./interface.js";
import type { CatalogDescriptor, SchemaDescriptor, TableDescriptor, ViewDescriptor, MacroDescriptor, SettingDescriptor, SecretTypeDescriptor, ForeignKeyDef, DefaultValue } from "./descriptors.js";
import { serializeColumnStatistics } from "../util/statistics.js";
import type { VgiFunction } from "../functions/types.js";
import type { FunctionRegistry } from "../functions/registry.js";
import { argumentSpecsToSchema } from "../arguments/argument-spec.js";
import { serializeSchema, serializeBatch, emptyBatch, batchFromColumns } from "../util/arrow.js";
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

  attach(name: string, options?: any): CatalogAttachResult {
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
      attachId,
      supportsTransactions: false,
      supportsTimeTravel: hasTimeTravel,
      catalogVersionFrozen: true,
      catalogVersion: this._version,
      attachIdRequired: true,
      defaultSchema: this._descriptor.defaultSchema ?? "main",
      settings,
      secretTypes,
      comment: this._descriptor.comment ?? null,
      tags: this._descriptor.tags ?? {},
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
    return this._descriptor.schemas.map(
      (s) =>
        new SchemaInfo(
          attachId,
          s.name,
          s.comment ?? null,
          s.tags ?? {}
        )
    );
  }

  override schemaGet(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): SchemaInfo | null {
    const desc = this._descriptor.schemas.find((s) => s.name === name);
    if (!desc) return null;
    return new SchemaInfo(
      attachId,
      desc.name,
      desc.comment ?? null,
      desc.tags ?? {}
    );
  }

  override schemaContentsTables(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): TableInfo[] {
    const schema = this._descriptor.schemas.find((s) => s.name === name);
    if (!schema || !schema.tables) return [];

    return schema.tables.map((t) => {
      // Resolve the column schema
      let colSchema: Schema;
      if (t.columns) {
        colSchema = t.columns;
      } else if (t.function) {
        const func = t.function;
        const dummyArgs = t.arguments ?? new Arguments();
        try {
          const bindRequest = {
            functionName: func.meta.name,
            arguments: dummyArgs,
            functionType: "table" as any,
            inputSchema: null,
            settings: null,
            secrets: null,
            attachId: attachId,
            transactionId: null,
            resolvedSecretsProvided: false,
          };
          const response = func.bind(bindRequest);
          colSchema = response.outputSchema;
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

      return new TableInfo(
        t.name,
        name,
        columns,
        notNullIndices,
        uniqueIndices,
        t.check ?? [],
        pkIndices,
        fkBytes,
        t.comment ?? null,
        t.tags ?? {},
        t.statistics != null && Object.keys(t.statistics).length > 0,
      );
    });
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

    return schema.views.map(
      (v) =>
        new ViewInfo(
          v.name,
          name,
          v.definition,
          v.comment ?? null,
          v.tags ?? {}
        )
    );
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
        // Filter by type (DuckDB sends uppercase: SCALAR_FUNCTION, TABLE_FUNCTION,
        // AGGREGATE_FUNCTION). Unknown filter values pass everything through.
        const t = type.toUpperCase();
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

        return new FunctionInfo({
          name: f.meta.name,
          schemaName: name,
          functionType: meta.functionType.toUpperCase(),
          functionArguments: argBytes,
          outputSchema: outputSchemaBytes,
          stability: meta.stability,
          nullHandling: meta.nullHandling,
          description: meta.description,
          examples: meta.examples.map((e) => ({ sql: e.sql, description: e.description })),
          categories: meta.categories,
          projectionPushdown: meta.projectionPushdown,
          filterPushdown: meta.filterPushdown,
          samplingPushdown: meta.samplingPushdown,
          supportedExpressionFilters: meta.supportedExpressionFilters,
          orderPreservation: meta.preservesOrder,
          maxWorkers: meta.maxWorkers,
          orderDependent: meta.orderDependent,
          distinctDependent: meta.distinctDependent,
          requiredSettings: meta.requiredSettings,
          requiredSecrets,
        });
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
        const t = type.toLowerCase();
        if (t === "scalar_macro" && m.macroType !== "scalar") return false;
        if (t === "table_macro" && m.macroType !== "table") return false;
        return true;
      })
      .map(
        (m) =>
          new MacroInfo(
            m.name,
            name,
            m.macroType === "scalar" ? MacroType.SCALAR : MacroType.TABLE,
            m.parameters,
            m.parameterDefaultValues ?? null,
            m.definition,
            m.comment ?? null,
            m.tags ?? {}
          )
      );
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

  override tableGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    atUnit?: string,
    atValue?: string,
    transactionId?: TransactionId
  ): TableInfo | null {
    validateAtParams(atUnit, atValue);

    // Find the table descriptor to check supports_time_travel
    const schema = this._descriptor.schemas.find((s) => s.name === schemaName);
    if (schema?.tables) {
      const tableDesc = schema.tables.find((t) => t.name === name);
      if (tableDesc && atUnit && !tableDesc.supportsTimeTravel) {
        throw new Error(`Table '${schemaName}.${name}' does not support time travel queries`);
      }
    }

    const tables = this.schemaContentsTables(attachId, schemaName, transactionId);
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
