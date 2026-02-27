// ReadOnlyCatalogInterface: derives catalog from registered functions + descriptors.

import { Schema, Field, Int64 } from "apache-arrow";
import {
  CatalogInterface,
  type AttachId,
  type TransactionId,
  type CatalogAttachResult,
  SchemaInfo,
  TableInfo,
  ViewInfo,
  FunctionInfo,
} from "./interface.js";
import type { CatalogDescriptor, SchemaDescriptor, TableDescriptor, ViewDescriptor } from "./descriptors.js";
import type { VgiFunction } from "../functions/types.js";
import type { FunctionRegistry } from "../functions/registry.js";
import { argumentSpecsToSchema } from "../arguments/argument-spec.js";
import { serializeSchema, serializeBatch } from "../util/arrow.js";
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
      throw new Error(`Unknown catalog: '${name}'`);
    }

    const attachId = new Uint8Array(16);
    crypto.getRandomValues(attachId);
    this._attachments.set(name, attachId);
    return {
      attachId,
      supportsTransactions: false,
      supportsTimeTravel: false,
      catalogVersionFrozen: false,
      catalogVersion: this._version,
      attachIdRequired: true,
      defaultSchema: this._descriptor.defaultSchema ?? "main",
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
      let columns: Uint8Array;
      if (t.columns) {
        columns = serializeSchema(t.columns);
      } else if (t.function) {
        // Function-backed table: bind to get output schema
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
          };
          const response = func.bind(bindRequest);
          columns = serializeSchema(response.outputSchema);
        } catch {
          columns = serializeSchema(new Schema([]));
        }
      } else {
        columns = serializeSchema(new Schema([]));
      }

      // Convert named not_null constraints to column indices
      const notNullIndices: number[] = [];
      // (simplified - would need to cross-reference column names)

      return new TableInfo(
        t.name,
        name,
        columns,
        notNullIndices,
        [], // unique constraints
        t.check ?? [],
        t.comment ?? null,
        t.tags ?? {}
      );
    });
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
        // Filter by type (DuckDB sends uppercase: SCALAR_FUNCTION, TABLE_FUNCTION)
        const t = type.toUpperCase();
        if (t === "SCALAR_FUNCTION" && f.kind !== "scalar") return false;
        if (t === "TABLE_FUNCTION" && f.kind !== "table" && f.kind !== "table_in_out") return false;
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

        return new FunctionInfo(
          f.meta.name,
          name,
          meta.functionType.toLowerCase(),
          argBytes,
          outputSchemaBytes,
          meta.stability,
          meta.nullHandling,
          meta.description,
          meta.examples.map((e) => ({ sql: e.sql, description: e.description })),
          meta.categories,
          meta.projectionPushdown,
          meta.filterPushdown,
          meta.preservesOrder,
          meta.maxWorkers,
          meta.orderDependent,
          meta.distinctDependent,
          meta.requiredSettings,
          null,
          {}
        );
      });
  }

  override tableScanFunctionGet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    atUnit?: string,
    atValue?: string,
    transactionId?: TransactionId
  ): any {
    // Find the table descriptor
    const schema = this._descriptor.schemas.find((s) => s.name === schemaName);
    if (!schema || !schema.tables) {
      throw new Error(`Table '${name}' not found in schema '${schemaName}'`);
    }
    const table = schema.tables.find((t) => t.name === name);
    if (!table) {
      throw new Error(`Table '${name}' not found in schema '${schemaName}'`);
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
    transactionId?: TransactionId
  ): TableInfo | null {
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

function bufferEquals(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function serializeArgsBatch(args: Arguments, schema: Schema): Uint8Array {
  // Serialize arguments as an Arrow IPC batch matching the expected schema
  // This creates a batch with positional args as numbered columns and named args
  const { serializeBatch, batchFromColumns } = require("../util/arrow.js");
  const values: Record<string, any[]> = {};

  for (let i = 0; i < args.positional.length; i++) {
    const field = schema.fields[i];
    if (field) {
      values[field.name] = [args.positional[i]];
    }
  }

  for (const [name, val] of args.named) {
    values[name] = [val];
  }

  if (Object.keys(values).length === 0) {
    // Empty arguments
    const { emptyBatch } = require("../util/arrow.js");
    return serializeBatch(emptyBatch(schema));
  }

  return serializeBatch(batchFromColumns(values, schema));
}
