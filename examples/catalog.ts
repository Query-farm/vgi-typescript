// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// InMemoryCatalog: full read-write CatalogInterface implementation.
// Matches Python vgi/examples/catalog.py

import {
  CatalogInterface,
  type AttachOpaqueData,
  type TransactionOpaqueData,
  type CatalogAttachResult,
  SchemaInfo,
  TableInfo,
  ViewInfo,
  FunctionInfo,
} from "../src/catalog/interface.js";
import { CatalogNotFoundError, CatalogAlreadyExistsError, CatalogReadOnlyError } from "../src/errors.js";

interface InMemorySchema {
  name: string;
  comment: string | null;
  tags: Record<string, string>;
  tables: Map<string, InMemoryTable>;
  views: Map<string, InMemoryView>;
}

interface InMemoryTable {
  name: string;
  schemaName: string;
  columns: Uint8Array;
  notNullConstraints: number[];
  uniqueConstraints: number[][];
  checkConstraints: string[];
  comment: string | null;
  tags: Record<string, string>;
}

interface InMemoryView {
  name: string;
  schemaName: string;
  definition: string;
  comment: string | null;
  tags: Record<string, string>;
}

export class InMemoryCatalog extends CatalogInterface {
  private _attachments = new Map<string, { attachOpaqueData: AttachOpaqueData; schemas: Map<string, InMemorySchema> }>();
  private _version = 1;

  catalogs(): string[] {
    return ["memory"];
  }

  attach(
    name: string,
    options?: any,
    _dataVersionSpec?: string | null,
    _implementationVersion?: string | null,
  ): CatalogAttachResult {
    if (!this.catalogs().includes(name)) {
      throw new Error(`Unknown catalog: '${name}'`);
    }

    const attachOpaqueData = new Uint8Array(16);
    crypto.getRandomValues(attachOpaqueData);

    const schemas = new Map<string, InMemorySchema>();
    schemas.set("main", {
      name: "main",
      comment: null,
      tags: {},
      tables: new Map(),
      views: new Map(),
    });

    const key = this._attachOpaqueDataKey(attachOpaqueData);
    this._attachments.set(key, { attachOpaqueData, schemas });

    return {
      attach_opaque_data: attachOpaqueData,
      supports_transactions: false,
      supports_time_travel: false,
      catalog_version_frozen: false,
      catalog_version: this._version,
      attach_opaque_data_required: true,
      default_schema: "main",
      resolved_data_version: null,
      resolved_implementation_version: null,
    };
  }

  detach(attachOpaqueData: AttachOpaqueData): void {
    const key = this._attachOpaqueDataKey(attachOpaqueData);
    this._attachments.delete(key);
  }

  version(attachOpaqueData: AttachOpaqueData, transactionOpaqueData?: TransactionOpaqueData): number {
    return this._version;
  }

  schemas(attachOpaqueData: AttachOpaqueData, transactionOpaqueData?: TransactionOpaqueData): SchemaInfo[] {
    const att = this._getAttachment(attachOpaqueData);
    return [...att.schemas.values()].map((s) => ({
      attach_opaque_data: attachOpaqueData,
      name: s.name,
      comment: s.comment ?? null,
      tags: s.tags ?? {},
    }));
  }

  override schemaGet(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): SchemaInfo | null {
    const att = this._getAttachment(attachOpaqueData);
    const s = att.schemas.get(name);
    if (!s) return null;
    return {
      attach_opaque_data: attachOpaqueData,
      name: s.name,
      comment: s.comment ?? null,
      tags: s.tags ?? {},
    };
  }

  override schemaCreate(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    comment?: string | null,
    tags?: any,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    if (att.schemas.has(name)) {
      throw new CatalogAlreadyExistsError("Schema", name);
    }
    att.schemas.set(name, {
      name,
      comment: comment ?? null,
      tags: tags ?? {},
      tables: new Map(),
      views: new Map(),
    });
    this._version++;
  }

  override schemaDrop(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    ignoreNotFound?: boolean,
    cascade?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    if (!att.schemas.has(name)) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("Schema", name);
    }
    att.schemas.delete(name);
    this._version++;
  }

  override schemaContentsTables(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): TableInfo[] {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(name);
    if (!schema) return [];
    return [...schema.tables.values()].map((t) => ({
      comment: t.comment ?? null,
      tags: t.tags ?? {},
      name: t.name,
      schema_name: t.schemaName,
      columns: t.columns,
      not_null_constraints: t.notNullConstraints,
      unique_constraints: t.uniqueConstraints,
      check_constraints: t.checkConstraints,
      primary_key_constraints: [],
      foreign_key_constraints: [],
      supports_insert: false,
      supports_update: false,
      supports_delete: false,
      supports_returning: false,
      supports_column_statistics: false,
      scan_function: new Uint8Array(0),
      insert_function: new Uint8Array(0),
      update_function: new Uint8Array(0),
      delete_function: new Uint8Array(0),
      cardinality_estimate: 0,
      cardinality_max: 0,
      required_filters: [],
    })));
  }

  override schemaContentsViews(
    attachOpaqueData: AttachOpaqueData,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): ViewInfo[] {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(name);
    if (!schema) return [];
    return [...schema.views.values()].map(
      (v) => ({
      comment: v.comment ?? null,
      tags: v.tags ?? {},
      name: v.name,
      schema_name: v.schemaName,
      definition: v.definition,
    })
    );
  }

  override tableGet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): TableInfo | null {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaName);
    if (!schema) return null;
    const t = schema.tables.get(name);
    if (!t) return null;
    return {
      comment: t.comment ?? null,
      tags: t.tags ?? {},
      name: t.name,
      schema_name: t.schemaName,
      columns: t.columns,
      not_null_constraints: t.notNullConstraints,
      unique_constraints: t.uniqueConstraints,
      check_constraints: t.checkConstraints,
      primary_key_constraints: [],
      foreign_key_constraints: [],
      supports_insert: false,
      supports_update: false,
      supports_delete: false,
      supports_returning: false,
      supports_column_statistics: false,
      scan_function: new Uint8Array(0),
      insert_function: new Uint8Array(0),
      update_function: new Uint8Array(0),
      delete_function: new Uint8Array(0),
      cardinality_estimate: 0,
      cardinality_max: 0,
      required_filters: [],
    };
  }

  override tableCreate(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    columns: Uint8Array,
    onConflict: string,
    notNullConstraints?: number[],
    uniqueConstraints?: number[][],
    checkConstraints?: string[],
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaName);
    if (!schema) throw new CatalogNotFoundError("Schema", schemaName);
    if (schema.tables.has(name)) {
      if (onConflict === "ignore") return;
      if (onConflict === "replace") {
        schema.tables.delete(name);
      } else {
        throw new CatalogAlreadyExistsError("Table", name);
      }
    }
    schema.tables.set(name, {
      name,
      schemaName,
      columns,
      notNullConstraints: notNullConstraints ?? [],
      uniqueConstraints: uniqueConstraints ?? [],
      checkConstraints: checkConstraints ?? [],
      comment: null,
      tags: {},
    });
    this._version++;
  }

  override tableDrop(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaName);
    if (!schema || !schema.tables.has(name)) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("Table", name);
    }
    schema.tables.delete(name);
    this._version++;
  }

  override tableCommentSet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaName);
    if (!schema) throw new CatalogNotFoundError("Schema", schemaName);
    const t = schema.tables.get(name);
    if (!t) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("Table", name);
    }
    t.comment = comment ?? null;
    this._version++;
  }

  override tableRename(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaName);
    if (!schema) throw new CatalogNotFoundError("Schema", schemaName);
    const t = schema.tables.get(name);
    if (!t) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("Table", name);
    }
    schema.tables.delete(name);
    t.name = newName;
    schema.tables.set(newName, t);
    this._version++;
  }

  override viewGet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): ViewInfo | null {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaName);
    if (!schema) return null;
    const v = schema.views.get(name);
    if (!v) return null;
    return ({
      comment: v.comment ?? null,
      tags: v.tags ?? {},
      name: v.name,
      schema_name: v.schemaName,
      definition: v.definition,
    });
  }

  override viewCreate(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    definition: string,
    onConflict: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaName);
    if (!schema) throw new CatalogNotFoundError("Schema", schemaName);
    if (schema.views.has(name)) {
      if (onConflict === "ignore") return;
      if (onConflict === "replace") {
        schema.views.delete(name);
      } else {
        throw new CatalogAlreadyExistsError("View", name);
      }
    }
    schema.views.set(name, {
      name,
      schemaName,
      definition,
      comment: null,
      tags: {},
    });
    this._version++;
  }

  override viewDrop(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaName);
    if (!schema || !schema.views.has(name)) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("View", name);
    }
    schema.views.delete(name);
    this._version++;
  }

  override viewRename(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaName);
    if (!schema) throw new CatalogNotFoundError("Schema", schemaName);
    const v = schema.views.get(name);
    if (!v) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("View", name);
    }
    schema.views.delete(name);
    v.name = newName;
    schema.views.set(newName, v);
    this._version++;
  }

  override viewCommentSet(
    attachOpaqueData: AttachOpaqueData,
    schemaName: string,
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaName);
    if (!schema) throw new CatalogNotFoundError("Schema", schemaName);
    const v = schema.views.get(name);
    if (!v) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("View", name);
    }
    v.comment = comment ?? null;
    this._version++;
  }

  // Helpers

  private _attachOpaqueDataKey(attachOpaqueData: AttachOpaqueData): string {
    return Array.from(attachOpaqueData).map((b) => b.toString(16).padStart(2, "0")).join("");
  }

  private _getAttachment(attachOpaqueData: AttachOpaqueData) {
    const key = this._attachOpaqueDataKey(attachOpaqueData);
    const att = this._attachments.get(key);
    if (!att) throw new Error("Catalog not attached");
    return att;
  }
}
