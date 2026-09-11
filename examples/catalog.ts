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
import { schemaPathDisplay, schemaPathKey } from "../src/schema-path.js";

interface InMemorySchema {
  path: string[];
  comment: string | null;
  tags: Record<string, string>;
  tables: Map<string, InMemoryTable>;
  views: Map<string, InMemoryView>;
}

interface InMemoryTable {
  name: string;
  schemaPath: string[];
  columns: Uint8Array;
  notNullConstraints: number[];
  uniqueConstraints: number[][];
  checkConstraints: string[];
  comment: string | null;
  tags: Record<string, string>;
}

interface InMemoryView {
  name: string;
  schemaPath: string[];
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
    schemas.set(schemaPathKey(["main"]), {
      path: ["main"],
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
      path: s.path,
      comment: s.comment ?? null,
      tags: s.tags ?? {},
    }));
  }

  override schemaGet(
    attachOpaqueData: AttachOpaqueData,
    path: string[],
    transactionOpaqueData?: TransactionOpaqueData
  ): SchemaInfo | null {
    const att = this._getAttachment(attachOpaqueData);
    const s = att.schemas.get(schemaPathKey(path));
    if (!s) return null;
    return {
      attach_opaque_data: attachOpaqueData,
      path: s.path,
      comment: s.comment ?? null,
      tags: s.tags ?? {},
    };
  }

  override schemaCreate(
    attachOpaqueData: AttachOpaqueData,
    path: string[],
    comment?: string | null,
    tags?: any,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const key = schemaPathKey(path);
    if (att.schemas.has(key)) {
      throw new CatalogAlreadyExistsError("Schema", schemaPathDisplay(path));
    }
    att.schemas.set(key, {
      path,
      comment: comment ?? null,
      tags: tags ?? {},
      tables: new Map(),
      views: new Map(),
    });
    this._version++;
  }

  override schemaDrop(
    attachOpaqueData: AttachOpaqueData,
    path: string[],
    ignoreNotFound?: boolean,
    cascade?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const key = schemaPathKey(path);
    if (!att.schemas.has(key)) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("Schema", schemaPathDisplay(path));
    }
    att.schemas.delete(key);
    this._version++;
  }

  override schemaContentsTables(
    attachOpaqueData: AttachOpaqueData,
    path: string[],
    transactionOpaqueData?: TransactionOpaqueData
  ): TableInfo[] {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaPathKey(path));
    if (!schema) return [];
    return [...schema.tables.values()].map((t) => ({
      comment: t.comment ?? null,
      tags: t.tags ?? {},
      name: t.name,
      schema_path: t.schemaPath,
      columns: t.columns,
      not_null_constraints: t.notNullConstraints,
      unique_constraints: t.uniqueConstraints,
      check_constraints: t.checkConstraints,
      primary_key_constraints: [],
      foreign_key_constraints: [],
      write_result_modes: {},
      supports_column_statistics: false,
      scan_function: new Uint8Array(0),
      insert_function: new Uint8Array(0),
      update_function: new Uint8Array(0),
      delete_function: new Uint8Array(0),
      cardinality_estimate: 0,
      cardinality_max: 0,
      required_filters: [],
    }));
  }

  override schemaContentsViews(
    attachOpaqueData: AttachOpaqueData,
    path: string[],
    transactionOpaqueData?: TransactionOpaqueData
  ): ViewInfo[] {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaPathKey(path));
    if (!schema) return [];
    return [...schema.views.values()].map(
      (v) => ({
      comment: v.comment ?? null,
      tags: v.tags ?? {},
      name: v.name,
      schema_path: v.schemaPath,
      definition: v.definition,
      column_comments: {},
    })
    );
  }

  override tableGet(
    attachOpaqueData: AttachOpaqueData,
    schemaPath: string[],
    name: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): TableInfo | null {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaPathKey(schemaPath));
    if (!schema) return null;
    const t = schema.tables.get(name);
    if (!t) return null;
    return {
      comment: t.comment ?? null,
      tags: t.tags ?? {},
      name: t.name,
      schema_path: t.schemaPath,
      columns: t.columns,
      not_null_constraints: t.notNullConstraints,
      unique_constraints: t.uniqueConstraints,
      check_constraints: t.checkConstraints,
      primary_key_constraints: [],
      foreign_key_constraints: [],
      write_result_modes: {},
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
    schemaPath: string[],
    name: string,
    columns: Uint8Array,
    onConflict: string,
    notNullConstraints?: number[],
    uniqueConstraints?: number[][],
    checkConstraints?: string[],
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaPathKey(schemaPath));
    if (!schema) throw new CatalogNotFoundError("Schema", schemaPathDisplay(schemaPath));
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
      schemaPath,
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
    schemaPath: string[],
    name: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaPathKey(schemaPath));
    if (!schema || !schema.tables.has(name)) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("Table", name);
    }
    schema.tables.delete(name);
    this._version++;
  }

  override tableCommentSet(
    attachOpaqueData: AttachOpaqueData,
    schemaPath: string[],
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaPathKey(schemaPath));
    if (!schema) throw new CatalogNotFoundError("Schema", schemaPathDisplay(schemaPath));
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
    schemaPath: string[],
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaPathKey(schemaPath));
    if (!schema) throw new CatalogNotFoundError("Schema", schemaPathDisplay(schemaPath));
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
    schemaPath: string[],
    name: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): ViewInfo | null {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaPathKey(schemaPath));
    if (!schema) return null;
    const v = schema.views.get(name);
    if (!v) return null;
    return ({
      comment: v.comment ?? null,
      tags: v.tags ?? {},
      name: v.name,
      schema_path: v.schemaPath,
      definition: v.definition,
      column_comments: {},
    });
  }

  override viewCreate(
    attachOpaqueData: AttachOpaqueData,
    schemaPath: string[],
    name: string,
    definition: string,
    onConflict: string,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaPathKey(schemaPath));
    if (!schema) throw new CatalogNotFoundError("Schema", schemaPathDisplay(schemaPath));
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
      schemaPath,
      definition,
      comment: null,
      tags: {},
    });
    this._version++;
  }

  override viewDrop(
    attachOpaqueData: AttachOpaqueData,
    schemaPath: string[],
    name: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaPathKey(schemaPath));
    if (!schema || !schema.views.has(name)) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("View", name);
    }
    schema.views.delete(name);
    this._version++;
  }

  override viewRename(
    attachOpaqueData: AttachOpaqueData,
    schemaPath: string[],
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaPathKey(schemaPath));
    if (!schema) throw new CatalogNotFoundError("Schema", schemaPathDisplay(schemaPath));
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
    schemaPath: string[],
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionOpaqueData?: TransactionOpaqueData
  ): void {
    const att = this._getAttachment(attachOpaqueData);
    const schema = att.schemas.get(schemaPathKey(schemaPath));
    if (!schema) throw new CatalogNotFoundError("Schema", schemaPathDisplay(schemaPath));
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
