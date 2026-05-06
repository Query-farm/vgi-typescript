// InMemoryCatalog: full read-write CatalogInterface implementation.
// Matches Python vgi/examples/catalog.py

import {
  CatalogInterface,
  type AttachId,
  type TransactionId,
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
  private _attachments = new Map<string, { attachId: AttachId; schemas: Map<string, InMemorySchema> }>();
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

    const attachId = new Uint8Array(16);
    crypto.getRandomValues(attachId);

    const schemas = new Map<string, InMemorySchema>();
    schemas.set("main", {
      name: "main",
      comment: null,
      tags: {},
      tables: new Map(),
      views: new Map(),
    });

    const key = this._attachIdKey(attachId);
    this._attachments.set(key, { attachId, schemas });

    return {
      attach_id: attachId,
      supports_transactions: false,
      supports_time_travel: false,
      catalog_version_frozen: false,
      catalog_version: this._version,
      attach_id_required: true,
      default_schema: "main",
      resolved_data_version: null,
      resolved_implementation_version: null,
    };
  }

  detach(attachId: AttachId): void {
    const key = this._attachIdKey(attachId);
    this._attachments.delete(key);
  }

  version(attachId: AttachId, transactionId?: TransactionId): number {
    return this._version;
  }

  schemas(attachId: AttachId, transactionId?: TransactionId): SchemaInfo[] {
    const att = this._getAttachment(attachId);
    return [...att.schemas.values()].map((s) => ({
      attach_id: attachId,
      name: s.name,
      comment: s.comment ?? null,
      tags: s.tags ?? {},
    }));
  }

  override schemaGet(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): SchemaInfo | null {
    const att = this._getAttachment(attachId);
    const s = att.schemas.get(name);
    if (!s) return null;
    return {
      attach_id: attachId,
      name: s.name,
      comment: s.comment ?? null,
      tags: s.tags ?? {},
    };
  }

  override schemaCreate(
    attachId: AttachId,
    name: string,
    comment?: string | null,
    tags?: any,
    transactionId?: TransactionId
  ): void {
    const att = this._getAttachment(attachId);
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
    attachId: AttachId,
    name: string,
    ignoreNotFound?: boolean,
    cascade?: boolean,
    transactionId?: TransactionId
  ): void {
    const att = this._getAttachment(attachId);
    if (!att.schemas.has(name)) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("Schema", name);
    }
    att.schemas.delete(name);
    this._version++;
  }

  override schemaContentsTables(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): TableInfo[] {
    const att = this._getAttachment(attachId);
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
    }));
  }

  override schemaContentsViews(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): ViewInfo[] {
    const att = this._getAttachment(attachId);
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
    attachId: AttachId,
    schemaName: string,
    name: string,
    transactionId?: TransactionId
  ): TableInfo | null {
    const att = this._getAttachment(attachId);
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
    };
  }

  override tableCreate(
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
    const att = this._getAttachment(attachId);
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
    attachId: AttachId,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    const att = this._getAttachment(attachId);
    const schema = att.schemas.get(schemaName);
    if (!schema || !schema.tables.has(name)) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("Table", name);
    }
    schema.tables.delete(name);
    this._version++;
  }

  override tableCommentSet(
    attachId: AttachId,
    schemaName: string,
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    const att = this._getAttachment(attachId);
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
    attachId: AttachId,
    schemaName: string,
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    const att = this._getAttachment(attachId);
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
    attachId: AttachId,
    schemaName: string,
    name: string,
    transactionId?: TransactionId
  ): ViewInfo | null {
    const att = this._getAttachment(attachId);
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
    attachId: AttachId,
    schemaName: string,
    name: string,
    definition: string,
    onConflict: string,
    transactionId?: TransactionId
  ): void {
    const att = this._getAttachment(attachId);
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
    attachId: AttachId,
    schemaName: string,
    name: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    const att = this._getAttachment(attachId);
    const schema = att.schemas.get(schemaName);
    if (!schema || !schema.views.has(name)) {
      if (ignoreNotFound) return;
      throw new CatalogNotFoundError("View", name);
    }
    schema.views.delete(name);
    this._version++;
  }

  override viewRename(
    attachId: AttachId,
    schemaName: string,
    name: string,
    newName: string,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    const att = this._getAttachment(attachId);
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
    attachId: AttachId,
    schemaName: string,
    name: string,
    comment?: string | null,
    ignoreNotFound?: boolean,
    transactionId?: TransactionId
  ): void {
    const att = this._getAttachment(attachId);
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

  private _attachIdKey(attachId: AttachId): string {
    return Array.from(attachId).map((b) => b.toString(16).padStart(2, "0")).join("");
  }

  private _getAttachment(attachId: AttachId) {
    const key = this._attachIdKey(attachId);
    const att = this._attachments.get(key);
    if (!att) throw new Error("Catalog not attached");
    return att;
  }
}
