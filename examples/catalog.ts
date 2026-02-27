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

  attach(name: string, options?: any): CatalogAttachResult {
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
      attachId,
      supportsTransactions: false,
      supportsTimeTravel: false,
      catalogVersionFrozen: false,
      catalogVersion: this._version,
      attachIdRequired: true,
      defaultSchema: "main",
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
    return [...att.schemas.values()].map(
      (s) => new SchemaInfo(attachId, s.name, s.comment, s.tags)
    );
  }

  override schemaGet(
    attachId: AttachId,
    name: string,
    transactionId?: TransactionId
  ): SchemaInfo | null {
    const att = this._getAttachment(attachId);
    const s = att.schemas.get(name);
    if (!s) return null;
    return new SchemaInfo(attachId, s.name, s.comment, s.tags);
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
    return [...schema.tables.values()].map(
      (t) =>
        new TableInfo(
          t.name,
          t.schemaName,
          t.columns,
          t.notNullConstraints,
          t.uniqueConstraints,
          t.checkConstraints,
          t.comment,
          t.tags
        )
    );
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
      (v) => new ViewInfo(v.name, v.schemaName, v.definition, v.comment, v.tags)
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
    return new TableInfo(
      t.name,
      t.schemaName,
      t.columns,
      t.notNullConstraints,
      t.uniqueConstraints,
      t.checkConstraints,
      t.comment,
      t.tags
    );
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
    return new ViewInfo(v.name, v.schemaName, v.definition, v.comment, v.tags);
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
