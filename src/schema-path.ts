// Copyright 2025, 2026 Query Farm LLC - https://query.farm

/** Raw identifier components of a catalog schema, from root to leaf. */
export type SchemaPath = string[];

/** Case-insensitive key used for catalog lookup and function dispatch. */
export function normalizeSchemaPath(path: Iterable<unknown> | string): string[] {
  return typeof path === "string" ? [path] : Array.from(path, String);
}

export function schemaPathKey(path: Iterable<unknown> | string): string {
  const normalized = normalizeSchemaPath(path);
  if (normalized.length === 0 || normalized.some((part) => part.length === 0)) {
    throw new Error("schema paths must contain at least one non-empty component");
  }
  return normalized.map((part) => part.toLowerCase()).join("\u0000");
}

export function schemaPathsEqual(left: Iterable<unknown> | string, right: Iterable<unknown> | string): boolean {
  return schemaPathKey(left) === schemaPathKey(right);
}

export function schemaPathDisplay(path: Iterable<unknown> | string): string {
  return normalizeSchemaPath(path).map((part) => JSON.stringify(part)).join(".");
}
