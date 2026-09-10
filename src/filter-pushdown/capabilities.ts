// Copyright 2025, 2026 Query Farm LLC - https://query.farm

import type { FunctionIdentity } from "./types.js";

const SUPPORTED_EXTENSION_FUNCTIONS = new Set(["duckdb.spatial/intersects_extent@1"]);

export function filterFunctionIdentityKey(identity: FunctionIdentity): string {
  return `${identity.namespace}/${identity.name}@${identity.version}`;
}

export function supportsExtensionFilterFunction(identity: FunctionIdentity): boolean {
  return SUPPORTED_EXTENSION_FUNCTIONS.has(filterFunctionIdentityKey(identity));
}
