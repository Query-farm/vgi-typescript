// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import { decodeMapValue } from "../../../util/arrow/index.js";

export function catalogStringMap(raw: any): Record<string, string> {
  raw = decodeMapValue(raw);
  const result: Record<string, string> = {};
  if (raw == null) return result;
  const entries = raw[Symbol.iterator] ? raw : Object.entries(raw);
  for (const entry of entries) {
    const [key, value] = Array.isArray(entry) ? entry : [entry.key, entry.value];
    if (typeof key !== "string" || typeof value !== "string") throw new Error("catalog metadata must map strings to strings");
    Object.defineProperty(result, key, { value, enumerable: true, writable: true, configurable: true });
  }
  return result;
}

