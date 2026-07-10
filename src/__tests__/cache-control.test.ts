// Copyright 2025, 2026 Query Farm LLC - https://query.farm

import { describe, expect, test } from "bun:test";
import {
  CACHE_ETAG_KEY,
  CACHE_EXPIRES_KEY,
  CACHE_LAST_MODIFIED_KEY,
  CACHE_NO_STORE_KEY,
  CACHE_NOT_MODIFIED_KEY,
  CACHE_REVALIDATABLE_KEY,
  CACHE_SCOPE_CATALOG,
  CACHE_SCOPE_KEY,
  CACHE_SCOPE_TRANSACTION,
  CACHE_STALE_IF_ERROR_KEY,
  CACHE_STALE_WHILE_REVALIDATE_KEY,
  CACHE_TTL_KEY,
  cacheControlMetadata,
} from "../cache-control.js";

describe("cacheControlMetadata", () => {
  test("renders ttl and always emits the default scope", () => {
    const md = cacheControlMetadata({ ttl: 300 });
    expect(md.get(CACHE_TTL_KEY)).toBe("300");
    expect(md.get(CACHE_SCOPE_KEY)).toBe(CACHE_SCOPE_CATALOG);
    expect(md.size).toBe(2);
  });

  test("omits unset optionals and false booleans", () => {
    const md = cacheControlMetadata({ ttl: 0, noStore: false, revalidatable: false, notModified: false });
    expect([...md.keys()].sort()).toEqual([CACHE_SCOPE_KEY, CACHE_TTL_KEY].sort());
    // ttl=0 is meaningful ("no-cache"), not an absent value.
    expect(md.get(CACHE_TTL_KEY)).toBe("0");
  });

  test("renders every field", () => {
    const md = cacheControlMetadata({
      ttl: 60,
      expires: "2026-07-10T00:00:00Z",
      scope: CACHE_SCOPE_TRANSACTION,
      noStore: true,
      etag: '"v1"',
      lastModified: "2026-07-09T00:00:00Z",
      revalidatable: true,
      staleWhileRevalidate: 10,
      staleIfError: 20,
      notModified: true,
    });
    expect(md.get(CACHE_TTL_KEY)).toBe("60");
    expect(md.get(CACHE_EXPIRES_KEY)).toBe("2026-07-10T00:00:00Z");
    expect(md.get(CACHE_SCOPE_KEY)).toBe(CACHE_SCOPE_TRANSACTION);
    expect(md.get(CACHE_NO_STORE_KEY)).toBe("1");
    expect(md.get(CACHE_ETAG_KEY)).toBe('"v1"');
    expect(md.get(CACHE_LAST_MODIFIED_KEY)).toBe("2026-07-09T00:00:00Z");
    expect(md.get(CACHE_REVALIDATABLE_KEY)).toBe("1");
    expect(md.get(CACHE_STALE_WHILE_REVALIDATE_KEY)).toBe("10");
    expect(md.get(CACHE_STALE_IF_ERROR_KEY)).toBe("20");
    expect(md.get(CACHE_NOT_MODIFIED_KEY)).toBe("1");
  });

  test("merges extra metadata, with cache keys winning on collision", () => {
    const extra = new Map([
      ["vgi_batch_index", "7"],
      [CACHE_SCOPE_KEY, "bogus"],
    ]);
    const md = cacheControlMetadata({ ttl: 5 }, extra);
    expect(md.get("vgi_batch_index")).toBe("7");
    expect(md.get(CACHE_SCOPE_KEY)).toBe(CACHE_SCOPE_CATALOG);
    // The caller's map is not mutated.
    expect(extra.get(CACHE_SCOPE_KEY)).toBe("bogus");
  });

  test("rejects an unknown scope", () => {
    expect(() => cacheControlMetadata({ scope: "global" as any })).toThrow(/scope must be one of/);
  });

  test("rejects negative durations", () => {
    expect(() => cacheControlMetadata({ ttl: -1 })).toThrow(/ttl must be >= 0/);
    expect(() => cacheControlMetadata({ staleWhileRevalidate: -1 })).toThrow(/staleWhileRevalidate must be >= 0/);
    expect(() => cacheControlMetadata({ staleIfError: -1 })).toThrow(/staleIfError must be >= 0/);
  });
});
