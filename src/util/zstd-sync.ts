// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Synchronous zstd, where the runtime has it.
//
// The state serializer is synchronous (vgi-rpc's StateSerializer contract), so
// it cannot use the async helpers vgi-rpc exposes. Bun has
// `Bun.zstdCompressSync`; Node >= 22.15 has `zlib.zstdCompressSync`, reached
// from ESM through `process.getBuiltinModule`. Anything else (workerd, a
// browser) gets `null` and callers fall back to storing bytes uncompressed.

/** A synchronous zstd codec. */
export interface ZstdSync {
  compress(data: Uint8Array): Uint8Array;
  decompress(data: Uint8Array): Uint8Array;
}

/** Level 3 is zstd's default: ~3.4x on an init request, ~50 us for 9 KB. */
const LEVEL = 3;

function resolveZstdSync(): ZstdSync | null {
  const bun = (globalThis as any).Bun;
  if (typeof bun?.zstdCompressSync === "function" && typeof bun?.zstdDecompressSync === "function") {
    return {
      compress: (data) => new Uint8Array(bun.zstdCompressSync(data, { level: LEVEL })),
      // Copied into a fresh buffer: Arrow readers want byteOffset 0.
      decompress: (data) => new Uint8Array(bun.zstdDecompressSync(data)),
    };
  }
  let zlib: any = null;
  try {
    zlib = (globalThis as any).process?.getBuiltinModule?.("node:zlib") ?? null;
  } catch {
    zlib = null;
  }
  if (typeof zlib?.zstdCompressSync === "function" && typeof zlib?.zstdDecompressSync === "function") {
    const params = { [zlib.constants.ZSTD_c_compressionLevel]: LEVEL };
    return {
      compress: (data) => new Uint8Array(zlib.zstdCompressSync(data, { params })),
      decompress: (data) => new Uint8Array(zlib.zstdDecompressSync(data)),
    };
  }
  return null;
}

let resolved: ZstdSync | null | undefined;

/** This runtime's synchronous zstd codec, or `null` when it has none. */
export function zstdSync(): ZstdSync | null {
  if (resolved === undefined) resolved = resolveZstdSync();
  return resolved;
}
