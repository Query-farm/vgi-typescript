// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

/**
 * The `content-hash` etag for `catalog_contents`.
 *
 * Mirrors vgi-python's `catalog_contents_digest()` byte for byte, so the same
 * snapshot (the same item bytes) gives the same etag in every SDK.
 */

import type { SchemaContents } from "../generated/vgi-protocol-types.js";

const KINDS = [
  "tables",
  "views",
  "scalar_functions",
  "aggregate_functions",
  "table_functions",
  "scalar_macros",
  "table_macros",
  "indexes",
] as const;

/**
 * Hex SHA-256 over a `catalog_contents` snapshot: the `content-hash` etag.
 *
 * Covers every schema's path and the exact item bytes of every kind, in
 * order, each length-prefixed (8-byte little-endian) so no two different
 * snapshots share an input:
 *
 *   u64(#schemas) { u64(#path) {u64(len) utf8(part)}*  u64(len) schema
 *                   ( u64(#items) {u64(len) item}* ) x 8 kinds }*
 *
 * Deterministic because the items' encoding is (map columns are written in
 * sorted key order, see `encodeASD`), so two builds of the same catalog hash
 * alike.
 *
 * @param schemas The snapshot, in wire order.
 * @returns The lowercase hex digest.
 */
export async function catalogContentsDigest(schemas: readonly SchemaContents[]): Promise<string> {
  const utf8 = new TextEncoder();
  const parts: Uint8Array[] = [];
  let total = 0;
  const count = (n: number) => {
    const b = new Uint8Array(8);
    new DataView(b.buffer).setBigUint64(0, BigInt(n), true);
    parts.push(b);
    total += 8;
  };
  const chunk = (data: Uint8Array) => {
    count(data.byteLength);
    parts.push(data);
    total += data.byteLength;
  };
  const chunks = (values: readonly Uint8Array[]) => {
    count(values.length);
    for (const v of values) chunk(v);
  };

  count(schemas.length);
  for (const entry of schemas) {
    chunks(entry.path.map((p) => utf8.encode(p)));
    chunk(entry.schema);
    for (const kind of KINDS) chunks(entry[kind]);
  }

  const buf = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    buf.set(p, off);
    off += p.byteLength;
  }
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", buf as BufferSource));
  let hex = "";
  for (const b of digest) hex += b.toString(16).padStart(2, "0");
  return hex;
}
