// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Per-output-row provenance wire encoding (batched correlated LATERAL): base64
// of a raw little-endian int32[] under `vgi_rpc.parent_row#b64`. Shared by the
// emit helper (functions/table-in-out.ts) and the auto-apply filter
// (filter-pushdown/collector.ts), which must drop the same rows from both.

/** Metadata key carrying per-output-row provenance: base64 of a raw
 *  little-endian int32[] mapping each output row to the input row that
 *  produced it. Shared by string with the C++ extension and vgi-python. */
export const PARENT_ROW_METADATA_KEY = "vgi_rpc.parent_row#b64";

/** Encode parent-row indices as the wire's base64 little-endian int32[]. */
export function encodeParentRows(parentRows: ArrayLike<number>): string {
  const raw = new Uint8Array(parentRows.length * 4);
  const dv = new DataView(raw.buffer);
  for (let i = 0; i < parentRows.length; i++) {
    dv.setInt32(i * 4, parentRows[i], true);
  }
  if (typeof (globalThis as any).Buffer !== "undefined") {
    return (globalThis as any).Buffer.from(raw).toString("base64");
  }
  let bin = "";
  for (let i = 0; i < raw.length; i++) bin += String.fromCharCode(raw[i]);
  return (globalThis as any).btoa(bin);
}

/** Decode the wire's base64 little-endian int32[] back into parent-row indices. */
export function decodeParentRows(encoded: string): Int32Array {
  let raw: Uint8Array;
  if (typeof (globalThis as any).Buffer !== "undefined") {
    raw = new Uint8Array((globalThis as any).Buffer.from(encoded, "base64"));
  } else {
    const bin = (globalThis as any).atob(encoded) as string;
    raw = new Uint8Array(bin.length);
    for (let i = 0; i < bin.length; i++) raw[i] = bin.charCodeAt(i);
  }
  const dv = new DataView(raw.buffer, raw.byteOffset, raw.byteLength);
  const out = new Int32Array(raw.byteLength >> 2);
  for (let i = 0; i < out.length; i++) out[i] = dv.getInt32(i * 4, true);
  return out;
}
