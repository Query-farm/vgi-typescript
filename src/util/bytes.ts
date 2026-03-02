// Shared byte-conversion utility. Browser-safe (no bare Buffer.isBuffer).

/** Convert any binary-ish value to a Uint8Array. Returns empty array for null/undefined. */
export function toUint8Array(val: any): Uint8Array {
  if (val instanceof Uint8Array) return val;
  if (val instanceof ArrayBuffer) return new Uint8Array(val);
  if (typeof Buffer !== "undefined" && Buffer.isBuffer(val))
    return new Uint8Array(val);
  if (val && val.buffer instanceof ArrayBuffer) {
    return new Uint8Array(val.buffer, val.byteOffset, val.byteLength);
  }
  return new Uint8Array(0);
}
