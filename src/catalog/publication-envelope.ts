// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Inspect IPC message framing before a backend can merge/drop record batches.
// The catalog's RPC envelope is exactly Schema + one 1-row RecordBatch. Its
// single binary field needs no dictionary messages. Inner metadata schemas are
// checked separately by the ordinary catalog codecs.
export function validateCatalogPublicationEnvelope(bytes: Uint8Array): void {
  try {
    const stream = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    let position = 0, messages = 0;
    while (position < bytes.length) {
      let length = stream.getInt32(position, true); position += 4;
      if (length === -1) { length = stream.getInt32(position, true); position += 4; }
      if (length === 0) {
        if (position !== bytes.length) throw new Error("trailing data");
        break;
      }
      if (length < 4 || length > bytes.length - position) throw new Error("invalid message length");
      const head = new DataView(bytes.buffer, bytes.byteOffset + position, length);
      const tableField = (table: number, field: number): number | null => {
        const vtable = table - head.getInt32(table, true);
        const vlength = head.getUint16(vtable, true);
        if (vlength < 4 || vlength % 2 || vtable + vlength > length) throw new Error("invalid vtable");
        if (4 + 2 * field >= vlength) return null;
        const offset = head.getUint16(vtable + 4 + 2 * field, true);
        return offset === 0 ? null : table + offset;
      };
      const follow = (pointer: number) => pointer + head.getUint32(pointer, true);
      const root = head.getUint32(0, true);
      const typePosition = tableField(root, 1);
      const type = typePosition == null ? 0 : head.getUint8(typePosition);
      if (type !== (messages === 0 ? 1 : messages === 1 ? 3 : -1)) throw new Error("expected one schema and one record batch");
      if (type === 3) {
        const header = tableField(root, 2);
        if (header == null) throw new Error("missing record header");
        const rows = tableField(follow(header), 0);
        if (rows == null || head.getBigInt64(rows, true) !== 1n) throw new Error("expected one response row");
      }
      const metadata = tableField(root, 4);
      if (metadata != null) {
        const vector = follow(metadata), count = head.getUint32(vector, true);
        if (count > length / 4) throw new Error("invalid metadata vector");
        for (let i = 0; i < count; i++) {
          const keyPointer = tableField(follow(vector + 4 + 4 * i), 0);
          if (keyPointer == null) continue;
          const keyStart = follow(keyPointer), keyLength = head.getUint32(keyStart, true);
          if (keyStart + 4 + keyLength > length) throw new Error("invalid metadata key");
          const key = new TextDecoder().decode(new Uint8Array(head.buffer, head.byteOffset + keyStart + 4, keyLength));
          if (key === "vgi_rpc.location" || key === "vgi_rpc.log_level") throw new Error("pointer/log metadata is not a catalog result");
        }
      }
      const bodyPosition = tableField(root, 3);
      const bodyLength = bodyPosition == null ? 0 : Number(head.getBigInt64(bodyPosition, true));
      position += length;
      if (!Number.isSafeInteger(bodyLength) || bodyLength < 0 || bodyLength > bytes.length - position) throw new Error("invalid body length");
      position += bodyLength;
      messages++;
    }
    if (messages !== 2) throw new Error("missing response batch");
  } catch (error) {
    throw new Error(`invalid published RPC envelope: ${error instanceof Error ? error.message : String(error)}`);
  }
}
