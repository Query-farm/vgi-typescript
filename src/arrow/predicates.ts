// Backend-agnostic Arrow type predicates. Both arrow-js and flechette
// expose `typeId` on every DataType with identical numeric values, so a
// single `t.typeId === N` check works regardless of the active backend.

import type { VgiDataType } from "./types.js";

export const TypeId = {
  Null: 1,
  Int: 2,
  Float: 3,
  Binary: 4,
  Utf8: 5,
  Bool: 6,
  Decimal: 7,
  Date: 8,
  Time: 9,
  Timestamp: 10,
  Interval: 11,
  List: 12,
  Struct: 13,
  Union: 14,
  FixedSizeBinary: 15,
  FixedSizeList: 16,
  Map: 17,
  Duration: 18,
  LargeBinary: 19,
  LargeUtf8: 20,
  Dictionary: -1,
} as const;

export function isNull(t: VgiDataType): boolean { return t.typeId === TypeId.Null; }
export function isInt(t: VgiDataType): boolean { return t.typeId === TypeId.Int; }
export function isFloat(t: VgiDataType): boolean { return t.typeId === TypeId.Float; }
export function isBinary(t: VgiDataType): boolean {
  return t.typeId === TypeId.Binary || t.typeId === TypeId.LargeBinary;
}
export function isUtf8(t: VgiDataType): boolean {
  return t.typeId === TypeId.Utf8 || t.typeId === TypeId.LargeUtf8;
}
export function isBool(t: VgiDataType): boolean { return t.typeId === TypeId.Bool; }
export function isDecimal(t: VgiDataType): boolean { return t.typeId === TypeId.Decimal; }
export function isDate(t: VgiDataType): boolean { return t.typeId === TypeId.Date; }
export function isTime(t: VgiDataType): boolean { return t.typeId === TypeId.Time; }
export function isTimestamp(t: VgiDataType): boolean { return t.typeId === TypeId.Timestamp; }
export function isInterval(t: VgiDataType): boolean { return t.typeId === TypeId.Interval; }
export function isList(t: VgiDataType): boolean { return t.typeId === TypeId.List; }
export function isStruct(t: VgiDataType): boolean { return t.typeId === TypeId.Struct; }
export function isUnion(t: VgiDataType): boolean { return t.typeId === TypeId.Union; }
export function isFixedSizeBinary(t: VgiDataType): boolean { return t.typeId === TypeId.FixedSizeBinary; }
export function isFixedSizeList(t: VgiDataType): boolean { return t.typeId === TypeId.FixedSizeList; }
export function isMap(t: VgiDataType): boolean { return t.typeId === TypeId.Map; }
export function isDuration(t: VgiDataType): boolean { return t.typeId === TypeId.Duration; }
export function isDictionary(t: VgiDataType): boolean { return t.typeId === TypeId.Dictionary; }

/** Runtime check: does `x` quack like a VgiBatch? Used at API boundaries
 *  where callers may pass either a batch or a plain column dict. Duck-typed
 *  because arrow-js and flechette use unrelated classes. */
export function isBatch(x: unknown): x is import("./types.js").VgiBatch {
  return (
    x != null &&
    typeof (x as any).numRows === "number" &&
    (x as any).schema != null &&
    Array.isArray((x as any).schema.fields)
  );
}
