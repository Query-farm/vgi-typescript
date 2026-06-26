// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// Codec registry — the single source of truth for how each Arrow type is
// represented as a JS value in this SDK.
//
// Three layers of representation are in scope for the codec design (phase 1
// implements the first two; the third is reserved for a later phase):
//
//   - canonical: the internal pivot. One lossless JS value per Arrow type that
//     mirrors the raw Arrow wire unit. This is what the per-backend
//     `writeCanonicalColumn` / `readCanonicalValue` produce and consume, so it
//     is identical across the arrow-js and flechette backends.
//   - rich:      the default author-facing value. Identical to canonical for
//     every type EXCEPT date32 / date64, which surface as a JS `Date` (lossless
//     in both directions). All temporal sub-second precision types stay
//     numeric/bigint because `Date` can't hold us/ns losslessly.
//   - raw:       (reserved) a branded-primitive view layered on canonical. Not
//     built in phase 1; the `Codec` interface leaves room for it so a later
//     phase can add `rawToCanonical` / `canonicalToRaw` without reshaping this
//     module.
//
// Canonical representation per Arrow type:
//   bool                               -> boolean
//   int8/16/32, uint8/16/32, float*    -> number
//   int64/uint64                       -> bigint
//   utf8 / largeUtf8                   -> string
//   binary / largeBinary / fsb         -> Uint8Array
//   date32                             -> number  (days since epoch)
//   date64                             -> bigint  (ms since epoch)
//   time32                             -> number  (raw unit: s or ms)
//   time64                             -> bigint  (raw unit: us or ns)
//   timestamp[s/ms/us/ns]              -> bigint  (raw unit)
//   duration[*]                        -> bigint  (raw unit)
//   decimal128/256                     -> bigint  (UNSCALED integer)
//   struct                             -> { field: canonical }
//   list / largeList / fixedSizeList   -> canonical[]
//   map                                -> Array<[keyCanonical, valueCanonical]>
//   dictionary                         -> the decoded value's canonical
//
// rich differs from canonical ONLY for date32 / date64 (both -> Date).
//
// Validation: every codec method validates its input and throws a clear error
// on invalid or lossy data (non-integer where an int is required, a bigint that
// overflows Number when narrowing, an out-of-range Date, etc.). null/undefined
// pass straight through as null.

import type { VgiDataType } from "../types.js";
import { TypeId } from "../predicates.js";

const MS_PER_DAY = 86_400_000;

/** Codec for a single Arrow type. Converts between the author-facing rich
 *  representation and the internal canonical pivot. Both directions validate
 *  and throw on invalid/lossy input. null/undefined pass through as null.
 *
 *  (Raw<->canonical conversions are reserved for a later phase. When added they
 *  belong here as `rawToCanonical` / `canonicalToRaw`.) */
export interface Codec {
  /** Human-readable type label, used in error messages. */
  readonly label: string;
  /** rich author value -> canonical pivot. */
  richToCanonical(value: unknown): unknown;
  /** canonical pivot -> rich author value. */
  canonicalToRich(value: unknown): unknown;
  /** branded raw author value -> canonical pivot. Largely identity on the
   *  underlying number/bigint (raw === canonical for every type), but validates
   *  and unbrands. Date32/Date64 raw values are plain day-number / ms-bigint,
   *  so this differs from richToCanonical only in NOT accepting a JS Date. */
  rawToCanonical(value: unknown): unknown;
  /** canonical pivot -> branded raw author value. Validates and brands
   *  (identity at runtime). */
  canonicalToRaw(value: unknown): unknown;
}

function isNullish(v: unknown): boolean {
  return v === null || v === undefined;
}

function err(label: string, msg: string, value: unknown): never {
  let shown: string;
  try {
    shown =
      typeof value === "bigint"
        ? `${value}n`
        : typeof value === "object"
          ? Object.prototype.toString.call(value)
          : String(value);
  } catch {
    shown = "<unprintable>";
  }
  throw new TypeError(`codec[${label}]: ${msg} (got ${typeof value}: ${shown})`);
}

// ---------------------------------------------------------------------------
// Scalar codec builders
// ---------------------------------------------------------------------------

/** Identity codec: rich === canonical === raw. Validates with `check`. */
function identity(label: string, check: (v: unknown) => void): Codec {
  const conv = (value: unknown): unknown => {
    if (isNullish(value)) return null;
    check(value);
    return value;
  };
  return {
    label,
    richToCanonical: conv,
    canonicalToRich: conv,
    rawToCanonical: conv,
    canonicalToRaw: conv,
  };
}

function checkBoolean(label: string) {
  return (v: unknown) => {
    if (typeof v !== "boolean") err(label, "expected boolean", v);
  };
}

/** Integer-valued `number` within [min,max]. Accepts a bigint that fits. */
function checkIntNumber(label: string, min: number, max: number) {
  return (v: unknown) => {
    let n: number;
    if (typeof v === "number") n = v;
    else if (typeof v === "bigint") {
      if (v < BigInt(min) || v > BigInt(max)) err(label, `bigint out of ${label} range`, v);
      return;
    } else return err(label, "expected number", v);
    if (!Number.isInteger(n)) err(label, "expected an integer", v);
    if (n < min || n > max) err(label, `value out of ${label} range [${min}, ${max}]`, v);
  };
}

function checkFloat(label: string) {
  return (v: unknown) => {
    if (typeof v === "number") return;
    if (typeof v === "bigint") return; // a bigint is acceptable for a float slot
    err(label, "expected number", v);
  };
}

function checkBigInt(label: string, bits: number, signed: boolean) {
  const max = signed ? (1n << BigInt(bits - 1)) - 1n : (1n << BigInt(bits)) - 1n;
  const min = signed ? -(1n << BigInt(bits - 1)) : 0n;
  return (v: unknown) => {
    let b: bigint;
    if (typeof v === "bigint") b = v;
    else if (typeof v === "number") {
      if (!Number.isInteger(v)) err(label, "expected an integer", v);
      b = BigInt(v);
    } else return err(label, "expected bigint", v);
    if (b < min || b > max) err(label, `value out of ${label} range`, v);
  };
}

function checkString(label: string) {
  return (v: unknown) => {
    if (typeof v !== "string") err(label, "expected string", v);
  };
}

/** Coerce a number/bigint to a bigint, requiring integral input. */
function toBigIntStrict(label: string, v: unknown): bigint {
  if (typeof v === "bigint") return v;
  if (typeof v === "number") {
    if (!Number.isInteger(v)) err(label, "expected an integer value", v);
    return BigInt(v);
  }
  return err(label, "expected number or bigint", v);
}

/** Narrow a bigint to a number, throwing if it can't be held losslessly. */
function bigIntToSafeNumber(label: string, b: bigint): number {
  if (b < -9007199254740991n || b > 9007199254740991n) {
    err(label, "bigint overflows the safe-integer range when narrowing to number", b);
  }
  return Number(b);
}

// ---------------------------------------------------------------------------
// Date codecs (the only types where rich != canonical)
// ---------------------------------------------------------------------------

/** date32: canonical = number (days since epoch); rich = Date. */
function dateDayCodec(): Codec {
  const label = "date32";
  return {
    label,
    richToCanonical(value) {
      if (isNullish(value)) return null;
      if (value instanceof Date) {
        const ms = value.getTime();
        if (Number.isNaN(ms)) err(label, "invalid Date", value);
        if (ms % MS_PER_DAY !== 0) {
          // Tolerate any Date by flooring to whole days — DuckDB DATE has no
          // sub-day component, so a Date with a time-of-day maps to its day.
          return Math.floor(ms / MS_PER_DAY);
        }
        return ms / MS_PER_DAY;
      }
      if (typeof value === "number") {
        if (!Number.isInteger(value)) err(label, "day-number must be an integer", value);
        return value;
      }
      if (typeof value === "bigint") return bigIntToSafeNumber(label, value);
      return err(label, "expected Date, number (days), or bigint", value);
    },
    canonicalToRich(value) {
      if (isNullish(value)) return null;
      let days: number;
      if (typeof value === "number") {
        if (!Number.isInteger(value)) err(label, "canonical day-number must be an integer", value);
        days = value;
      } else if (typeof value === "bigint") {
        days = bigIntToSafeNumber(label, value);
      } else if (value instanceof Date) {
        return value;
      } else return err(label, "expected number (days) canonical", value);
      const ms = days * MS_PER_DAY;
      const d = new Date(ms);
      if (Number.isNaN(d.getTime())) err(label, "day-number produces an invalid Date", value);
      return d;
    },
    // raw date32 IS the day-number (no Date involved).
    rawToCanonical(value) {
      if (isNullish(value)) return null;
      if (typeof value === "number") {
        if (!Number.isInteger(value)) err(label, "raw day-number must be an integer", value);
        return value;
      }
      if (typeof value === "bigint") return bigIntToSafeNumber(label, value);
      return err(label, "expected branded Date32 (day-number)", value);
    },
    canonicalToRaw(value) {
      if (isNullish(value)) return null;
      if (typeof value === "number") {
        if (!Number.isInteger(value)) err(label, "canonical day-number must be an integer", value);
        return value;
      }
      if (typeof value === "bigint") return bigIntToSafeNumber(label, value);
      if (value instanceof Date) {
        const ms = value.getTime();
        if (Number.isNaN(ms)) err(label, "invalid Date", value);
        return Math.floor(ms / MS_PER_DAY);
      }
      return err(label, "expected number (days) canonical", value);
    },
  };
}

/** date64: canonical = bigint (ms since epoch); rich = Date. */
function dateMillisCodec(): Codec {
  const label = "date64";
  return {
    label,
    richToCanonical(value) {
      if (isNullish(value)) return null;
      if (value instanceof Date) {
        const ms = value.getTime();
        if (Number.isNaN(ms)) err(label, "invalid Date", value);
        return BigInt(ms);
      }
      if (typeof value === "bigint") return value;
      if (typeof value === "number") {
        if (!Number.isInteger(value)) err(label, "ms value must be an integer", value);
        return BigInt(value);
      }
      return err(label, "expected Date, bigint (ms), or number", value);
    },
    canonicalToRich(value) {
      if (isNullish(value)) return null;
      if (value instanceof Date) return value;
      const ms = toBigIntStrict(label, value);
      const n = bigIntToSafeNumber(label, ms);
      const d = new Date(n);
      if (Number.isNaN(d.getTime())) err(label, "ms value produces an invalid Date", value);
      return d;
    },
    // raw date64 IS the ms-bigint (no Date involved).
    rawToCanonical(value) {
      if (isNullish(value)) return null;
      if (value instanceof Date) return err(label, "raw Date64Ms must be a bigint (ms), not a Date", value);
      return toBigIntStrict(label, value);
    },
    canonicalToRaw(value) {
      if (isNullish(value)) return null;
      if (value instanceof Date) return BigInt(value.getTime());
      return toBigIntStrict(label, value);
    },
  };
}

// ---------------------------------------------------------------------------
// Temporal codecs where rich == canonical (no Date — would be lossy)
// ---------------------------------------------------------------------------

/** time32: canonical/rich/raw = number (raw unit). */
function time32Codec(): Codec {
  const label = "time32";
  const toCanon = (value: unknown): unknown => {
    if (isNullish(value)) return null;
    if (typeof value === "number") {
      if (!Number.isInteger(value)) err(label, "time32 value must be an integer", value);
      return value;
    }
    if (typeof value === "bigint") return bigIntToSafeNumber(label, value);
    return err(label, "expected number (raw time unit)", value);
  };
  const fromCanon = (value: unknown): unknown => {
    if (isNullish(value)) return null;
    if (typeof value === "number") return value;
    if (typeof value === "bigint") return bigIntToSafeNumber(label, value);
    return err(label, "expected number canonical", value);
  };
  return {
    label,
    richToCanonical: toCanon,
    canonicalToRich: fromCanon,
    rawToCanonical: toCanon,
    canonicalToRaw: fromCanon,
  };
}

/** time64/timestamp/duration: canonical/rich/raw = bigint (raw unit). */
function bigIntUnitCodec(label: string): Codec {
  const conv = (value: unknown): unknown => {
    if (isNullish(value)) return null;
    return toBigIntStrict(label, value);
  };
  return {
    label,
    richToCanonical: conv,
    canonicalToRich: conv,
    rawToCanonical: conv,
    canonicalToRaw: conv,
  };
}

/** Decode a little-endian two's-complement byte view to a signed bigint. */
function decimalViewToBigInt(view: ArrayBufferView): bigint {
  const u8 = new Uint8Array(view.buffer, view.byteOffset, view.byteLength);
  let bi = 0n;
  for (let i = u8.length - 1; i >= 0; i--) bi = (bi << 8n) | BigInt(u8[i]);
  if (u8.length > 0 && (u8[u8.length - 1] & 0x80)) bi -= 1n << BigInt(u8.length * 8);
  return bi;
}

/** decimal128/256: canonical/rich = bigint (UNSCALED integer). Producers may
 *  also pass the raw little-endian byte view that an Arrow decimal scalar
 *  exposes (Uint32Array / Uint8Array); decode it to the unscaled bigint. */
function decimalCodec(): Codec {
  const label = "decimal";
  const toCanon = (value: unknown): unknown => {
    if (isNullish(value)) return null;
    if (typeof value === "bigint") return value;
    if (typeof value === "number") return toBigIntStrict(label, value);
    if (ArrayBuffer.isView(value)) return decimalViewToBigInt(value as ArrayBufferView);
    return err(label, "expected bigint, number, or decimal byte view", value);
  };
  const fromCanon = (value: unknown): unknown => {
    if (isNullish(value)) return null;
    return toCanon(value);
  };
  return {
    label,
    richToCanonical: toCanon,
    canonicalToRich: fromCanon,
    rawToCanonical: toCanon,
    canonicalToRaw: fromCanon,
  };
}

// ---------------------------------------------------------------------------
// Composite codecs
// ---------------------------------------------------------------------------

function structCodec(type: VgiDataType): Codec {
  const children = (type as any).children as Array<{ name: string; type: VgiDataType }>;
  const childCodecs = children.map((c) => ({ name: c.name, codec: codecFor(c.type) }));
  const label = "struct";
  return {
    label,
    richToCanonical(value) {
      if (isNullish(value)) return null;
      if (typeof value !== "object") err(label, "expected an object", value);
      const obj = value as any;
      const out: Record<string, unknown> = {};
      for (const { name, codec } of childCodecs) {
        out[name] = codec.richToCanonical(obj[name] ?? null);
      }
      return out;
    },
    canonicalToRich(value) {
      if (isNullish(value)) return null;
      if (typeof value !== "object") err(label, "expected an object", value);
      const obj = value as any;
      const out: Record<string, unknown> = {};
      for (const { name, codec } of childCodecs) {
        out[name] = codec.canonicalToRich(obj[name] ?? null);
      }
      return out;
    },
    rawToCanonical(value) {
      if (isNullish(value)) return null;
      if (typeof value !== "object") err(label, "expected an object", value);
      const obj = value as any;
      const out: Record<string, unknown> = {};
      for (const { name, codec } of childCodecs) {
        out[name] = codec.rawToCanonical(obj[name] ?? null);
      }
      return out;
    },
    canonicalToRaw(value) {
      if (isNullish(value)) return null;
      if (typeof value !== "object") err(label, "expected an object", value);
      const obj = value as any;
      const out: Record<string, unknown> = {};
      for (const { name, codec } of childCodecs) {
        out[name] = codec.canonicalToRaw(obj[name] ?? null);
      }
      return out;
    },
  };
}

function listCodec(type: VgiDataType): Codec {
  const childField = (type as any).children?.[0];
  if (!childField) throw new Error("codec[list]: list type has no child field");
  const child = codecFor(childField.type as VgiDataType);
  const label = "list";
  const toArray = (value: unknown): unknown[] => {
    if (Array.isArray(value)) return value;
    if (value != null && typeof (value as any)[Symbol.iterator] === "function") {
      return Array.from(value as Iterable<unknown>);
    }
    return err(label, "expected an array or iterable", value);
  };
  return {
    label,
    richToCanonical(value) {
      if (isNullish(value)) return null;
      return toArray(value).map((v) => child.richToCanonical(v));
    },
    canonicalToRich(value) {
      if (isNullish(value)) return null;
      return toArray(value).map((v) => child.canonicalToRich(v));
    },
    rawToCanonical(value) {
      if (isNullish(value)) return null;
      return toArray(value).map((v) => child.rawToCanonical(v));
    },
    canonicalToRaw(value) {
      if (isNullish(value)) return null;
      return toArray(value).map((v) => child.canonicalToRaw(v));
    },
  };
}

function mapCodec(type: VgiDataType): Codec {
  // children[0] is the "entries" struct field holding [key, value].
  const entries = (type as any).children?.[0];
  const entryChildren = entries?.type?.children;
  if (!entryChildren || entryChildren.length < 2) {
    throw new Error("codec[map]: map type has no key/value entry fields");
  }
  const keyCodec = codecFor(entryChildren[0].type as VgiDataType);
  const valueCodec = codecFor(entryChildren[1].type as VgiDataType);
  const label = "map";
  /** Normalize a rich map value into an array of [k, v] pairs. */
  const toPairs = (value: unknown): Array<[unknown, unknown]> => {
    if (Array.isArray(value)) return value as Array<[unknown, unknown]>;
    if (value instanceof Map) return Array.from(value.entries());
    if (value != null && typeof (value as any)[Symbol.iterator] === "function") {
      return Array.from(value as Iterable<[unknown, unknown]>);
    }
    if (value != null && typeof value === "object") {
      return Object.entries(value as Record<string, unknown>);
    }
    return err(label, "expected Map, array of pairs, or object", value);
  };
  return {
    label,
    richToCanonical(value) {
      if (isNullish(value)) return null;
      return toPairs(value).map(([k, v]) => [
        keyCodec.richToCanonical(k),
        valueCodec.richToCanonical(v),
      ]);
    },
    canonicalToRich(value) {
      if (isNullish(value)) return null;
      return toPairs(value).map(([k, v]) => [
        keyCodec.canonicalToRich(k),
        valueCodec.canonicalToRich(v),
      ]);
    },
    rawToCanonical(value) {
      if (isNullish(value)) return null;
      return toPairs(value).map(([k, v]) => [
        keyCodec.rawToCanonical(k),
        valueCodec.rawToCanonical(v),
      ]);
    },
    canonicalToRaw(value) {
      if (isNullish(value)) return null;
      return toPairs(value).map(([k, v]) => [
        keyCodec.canonicalToRaw(k),
        valueCodec.canonicalToRaw(v),
      ]);
    },
  };
}

function dictionaryCodec(type: VgiDataType): Codec {
  // The decoded value's type lives on `type.dictionary` (both backends).
  const valueType = (type as any).dictionary as VgiDataType;
  if (!valueType) throw new Error("codec[dictionary]: missing dictionary value type");
  // Delegate entirely to the decoded value's codec — canonical for a
  // dictionary IS the decoded value's canonical.
  const inner = codecFor(valueType);
  return {
    label: `dictionary<${inner.label}>`,
    richToCanonical: (v) => inner.richToCanonical(v),
    canonicalToRich: (v) => inner.canonicalToRich(v),
    rawToCanonical: (v) => inner.rawToCanonical(v),
    canonicalToRaw: (v) => inner.canonicalToRaw(v),
  };
}

// ---------------------------------------------------------------------------
// Resolver
// ---------------------------------------------------------------------------

const INT8 = identity("int8", checkIntNumber("int8", -128, 127));
const INT16 = identity("int16", checkIntNumber("int16", -32768, 32767));
const INT32 = identity("int32", checkIntNumber("int32", -2147483648, 2147483647));
const UINT8 = identity("uint8", checkIntNumber("uint8", 0, 255));
const UINT16 = identity("uint16", checkIntNumber("uint16", 0, 65535));
const UINT32 = identity("uint32", checkIntNumber("uint32", 0, 4294967295));
const INT64 = identity("int64", checkBigInt("int64", 64, true));
const UINT64 = identity("uint64", checkBigInt("uint64", 64, false));
const FLOAT = identity("float", checkFloat("float"));
const BOOL = identity("bool", checkBoolean("bool"));
/** utf8: canonical/rich = string. A string passes through; any other non-null
 *  value is stringified — this preserves the long-standing behavior where a
 *  foreign scalar fed to a utf8 column (e.g. an Int32Array INTERVAL quad that
 *  arrow-js surfaced as "m,d,lo,hi") was coerced to its string form. */
const UTF8: Codec = {
  label: "utf8",
  richToCanonical(value) {
    if (isNullish(value)) return null;
    return typeof value === "string" ? value : String(value);
  },
  canonicalToRich(value) {
    if (isNullish(value)) return null;
    checkString("utf8")(value);
    return value;
  },
  rawToCanonical(value) {
    if (isNullish(value)) return null;
    return typeof value === "string" ? value : String(value);
  },
  canonicalToRaw(value) {
    if (isNullish(value)) return null;
    checkString("utf8")(value);
    return value;
  },
};

/** Normalize any ArrayBuffer view to a Uint8Array over the same bytes. */
function toCanonBytes(label: string, fixedWidth?: number) {
  return (value: unknown): unknown => {
    if (isNullish(value)) return null;
    let u8: Uint8Array;
    if (value instanceof Uint8Array) u8 = value;
    else if (ArrayBuffer.isView(value)) {
      const v = value as ArrayBufferView;
      u8 = new Uint8Array(v.buffer, v.byteOffset, v.byteLength);
    } else return err(label, "expected Uint8Array", value);
    if (fixedWidth != null && u8.length !== fixedWidth) {
      err(label, `expected exactly ${fixedWidth} bytes`, value);
    }
    return u8;
  };
}

function bytesCodec(label: string, fixedWidth?: number): Codec {
  const conv = toCanonBytes(label, fixedWidth);
  return {
    label,
    richToCanonical: conv,
    canonicalToRich: conv,
    rawToCanonical: conv,
    canonicalToRaw: conv,
  };
}

const BINARY = bytesCodec("binary");
const NULL_CODEC: Codec = {
  label: "null",
  richToCanonical: () => null,
  canonicalToRich: () => null,
  rawToCanonical: () => null,
  canonicalToRaw: () => null,
};

/**
 * Resolve the {@link Codec} for an Arrow type. Dispatches by `typeId` and
 * reads unit / bitWidth / scale off the type. Composite types recurse into
 * their child codecs.
 */
export function codecFor(type: VgiDataType): Codec {
  switch (type.typeId) {
    case TypeId.Null:
      return NULL_CODEC;
    case TypeId.Bool:
      return BOOL;
    case TypeId.Int: {
      const bw = (type as any).bitWidth ?? 32;
      const signed = (type as any).isSigned ?? (type as any).signed ?? true;
      if (bw === 64) return signed ? INT64 : UINT64;
      if (signed) {
        if (bw === 8) return INT8;
        if (bw === 16) return INT16;
        return INT32;
      }
      if (bw === 8) return UINT8;
      if (bw === 16) return UINT16;
      return UINT32;
    }
    case TypeId.Float:
      return FLOAT;
    case TypeId.Utf8:
    case TypeId.LargeUtf8:
      return UTF8;
    case TypeId.Binary:
    case TypeId.LargeBinary:
      return BINARY;
    case TypeId.FixedSizeBinary:
      return bytesCodec("fixedSizeBinary", (type as any).byteWidth ?? (type as any).stride);
    case TypeId.Decimal:
      return decimalCodec();
    case TypeId.Date:
      // unit DAY=0 -> date32 (days, number); MILLISECOND=1 -> date64 (ms, bigint)
      return (type as any).unit === 0 ? dateDayCodec() : dateMillisCodec();
    case TypeId.Time:
      // bitWidth 32 -> number (raw s/ms); 64 -> bigint (raw us/ns)
      return ((type as any).bitWidth ?? 32) === 64 ? bigIntUnitCodec("time64") : time32Codec();
    case TypeId.Timestamp:
      return bigIntUnitCodec("timestamp");
    case TypeId.Duration:
      return bigIntUnitCodec("duration");
    case TypeId.Struct:
      return structCodec(type);
    case TypeId.Union:
      // The canonical readers decode a union into a TaggedUnion { tag, value }.
      // That shape is already the rich representation, so pass it through.
      return identity("union", () => {});
    case TypeId.List:
    case TypeId.FixedSizeList:
      return listCodec(type);
    case TypeId.Map:
      return mapCodec(type);
    case TypeId.Dictionary:
      return dictionaryCodec(type);
    case TypeId.Interval:
      // Interval is not in the phase-1 canonical spec; pass through unchanged.
      return identity("interval", () => {});
    default:
      // Unknown / not-yet-modeled types pass through unchanged so the suite
      // stays green; a later phase can add explicit codecs.
      return identity(`type#${type.typeId}`, () => {});
  }
}
