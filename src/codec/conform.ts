// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

/**
 * Wire conformance for protocol records.
 *
 * vgi-python is the reference for the wire shape, and two of its rules are not
 * expressed by a record's Arrow schema nullability flags alone:
 *
 *  1. A **non-nullable list or map column is never null.** Python's dataclass
 *     defaults a `list[...]` / `dict[...]` field to an empty container, so an
 *     "absent" list goes out as `[]`. A JS caller that leaves the field out (or
 *     passes `null`) would otherwise put a null in a column the schema calls
 *     non-null — `CatalogInfo.releases`, `CatalogInfo.attach_option_specs`,
 *     ... — which a strict client rejects as a protocol error.
 *
 *  2. **List items and map values are never null** unless the Python annotation
 *     says so (`list[str | None]`). pyarrow marks every list item field nullable,
 *     so the generated schemas carry `item: nullable=true` everywhere and cannot
 *     tell the two apart. In the whole protocol exactly one field has optional
 *     items — `argument_names` on `BindRequest` / `AggregateBindRequest` (a
 *     positional argument has no name). Every other null item is a bug on the
 *     sending side (e.g. `BindResponse.lookup_names` built from an unset secret
 *     name, where Python sends `""`).
 *
 * `conformRecord` applies both rules to a record about to be encoded: a null or
 * absent non-nullable list/map becomes empty, and a null list item / map value
 * becomes its type's zero value (`""`, `0`, `false`, empty bytes, empty list),
 * which is also what the C++ client reads such an item as. It never mutates its
 * input (catalog items may be frozen and shared) and returns the input itself
 * when nothing needed changing. Nullable columns, and scalars, pass through
 * untouched: a null in a non-nullable scalar column has no safe default and is
 * left for the caller to fix (see `wireViolations`).
 */

import {
  type VgiSchema,
  type VgiField,
  type VgiDataType,
  isBinary,
  isBool,
  isDictionary,
  isFloat,
  isInt,
  isList,
  isMap,
  isStruct,
  isUtf8,
} from "../arrow/index.js";

/**
 * Top-level / struct-child field names whose list items may legitimately be
 * null (the Python annotation is `list[X | None]`). Derived from vgi-python by
 * walking every protocol dataclass's type hints; keep in sync if the protocol
 * adds another optional-item list.
 */
export const OPTIONAL_ITEM_FIELDS: ReadonlySet<string> = new Set(["argument_names"]);

function children(type: VgiDataType): VgiField[] {
  return ((type as any).children ?? []) as VgiField[];
}

function mapValueType(type: VgiDataType): VgiDataType {
  const entries = children(type)[0].type;
  return children(entries)[1].type;
}

function mapPairs(raw: any): Array<[unknown, unknown]> {
  if (Array.isArray(raw)) return raw.map((e: any) => (Array.isArray(e) ? [e[0], e[1]] : [e?.key, e?.value]));
  if (raw instanceof Map) return Array.from(raw.entries());
  if (typeof raw[Symbol.iterator] === "function") return Array.from(raw as Iterable<[unknown, unknown]>);
  return Object.entries(raw);
}

/** The value a non-null slot of `type` takes when the sender left it null. */
export function zeroValue(type: VgiDataType): unknown {
  if (isUtf8(type) || isDictionary(type)) return "";
  if (isBinary(type)) return new Uint8Array(0);
  if (isBool(type)) return false;
  if (isInt(type)) return (type as any).bitWidth === 64 ? 0n : 0;
  if (isFloat(type)) return 0;
  if (isList(type) || isMap(type)) return [];
  if (isStruct(type)) {
    const out: Record<string, unknown> = {};
    for (const c of children(type)) out[c.name] = c.nullable ? null : conformValue(c.type, undefined, false, c.name);
    return out;
  }
  return null;
}

/** Whether `type` contains a list or map anywhere (memoized per type object). */
const HAS_CONTAINER = new WeakMap<object, boolean>();
function hasContainer(type: VgiDataType): boolean {
  const cached = HAS_CONTAINER.get(type as object);
  if (cached !== undefined) return cached;
  let has = isList(type) || isMap(type);
  if (!has && isStruct(type)) has = children(type).some((c) => hasContainer(c.type));
  HAS_CONTAINER.set(type as object, has);
  return has;
}

/**
 * Conform one value of `type`. `nullable` is the slot's nullability; `name` is
 * the field name that owns the slot (list items inherit their list's name, for
 * the `OPTIONAL_ITEM_FIELDS` lookup).
 */
function conformValue(type: VgiDataType, value: any, nullable: boolean, name: string): any {
  if (value == null) {
    if (nullable) return value ?? null;
    if (isList(type) || isMap(type)) return [];
    return value;
  }
  if (!hasContainer(type)) return value;

  if (isList(type)) {
    const itemType = children(type)[0].type;
    const itemsNullable = OPTIONAL_ITEM_FIELDS.has(name);
    const items = Array.isArray(value) ? value : Array.from(value as Iterable<unknown>);
    let changed = !Array.isArray(value);
    const out = items.map((item) => {
      const next = item == null && !itemsNullable ? zeroValue(itemType) : conformValue(itemType, item, true, name);
      if (next !== item) changed = true;
      return next;
    });
    return changed ? out : value;
  }

  if (isMap(type)) {
    const valueType = mapValueType(type);
    const pairs = mapPairs(value);
    let changed = false;
    const out = pairs.map(([k, v]): [unknown, unknown] => {
      const next = v == null ? zeroValue(valueType) : conformValue(valueType, v, true, name);
      if (next !== v) changed = true;
      return [k, next];
    });
    return changed ? out : value;
  }

  if (isStruct(type)) {
    let out: Record<string, any> | null = null;
    for (const c of children(type)) {
      const v = value[c.name];
      const next = conformValue(c.type, v, c.nullable, c.name);
      if (next !== v) {
        (out ??= { ...value })[c.name] = next;
      }
    }
    return out ?? value;
  }
  return value;
}

/**
 * `obj` conformed to `schema`'s wire rules (see the module comment). Returns
 * `obj` itself when nothing changed.
 */
export function conformRecord<T extends Record<string, any>>(schema: VgiSchema, obj: T): T {
  let out: Record<string, any> | null = null;
  for (const f of schema.fields) {
    const v = obj[f.name];
    const next = conformValue(f.type, v, f.nullable, f.name);
    if (next !== v) {
      (out ??= { ...obj })[f.name] = next;
    }
  }
  return (out ?? obj) as T;
}

/**
 * Column-dict form of `conformRecord` for `batchFromColumns` callers: each
 * value is a one-element array holding the row's value.
 */
export function conformColumns(
  schema: VgiSchema,
  columns: Record<string, unknown[]>,
): Record<string, unknown[]> {
  let out: Record<string, unknown[]> | null = null;
  for (const f of schema.fields) {
    const col = columns[f.name];
    const rows = col ?? [undefined];
    let changed = col === undefined;
    const next = rows.map((v) => {
      const c = conformValue(f.type, v, f.nullable, f.name);
      if (c !== v) changed = true;
      return c;
    });
    if (changed) {
      (out ??= { ...columns })[f.name] = next;
    }
  }
  return out ?? columns;
}
