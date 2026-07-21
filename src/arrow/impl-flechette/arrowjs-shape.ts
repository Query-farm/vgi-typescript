// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// arrow-js shape for flechette *values* — the plain objects that have no
// prototype to patch (types and fields), as opposed to the class methods
// handled in ./compat.ts.
//
// Two differences bite:
//
//   * Int types spell signedness `signed`; arrow-js spells it `isSigned`.
//     Worker code branches on it to pick a promoted result type —
//     `examples/scalar.ts`'s `promoteForAddition` reads `isSigned` and, when
//     it comes back `undefined`, promotes a signed BIGINT to UINT64, after
//     which the codec rejects every negative value. That is a wrong answer,
//     not a missing property.
//
//   * A decoded flechette Field carries `metadata: null` when the wire had no
//     custom_metadata; arrow-js always materializes an empty `Map`. Shared
//     code reads `field.metadata.get(…)` unconditionally (describe.json's
//     column comments, macro `vgi_doc` parameter docs), so `null` is a
//     TypeError rather than "no metadata".
//
// Both are applied to decoded schemas at the IPC boundary. Kept in a
// dependency-free module so `schema.ts` / `normalize-type.ts` can use it
// without pulling in `compat.ts`, which depends on the canonical write path.

/** Arrow `Type.Int` — the enum value is identical in both libraries. */
const TYPE_INT = 2;

/**
 * Attach `isSigned` to a flechette Int type, recursing through children /
 * dictionary / index types.
 *
 * The alias is a non-enumerable getter so it never shows up in structural
 * comparisons, IPC encoding or `Object.keys` — flechette keeps writing
 * `signed`, and this is purely a read-side view of it. Mutating in place is
 * safe: these objects are freshly built per decode.
 */
export function aliasIntSigned<T>(type: T): T {
  const t = type as any;
  if (t == null || typeof t !== "object") return type;
  if (t.typeId === TYPE_INT && !("isSigned" in t)) {
    Object.defineProperty(t, "isSigned", {
      get(this: any) {
        return this.signed;
      },
      enumerable: false,
      configurable: true,
    });
  }
  if (Array.isArray(t.children)) {
    for (const child of t.children) normalizeField(child);
  }
  if (t.dictionary) aliasIntSigned(t.dictionary);
  if (t.indices) aliasIntSigned(t.indices);
  return type;
}

/** Normalize one field: an always-present metadata Map, plus its type. */
function normalizeField(field: any): void {
  if (field == null || typeof field !== "object") return;
  if (!field.metadata) field.metadata = new Map<string, string>();
  aliasIntSigned(field.type ?? field);
}

/**
 * Bring a freshly decoded flechette schema up to the arrow-js shape shared
 * code expects: every field (and nested child field) has a metadata `Map`,
 * and every Int type answers to `isSigned`.
 */
export function normalizeDecodedSchema<T>(schema: T): T {
  const s = schema as any;
  if (s == null || typeof s !== "object") return schema;
  if (!s.metadata) s.metadata = new Map<string, string>();
  if (Array.isArray(s.fields)) {
    for (const f of s.fields) normalizeField(f);
  }
  return schema;
}
