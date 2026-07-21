// Copyright 2025, 2026 Query Farm LLC - https://query.farm
//
// `isSigned` alias for flechette Int types.
//
// arrow-js spells integer signedness `isSigned`; flechette spells it `signed`.
// Worker code branches on it to pick a promoted result type — `examples/scalar.ts`'s
// `promoteForAddition` reads `isSigned` and, when it comes back `undefined`,
// promotes a signed BIGINT to UINT64, after which the codec rejects every
// negative value (`codec[uint64]: value out of uint64 range`). That is a wrong
// answer, not a missing method, so the alias is load-bearing.
//
// Arrow types are plain object literals with no shared prototype, so this has
// to be attached per instance: the IPC decode path applies it to every decoded
// schema, and the facade's own type constructors apply it to what they build.
//
// Kept in its own dependency-free module so `normalize-type.ts` and
// `schema.ts` can use it without pulling in `compat.ts`, which depends on the
// canonical write path.

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
    for (const child of t.children) aliasIntSigned(child?.type ?? child);
  }
  if (t.dictionary) aliasIntSigned(t.dictionary);
  if (t.indices) aliasIntSigned(t.indices);
  return type;
}

/** Apply {@link aliasIntSigned} to every field type of a decoded schema. */
export function aliasSchemaIntSigned<T>(schema: T): T {
  const fields = (schema as any)?.fields;
  if (Array.isArray(fields)) {
    for (const f of fields) aliasIntSigned(f?.type);
  }
  return schema;
}
