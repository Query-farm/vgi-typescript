// Copyright 2025, 2026 Query Farm LLC - https://query.farm

/**
 * The type of `T` with every property whose value may be `undefined` made
 * optional and stripped of `undefined` — the shape `omitUndefined` returns.
 * It is assignable to option bags declared `{ k?: V }` under
 * `exactOptionalPropertyTypes`.
 */
export type OmitUndefined<T> = {
  [K in keyof T as undefined extends T[K] ? never : K]: T[K];
} & {
  [K in keyof T as undefined extends T[K] ? K : never]?: Exclude<T[K], undefined>;
};

/**
 * Copy `obj` without the keys whose value is `undefined`.
 *
 * Used when forwarding optional settings to an options bag owned by another
 * package (vgi-rpc, Bun) whose optional properties do not admit an explicit
 * `undefined`. Readers of such bags treat an absent key and an `undefined`
 * one identically (`??`, destructuring defaults), so this only changes the
 * static type.
 */
export function omitUndefined<T extends object>(obj: T): OmitUndefined<T> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(obj)) {
    if (v !== undefined) out[k] = v;
  }
  return out as OmitUndefined<T>;
}
