// Copyright 2025, 2026 Query Farm LLC - https://query.farm
import type { MacroDescriptor } from "./descriptors.js";
import type { MacroOverload } from "../generated/vgi-client.js";
import { deserializeBatch, projectBatch, serializeBatch } from "../arrow/index.js";

/** Convert a simple descriptor without treating default-value types as declarations. */
export function macroOverloads(macro: MacroDescriptor, owner: string[]): MacroOverload[] {
  if (macro.overloads != null) {
    if (macro.parameters.length || macro.definition || macro.parameterDefaultValues ||
        Object.keys(macro.parameterDocs ?? {}).length) throw new Error("macro family conflicts with single-definition descriptor");
    if (!macro.overloads.length) throw new Error("macro overload family must be nonempty");
    for (const overload of macro.overloads) {
      if (overload.parameters.some(p => p.default_expression != null)) throw new Error("discovery macro defaults must be resolved values");
      if (overload.macro_type !== macro.macroType || JSON.stringify(overload.schema_path) !== JSON.stringify(owner)) {
        throw new Error("macro overload owner and kind must equal the parent macro");
      }
    }
    return macro.overloads;
  }
  if (!macro.definition.trim() || macro.definition.includes("\0")) throw new Error("macro definition must be nonempty SQL without NUL");
  const defaults = macro.parameterDefaultValues?.length ? deserializeBatch(macro.parameterDefaultValues) : null;
  if (defaults && defaults.numRows !== 1) throw new Error("macro defaults require exactly one row");
  for (const name of [...(defaults?.schema.fields.map(f => f.name) ?? []), ...Object.keys(macro.parameterDocs ?? {})]) {
    if (!macro.parameters.includes(name)) throw new Error(`unknown macro parameter '${name}'`);
  }
  const defaultNames = defaults?.schema.fields.map(f => f.name) ?? [];
  if (new Set(defaultNames).size !== defaultNames.length) throw new Error("duplicate macro default");
  const seen = new Set<string>();
  let hasDefault = false;
  const parameters = macro.parameters.map(name => {
    if (!name || name.includes("\0") || seen.has(name.toLowerCase())) throw new Error("invalid or duplicate macro parameter");
    seen.add(name.toLowerCase());
    const index = defaults?.schema.fields.findIndex(f => f.name === name) ?? -1;
    if (index < 0 && hasDefault) throw new Error("required macro parameter cannot follow a default");
    hasDefault ||= index >= 0;
    return { name, declared_type: null, default_expression: null,
      default_value: index < 0 ? null : serializeBatch(projectBatch([index], defaults!)),
      documentation: macro.parameterDocs?.[name] ?? null };
  });
  return [{ parameters, definition: macro.definition, profile: "duckdb.macro.v1",
    schema_path: [...owner], macro_type: macro.macroType,
    evaluation_context: { timezone: "UTC", collation: "binary", default_order: "asc",
      default_null_order: "nulls_last", preserve_insertion_order: true } }];
}
