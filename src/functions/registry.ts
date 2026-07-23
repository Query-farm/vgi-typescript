// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Function registry: name -> VgiFunction lookup with overload resolution.

import {
  type VgiDataType,
  isBinary, isBool, isDecimal, isFloat, isInt, isNull, isUtf8,
  typeSignature,
} from "../arrow/index.js";
import type { VgiSchema } from "../arrow/index.js";
import type { VgiFunction } from "./types.js";
import type { Arguments } from "../arguments/arguments.js";
import type { ArgumentSpec } from "../arguments/argument-spec.js";
import { FunctionNotFoundError } from "../errors.js";

export interface OverloadContext {
  arguments?: Arguments;
  inputSchema?: VgiSchema | null;
  isScalar?: boolean;
  /**
   * Catalog schema that declares the function, from `BindRequest.schema_name`.
   * A worker may register the same name in more than one schema, so the bare
   * name is not a unique key. When set, resolution is scoped to that schema and
   * never falls back to another one; when absent, every schema is searched.
   */
  schemaName?: string | null;
  /**
   * Catalog that owns the function, resolved from the bind's
   * `attach_opaque_data`. Two catalogs served by one worker may each declare
   * the same schema *and* function name, in which case only the catalog tells
   * them apart. Absent for calls with no attachment.
   */
  catalogName?: string | null;
}

const EXACT_MATCH_SCORE = 2;
const FAMILY_MATCH_SCORE = 1;

/**
 * Exact structural type identity.
 *
 * `toString()` cannot be used: arrow-js DataTypes stringify to a meaningful
 * name, flechette's plain-object types all stringify to "[object Object]".
 * Under flechette that made every exact-match test fail, every overload score
 * collapse to the family tie, and `type_info(42::BIGINT)` resolve to the
 * INTEGER overload.
 */
function typesIdentical(a: VgiDataType, b: VgiDataType): boolean {
  return typeSignature(a) === typeSignature(b);
}

function typesCompatible(actual: VgiDataType, declared: VgiDataType): boolean {
  if (typesIdentical(actual, declared)) return true;
  // Integer family
  if (isInt(actual) && isInt(declared)) return true;
  // Float/decimal family
  if ((isFloat(actual) || isDecimal(actual)) &&
      (isFloat(declared) || isDecimal(declared))) return true;
  // String family (Utf8, LargeUtf8)
  if (isUtf8(actual) && isUtf8(declared)) return true;
  // Binary family
  if (isBinary(actual) && isBinary(declared)) return true;
  // Boolean
  if (isBool(actual) && isBool(declared)) return true;
  return false;
}

function scoreTypes(
  specs: ArgumentSpec[],
  actualTypes: (VgiDataType | null)[],
): { score: number; matched: boolean } {
  let score = 0;
  let varArgsSpec: ArgumentSpec | null = null;

  for (let i = 0; i < specs.length; i++) {
    const spec = specs[i];
    if (spec.isVarargs) varArgsSpec = spec;
    if (i >= actualTypes.length) break;
    if (spec.isAnyType || isNull(spec.arrowType)) continue;
    const actual = actualTypes[i];
    if (actual === null) continue;
    if (typesIdentical(actual, spec.arrowType)) {
      score += EXACT_MATCH_SCORE;
    } else if (typesCompatible(actual, spec.arrowType)) {
      score += FAMILY_MATCH_SCORE;
    } else {
      return { score, matched: false };
    }
  }

  // Score varargs tail beyond declared specs
  if (varArgsSpec && !varArgsSpec.isAnyType && !isNull(varArgsSpec.arrowType)) {
    for (let i = specs.length; i < actualTypes.length; i++) {
      const actual = actualTypes[i];
      if (actual === null) continue;
      if (typesIdentical(actual, varArgsSpec.arrowType)) {
        score += EXACT_MATCH_SCORE;
      } else if (typesCompatible(actual, varArgsSpec.arrowType)) {
        score += FAMILY_MATCH_SCORE;
      } else {
        return { score, matched: false };
      }
    }
  }

  return { score, matched: true };
}

function filterByArgumentTypes(
  candidates: VgiFunction[],
  args: Arguments,
  inputSchema: VgiSchema | null | undefined,
  isScalar: boolean,
): VgiFunction[] {
  const scored: { score: number; func: VgiFunction }[] = [];

  for (const func of candidates) {
    const specs = func.argumentSpecs;
    let score = 0;
    let matched = true;

    if (isScalar) {
      const constSpecs = specs.filter(s => s.isConst);
      const colSpecs = specs.filter(s => !s.isConst && typeof s.position === "number");

      // Score const specs against positional argument types
      const constTypes: (VgiDataType | null)[] = [];
      const argsSchema = args.argumentsSchema;
      for (let i = 0; i < args.positional.length; i++) {
        const field = argsSchema?.fields.find(f => f.name === `positional_${i}`);
        constTypes.push(field ? field.type : null);
      }
      const r1 = scoreTypes(constSpecs, constTypes);
      score += r1.score;
      matched = r1.matched;

      // Score column specs against inputSchema
      if (matched && inputSchema) {
        const colTypes: (VgiDataType | null)[] = [];
        let varArgsColSpec: ArgumentSpec | null = null;
        for (const spec of colSpecs) {
          if (spec.isVarargs) varArgsColSpec = spec;
          const pos = spec.position as number;
          colTypes.push(pos < inputSchema.fields.length ? inputSchema.fields[pos].type : null);
        }
        // Append varargs tail from inputSchema
        if (varArgsColSpec) {
          const start = (varArgsColSpec.position as number) + 1;
          for (let i = start; i < inputSchema.fields.length; i++) {
            colTypes.push(inputSchema.fields[i].type);
          }
        }
        const r2 = scoreTypes(colSpecs, colTypes);
        score += r2.score;
        matched = r2.matched;
      }
    } else {
      // Table functions: compare declared positional specs against the
      // argument types.
      const posSpecs = specs
        .filter(s => typeof s.position === "number" && !s.isTableInput)
        .sort((a, b) => (a.position as number) - (b.position as number));

      const posTypes: (VgiDataType | null)[] = [];
      if (func.meta.inputFromArgs) {
        // Blended ("UNNEST-style"): the positional args ARE the per-row input
        // columns, so they are NOT on the wire (args.positional is empty).
        // Score the declared positional specs against the INPUT_SCHEMA column
        // types instead (coercibly, via scoreTypes), so same-arity overloads
        // disambiguate by type — matching what DuckDB's binder resolved.
        // Expand a trailing varargs spec across the remaining input columns
        // (mirrors the scalar column path).
        if (inputSchema) {
          let varArgsSpec: ArgumentSpec | null = null;
          for (const spec of posSpecs) {
            if (spec.isVarargs) varArgsSpec = spec;
            const pos = spec.position as number;
            posTypes.push(pos < inputSchema.fields.length ? inputSchema.fields[pos].type : null);
          }
          if (varArgsSpec) {
            for (let i = (varArgsSpec.position as number) + 1; i < inputSchema.fields.length; i++) {
              posTypes.push(inputSchema.fields[i].type);
            }
          }
        }
      } else {
        const argsSchema = args.argumentsSchema;
        for (let i = 0; i < args.positional.length; i++) {
          const field = argsSchema?.fields.find(f => f.name === `positional_${i}`);
          posTypes.push(field ? field.type : null);
        }
      }
      const r = scoreTypes(posSpecs, posTypes);
      score += r.score;
      matched = r.matched;
    }

    if (matched) {
      scored.push({ score, func });
    }
  }

  if (scored.length === 0) return [];
  const maxScore = Math.max(...scored.map(s => s.score));
  return scored.filter(s => s.score === maxScore).map(s => s.func);
}

/** Key for the schema-scoped index: lowercased schema, then function name. */
function schemaKey(schemaName: string, functionName: string): string {
  return `${schemaName.toLowerCase()}\u0000${functionName}`;
}

/** Key for the catalog-scoped index: lowercased catalog, then the schema key. */
function catalogKey(catalogName: string, schemaName: string, functionName: string): string {
  return `${catalogName.toLowerCase()}\u0000${schemaKey(schemaName, functionName)}`;
}

export class FunctionRegistry {
  private _functions: Map<string, VgiFunction[]> = new Map();
  // (lowercased schema, function name) -> functions declared in that schema.
  // Populated by registerInSchema(), which catalog construction calls for every
  // function a SchemaDescriptor lists. Lets a schema-qualified bind pick the
  // right implementation when one name is registered in several schemas.
  private _bySchema: Map<string, VgiFunction[]> = new Map();
  // (lowercased catalog, lowercased schema, function name) -> functions. Needed
  // because two catalogs in one worker may declare the same schema AND name.
  private _byCatalog: Map<string, VgiFunction[]> = new Map();

  register(func: VgiFunction): void {
    const name = func.meta.name;
    if (!this._functions.has(name)) {
      this._functions.set(name, []);
    }
    this._functions.get(name)!.push(func);
  }

  /**
   * Record that `func` is declared in `schemaName`, in addition to the flat
   * by-name index. Idempotent, and independent of `register()` so a function
   * reachable from several schemas (e.g. a scan function referenced by tables in
   * more than one schema) resolves from any of them.
   */
  registerInSchema(func: VgiFunction, schemaName: string, catalogName?: string): void {
    const add = (map: Map<string, VgiFunction[]>, key: string) => {
      const bucket = map.get(key);
      if (!bucket) {
        map.set(key, [func]);
        return;
      }
      if (!bucket.includes(func)) bucket.push(func);
    };
    add(this._bySchema, schemaKey(schemaName, func.meta.name));
    if (catalogName) {
      add(this._byCatalog, catalogKey(catalogName, schemaName, func.meta.name));
    }
  }

  /** Schemas that declare `functionName`, sorted — for error messages. */
  schemasFor(functionName: string): string[] {
    const out: string[] = [];
    for (const key of this._bySchema.keys()) {
      const [schema, name] = key.split("\u0000");
      if (name === functionName) out.push(schema);
    }
    return out.sort();
  }

  get(name: string, context?: OverloadContext): VgiFunction {
    let candidates = this._functions.get(name);

    // A schema-qualified lookup is exact: only functions declared in that schema
    // are candidates, so a name registered in two schemas dispatches to the one
    // the caller named rather than colliding as an overload.
    // Catalog first: it is the only key that separates two catalogs declaring
    // the same schema and name.
    const catalogName = context?.catalogName;
    const schemaName = context?.schemaName;
    if (catalogName && schemaName) {
      const scoped = this._byCatalog.get(catalogKey(catalogName, schemaName, name));
      if (scoped && scoped.length > 0) {
        return this._disambiguate(name, scoped, context);
      }
    }
    if (schemaName) {
      const scoped = this._bySchema.get(schemaKey(schemaName, name));
      if (scoped && scoped.length > 0) {
        candidates = scoped;
      } else if (candidates && candidates.length > 0) {
        const schemas = this.schemasFor(name);
        if (schemas.length > 0) {
          throw new Error(
            `Function '${name}' is not registered in schema '${schemaName}'. ` +
              `It is available in: [${schemas.join(", ")}]`,
          );
        }
      }
    }

    if (!candidates || candidates.length === 0) {
      throw new FunctionNotFoundError(name, this.names());
    }

    return this._disambiguate(name, candidates, context);
  }

  /**
   * Pick one function from an already-scoped candidate list, by argument shape
   * and types. Scoping (catalog / schema) happens in `get()`; by the time this
   * runs, every candidate is a legitimate overload of the same name.
   */
  private _disambiguate(
    name: string,
    candidates: VgiFunction[],
    context?: OverloadContext,
  ): VgiFunction {
    // Fast path: single candidate
    if (candidates.length === 1) {
      return candidates[0];
    }

    // No context: return first (backward compat)
    if (!context?.arguments) {
      return candidates[0];
    }

    const args = context.arguments;
    const isScalar = context.isScalar ?? (candidates[0].kind === "scalar");

    // Stage 1: Count-based filtering
    let matches: VgiFunction[];
    if (isScalar) {
      // For scalars, count const specs and match against args.positional.length
      const numConstArgs = args.positional.length;
      matches = candidates.filter(func => {
        const constSpecs = func.argumentSpecs.filter(s => s.isConst);
        const hasVarargs = constSpecs.some(s => s.isVarargs);
        const nonVarargConst = constSpecs.filter(s => !s.isVarargs).length;
        if (hasVarargs) {
          return numConstArgs >= nonVarargConst;
        }
        return constSpecs.length === numConstArgs;
      });
      // If count filtering didn't help, also try matching by column count
      if (matches.length === 0 || matches.length === candidates.length) {
        // Fall back to all candidates for type-based filtering
        matches = candidates;
      }
    } else {
      // For table functions, count positional specs
      const numArgs = args.positional.length;
      matches = candidates.filter(func => {
        const posSpecs = func.argumentSpecs.filter(
          s => typeof s.position === "number" && !s.isTableInput
        );
        const hasVarargs = posSpecs.some(s => s.isVarargs);
        const nonVarargs = posSpecs.filter(s => !s.isVarargs).length;
        if (func.meta.inputFromArgs) {
          // Blended ("UNNEST-style") overload: the positional params ARE the
          // per-row input columns, so they are NOT on the wire
          // (args.positional is empty). Resolve by INPUT-COLUMN count (arity)
          // against the declared positional params instead — e.g.
          // geo_encode(52,13) -> 2 input cols -> the 2-positional overload.
          // Named (str-position) args still come from the wire arguments.
          const numInputCols = context.inputSchema?.fields.length ?? 0;
          if (hasVarargs) {
            if (numInputCols < nonVarargs) return false;
          } else if (numInputCols !== posSpecs.length) {
            return false;
          }
          // Unknown named argument disqualifies the overload.
          const validNamed = new Set(
            func.argumentSpecs
              .filter(s => typeof s.position === "string")
              .map(s => s.position as string),
          );
          for (const key of args.named.keys()) {
            if (!validNamed.has(key)) return false;
          }
          return true;
        }
        if (hasVarargs) {
          return numArgs >= nonVarargs;
        }
        return posSpecs.length === numArgs;
      });
      if (matches.length === 0) {
        matches = candidates;
      }
    }

    if (matches.length === 1) {
      return matches[0];
    }

    // Stage 2: Type-based filtering
    const typeFiltered = filterByArgumentTypes(
      matches,
      args,
      context.inputSchema,
      isScalar,
    );

    if (typeFiltered.length === 1) {
      return typeFiltered[0];
    }
    if (typeFiltered.length > 1) {
      return typeFiltered[0]; // Best score wins
    }

    // Fallback: return first candidate
    return candidates[0];
  }

  has(name: string): boolean {
    return this._functions.has(name) && this._functions.get(name)!.length > 0;
  }

  names(): string[] {
    return [...this._functions.keys()].sort();
  }

  all(): VgiFunction[] {
    const result: VgiFunction[] = [];
    for (const candidates of this._functions.values()) {
      result.push(...candidates);
    }
    return result;
  }
}
