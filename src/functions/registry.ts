// Function registry: name -> VgiFunction lookup with overload resolution.

import { DataType, Null } from "@query-farm/apache-arrow";
import type { Schema } from "@query-farm/apache-arrow";
import type { VgiFunction } from "./types.js";
import type { Arguments } from "../arguments/arguments.js";
import type { ArgumentSpec } from "../arguments/argument-spec.js";
import { FunctionNotFoundError } from "../errors.js";

export interface OverloadContext {
  arguments?: Arguments;
  inputSchema?: Schema | null;
  isScalar?: boolean;
}

const EXACT_MATCH_SCORE = 2;
const FAMILY_MATCH_SCORE = 1;

function typesCompatible(actual: DataType, declared: DataType): boolean {
  if (actual.toString() === declared.toString()) return true;
  // Integer family
  if (DataType.isInt(actual) && DataType.isInt(declared)) return true;
  // Float/decimal family
  if ((DataType.isFloat(actual) || DataType.isDecimal(actual)) &&
      (DataType.isFloat(declared) || DataType.isDecimal(declared))) return true;
  // String family (Utf8, LargeUtf8)
  if (DataType.isUtf8(actual) && DataType.isUtf8(declared)) return true;
  // Binary family
  if (DataType.isBinary(actual) && DataType.isBinary(declared)) return true;
  // Boolean
  if (DataType.isBool(actual) && DataType.isBool(declared)) return true;
  return false;
}

function scoreTypes(
  specs: ArgumentSpec[],
  actualTypes: (DataType | null)[],
): { score: number; matched: boolean } {
  let score = 0;
  let varArgsSpec: ArgumentSpec | null = null;

  for (let i = 0; i < specs.length; i++) {
    const spec = specs[i];
    if (spec.isVarargs) varArgsSpec = spec;
    if (i >= actualTypes.length) break;
    if (spec.isAnyType || spec.arrowType instanceof Null) continue;
    const actual = actualTypes[i];
    if (actual === null) continue;
    if (actual.toString() === spec.arrowType.toString()) {
      score += EXACT_MATCH_SCORE;
    } else if (typesCompatible(actual, spec.arrowType)) {
      score += FAMILY_MATCH_SCORE;
    } else {
      return { score, matched: false };
    }
  }

  // Score varargs tail beyond declared specs
  if (varArgsSpec && !varArgsSpec.isAnyType && !(varArgsSpec.arrowType instanceof Null)) {
    for (let i = specs.length; i < actualTypes.length; i++) {
      const actual = actualTypes[i];
      if (actual === null) continue;
      if (actual.toString() === varArgsSpec.arrowType.toString()) {
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
  inputSchema: Schema | null | undefined,
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
      const constTypes: (DataType | null)[] = [];
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
        const colTypes: (DataType | null)[] = [];
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
      // Table functions: compare positional arg types
      const posSpecs = specs
        .filter(s => typeof s.position === "number" && !s.isTableInput)
        .sort((a, b) => (a.position as number) - (b.position as number));

      const posTypes: (DataType | null)[] = [];
      const argsSchema = args.argumentsSchema;
      for (let i = 0; i < args.positional.length; i++) {
        const field = argsSchema?.fields.find(f => f.name === `positional_${i}`);
        posTypes.push(field ? field.type : null);
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

export class FunctionRegistry {
  private _functions: Map<string, VgiFunction[]> = new Map();

  register(func: VgiFunction): void {
    const name = func.meta.name;
    if (!this._functions.has(name)) {
      this._functions.set(name, []);
    }
    this._functions.get(name)!.push(func);
  }

  get(name: string, context?: OverloadContext): VgiFunction {
    const candidates = this._functions.get(name);
    if (!candidates || candidates.length === 0) {
      throw new FunctionNotFoundError(name, this.names());
    }

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
