// Function registry: name -> VgiFunction lookup.

import type { VgiFunction } from "./types.js";
import { FunctionNotFoundError } from "../errors.js";

export class FunctionRegistry {
  private _functions: Map<string, VgiFunction[]> = new Map();

  register(func: VgiFunction): void {
    const name = func.meta.name;
    if (!this._functions.has(name)) {
      this._functions.set(name, []);
    }
    this._functions.get(name)!.push(func);
  }

  get(name: string): VgiFunction {
    const candidates = this._functions.get(name);
    if (!candidates || candidates.length === 0) {
      throw new FunctionNotFoundError(name, this.names());
    }
    // Return first candidate (TODO: argument-based disambiguation)
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
