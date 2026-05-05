// VGI-specific error classes.

export class VgiError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "VgiError";
  }
}

export class RowCountMismatchError extends VgiError {
  constructor(expected: number, actual: number) {
    super(
      `Scalar function output row count (${actual}) does not match ` +
        `input row count (${expected})`
    );
    this.name = "RowCountMismatchError";
  }
}

export class FunctionNotFoundError extends VgiError {
  constructor(name: string, available?: string[]) {
    super(`Unknown function '${name}'`);
    this.name = "FunctionNotFoundError";
  }
}

export class CatalogReadOnlyError extends VgiError {
  constructor(operation: string) {
    super(`catalog is read-only: ${operation} is not supported`);
    this.name = "CatalogReadOnlyError";
  }
}

export class CatalogNotFoundError extends VgiError {
  constructor(entity: string, name: string) {
    super(`${entity} '${name}' not found`);
    this.name = "CatalogNotFoundError";
  }
}

export class CatalogAlreadyExistsError extends VgiError {
  constructor(entity: string, name: string) {
    super(`${entity} '${name}' already exists`);
    this.name = "CatalogAlreadyExistsError";
  }
}

export class ArgumentValidationError extends VgiError {
  constructor(message: string) {
    super(message);
    this.name = "ArgumentValidationError";
  }
}

export class NoCatalogError extends VgiError {
  constructor() {
    super("No catalog is configured for this worker");
    this.name = "NoCatalogError";
  }
}
