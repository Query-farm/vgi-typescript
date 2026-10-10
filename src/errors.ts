// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// VGI-specific error classes.
//
// Each class declares the canonical gRPC-style code vgi-rpc hoists onto the
// EXCEPTION batch as `vgi_rpc.error_code` (vgi-rpc WIRE_PROTOCOL.md §8), so a
// client — and DuckDB's `errors_as_json` — can tell "your input was wrong"
// from a worker bug. The mapping is shared with every other VGI SDK:
//
//   INVALID_ARGUMENT     argument validation / type rejection
//   NOT_FOUND            unknown function, table, schema, catalog
//   ALREADY_EXISTS       creating something that is already there
//   FAILED_PRECONDITION  writes against a read-only catalog
//   UNIMPLEMENTED        an operation the SDK does not support
//
// Anything else (RowCountMismatchError, internal invariants) stays UNKNOWN.

import type { ErrorCode } from "@query-farm/vgi-rpc";

export class VgiError extends Error {
  /** Canonical code hoisted as `vgi_rpc.error_code`; unset means `UNKNOWN`. */
  readonly errorCode?: ErrorCode;

  constructor(message: string, code?: ErrorCode) {
    super(message);
    this.name = "VgiError";
    if (code !== undefined) this.errorCode = code;
  }
}

/** A worker bug (the function returned the wrong number of rows): UNKNOWN. */
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
    super(`Unknown function '${name}'`, "NOT_FOUND");
    this.name = "FunctionNotFoundError";
  }
}

export class CatalogReadOnlyError extends VgiError {
  constructor(operation: string) {
    super(`catalog is read-only: ${operation} is not supported`, "FAILED_PRECONDITION");
    this.name = "CatalogReadOnlyError";
  }
}

export class CatalogNotFoundError extends VgiError {
  constructor(entity: string, name: string) {
    super(`${entity} '${name}' not found`, "NOT_FOUND");
    this.name = "CatalogNotFoundError";
  }
}

export class CatalogAlreadyExistsError extends VgiError {
  constructor(entity: string, name: string) {
    super(`${entity} '${name}' already exists`, "ALREADY_EXISTS");
    this.name = "CatalogAlreadyExistsError";
  }
}

export class ArgumentValidationError extends VgiError {
  constructor(message: string) {
    super(message, "INVALID_ARGUMENT");
    this.name = "ArgumentValidationError";
  }
}

export class NoCatalogError extends VgiError {
  constructor() {
    super("No catalog is configured for this worker", "UNIMPLEMENTED");
    this.name = "NoCatalogError";
  }
}
