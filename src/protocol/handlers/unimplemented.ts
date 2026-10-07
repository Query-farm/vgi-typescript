// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// vgi.v2 methods this SDK registers but does not implement.
//
// "The protocol is the unit of optionality": every SDK hosts EVERY vgi.v2
// method with exactly the reference's schemas, so vgi_rpc.Reflection.v1
// reports one vgi.v2 protocol hash everywhere (see VGI_V2_PROTOCOL_HASH in
// ../dispatch.ts). A method this port has no implementation for is still
// registered -- with the reference's params/result schemas -- and answers
// every call with vgi-rpc's MethodNotImplementedError: error_code
// UNIMPLEMENTED, error_kind method_not_implemented. Never a silent success.
//
// When one of these gains a real implementation, move it out of this list
// into its handler module; the schemas must not change (the hash test fails
// if they do).

import { MethodNotImplementedError, type Protocol } from "@query-farm/vgi-rpc";
import type { VgiSchema } from "../../arrow/index.js";
import {
  CatalogIndexCreateParamsSchema,
  CatalogIndexDropParamsSchema,
  CatalogTableColumnCommentSetParamsSchema,
  CatalogTableDeleteFunctionGetParamsSchema,
  CatalogTableInsertFunctionGetParamsSchema,
  CatalogTableUpdateFunctionGetParamsSchema,
} from "../../generated/vgi-protocol-schemas.js";
import { REQUEST_PARAMS_SCHEMA, RESULT_BINARY_SCHEMA } from "./shared.js";
import { emptyResultSchema } from "./catalog/shared.js";

/** One unimplemented method: its wire name and the reference's schemas. */
interface UnimplementedMethod {
  readonly name: string;
  readonly params: VgiSchema;
  readonly result: VgiSchema;
}

/**
 * The vgi.v2 methods (of the reference's 72) this SDK does not implement.
 * Exported so a test can assert each one refuses with UNIMPLEMENTED.
 */
export const UNIMPLEMENTED_METHODS: readonly UnimplementedMethod[] = [
  // Streaming and window aggregates: request/result are wrapped ASD blobs.
  { name: "aggregate_streaming_open", params: REQUEST_PARAMS_SCHEMA, result: RESULT_BINARY_SCHEMA },
  { name: "aggregate_streaming_chunk", params: REQUEST_PARAMS_SCHEMA, result: RESULT_BINARY_SCHEMA },
  { name: "aggregate_streaming_close", params: REQUEST_PARAMS_SCHEMA, result: RESULT_BINARY_SCHEMA },
  { name: "aggregate_window_init", params: REQUEST_PARAMS_SCHEMA, result: RESULT_BINARY_SCHEMA },
  { name: "aggregate_window", params: REQUEST_PARAMS_SCHEMA, result: RESULT_BINARY_SCHEMA },
  { name: "aggregate_window_batch", params: REQUEST_PARAMS_SCHEMA, result: RESULT_BINARY_SCHEMA },
  { name: "aggregate_window_destructor", params: REQUEST_PARAMS_SCHEMA, result: RESULT_BINARY_SCHEMA },
  // Index DDL and per-column comments.
  { name: "catalog_index_create", params: CatalogIndexCreateParamsSchema, result: emptyResultSchema },
  { name: "catalog_index_drop", params: CatalogIndexDropParamsSchema, result: emptyResultSchema },
  { name: "catalog_table_column_comment_set", params: CatalogTableColumnCommentSetParamsSchema, result: emptyResultSchema },
  // Per-table DML function getters.
  { name: "catalog_table_insert_function_get", params: CatalogTableInsertFunctionGetParamsSchema, result: RESULT_BINARY_SCHEMA },
  { name: "catalog_table_update_function_get", params: CatalogTableUpdateFunctionGetParamsSchema, result: RESULT_BINARY_SCHEMA },
  { name: "catalog_table_delete_function_get", params: CatalogTableDeleteFunctionGetParamsSchema, result: RESULT_BINARY_SCHEMA },
];

/** The message every unimplemented method answers with. */
export function unimplementedMessage(method: string): string {
  return `${method} is not implemented by this worker`;
}

export function registerUnimplementedMethods(protocol: Protocol): void {
  for (const m of UNIMPLEMENTED_METHODS) {
    protocol.unary(m.name, {
      params: m.params,
      result: m.result,
      handler: async () => {
        throw new MethodNotImplementedError(unimplementedMessage(m.name));
      },
    });
  }
}
