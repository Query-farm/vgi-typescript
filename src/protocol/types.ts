// Wire-format protocol types matching Python's ArrowSerializableDataclass definitions.

import type { Schema, RecordBatch } from "@query-farm/apache-arrow";
import type { Arguments } from "../arguments/arguments.js";
import { FunctionType, TableInOutPhase, type TableCardinality } from "../types.js";

export interface BindRequest {
  functionName: string;
  arguments: Arguments;
  functionType: FunctionType;
  inputSchema: Schema | null;
  settings: RecordBatch | null;
  secrets: RecordBatch | null;
  attachId: Uint8Array | null;
  transactionId: Uint8Array | null;
  resolvedSecretsProvided: boolean;
}

export interface BindResponse {
  outputSchema: Schema;
  opaqueData: Uint8Array | null;
  lookupSecretTypes?: string[];
  lookupScopes?: string[];
  lookupNames?: string[];
}

export interface InitRequest {
  bindCall: BindRequest;
  outputSchema: Schema;
  bindOpaqueData: Uint8Array | null;
  projectionIds: number[] | null;
  pushdownFilters: RecordBatch | null;
  /**
   * Join-key value batches, one per join-keys column. Keyed by the column
   * name inside each batch's schema. Populated when DuckDB promotes
   * IN/OR lists or join predicates to batched join-keys pushdowns.
   */
  joinKeys: RecordBatch[];
  phase: TableInOutPhase | null;
  executionId: Uint8Array | null;
  initOpaqueData: Uint8Array | null;
}

export interface GlobalInitResponse {
  maxWorkers: number;
  executionId: Uint8Array;
  opaqueData: Uint8Array | null;
}

export interface TableFunctionCardinalityRequest {
  bindCall: BindRequest;
  bindOpaqueData: Uint8Array | null;
}

export { TableCardinality };
