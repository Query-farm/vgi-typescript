// VgiClient error class + RpcClient wrapper that translates RpcError into
// VgiClientError, preserving the worker's remote traceback.

import { RpcError, type RpcClient } from "vgi-rpc";

/** Error thrown by VgiClient when an RPC call fails or returns unexpected data. */
export class VgiClientError extends Error {
  /** Remote traceback from the worker, when an RpcError carried one. */
  readonly remoteTraceback?: string;
  /** Underlying error type from the worker (e.g. "ValueError"), when known. */
  readonly errorType?: string;

  constructor(
    message: string,
    options?: { remoteTraceback?: string; errorType?: string; cause?: unknown },
  ) {
    super(message, options?.cause != null ? { cause: options.cause } : undefined);
    this.name = "VgiClientError";
    if (options?.remoteTraceback) this.remoteTraceback = options.remoteTraceback;
    if (options?.errorType) this.errorType = options.errorType;
  }

  /**
   * Build a VgiClientError from an underlying RpcError, preserving the
   * remote error type, message, and traceback so the worker-raised exception
   * shows up at the top of the stack rather than buried under VGI framing.
   * Mirrors Python's ClientError.from_rpc_error.
   */
  static fromRpcError(e: RpcError): VgiClientError {
    const head = `${e.errorType}: ${e.errorMessage}`;
    const message = e.remoteTraceback
      ? `${head}\n\nRemote traceback:\n${e.remoteTraceback}`
      : head;
    return new VgiClientError(message, {
      remoteTraceback: e.remoteTraceback || undefined,
      errorType: e.errorType,
      cause: e,
    });
  }
}

/**
 * Wrap an RpcClient so any RpcError thrown by call/stream is rethrown as a
 * VgiClientError carrying the worker's remote traceback. Other errors
 * propagate unchanged.
 */
export function wrapRpcWithErrorEnrichment(rpc: RpcClient): RpcClient {
  return {
    async call(method, params) {
      try {
        return await rpc.call(method, params);
      } catch (e) {
        if (e instanceof RpcError) throw VgiClientError.fromRpcError(e);
        throw e;
      }
    },
    async stream(method, params) {
      try {
        return await rpc.stream(method, params);
      } catch (e) {
        if (e instanceof RpcError) throw VgiClientError.fromRpcError(e);
        throw e;
      }
    },
    describe: () => rpc.describe(),
    close: () => rpc.close(),
  };
}
