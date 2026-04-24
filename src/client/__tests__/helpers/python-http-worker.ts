// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

// Connects a VgiClient to a pre-started vgi-python `vgi-example-http`
// worker. The worker URL is read from $VGI_PYTHON_HTTP_WORKER — tests are
// expected to be launched via `make test-client` which spawns the worker,
// discovers its port, and exports the env var before running bun:test.
//
// Why not spawn from the test: bun:test's subprocess stdio is unreliable
// (stdout EOFs immediately under bun:test even though the subprocess is
// actively producing output when spawned any other way — we hit the same
// issue with Bun.spawn earlier). Starting the worker from `make` sidesteps
// that entirely.

import { httpConnect } from "vgi-rpc";
import { VgiClient } from "../../../index.js";

export function pythonHttpWorkerAvailable(): boolean {
  return Boolean(process.env.VGI_PYTHON_HTTP_WORKER);
}

export interface PythonHttpWorkerHandle {
  /** Ready-to-use VgiClient over HTTP. */
  client: VgiClient;
  /** Worker URL (from $VGI_PYTHON_HTTP_WORKER). */
  url: string;
  /** Close the RPC client. Does not stop the worker — lifecycle lives in the Makefile. */
  stop(): Promise<void>;
}

/**
 * Connect to the already-running Python HTTP worker whose URL lives in
 * $VGI_PYTHON_HTTP_WORKER. Throws if the env var isn't set.
 */
export async function startPythonHttpWorker(): Promise<PythonHttpWorkerHandle> {
  const url = process.env.VGI_PYTHON_HTTP_WORKER;
  if (!url) {
    throw new Error(
      "VGI_PYTHON_HTTP_WORKER is not set. Launch tests via `make test-client` " +
      "which spawns vgi-example-http and exports the URL.",
    );
  }
  const rpc = httpConnect(url);
  const client = new VgiClient(rpc);
  return {
    client,
    url,
    async stop(): Promise<void> {
      try { client.close(); } catch { /* */ }
    },
  };
}
