// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

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

import { httpConnect } from "@query-farm/vgi-rpc";
import { VgiClient } from "../../../index.js";

export function pythonHttpWorkerAvailable(): boolean {
  return Boolean(process.env.VGI_PYTHON_HTTP_WORKER);
}

export function pythonVersionedHttpWorkerAvailable(): boolean {
  return Boolean(process.env.VGI_PYTHON_VERSIONED_HTTP_WORKER);
}

export function pythonAttachOptionsHttpWorkerAvailable(): boolean {
  return Boolean(process.env.VGI_PYTHON_ATTACH_OPTIONS_HTTP_WORKER);
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
  return connectTo(process.env.VGI_PYTHON_HTTP_WORKER, "VGI_PYTHON_HTTP_WORKER");
}

/**
 * Connect to the already-running Python *versioned* HTTP worker whose URL
 * lives in $VGI_PYTHON_VERSIONED_HTTP_WORKER. Throws if not set.
 */
export async function startPythonVersionedHttpWorker(): Promise<PythonHttpWorkerHandle> {
  return connectTo(process.env.VGI_PYTHON_VERSIONED_HTTP_WORKER, "VGI_PYTHON_VERSIONED_HTTP_WORKER");
}

/**
 * Connect to the already-running Python *attach-options* HTTP worker whose
 * URL lives in $VGI_PYTHON_ATTACH_OPTIONS_HTTP_WORKER.
 */
export async function startPythonAttachOptionsHttpWorker(): Promise<PythonHttpWorkerHandle> {
  return connectTo(
    process.env.VGI_PYTHON_ATTACH_OPTIONS_HTTP_WORKER,
    "VGI_PYTHON_ATTACH_OPTIONS_HTTP_WORKER",
  );
}

async function connectTo(url: string | undefined, envName: string): Promise<PythonHttpWorkerHandle> {
  if (!url) {
    throw new Error(
      `${envName} is not set. Launch tests via \`make test-client\` ` +
      `which spawns the Python worker(s) and exports the URL(s).`,
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
