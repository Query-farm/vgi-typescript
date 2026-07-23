// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// VGI Worker: main entry point for running a VGI function server.

import { VgiRpcServer, serveUnix, serveTcp, serveStream as serveStreamRpc } from "@query-farm/vgi-rpc";
import { FunctionRegistry } from "./functions/registry.js";
import type { VgiFunction } from "./functions/types.js";
import { buildVgiProtocol } from "./protocol/dispatch.js";
import type { Protocol } from "@query-farm/vgi-rpc";
import type { CatalogDescriptor } from "./catalog/descriptors.js";
import type { CatalogInterface } from "./catalog/interface.js";
import { ReadOnlyCatalogInterface } from "./catalog/read-only.js";

// argv parser for the launcher's `--unix PATH` / `--tcp [HOST:]PORT` /
// `--idle-timeout SEC` contract. The C++ launcher (vgi extension) appends
// these to the worker's argv whenever LOCATION uses the `launch:` scheme.
// Unknown args pass through.
interface LauncherArgs {
  unixPath?: string;
  /** TCP bind host, defaulting to 127.0.0.1 when `--tcp` gives a bare port. */
  tcpHost?: string;
  /** TCP bind port; present iff `--tcp` was passed. */
  tcpPort?: number;
  idleTimeout?: number;
}

// Parse a `[HOST:]PORT` --tcp bind spec into a LauncherArgs. A bare PORT (no
// colon) binds 127.0.0.1; an empty host (leading ":") also defaults to loopback.
function parseTcpSpec(spec: string, out: LauncherArgs): void {
  const idx = spec.lastIndexOf(":");
  if (idx >= 0) {
    const h = spec.slice(0, idx);
    out.tcpHost = h === "" ? "127.0.0.1" : h;
    out.tcpPort = Number(spec.slice(idx + 1));
  } else {
    out.tcpHost = "127.0.0.1";
    out.tcpPort = Number(spec);
  }
}

function parseLauncherArgs(argv: readonly string[]): LauncherArgs {
  const out: LauncherArgs = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--unix" && i + 1 < argv.length) {
      out.unixPath = argv[++i];
    } else if (a.startsWith("--unix=")) {
      out.unixPath = a.slice("--unix=".length);
    } else if (a === "--tcp" && i + 1 < argv.length) {
      parseTcpSpec(argv[++i], out);
    } else if (a.startsWith("--tcp=")) {
      parseTcpSpec(a.slice("--tcp=".length), out);
    } else if (a === "--idle-timeout" && i + 1 < argv.length) {
      out.idleTimeout = Number(argv[++i]);
    } else if (a.startsWith("--idle-timeout=")) {
      out.idleTimeout = Number(a.slice("--idle-timeout=".length));
    }
  }
  return out;
}

export interface WorkerConfig {
  functions?: VgiFunction[];
  catalog?: CatalogDescriptor;
  catalogInterface?: CatalogInterface;
  /** Factory that receives the built ReadOnlyCatalogInterface and returns a custom catalog. */
  catalogInterfaceFactory?: (base: ReadOnlyCatalogInterface) => CatalogInterface;
  catalogName?: string;
  /**
   * Registry to dispatch through. Pass the same instance used to build a
   * pre-constructed `catalogInterface` — catalogs record which schema (and
   * catalog) declares each function on the registry they are given, and
   * schema-qualified resolution only works if that is the registry the worker
   * dispatches on. Omit to have the worker create its own.
   */
  registry?: FunctionRegistry;
}

export class Worker {
  private _registry: FunctionRegistry;
  private _catalogInterface?: CatalogInterface;
  private _catalogName?: string;

  constructor(config: WorkerConfig) {
    // Reuse the caller's registry when given: a catalogInterface built
    // elsewhere recorded its per-schema function ownership there, and that
    // index is what makes a schema-qualified bind resolve correctly.
    this._registry = config.registry ?? new FunctionRegistry();
    this._catalogName = config.catalogName;

    // Register explicit functions
    if (config.functions) {
      for (const func of config.functions) {
        this._registry.register(func);
      }
    }

    // Register functions from catalog descriptor
    if (config.catalog) {
      for (const schema of config.catalog.schemas) {
        if (schema.functions) {
          for (const func of schema.functions) {
            this._registry.register(func);
            // Also index by owning schema + catalog so a schema-qualified bind
            // resolves when one name is declared in more than one schema.
            this._registry.registerInSchema(func, schema.name, config.catalog.name);
          }
        }
      }
    }

    // Set up catalog interface
    if (config.catalogInterface) {
      this._catalogInterface = config.catalogInterface;
    } else if (config.catalog) {
      const base = new ReadOnlyCatalogInterface(
        config.catalog,
        this._registry
      );
      this._catalogInterface = config.catalogInterfaceFactory
        ? config.catalogInterfaceFactory(base)
        : base;
    }
  }

  run(argv: readonly string[] = process.argv.slice(2)): void {
    process.stderr.write(`[worker] starting, pid=${process.pid}\n`);
    try {
      const protocol = this.buildProtocol();
      process.stderr.write(`[worker] protocol built\n`);

      const launcher = parseLauncherArgs(argv);
      if (launcher.unixPath !== undefined) {
        // VGI_WORKER_IDLE_TIMEOUT overrides whatever the C++ launcher
        // appended via --idle-timeout. Useful in test setups where the
        // launcher's default of 300 s leaves stale workers around long
        // after the suite has finished — set 5 (or any small value) and
        // the worker self-shuts-down promptly when the suite ends.
        const envOverride = process.env.VGI_WORKER_IDLE_TIMEOUT;
        const idleTimeout = envOverride !== undefined && envOverride !== ""
          ? Number(envOverride)
          : launcher.idleTimeout;
        process.stderr.write(
          `[worker] AF_UNIX mode: ${launcher.unixPath} idle=${idleTimeout ?? 300}s${envOverride !== undefined && envOverride !== "" ? " (env override)" : ""}\n`,
        );
        serveUnix(protocol, {
          unixPath: launcher.unixPath,
          idleTimeout,
        }).then((handle) => handle.done).then(() => {
          process.stderr.write(`[worker] serveUnix done (idle shutdown)\n`);
        }).catch((err: any) => {
          process.stderr.write(`Worker error: ${err.message}\n${err.stack}\n`);
          process.exit(1);
        });
        return;
      }

      if (launcher.tcpPort !== undefined) {
        // Raw Arrow-IPC framing over TCP — the network sibling of the
        // AF_UNIX launcher transport. serveTcp prints the
        // `TCP:<host>:<port>` discovery line itself (with the actual bound
        // port when tcpPort is 0). NO auth/TLS — loopback/trusted networks
        // only; use the HTTP transport for untrusted networks.
        const envOverride = process.env.VGI_WORKER_IDLE_TIMEOUT;
        const idleTimeout = envOverride !== undefined && envOverride !== ""
          ? Number(envOverride)
          : launcher.idleTimeout;
        process.stderr.write(
          `[worker] TCP mode: ${launcher.tcpHost}:${launcher.tcpPort} idle=${idleTimeout ?? 300}s${envOverride !== undefined && envOverride !== "" ? " (env override)" : ""}\n`,
        );
        serveTcp(protocol, {
          host: launcher.tcpHost,
          port: launcher.tcpPort,
          idleTimeout,
        }).then((handle) => handle.done).then(() => {
          process.stderr.write(`[worker] serveTcp done (idle shutdown)\n`);
        }).catch((err: any) => {
          process.stderr.write(`Worker error: ${err.message}\n${err.stack}\n`);
          process.exit(1);
        });
        return;
      }

      const server = new VgiRpcServer(protocol);
      process.stderr.write(`[worker] server created, calling run()\n`);
      server.run().then(() => {
        process.stderr.write(`[worker] server.run() resolved cleanly\n`);
      }).catch((err) => {
        process.stderr.write(`Worker error: ${err.message}\n${err.stack}\n`);
        process.exit(1);
      });
    } catch (err: any) {
      process.stderr.write(`Worker init error: ${err.message}\n${err.stack}\n`);
      process.exit(1);
    }
  }

  /** Build the VGI protocol from this worker's registry + catalog interface. */
  private buildProtocol(): Protocol {
    return buildVgiProtocol({
      registry: this._registry,
      catalogInterface: this._catalogInterface,
      catalogName: this._catalogName,
    });
  }

  /**
   * Serve this worker over a caller-provided byte-stream pair, instead of the
   * argv-selected stdio/unix/tcp transports `run()` uses. Resolves when the
   * `readable` ends.
   *
   * This is the seam for transports that aren't a process/socket — most notably
   * a **Web Worker**: bridge the worker's `MessagePort` to a Node `Duplex`
   * (readable side ← `port.on('message')`, writable side → `port.postMessage`)
   * and pass it as both arguments. The same code runs in a browser Web Worker.
   *
   * @param readable incoming request bytes (web `ReadableStream` or Node `Readable`).
   * @param writable outgoing response sink (a `net.Socket`/`Duplex`, or an fd
   *   number). Omit for the stdout fd.
   */
  async serveStream(
    readable: ReadableStream<Uint8Array> | NodeJS.ReadableStream,
    writable?: number | import("node:net").Socket | import("@query-farm/vgi-rpc").ByteSink,
  ): Promise<void> {
    await serveStreamRpc(this.buildProtocol(), { readable, writable });
  }
}
