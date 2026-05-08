// VGI Worker: main entry point for running a VGI function server.

import { VgiRpcServer, serveUnix } from "vgi-rpc";
import { FunctionRegistry } from "./functions/registry.js";
import type { VgiFunction } from "./functions/types.js";
import { buildVgiProtocol } from "./protocol/dispatch.js";
import type { CatalogDescriptor } from "./catalog/descriptors.js";
import type { CatalogInterface } from "./catalog/interface.js";
import { ReadOnlyCatalogInterface } from "./catalog/read-only.js";

// argv parser for the launcher's `--unix PATH` / `--idle-timeout SEC`
// contract. The C++ launcher (vgi extension) appends these to the worker's
// argv whenever LOCATION uses the `launch:` scheme. Unknown args pass through.
interface LauncherArgs {
  unixPath?: string;
  idleTimeout?: number;
}

function parseLauncherArgs(argv: readonly string[]): LauncherArgs {
  const out: LauncherArgs = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--unix" && i + 1 < argv.length) {
      out.unixPath = argv[++i];
    } else if (a.startsWith("--unix=")) {
      out.unixPath = a.slice("--unix=".length);
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
}

export class Worker {
  private _registry: FunctionRegistry;
  private _catalogInterface?: CatalogInterface;
  private _catalogName?: string;

  constructor(config: WorkerConfig) {
    this._registry = new FunctionRegistry();
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
      const protocol = buildVgiProtocol({
        registry: this._registry,
        catalogInterface: this._catalogInterface,
        catalogName: this._catalogName,
      });
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
}
