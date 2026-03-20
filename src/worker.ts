// VGI Worker: main entry point for running a VGI function server.

import { VgiRpcServer } from "vgi-rpc";
import { FunctionRegistry } from "./functions/registry.js";
import type { VgiFunction } from "./functions/types.js";
import { buildVgiProtocol } from "./protocol/dispatch.js";
import type { CatalogDescriptor } from "./catalog/descriptors.js";
import type { CatalogInterface } from "./catalog/interface.js";
import { ReadOnlyCatalogInterface } from "./catalog/read-only.js";

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

  run(): void {
    process.stderr.write(`[worker] starting, pid=${process.pid}\n`);
    try {
      const protocol = buildVgiProtocol({
        registry: this._registry,
        catalogInterface: this._catalogInterface,
        catalogName: this._catalogName,
      });
      process.stderr.write(`[worker] protocol built\n`);

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
