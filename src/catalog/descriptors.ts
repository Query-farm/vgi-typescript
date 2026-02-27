// Declarative catalog descriptor types.

import type { Schema } from "apache-arrow";
import type { VgiFunction } from "../functions/types.js";
import type { Arguments } from "../arguments/arguments.js";

export interface TableDescriptor {
  name: string;
  columns?: Schema;
  function?: VgiFunction;
  arguments?: Arguments;
  notNull?: string[];
  unique?: string[][];
  check?: string[];
  comment?: string;
  tags?: Record<string, string>;
}

export interface ViewDescriptor {
  name: string;
  definition: string;
  comment?: string;
  tags?: Record<string, string>;
}

export interface SchemaDescriptor {
  name: string;
  tables?: TableDescriptor[];
  views?: ViewDescriptor[];
  functions?: VgiFunction[];
  comment?: string;
  tags?: Record<string, string>;
}

export interface CatalogDescriptor {
  name: string;
  defaultSchema?: string;
  schemas: SchemaDescriptor[];
}
