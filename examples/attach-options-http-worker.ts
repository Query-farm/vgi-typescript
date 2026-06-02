// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// HTTP variant of the attach-options example worker. Same semantics as
// examples/attach-options-worker.ts (subprocess), just fronted by an HTTP
// server for integration tests over the HTTP transport.
// Prints PORT:<n> to stdout for test discovery.

import { createHttpHandler, unpackStateToken } from "@query-farm/vgi-rpc";
import type { OutputCollector } from "@query-farm/vgi-rpc";
import { arrowStateSerializer } from "../src/protocol/state-serializer.js";
import { FunctionRegistry } from "../src/functions/registry.js";
import { buildVgiProtocol } from "../src/protocol/dispatch.js";
import {
  Bool,
  Binary,
  Date_,
  DateUnit,
  Decimal,
  Field,
  Float32,
  Float64,
  Int8,
  Int16,
  Int32,
  Int64,
  List,
  RecordBatch,
  Schema,
  Struct,
  TimeUnit,
  Timestamp,
  Time,
  Uint8,
  Uint16,
  Uint32,
  Uint64,
  Utf8,
} from "@query-farm/apache-arrow";
import {
  ReadOnlyCatalogInterface,
  type CatalogAttachResult,
  type CatalogDescriptor,
  type CatalogInfo,
  defineTableFunction,
  type TableProcessParams,
} from "../src/index.js";
import {
  type AttachOptionSpec,
  serializeAttachOptionSpecs,
} from "../src/catalog/attach-option.js";
import { batchFromColumns, deserializeBatch, serializeBatch } from "../src/util/arrow/index.js";

// (same specs as attach-options-worker.ts — kept inline so both workers
// can be run independently without a shared fixture file)

const CATALOG_NAME = "attach_options";
const ATTACH_ID_SEP = 0x00;
const UUID_BYTES = 16;

const ATTACH_OPTION_SPECS: AttachOptionSpec[] = [
  { name: "opt_bool", description: "Boolean option", type: new Bool(), default: true },
  { name: "opt_int8", description: "int8", type: new Int8(), default: -8 },
  { name: "opt_int16", description: "int16", type: new Int16(), default: -16 },
  { name: "opt_int32", description: "int32", type: new Int32(), default: -32 },
  { name: "opt_int64", description: "int64", type: new Int64(), default: -64n },
  { name: "opt_uint8", description: "uint8", type: new Uint8(), default: 8 },
  { name: "opt_uint16", description: "uint16", type: new Uint16(), default: 16 },
  { name: "opt_uint32", description: "uint32", type: new Uint32(), default: 32 },
  { name: "opt_uint64", description: "uint64", type: new Uint64(), default: 64n },
  { name: "opt_float32", description: "float32", type: new Float32(), default: 1.5 },
  { name: "opt_float64", description: "float64", type: new Float64(), default: 2.5 },
  { name: "opt_string", description: "UTF-8 string", type: new Utf8(), default: "hello" },
  { name: "opt_blob", description: "Binary blob", type: new Binary(), default: new Uint8Array([0, 1, 2]) },
  { name: "opt_date", description: "Date", type: new Date_(DateUnit.DAY), default: null },
  { name: "opt_time", description: "Time of day", type: new Time(TimeUnit.MICROSECOND, 64), default: null },
  { name: "opt_timestamp", description: "Naive timestamp", type: new Timestamp(TimeUnit.MICROSECOND), default: null },
  { name: "opt_timestamp_tz", description: "Timestamp with UTC tz", type: new Timestamp(TimeUnit.MICROSECOND, "UTC"), default: null },
  { name: "opt_decimal", description: "Decimal(18,4)", type: new Decimal(4, 18, 128), default: null },
  {
    name: "opt_list",
    description: "List of int64",
    type: new List(new Field("item", new Int64(), true)),
    default: [1n, 2n, 3n],
  },
  {
    name: "opt_struct",
    description: "Struct",
    type: new Struct([new Field("a", new Int64(), true), new Field("b", new Utf8(), true)]),
    default: { a: 1n, b: "x" },
  },
];

const ECHO_SCHEMA = new Schema(
  ATTACH_OPTION_SPECS.map((s) => new Field(s.name, s.type, true)),
);

function randomUuidBytes(): Uint8Array {
  const b = new Uint8Array(UUID_BYTES);
  crypto.getRandomValues(b);
  return b;
}

function buildEchoBatch(received: Record<string, unknown>): RecordBatch {
  const columns: Record<string, unknown[]> = {};
  for (const spec of ATTACH_OPTION_SPECS) {
    const v = received[spec.name];
    columns[spec.name] = [v === undefined ? spec.default : v];
  }
  return batchFromColumns(columns, ECHO_SCHEMA);
}

function encodeAttachOpaqueData(received: Record<string, unknown>): Uint8Array {
  const ipc = serializeBatch(buildEchoBatch(received));
  const out = new Uint8Array(UUID_BYTES + 1 + ipc.byteLength);
  out.set(randomUuidBytes(), 0);
  out[UUID_BYTES] = ATTACH_ID_SEP;
  out.set(ipc, UUID_BYTES + 1);
  return out;
}

function decodeAttachOpaqueData(attachOpaqueData: Uint8Array): RecordBatch {
  if (attachOpaqueData.byteLength <= UUID_BYTES + 1 || attachOpaqueData[UUID_BYTES] !== ATTACH_ID_SEP) {
    throw new Error("attach_opaque_data does not carry an options payload");
  }
  return deserializeBatch(attachOpaqueData.subarray(UUID_BYTES + 1));
}

interface EchoState { emitted: boolean }

const echo_attach_options = defineTableFunction<Record<string, never>, EchoState>({
  name: "echo_attach_options",
  description: "Echo the attach-time option values carried in attach_opaque_data",
  categories: ["generator", "testing"],
  onBind: () => ({ outputSchema: ECHO_SCHEMA }),
  initialState: () => ({ emitted: false }),
  process: (
    params: TableProcessParams<Record<string, never>>,
    state: EchoState,
    out: OutputCollector,
  ) => {
    if (state.emitted) {
      out.finish();
      return;
    }
    const attachOpaqueData = params.initCall.bind_call.attach_opaque_data;
    if (!attachOpaqueData) {
      throw new Error("echo_attach_options requires an attach_opaque_data");
    }
    out.emit(decodeAttachOpaqueData(attachOpaqueData));
    state.emitted = true;
  },
});

class AttachOptionsCatalog extends ReadOnlyCatalogInterface {
  override catalogsInfo(): CatalogInfo[] {
    return [{
      name: CATALOG_NAME,
      implementation_version: null,
      data_version_spec: null,
      attach_option_specs: serializeAttachOptionSpecs(ATTACH_OPTION_SPECS),
    }];
  }
  override attach(
    name: string,
    options?: Record<string, unknown>,
    _dataVersionSpec?: string | null,
    _implementationVersion?: string | null,
  ): CatalogAttachResult {
    const base = super.attach(name, options);
    return { ...base, attach_opaque_data: encodeAttachOpaqueData(options ?? {}) };
  }
}

const descriptor: CatalogDescriptor = {
  name: CATALOG_NAME,
  defaultSchema: "main",
  schemas: [
    { name: "main", functions: [echo_attach_options] },
  ],
};

// ============================================================================
// HTTP server wiring
// ============================================================================

const registry = new FunctionRegistry();
registry.register(echo_attach_options);
const signingKey = crypto.getRandomValues(new Uint8Array(32));
const tokenTtl = 3600;

const protocol = buildVgiProtocol({
  registry,
  catalogInterface: new AttachOptionsCatalog(descriptor, registry),
  catalogName: CATALOG_NAME,
  recoverExchangeState: (opaqueData: Uint8Array) => {
    const tokenString = new TextDecoder().decode(opaqueData);
    const unpacked = unpackStateToken(tokenString, signingKey, tokenTtl);
    return arrowStateSerializer.deserialize(unpacked.stateBytes);
  },
});

const handler = createHttpHandler(protocol, {
  prefix: "/vgi",
  serverId: "vgi-example-attach-options-http",
  signingKey,
  tokenTtl,
  stateSerializer: arrowStateSerializer,
});

const server = Bun.serve({ port: 0, fetch: handler });
console.log(`PORT:${server.port}`);
