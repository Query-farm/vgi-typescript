// TS port of vgi-python/vgi/examples/attach_options.py — a worker that
// declares attach-time options of many Arrow types and echoes them back
// via a table function. Exercises the attach-time options pipeline
// end-to-end (pre-attach discovery, per-option type validation done by
// the C++ extension, worker receipt, attach_id round-trip).
//
// Options are encoded into attach_id on attach, so they survive pooled
// worker reuse (subprocess) and stateless dispatch (HTTP) without any
// per-attach state on the catalog.
//
// Registered as bin/vgi-example-attach-options-worker. Matches the wire
// contract the extension's integration/attach/attach_options_echo.test
// expects.

import {
  Bool,
  Binary,
  Date_,
  DateUnit,
  DataType,
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
  Worker,
  type CatalogAttachResult,
  type CatalogDescriptor,
  type CatalogInfo,
  defineTableFunction,
  type TableProcessParams,
} from "../src/index.js";
import { FunctionRegistry } from "../src/functions/registry.js";
import type { OutputCollector } from "vgi-rpc";
import {
  type AttachOptionSpec,
  serializeAttachOptionSpecs,
} from "../src/catalog/attach-option.js";
import { batchFromColumns, deserializeBatch, serializeBatch } from "../src/util/arrow/index.js";

// ============================================================================
// Declared attach-time options (one per supported Arrow type family)
// ============================================================================

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

// Output schema of echo_attach_options: one column per declared option.
const ECHO_SCHEMA = new Schema(
  ATTACH_OPTION_SPECS.map((s) => new Field(s.name, s.type, true)),
);

// ============================================================================
// attach_id encoding / decoding
// ============================================================================

function randomUuidBytes(): Uint8Array {
  const buf = new Uint8Array(UUID_BYTES);
  crypto.getRandomValues(buf);
  return buf;
}

function buildEchoBatch(received: Record<string, unknown>): RecordBatch {
  // Merge received values with declared defaults so missing options fall
  // back — matches Python side `_build_echo_batch`.
  const columns: Record<string, unknown[]> = {};
  for (const spec of ATTACH_OPTION_SPECS) {
    const v = received[spec.name];
    columns[spec.name] = [v === undefined ? spec.default : v];
  }
  return batchFromColumns(columns, ECHO_SCHEMA);
}

function encodeAttachId(received: Record<string, unknown>): Uint8Array {
  const batch = buildEchoBatch(received);
  const ipc = serializeBatch(batch);
  const out = new Uint8Array(UUID_BYTES + 1 + ipc.byteLength);
  out.set(randomUuidBytes(), 0);
  out[UUID_BYTES] = ATTACH_ID_SEP;
  out.set(ipc, UUID_BYTES + 1);
  return out;
}

function decodeAttachId(attachId: Uint8Array): RecordBatch {
  if (attachId.byteLength <= UUID_BYTES + 1 || attachId[UUID_BYTES] !== ATTACH_ID_SEP) {
    throw new Error("attach_id does not carry an options payload");
  }
  const ipc = attachId.subarray(UUID_BYTES + 1);
  return deserializeBatch(ipc);
}

// ============================================================================
// echo_attach_options table function
// ============================================================================

interface EchoState { emitted: boolean }

const echo_attach_options = defineTableFunction<Record<string, never>, EchoState>({
  name: "echo_attach_options",
  description: "Echo the attach-time option values carried in attach_id",
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
    const attachId = params.initCall.bind_call.attach_id;
    if (!attachId) {
      throw new Error("echo_attach_options requires an attach_id");
    }
    const batch = decodeAttachId(attachId);
    out.emit(batch);
    state.emitted = true;
  },
});

// ============================================================================
// Catalog interface
// ============================================================================

// Extend ReadOnlyCatalogInterface so schemaContentsFunctions / schemaGet /
// tableGet / etc. all Just Work from the descriptor. We only need to
// override (a) catalogsInfo to advertise attach_option_specs, and
// (b) attach to encode received options into attach_id.
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
    return { ...base, attach_id: encodeAttachId(options ?? {}) };
  }
}

const descriptor: CatalogDescriptor = {
  name: CATALOG_NAME,
  defaultSchema: "main",
  schemas: [
    { name: "main", functions: [echo_attach_options] },
  ],
};

const registry = new FunctionRegistry();
registry.register(echo_attach_options);

const worker = new Worker({
  catalog: descriptor,
  catalogInterfaceFactory: () => new AttachOptionsCatalog(descriptor, registry),
});

worker.run();
