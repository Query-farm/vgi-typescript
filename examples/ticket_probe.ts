// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// `ticket_probe`: the cross-SDK fixture catalog for attach tickets.
//
// TypeScript port of vgi-python's `vgi/_test_fixtures/ticket_probe.py`. Every
// SDK's fixture worker serves this catalog identically, and the extension's
// `attach_ticket/*.test` sqllogictests run against each of them. The contract
// (vgi-python `docs/protocol/vgi-attach-tickets.md`, "Fixture catalog"):
//
// - Catalog `ticket_probe`, default schema `main`.
// - Attach options, in this order: `region` (VARCHAR, not required, not
//   secret, default 'us-east-1'), then `api_key` (VARCHAR, required, secret,
//   no default).
// - Table `main.probe`, backed by the table function `main.ticket_probe` (no
//   arguments): exactly one row, `region VARCHAR` (the attached value, or the
//   default) and `api_key_sha256 VARCHAR` (the first 12 lowercase hex of
//   SHA-256(UTF-8(api_key))). The key itself is never returned.
//
// So a reattach with nothing but `vgi_attach_ticket` reading the same row
// proves the secret option took effect without travelling again.

import { createHash } from "node:crypto";
import { Field, Schema, Utf8 } from "@query-farm/apache-arrow";
import {
  batchFromColumns,
  type CatalogAttachResult,
  type CatalogDescriptor,
  type CatalogInfo,
  defineTableFunction,
  type FunctionRegistry,
  ReadOnlyCatalogInterface,
  type VgiFunction,
} from "../src/index.js";
import { type AttachOptionSpec, serializeAttachOptionSpecs } from "../src/catalog/attach-option.js";

export const TICKET_PROBE_CATALOG = "ticket_probe";
const DEFAULT_REGION = "us-east-1";

const PROBE_SCHEMA = new Schema([
  new Field("region", new Utf8(), true),
  new Field("api_key_sha256", new Utf8(), true),
]);

/** The first 12 lowercase hex characters of SHA-256(UTF-8(apiKey)). */
export function apiKeyDigest(apiKey: string): string {
  return createHash("sha256").update(apiKey, "utf8").digest("hex").slice(0, 12);
}

const ATTACH_OPTION_SPECS: AttachOptionSpec[] = [
  { name: "region", description: "Region the probe reports back", type: new Utf8(), default: DEFAULT_REGION },
  {
    name: "api_key",
    description: "API key; only its digest is ever returned",
    type: new Utf8(),
    required: true,
    secret: true,
  },
];

// attach_opaque_data layout: byte 0 is left for the CompositeCatalogInterface
// route byte (it overwrites byte 0 of whatever a backend returns), then
// UTF-8(region) 0x00 UTF-8(digest).
const UTF8 = new TextEncoder();
const UTF8_DECODE = new TextDecoder();

function encodeAttach(region: string, digest: string): Uint8Array {
  const r = UTF8.encode(region);
  const d = UTF8.encode(digest);
  const out = new Uint8Array(1 + r.length + 1 + d.length);
  out.set(r, 1);
  out[1 + r.length] = 0;
  out.set(d, 2 + r.length);
  return out;
}

const ticket_probe = defineTableFunction<Record<string, never>, { emitted: boolean }>({
  name: "ticket_probe",
  description: "Report the attach options of this ticket_probe attach (the api_key only as a digest)",
  categories: ["generator", "testing"],
  onBind: () => ({ outputSchema: PROBE_SCHEMA }),
  initialState: () => ({ emitted: false }),
  process: (params, state, out) => {
    if (state.emitted) {
      out.finish();
      return;
    }
    const raw = params.initCall.bind_call.attach_opaque_data as Uint8Array | null | undefined;
    const sep = raw ? raw.indexOf(0, 1) : -1;
    if (!raw || sep < 0) {
      throw new Error("ticket_probe must be read through an attach of the ticket_probe catalog");
    }
    const region = UTF8_DECODE.decode(raw.subarray(1, sep));
    const digest = UTF8_DECODE.decode(raw.subarray(sep + 1));
    out.emit(batchFromColumns({ region: [region], api_key_sha256: [digest] }, params.outputSchema));
    state.emitted = true;
  },
});

export const ticketProbeFunctions: VgiFunction[] = [ticket_probe];

export const ticketProbeDescriptor: CatalogDescriptor = {
  name: TICKET_PROBE_CATALOG,
  defaultSchema: "main",
  comment: "Attach-ticket probe: one plain and one secret attach option",
  schemas: [
    {
      name: "main",
      tables: [
        {
          name: "probe",
          columns: PROBE_SCHEMA,
          function: ticket_probe,
          comment: "The options this attach was made with",
        },
      ],
      functions: [ticket_probe],
    },
  ],
};

/** Validates the options, then carries `region` and the key digest in the attach. */
export class TicketProbeCatalog extends ReadOnlyCatalogInterface {
  override attachOptionSpecs(_name: string): AttachOptionSpec[] {
    return ATTACH_OPTION_SPECS;
  }

  override catalogsInfo(): CatalogInfo[] {
    return super.catalogsInfo().map((info) => ({
      ...info,
      attach_option_specs: serializeAttachOptionSpecs(ATTACH_OPTION_SPECS),
    }));
  }

  override async attach(
    name: string,
    options?: Record<string, unknown>,
    dataVersionSpec?: string | null,
    implementationVersion?: string | null,
  ): Promise<CatalogAttachResult> {
    // super.attach() refuses a missing `api_key` via attachOptionSpecs().
    const base = await super.attach(name, options, dataVersionSpec, implementationVersion);
    const lowered: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(options ?? {})) lowered[k.toLowerCase()] = v;
    const region = String(lowered.region ?? "") || DEFAULT_REGION;
    const digest = apiKeyDigest(String(lowered.api_key));
    return { ...base, attach_opaque_data: encodeAttach(region, digest) };
  }
}

/** Build the catalog over `registry` (which must have {@link ticketProbeFunctions} registered). */
export function createTicketProbeCatalog(registry: FunctionRegistry): TicketProbeCatalog {
  return new TicketProbeCatalog(ticketProbeDescriptor, registry);
}
