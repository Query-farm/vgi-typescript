// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// Protocol 3.4 argument references and the raw-preserving bind call
// (vgi docs/large-values-3.4.md §2, §3.3, §3.4, §5a).
//
// - BindRequest decodes through the generated Nested<BindRequest> wrapper and
//   re-encodes as exactly the bytes received;
// - exactly one of `arguments` / `arguments_location` (`invalid_bind_request`);
// - resolution at the request's entry, with the spec's refusal kinds and the
//   prepared-statement message copied from vgi-python;
// - rule 11 (§3.4 test 4): opening the sealed attach / transaction in place
//   never changes what the embedding request re-encodes as;
// - the split-token fingerprint's tagged arguments input.

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import {
  ArgumentsRefIntegrityError,
  ArgumentsRefMissingError,
  AuthContext,
  InvalidArgumentsRefError,
} from "@query-farm/vgi-rpc";
import { Arguments } from "../../arguments/arguments.js";
import { deserializeBatch, schema, field, binary, serializeBatch } from "../../arrow/index.js";
import { FunctionType } from "../../types.js";
import { batchToScalarDict } from "../../util/arrow/index.js";
import {
  ARGUMENTS_REF_MISSING_MESSAGE,
  INVALID_BIND_REQUEST_MESSAGE,
  argumentsRefBytesFromEnv,
  argumentsRefHeaderValue,
  argumentsResolved,
  resolveBindCall,
} from "../arguments-ref.js";
import { sealAttach, sealTransaction } from "../handlers/catalog/shared.js";
import { openBindCallOpaque } from "../handlers/function.js";
import { serializeArguments } from "../serializers/arguments.js";
import { bindCallBytes, decodeBindCall, receivedBindCall, serializeBindRequest } from "../serializers/bind.js";
import { deserializeInitRequest, serializeInitRequest } from "../serializers/init.js";
import { fingerprintInputs } from "../serializers/splits.js";
import type { BindRequest, InitRequest } from "../types.js";

const corpus = new URL("../../../conformance/nested-binary/", import.meta.url);
const file = (name: string): Uint8Array => new Uint8Array(readFileSync(new URL(name, corpus)));

/** A one-row record batch's IPC bytes -> the record's column dict. */
const dictOf = (bytes: Uint8Array): Record<string, any> => batchToScalarDict(deserializeBatch(bytes));

const SHA = "ab".repeat(32);

function bindCall(overrides: Partial<BindRequest> = {}): BindRequest {
  return {
    function_name: "blob_param_echo",
    arguments: new Arguments(["hello"]),
    function_type: FunctionType.TABLE,
    input_schema: null,
    settings: null,
    secrets: null,
    attach_opaque_data: null,
    transaction_opaque_data: null,
    resolved_secrets_provided: false,
    schema_path: ["main"],
    ...overrides,
  };
}

/** The bind call as a client sends it with its arguments uploaded: arguments null, location set. */
function byReference(ref = new Uint8Array([9, 9, 9]), size = 4096, sha256 = SHA): Uint8Array {
  return serializeBatch(serializeBindRequest(bindCall({ arguments_location: { ref, size, sha256 } })));
}

function initRequest(bind: BindRequest): InitRequest {
  return {
    bind_call: bind,
    output_schema: schema([field("v", binary(), true)]),
    bind_opaque_data: null,
    projection_ids: null,
    pushdown_filters: null,
    join_keys: [],
    split_tokens: null,
    row_limit: null,
    phase: null,
    finalize_state_id: null,
    execution_id: null,
    init_opaque_data: null,
    substream_id: null,
    order_by_column_name: null,
    order_by_direction: null,
    order_by_null_order: null,
    order_by_limit: null,
    tablesample_percentage: null,
    tablesample_seed: null,
  };
}

async function refusal(fn: () => unknown): Promise<any> {
  try {
    await fn();
  } catch (error) {
    return error;
  }
  throw new Error("expected a refusal");
}

describe("exactly one of arguments / arguments_location (spec §2, §3.4 test 3)", () => {
  test("arguments = null with a location decodes, its arguments pending resolution", () => {
    const bytes = file("bind-arguments-null.arrow");
    const decoded = decodeBindCall(bytes);
    expect(decoded.arguments_location).not.toBeNull();
    expect(decoded.arguments_location!.size).toBeGreaterThan(0);
    expect(decoded.arguments_location!.sha256).toMatch(/^[0-9a-f]{64}$/);
    expect(argumentsResolved(decoded)).toBe(false);
    // Never silently zero arguments: every read refuses until resolved.
    expect(() => decoded.arguments.get(0)).toThrow(/not resolved/);
    expect(() => decoded.arguments.positional).toThrow(/not resolved/);
    expect(bindCallBytes(decoded)).toEqual(bytes);
  });

  for (const name of ["bind-both-null.arrow", "bind-both-set.arrow"]) {
    test(`${name} is refused with invalid_bind_request`, async () => {
      const error = await refusal(() => decodeBindCall(file(name)));
      expect(error.errorKind).toBe("invalid_bind_request");
      expect(error.errorCode).toBe("INVALID_ARGUMENT");
      expect(error.message).toBe(INVALID_BIND_REQUEST_MESSAGE);
    });
  }

  test("a malformed location is invalid_arguments_ref", async () => {
    const error = await refusal(() => decodeBindCall(byReference(new Uint8Array([1]), 10, "XYZ")));
    expect(error.errorKind).toBe("invalid_arguments_ref");
    expect(error.message).toStartWith("invalid_arguments_ref: ");
  });
});

describe("resolution at the request's entry (spec §5a)", () => {
  const argumentsIpc = serializeArguments(new Arguments(["hello", 42n]));

  test("resolves through the context, in place, and still re-encodes as the reference", async () => {
    const bytes = byReference(new Uint8Array([7, 7]), argumentsIpc.byteLength, SHA);
    const decoded = decodeBindCall(bytes);
    const seen: Array<{ ref: Uint8Array; size: number; sha256: string }> = [];
    await resolveBindCall(decoded, {
      resolveArgumentRef: async (ref, location) => {
        seen.push({ ref, ...location });
        return argumentsIpc;
      },
    });
    expect(seen).toEqual([{ ref: new Uint8Array([7, 7]), size: argumentsIpc.byteLength, sha256: SHA }]);
    expect(argumentsResolved(decoded)).toBe(true);
    expect(decoded.arguments.get(0)).toBe("hello");
    expect(decoded.arguments_location).not.toBeNull();
    // The resolved value never reaches the wire: the bind call, and any
    // request embedding it, re-encode as the received reference.
    expect(bindCallBytes(decoded)).toEqual(bytes);
    const init = dictOf(serializeBatch(serializeInitRequest(initRequest(decoded))));
    expect(new Uint8Array(init.bind_call)).toEqual(bytes);
    // Resolving again is a no-op (no second fetch).
    await resolveBindCall(decoded, { resolveArgumentRef: async () => { throw new Error("fetched twice"); } });
  });

  test("a copy built from fields re-serializes as arguments_location, never the value", async () => {
    const decoded = decodeBindCall(byReference(new Uint8Array([7]), argumentsIpc.byteLength));
    await resolveBindCall(decoded, { resolveArgumentRef: async () => argumentsIpc });
    const copy = { ...decoded, attach_opaque_data: null };
    expect(receivedBindCall(copy)).toBeUndefined();
    const wire = dictOf(serializeBatch(serializeBindRequest(copy)));
    expect(wire.arguments).toBeNull();
    expect(wire.arguments_location).not.toBeNull();
  });

  test("an object storage reports gone is arguments_ref_missing with the prepared-statement message", async () => {
    const decoded = decodeBindCall(byReference());
    const error = await refusal(() =>
      resolveBindCall(decoded, {
        resolveArgumentRef: async () => {
          throw new ArgumentsRefMissingError("the uploaded argument object is gone (expired or deleted)");
        },
      }),
    );
    expect(error.errorKind).toBe("arguments_ref_missing");
    expect(error.errorCode).toBe("NOT_FOUND");
    expect(error.message).toBe(ARGUMENTS_REF_MISSING_MESSAGE);
    expect(ARGUMENTS_REF_MISSING_MESSAGE).toBe(
      "this prepared statement's large argument has expired; prepare it again",
    );
  });

  test("invalid and integrity refusals keep their kind and name it in the message", async () => {
    const invalid = await refusal(() =>
      resolveBindCall(decodeBindCall(byReference()), {
        resolveArgumentRef: async () => {
          throw new InvalidArgumentsRefError("argument reference does not open");
        },
      }),
    );
    expect(invalid.errorKind).toBe("invalid_arguments_ref");
    expect(invalid.errorCode).toBe("INVALID_ARGUMENT");
    expect(invalid.message).toBe("invalid_arguments_ref: argument reference does not open");

    const integrity = await refusal(() =>
      resolveBindCall(decodeBindCall(byReference()), {
        resolveArgumentRef: async () => {
          throw new ArgumentsRefIntegrityError("argument object sha256 mismatch");
        },
      }),
    );
    expect(integrity.errorKind).toBe("arguments_ref_integrity");
    expect(integrity.errorCode).toBe("DATA_LOSS");
    expect(integrity.message).toStartWith("arguments_ref_integrity: ");
  });

  test("no context to resolve with is invalid_arguments_ref", async () => {
    const error = await refusal(() => resolveBindCall(decodeBindCall(byReference()), undefined));
    expect(error.errorKind).toBe("invalid_arguments_ref");
  });
});

describe("rule 11: opened opaque values live beside the wrapper (spec §3.4 test 4)", () => {
  test("re-encoding after the entry's open emits the received bind_call bytes", async () => {
    const signingKey = new Uint8Array(32).fill(5);
    const auth = AuthContext.anonymous();
    const attach = await sealAttach(new TextEncoder().encode("catalog-bytes"), auth, signingKey);
    const txn = await sealTransaction(new TextEncoder().encode("txn-7"), attach, auth, signingKey);
    const received = serializeBatch(
      serializeBindRequest(bindCall({ attach_opaque_data: attach, transaction_opaque_data: txn })),
    );
    const embedding = serializeBatch(
      serializeInitRequest(initRequest(decodeBindCall(received))),
    );

    // The request's entry: decode the embedding request, open in place.
    const request = deserializeInitRequest(dictOf(embedding));
    await openBindCallOpaque(request.bind_call, auth, signingKey);
    expect(new TextDecoder().decode(request.bind_call.attach_opaque_data!)).toBe("catalog-bytes");
    expect(new TextDecoder().decode(request.bind_call.transaction_opaque_data!)).toBe("txn-7");

    // Re-encode: the sealed envelopes go back out, never the plaintext.
    const reencoded = dictOf(serializeBatch(serializeInitRequest(request)));
    expect(new Uint8Array(reencoded.bind_call)).toEqual(received);
    expect(bindCallBytes(request.bind_call)).toEqual(received);
  });

  test("the noncanonical corpus bind call survives decode, open and re-encode byte for byte", async () => {
    const embedding = file("init-noncanonical.arrow");
    const expected = file("bind_call.bin");
    const request = deserializeInitRequest(dictOf(embedding));
    // No signing key: the open is the subprocess pass-through, still in place.
    await openBindCallOpaque(request.bind_call, undefined, undefined);
    const reencoded = dictOf(serializeBatch(serializeInitRequest(request)));
    expect(new Uint8Array(reencoded.bind_call)).toEqual(expected);
  });
});

describe("split-token fingerprint (spec §5a)", () => {
  test("a location contributes size and sha256, never the sealed ref", () => {
    const a = fingerprintInputs(byReference(new Uint8Array([1, 2, 3]), 4096, SHA));
    const b = fingerprintInputs(byReference(new Uint8Array([4, 5, 6, 7]), 4096, SHA));
    expect(a.args).toEqual(b.args);
    expect(a.args[0]).toBe(0x01);
    expect(a.args.byteLength).toBe(1 + 8 + 32);
    const otherSha = fingerprintInputs(byReference(new Uint8Array([1, 2, 3]), 4096, "cd".repeat(32)));
    expect(otherSha.args).not.toEqual(a.args);
    const otherSize = fingerprintInputs(byReference(new Uint8Array([1, 2, 3]), 4097, SHA));
    expect(otherSize.args).not.toEqual(a.args);
  });

  test("inline arguments are tagged apart from a location", () => {
    const inline = fingerprintInputs(serializeBatch(serializeBindRequest(bindCall())));
    expect(inline.args[0]).toBe(0x00);
    const ipc = serializeArguments(new Arguments(["hello"]));
    expect(inline.args.subarray(9)).toEqual(ipc);
  });
});

describe("VGI-Arguments-Ref-Bytes (spec §5)", () => {
  const objectStore = { putObject: async () => "id", get: async () => new Uint8Array(0), upload: async () => "" };
  const provider = { generateUploadUrl: () => ({ uploadUrl: "", downloadUrl: "", expiresAt: new Date() }) };

  test("advertised (4 MiB by default) when the server vends upload URLs over an ObjectStore", () => {
    expect(argumentsRefHeaderValue({ uploadUrlProvider: provider, externalLocation: { storage: objectStore }, env: {} }))
      .toBe(4 * 1024 * 1024);
    expect(
      argumentsRefHeaderValue({
        uploadUrlProvider: provider,
        externalLocation: { storage: objectStore },
        env: { VGI_ARGUMENTS_REF_BYTES: "1000" },
      }),
    ).toBe(1000);
  });

  test("never advertised without upload URLs, without an ObjectStore, or with the test switch", () => {
    expect(argumentsRefHeaderValue({ externalLocation: { storage: objectStore }, env: {} })).toBeNull();
    expect(
      argumentsRefHeaderValue({ uploadUrlProvider: provider, externalLocation: { storage: { upload: async () => "" } }, env: {} }),
    ).toBeNull();
    expect(
      argumentsRefHeaderValue({
        uploadUrlProvider: provider,
        externalLocation: { storage: objectStore },
        env: { VGI_TEST_DISABLE_ARGUMENTS_REF: "1" },
      }),
    ).toBeNull();
  });

  test("a malformed VGI_ARGUMENTS_REF_BYTES stops startup", () => {
    for (const bad of ["0", "-1", "012", "1e6", "abc"]) {
      expect(() => argumentsRefBytesFromEnv({ VGI_ARGUMENTS_REF_BYTES: bad })).toThrow(/VGI_ARGUMENTS_REF_BYTES/);
    }
  });
});
