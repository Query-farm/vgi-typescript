// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Tests for the AEAD opaque-data envelope primitive (src/crypto.ts).

import { describe, test, expect } from "bun:test";
import type { AuthContext } from "@query-farm/vgi-rpc";
import {
  sealBytes,
  openBytes,
  attachAad,
  transactionAad,
  ATTACH_ENVELOPE_VERSION,
  TRANSACTION_ENVELOPE_VERSION,
  OpaqueDataRejectedError,
} from "../crypto.js";

const KEY = new TextEncoder().encode("operator-supplied-signing-key");

function auth(domain: string, principal: string): AuthContext {
  return { domain, principal, authenticated: principal !== "" } as AuthContext;
}

const PAYLOAD = new TextEncoder().encode("readonly-catalog-");

describe("crypto: AEAD opaque-data envelopes", () => {
  test("seal/open round-trips under the same identity", async () => {
    const aad = attachAad(auth("test", "alice"));
    const token = await sealBytes(PAYLOAD, KEY, aad, ATTACH_ENVELOPE_VERSION);
    expect(token.length).toBeGreaterThan(PAYLOAD.length); // version + nonce + tag overhead
    const opened = await openBytes(token, KEY, aad, ATTACH_ENVELOPE_VERSION);
    expect(new TextDecoder().decode(opened)).toBe("readonly-catalog-");
  });

  test("rejects a different principal", async () => {
    const token = await sealBytes(PAYLOAD, KEY, attachAad(auth("test", "alice")), ATTACH_ENVELOPE_VERSION);
    await expect(
      openBytes(token, KEY, attachAad(auth("test", "bob")), ATTACH_ENVELOPE_VERSION),
    ).rejects.toBeInstanceOf(OpaqueDataRejectedError);
  });

  test("rejects a different auth domain", async () => {
    const token = await sealBytes(PAYLOAD, KEY, attachAad(auth("idp-a", "alice")), ATTACH_ENVELOPE_VERSION);
    await expect(
      openBytes(token, KEY, attachAad(auth("idp-b", "alice")), ATTACH_ENVELOPE_VERSION),
    ).rejects.toBeInstanceOf(OpaqueDataRejectedError);
  });

  test("rejects an anonymous replay of an authenticated envelope", async () => {
    const token = await sealBytes(PAYLOAD, KEY, attachAad(auth("test", "alice")), ATTACH_ENVELOPE_VERSION);
    await expect(
      openBytes(token, KEY, attachAad(undefined), ATTACH_ENVELOPE_VERSION),
    ).rejects.toBeInstanceOf(OpaqueDataRejectedError);
  });

  test("rejects the wrong key", async () => {
    const aad = attachAad(auth("test", "alice"));
    const token = await sealBytes(PAYLOAD, new TextEncoder().encode("key-a"), aad, ATTACH_ENVELOPE_VERSION);
    await expect(
      openBytes(token, new TextEncoder().encode("key-b"), aad, ATTACH_ENVELOPE_VERSION),
    ).rejects.toBeInstanceOf(OpaqueDataRejectedError);
  });

  test("rejects a tampered, malformed, or wrong-version token", async () => {
    const aad = attachAad(auth("test", "alice"));
    const token = await sealBytes(PAYLOAD, KEY, aad, ATTACH_ENVELOPE_VERSION);
    const tampered = Uint8Array.from(token);
    tampered[tampered.length - 1] ^= 0x01;
    await expect(openBytes(tampered, KEY, aad, ATTACH_ENVELOPE_VERSION)).rejects.toBeInstanceOf(
      OpaqueDataRejectedError,
    );
    await expect(
      openBytes(new TextEncoder().encode("garbage"), KEY, aad, ATTACH_ENVELOPE_VERSION),
    ).rejects.toBeInstanceOf(OpaqueDataRejectedError);
    await expect(openBytes(token, KEY, aad, ATTACH_ENVELOPE_VERSION + 1)).rejects.toBeInstanceOf(
      OpaqueDataRejectedError,
    );
  });

  test("transaction envelope is bound to its parent attach", async () => {
    const a = auth("test", "alice");
    const attachA = new TextEncoder().encode("attach-envelope-A");
    const attachB = new TextEncoder().encode("attach-envelope-B");
    const token = await sealBytes(
      new TextEncoder().encode("tx"),
      KEY,
      transactionAad(a, attachA),
      TRANSACTION_ENVELOPE_VERSION,
    );
    // Correct parent attach: opens.
    const opened = await openBytes(token, KEY, transactionAad(a, attachA), TRANSACTION_ENVELOPE_VERSION);
    expect(new TextDecoder().decode(opened)).toBe("tx");
    // Same principal, different parent attach: rejected.
    await expect(
      openBytes(token, KEY, transactionAad(a, attachB), TRANSACTION_ENVELOPE_VERSION),
    ).rejects.toBeInstanceOf(OpaqueDataRejectedError);
  });

  test("a non-32-byte key is normalized (SHA-256) and interoperates", async () => {
    const shortKey = new TextEncoder().encode("short");
    const aad = attachAad(auth("test", "alice"));
    const token = await sealBytes(PAYLOAD, shortKey, aad, ATTACH_ENVELOPE_VERSION);
    const opened = await openBytes(token, shortKey, aad, ATTACH_ENVELOPE_VERSION);
    expect(new TextDecoder().decode(opened)).toBe("readonly-catalog-");
  });
});
