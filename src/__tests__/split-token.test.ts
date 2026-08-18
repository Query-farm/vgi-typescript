// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// Cross-SDK conformance for the split-token envelope (src/split-token.ts).
//
// The envelope is the one part of the splits change where five independent
// implementations can silently diverge AND where diverging is a vulnerability.
// Behavioural tests miss that: each SDK is self-consistent, so a disagreement on
// anchor_len endianness or fingerprint truncation only surfaces when a token
// crosses SDKs. These vectors are byte-level and shared — every SDK parses them
// and reproduces the deterministic ones byte-for-byte.

import { describe, test, expect } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { AuthContext } from "@query-farm/vgi-rpc";
import {
  buildSplitToken,
  openSplitToken,
  SplitTokenError,
  SPLIT_SNAPSHOT_EXPIRED,
  SPLIT_TOKEN_INVALID,
  SPLIT_TOKEN_FORMAT_VERSION,
} from "../split-token.js";

const FIXTURES = join(import.meta.dir, "data", "split_tokens");

interface Manifest {
  format_version: number;
  key_hex: string;
  fingerprint_hex: string;
  anchor_hex: string;
  payload: string;
  cases: Array<{
    name: string;
    verdict: string;
    note: string;
    reproducible: boolean;
    worker_keyed: boolean;
  }>;
}

const manifest: Manifest = JSON.parse(readFileSync(join(FIXTURES, "manifest.json"), "utf8"));

function fromHex(s: string): Uint8Array {
  const out = new Uint8Array(s.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(s.slice(i * 2, i * 2 + 2), 16);
  return out;
}

function vector(name: string): Uint8Array {
  return new Uint8Array(readFileSync(join(FIXTURES, `${name}.bin`)));
}

function auth(domain: string, principal: string): AuthContext {
  return { domain, principal, authenticated: principal !== "" } as AuthContext;
}

const KEY = fromHex(manifest.key_hex);
const FINGERPRINT = fromHex(manifest.fingerprint_hex);
const ANCHOR = fromHex(manifest.anchor_hex);
const PAYLOAD = new TextEncoder().encode(manifest.payload);

describe("shared cross-SDK vectors", () => {
  test("the fixture set targets this SDK's format version", () => {
    expect(manifest.format_version).toBe(SPLIT_TOKEN_FORMAT_VERSION);
  });

  for (const c of manifest.cases) {
    test(`${c.name} reaches its recorded verdict`, async () => {
      // The manifest states the key state rather than each SDK inferring it:
      // the alg:none vector is a structurally VALID unsealed token whose whole
      // point is that a KEYED worker refuses it, so guessing from the token
      // would test the opposite of the rule.
      const opts = {
        signingKey: c.worker_keyed ? KEY : undefined,
        expectedFingerprint: FINGERPRINT,
        currentAnchor: ANCHOR,
      };

      if (c.verdict === "ok") {
        expect(await openSplitToken(vector(c.name), opts)).toEqual(PAYLOAD);
        return;
      }

      let err: unknown;
      try {
        await openSplitToken(vector(c.name), opts);
      } catch (e) {
        err = e;
      }
      expect(err, `${c.name} was ACCEPTED (${c.note})`).toBeInstanceOf(SplitTokenError);
      expect((err as SplitTokenError).kind, c.note).toBe(c.verdict);
    });
  }

  test("the deterministic vector is reproduced byte-for-byte", async () => {
    // Proves the STAMPING side agrees too: a parser can be permissive enough to
    // accept another SDK's bytes while emitting bytes that SDK would reject.
    // Only the unsealed vector applies — a sealed token carries a random nonce.
    const got = await buildSplitToken({
      payload: PAYLOAD,
      fingerprint: FINGERPRINT,
      anchor: ANCHOR,
    });
    expect(got).toEqual(vector("valid_unsealed"));
  });
});

describe("the alg:none refusal", () => {
  // Stated directly rather than via a fixture name, because it is the rule most
  // likely to be "simplified" away by a later reader: flags is
  // attacker-controlled plaintext, so a parser that trusts bit 0 lets any caller
  // forge a split against a fully-keyed worker.
  const key = new Uint8Array(32).fill(0x2a);
  const fingerprint = new Uint8Array(16).fill(0x07);
  const anchor = new Uint8Array(8);

  test("a keyed worker refuses an unsealed token", async () => {
    const forged = await buildSplitToken({
      payload: new TextEncoder().encode("file=evil"),
      fingerprint,
      anchor,
    });
    expect(openSplitToken(forged, { signingKey: key, expectedFingerprint: fingerprint })).rejects.toThrow(
      SplitTokenError,
    );
  });

  test("a keyless worker cannot open a sealed token", async () => {
    const sealed = await buildSplitToken({
      payload: new TextEncoder().encode("file=ok"),
      fingerprint,
      anchor,
      signingKey: key,
    });
    expect(openSplitToken(sealed, { expectedFingerprint: fingerprint })).rejects.toThrow(SplitTokenError);
  });

  test("the seal round-trips, and a wrong key does not", async () => {
    const sealed = await buildSplitToken({
      payload: new TextEncoder().encode("file=ok"),
      fingerprint,
      anchor,
      signingKey: key,
    });
    expect(await openSplitToken(sealed, { signingKey: key, expectedFingerprint: fingerprint })).toEqual(
      new TextEncoder().encode("file=ok"),
    );
    const wrong = new Uint8Array(32).fill(0x2b);
    expect(
      openSplitToken(sealed, { signingKey: wrong, expectedFingerprint: fingerprint }),
    ).rejects.toThrow(SplitTokenError);
  });
});

test("a token is bound to the principal it was minted for", async () => {
  // Dropping this while keeping it on attach would be a regression, and a split
  // token names data (files, offsets, tenant partitions).
  const key = new Uint8Array(32).fill(0x11);
  const fingerprint = new Uint8Array(16).fill(0x05);
  const anchor = new Uint8Array(8);
  const alice = auth("test", "alice");
  const bob = auth("test", "bob");

  const token = await buildSplitToken({
    payload: new TextEncoder().encode("tenant=alice"),
    fingerprint,
    anchor,
    signingKey: key,
    auth: alice,
  });
  expect(await openSplitToken(token, { signingKey: key, auth: alice })).toEqual(
    new TextEncoder().encode("tenant=alice"),
  );
  expect(openSplitToken(token, { signingKey: key, auth: bob })).rejects.toThrow(SplitTokenError);
});

test("expiry and invalidity are distinguishable", async () => {
  // Only one of them means "re-run the query", and keeping the anchor in the
  // PLAINTEXT header is what makes the distinction expressible at all — inside
  // the AAD both would collapse into one tag-check failure.
  const fingerprint = new Uint8Array(16).fill(0x09);
  const old = new Uint8Array(8);
  old[0] = 47;
  const current = new Uint8Array(8);
  current[0] = 48;

  const token = await buildSplitToken({
    payload: new TextEncoder().encode("file=1"),
    fingerprint,
    anchor: old,
  });

  try {
    await openSplitToken(token, { expectedFingerprint: fingerprint, currentAnchor: current });
    throw new Error("stale anchor was accepted");
  } catch (e) {
    expect((e as SplitTokenError).kind).toBe(SPLIT_SNAPSHOT_EXPIRED);
  }

  try {
    await openSplitToken(token, {
      expectedFingerprint: new Uint8Array(16).fill(0x0a),
      currentAnchor: old,
    });
    throw new Error("wrong bind was accepted");
  } catch (e) {
    expect((e as SplitTokenError).kind).toBe(SPLIT_TOKEN_INVALID);
  }
});
