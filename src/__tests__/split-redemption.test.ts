// Copyright 2025, 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0

// A split token must be REDEEMED, not merely minted.
//
// Minting alone is the dangerous half-implementation: the worker advertises
// supports_splits, the client plans N splits and issues N inits each carrying
// one token, and a worker that ignores those tokens runs its whole scan N times.
// The query then answers N× its real row count, silently — the exact failure
// class splits exist to prevent, produced by the mechanism meant to prevent it.

import { describe, test, expect } from "bun:test";
import {
  buildSplitToken,
  openSplitToken,
  bindFingerprint,
  splitAnchor,
} from "../split-token.js";

describe("split redemption", () => {
  test("a stamped token round-trips back to the worker's own payload", async () => {
    const fingerprint = await bindFingerprint("main", "split_seq", new Uint8Array([1, 2]), new Uint8Array(0), new Uint8Array(0));
    const anchor = splitAnchor(47);
    const payload = new TextEncoder().encode("rows=0..250");

    const token = await buildSplitToken({ payload, fingerprint, anchor });
    const opened = await openSplitToken(token, { expectedFingerprint: fingerprint, currentAnchor: anchor });

    expect(opened).toEqual(payload);
  });

  test("each split's payload survives independently", async () => {
    // The redemption path handles a LIST of tokens, because an engine whose
    // partition count is its concurrency bin-packs and reads a whole group.
    const fingerprint = await bindFingerprint("main", "split_seq", new Uint8Array(), new Uint8Array(), new Uint8Array());
    const anchor = splitAnchor(1);
    const payloads = [0, 1, 2].map((i) => new TextEncoder().encode(`slice-${i}`));

    const tokens = await Promise.all(
      payloads.map((payload) => buildSplitToken({ payload, fingerprint, anchor })),
    );
    const opened = await Promise.all(
      tokens.map((t) => openSplitToken(t, { expectedFingerprint: fingerprint })),
    );

    expect(opened).toEqual(payloads);
  });

  test("a token minted for another bind is refused before the payload is reachable", async () => {
    const mine = await bindFingerprint("main", "a", new Uint8Array(), new Uint8Array(), new Uint8Array());
    const theirs = await bindFingerprint("main", "b", new Uint8Array(), new Uint8Array(), new Uint8Array());
    const token = await buildSplitToken({
      payload: new TextEncoder().encode("not-for-you"),
      fingerprint: theirs,
      anchor: splitAnchor(1),
    });

    expect(openSplitToken(token, { expectedFingerprint: mine })).rejects.toThrow();
  });
});
