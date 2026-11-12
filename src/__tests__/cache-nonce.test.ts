// Copyright 2026 Query Farm LLC - https://query.farm
// SPDX-License-Identifier: LicenseRef-QueryFarm-Source-Available-1.0
// The result-cache fixtures (examples/cache.ts) must mint nonces that are
// unique ACROSS worker processes, not just within one: the client subprocess
// pool may serve two cache MISSes from two different processes, and
// vgi test/sql/integration/cache/transaction_scope.test asserts their nonces
// differ. The pid lives in the high bits.

import { describe, expect, test } from "bun:test";
import { NONCE_PID_SHIFT, nextNonce } from "../../examples/cache.js";

describe("cache fixture nonces", () => {
  test("monotonic within a process and tagged with the pid", () => {
    const a = nextNonce();
    const b = nextNonce();
    expect(b).toBe(a + 1);
    for (const n of [a, b]) {
      expect(Number.isSafeInteger(n)).toBe(true);
      expect(Math.floor(n / NONCE_PID_SHIFT)).toBe(process.pid);
    }
  });
});
